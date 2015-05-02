/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.StorageDirId;
import tachyon.UnderFileSystem;
import tachyon.conf.UserConf;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.NetAddress;
import tachyon.thrift.WorkerInfo;
import tachyon.util.NetworkUtils;
import tachyon.util.PageUtils;
import tachyon.worker.BlockReader;
import tachyon.worker.nio.DataServerMessage;

/**
 * <code>InputStream</code> interface implementation of TachyonFile. It can only be gotten by
 * calling the methods in <code>tachyon.client.TachyonFile</code>, but can not be initialized by the
 * client code.
 */
public class BlockInStream extends InStream {
  // A logger
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  // The number of bytes to read remotely every time we need to do a remote read. This is rounded to
  // the highest page multiple.
  private static final int BUFFER_SIZE = (int) PageUtils
      .ceilingPageMultiple(UserConf.get().REMOTE_READ_BUFFER_SIZE_BYTE);

  // The index of the block in the file
  private final int mBlockIndex;
  // The block info of the block we are reading
  private ClientBlockInfo mBlockInfo;
  // The block lock id we use to lock the block, if we do
  private int mBlockLockId;

  /**
   * For each worker that stores the block, we also have a list of storage directories that pages of
   * the block are in. While reading, we want to consider the workers in order of storage tier, from
   * lowest to highest, so we build a sorted list of NetAddress, storageDirId pairs.
   */
  private static class WorkerInfoPair implements Comparable<WorkerInfoPair> {
    public WorkerInfoPair(NetAddress mAddress, long mStorageDirId) {
      super();
      this.mAddress = mAddress;
      this.mStorageDirId = mStorageDirId;
    }

    private NetAddress mAddress;
    private long mStorageDirId;

    public NetAddress getAddress() {
      return mAddress;
    }

    public void setAddress(NetAddress address) {
      this.mAddress = address;
    }

    public long getStorageDirId() {
      return mStorageDirId;
    }

    public void setStorageDirId(long storageDirId) {
      this.mStorageDirId = storageDirId;
    }

    @Override
    public int compareTo(WorkerInfoPair o) {
      return StorageDirId.compareStorageLevel(mStorageDirId, o.getStorageDirId());
    }
  }

  // A list of workers sorted by storage tier
  List<WorkerInfoPair> mSortedWorkers;

  // The position in the block we are currently at, relative to the block. The position relative to
  // the file would be mBlockInfo.offset + mBlockPos.
  private long mBlockPos = 0;

  // A BlockReader for the portion of the block stored locally. If there is no
  // local block, it is null.
  private final BlockReader mLocalBlockReader;

  // A ByteBuffer storing a range of pages read locally, remotely, or from the
  // UnderFS. For caching simplicity, the buffer can only contain entire pages
  // and must start on a page boundary.
  private ByteBuffer mBuffer;
  // The page that the current mBuffer starts on. If it's -1, then the buffer
  // has nothing. Otherwise, position 0 on the remote buffer corresponds to the
  // first byte in the block that this page refers to.
  private int mBufferStartPage = -1;

  // A FileChannel storing a page file we have open locally, or null if it
  // refers to nothing. For large local reads, it's faster to read directly from
  // a FileChannel than to memory-map the page.
  private FileChannel mLocalPageChannel = null;
  // The page that the local page channel refers to, or -1 if it has nothing.
  private int mLocalPageChannelStartPage = -1;
  // The minimum read size that would warrant a file channel read.
  private static final long MINIMUM_FILE_CHANNEL_READ_SIZE = 32 * Constants.KB;

  // In order to predict whether to do a file channel read or a byte buffer
  // read, we keep track of the average read size.
  private long mNumBytesRead = 0;
  private long mNumReads = 0;

  // An input stream for the checkpointed copy of the block. If we are ever unable to read part of
  // the block locally or from the workers, we use this checkpoint stream
  private InputStream mCheckpointInputStream;
  // The page of the block that the checkpoint stream is on. If it's -1, the stream is not
  // initialized. If we're caching, the stream can only ever read entire pages, so its
  // position will only ever be on a page boundary. Otherwise, the position can be whatever.
  private long mCheckpointPos = -1;
  // The under filesystem configuration that we use to set up the checkpoint input stream
  private final Object mUFSConf;

  // Whether we are re-caching the block
  private boolean mRecache;
  // If we are re-caching the block, we have a ClientBlockCacher to deal with caching pages
  private final ClientBlockCacher mBlockCacher;

  // Whether the stream is closed
  private boolean mClosed = false;

  BlockInStream(TachyonFile file, ReadType readType, int blockIndex) throws IOException {
    this(file, readType, blockIndex, file.getUFSConf());
  }

  BlockInStream(TachyonFile file, ReadType readType, int blockIndex, Object ufsConf)
      throws IOException {
    super(file, readType);
    mBlockIndex = blockIndex;
    mBlockInfo = file.getClientBlockInfo(mBlockIndex);
    mSortedWorkers = buildSortedWorkers(mBlockInfo);

    // Try to promote the block if requested
    if (readType.isPromote()) {
      if (!file.promoteBlock(mBlockIndex)) {
        LOG.debug("Failed to promote block");
      }
    }

    // Try to lock the block and get a local block reader. We will unlock the block once the stream
    // is closed.
    mBlockLockId = mTachyonFS.getBlockLockId();
    String localBlockDir = mTachyonFS.lockBlock(mBlockInfo.getBlockId(), mBlockLockId);
    if (localBlockDir == null) {
      // There is no local block directory
      mLocalBlockReader = null;
    } else {
      // Create a local block reader and get the mapped pages
      mLocalBlockReader = new BlockReader(localBlockDir);
    }

    // Set the UnderFS configuration
    mUFSConf = ufsConf;

    // Set up the ClientBlockCacher if we're re-caching
    mRecache = readType.isCache();
    if (mRecache) {
      mBlockCacher = new ClientBlockCacher(file, mBlockInfo.getBlockId());
    } else {
      mBlockCacher = null;
    }
  }

  /**
   * Builds a sorted list of WorkerInfoPairs from the given client block info. We filter out any
   * local worker addresses.
   *
   * @param blockInfo the metadata to create a sorted worker list out of
   * @return a list of WorkerInfoPairs sorted by storage tier level
   */
  private static List<WorkerInfoPair> buildSortedWorkers(ClientBlockInfo blockInfo) {
    List<WorkerInfoPair> ret = new ArrayList<WorkerInfoPair>();
    for (WorkerInfo worker : blockInfo.getWorkers()) {
      try {
        String host = worker.getAddress().mHost;
        if (host.equals(InetAddress.getLocalHost().getHostName())
            || host.equals(InetAddress.getLocalHost().getHostAddress())
            || host.equals(NetworkUtils.getLocalHostName())) {
          continue;
        }
      } catch (IOException e) {
        LOG.error("Encountered an error: ", e);
      }
      for (Long storageDirId : worker.getStorageDirIds()) {
        ret.add(new WorkerInfoPair(worker.getAddress(), storageDirId));
      }
    }
    Collections.sort(ret);
    return ret;
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    if (mRecache) {
      mBlockCacher.close();
    }
    if (mCheckpointPos != -1) {
      mCheckpointInputStream.close();
    }
    if (mLocalBlockReader != null) {
      mTachyonFS.unlockBlock(mBlockInfo.getBlockId(), mBlockLockId);
    }
    if (mLocalPageChannel != null) {
      mLocalPageChannel.close();
    }
    mClosed = true;
  }

  @Override
  public int read() throws IOException {
    byte[] b = new byte[1];
    if (read(b) == -1) {
      return -1;
    }
    return (int) b[0] & 0xFF;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    } else if (mBlockPos == mBlockInfo.length) {
      return -1;
    } else if (mBlockPos + len > mBlockInfo.getLength()) {
      // If the length exceeds the block length, set it to go to the end of the block
      len = (int) (mBlockInfo.getLength() - mBlockPos);
    }
    int end = off + len;
    int bytesRead;
    if (off < end && mBuffer != null) {
      // Read from mBuffer, since that should technically be the fastest
      long remoteBufferOffset = PageUtils.getPageOffset(mBufferStartPage);
      if (mBufferStartPage != -1 && mBlockPos >= remoteBufferOffset
          && mBlockPos < remoteBufferOffset + mBuffer.limit()) {
        // Set the position of the buffer to the block position relative to the buffer offset
        mBuffer.position((int) (mBlockPos - remoteBufferOffset));
        int bytesToRead = Math.min(mBuffer.remaining(), len);
        mBuffer.get(b, off, bytesToRead);
        off += bytesToRead;
        mBlockPos += bytesToRead;
      }
    }
    if (off < end) {
      // Read as much as possible locally
      bytesRead = readLocal(b, off, end - off);
      off += bytesRead;
      mBlockPos += bytesRead;
    }
    if (off < end) {
      // Read as much as possible remotely
      bytesRead = readRemote(b, off, end - off);
      off += bytesRead;
      mBlockPos += bytesRead;
    }
    if (off < end) {
      // Read the rest from the under file system
      bytesRead = readRestFromUnderFS(b, off, end - off);
      off += bytesRead;
      mBlockPos += bytesRead;
    }
    if (off < end) {
      LOG.error("Failed to read at position " + mBlockPos + " in block " + mBlockInfo.getBlockId()
          + " from workers or underfs");
    }
    // Update the read statistics
    bytesRead = len - (end - off);
    mNumBytesRead += bytesRead;
    mNumReads ++;
    return bytesRead;
  }

  private int readLocal(byte[] b, int off, int len) throws IOException {
    if (mLocalBlockReader == null) {
      return 0;
    }
    int bytesRead = 0;
    while (bytesRead < len) {
      int readPage = PageUtils.getPageId(mBlockPos + bytesRead);
      int relativePagePosition = (int) (mBlockPos + bytesRead - PageUtils.getPageOffset(readPage));
      // If we have a local page open that is on the page we want, we use that,
      // otherwise we open a new one
      if (mLocalPageChannelStartPage != readPage) {
        if (mLocalPageChannel != null) {
          mLocalPageChannel.close();
        }
        mLocalPageChannel = mLocalBlockReader.getPageChannel(readPage);
      }
      if (mLocalPageChannel == null) {
        mLocalPageChannelStartPage = -1;
        return bytesRead;
      }
      mLocalPageChannelStartPage = readPage;
      int bytesToRead =
          Math.min(len - bytesRead, (int) mLocalPageChannel.size() - relativePagePosition);
      // If our average read size is 0 or greater than or equal to the
      // threshold, read directly from the FileChannel.
      if (mNumReads == 0 || (mNumBytesRead / mNumReads) >= MINIMUM_FILE_CHANNEL_READ_SIZE) {
        mLocalPageChannel.position(relativePagePosition);
        bytesRead += mLocalPageChannel.read(ByteBuffer.wrap(b, off + bytesRead, bytesToRead));
      } else {
        // Otherwise, we the map the channel into memory. Hopefully this
        // memory-mapping will pay off since future reads will also be small
        mBufferStartPage = readPage;
        mBuffer = mLocalPageChannel.map(FileChannel.MapMode.READ_ONLY, 0, mLocalPageChannel.size());
        mBuffer.position(relativePagePosition);
        mBuffer.get(b, off + bytesRead, bytesToRead);
        bytesRead += bytesToRead;
      }
    }
    return bytesRead;
  }

  private int readRemote(byte[] b, int off, int len) throws IOException {
    // Try and read remotely starting from the page that mBlockPos is on
    long remoteBufferOffset = PageUtils.getPageOffset(PageUtils.getPageId(mBlockPos));
    long remoteLength = Math.min(BUFFER_SIZE, mBlockInfo.getLength() - remoteBufferOffset);
    mBuffer =
        readRemoteByteBuffer(mTachyonFS, mBlockInfo, mSortedWorkers, remoteBufferOffset,
            remoteLength);
    if (mBuffer == null) {
      // We failed to fetch anything remotely, so the remote buffer is null, and
      // we return 0 bytes read
      mBufferStartPage = -1;
      return 0;
    }
    mBufferStartPage = PageUtils.getPageId(remoteBufferOffset);
    // Now, we should be able to read into the buffer. We cache the entire read
    // buffer if we're re-caching, then rewind it back to the beginning.
    if (mRecache) {
      mBlockCacher.writePages(mBlockInfo, PageUtils.getPageId(remoteBufferOffset), mBuffer);
      mBuffer.rewind();
    }
    assert mBlockPos >= remoteBufferOffset && mBlockPos < remoteBufferOffset + mBuffer.limit();
    mBuffer.position((int) (mBlockPos - remoteBufferOffset));
    int bytesToRead = Math.min(mBuffer.remaining(), len);
    mBuffer.get(b, off, bytesToRead);
    return bytesToRead;
  }

  public static ByteBuffer readRemoteByteBuffer(TachyonFS tachyonFS, ClientBlockInfo blockInfo,
      long offset, long len) {
    return readRemoteByteBuffer(tachyonFS, blockInfo, buildSortedWorkers(blockInfo), offset, len);
  }

  private static ByteBuffer readRemoteByteBuffer(TachyonFS tachyonFS, ClientBlockInfo blockInfo,
      List<WorkerInfoPair> sortedWorkers, long offset, long len) {
    ByteBuffer buf = null;

    try {
      // We are given a list of Workers sorted by the storage tier they are in (so workers with the
      // pages in memory come before workers in ssd, etc).
      for (WorkerInfoPair workerPair : sortedWorkers) {
        String host = workerPair.getAddress().mHost;
        int port = workerPair.getAddress().mSecondaryPort;

        if (host.equals(InetAddress.getLocalHost().getHostName())
            || host.equals(InetAddress.getLocalHost().getHostAddress())
            || host.equals(NetworkUtils.getLocalHostName())) {
          continue;
        }
        LOG.debug(host + ":" + port + " current host is " + NetworkUtils.getLocalHostName() + " "
            + NetworkUtils.getLocalIpAddress());

        try {
          buf =
              retrieveByteBufferFromRemoteMachine(new InetSocketAddress(host, port),
                  blockInfo.blockId, offset, len);
          if (buf != null) {
            break;
          }
        } catch (IOException e) {
          LOG.error("Fail to retrieve byte buffer for block " + blockInfo.blockId + " from remote "
              + host + ":" + port + " with offset " + offset + " and length " + len, e);
          buf = null;
        }
      }
    } catch (IOException e) {
      LOG.error("Failed to get read data from remote ", e);
      buf = null;
    }

    return buf;
  }

  private static ByteBuffer retrieveByteBufferFromRemoteMachine(InetSocketAddress address,
      long blockId, long offset, long length) throws IOException {
    SocketChannel socketChannel = SocketChannel.open();
    try {
      socketChannel.connect(address);

      LOG.debug("Connected to remote machine " + address + " sent");
      DataServerMessage sendMsg =
          DataServerMessage.createBlockRequestMessage(blockId, offset, length);
      while (!sendMsg.finishSending()) {
        sendMsg.send(socketChannel);
      }

      LOG.debug("Data " + blockId + " to remote machine " + address + " sent");

      // Since we're setting toSend to false, the other arguments don't really matter
      DataServerMessage recvMsg =
          DataServerMessage.createBlockResponseMessage(false, blockId, 0, 0, null);
      while (!recvMsg.isMessageReady()) {
        int numRead = recvMsg.recv(socketChannel);
        if (numRead == -1) {
          LOG.warn("Read nothing");
        }
      }
      LOG.debug("Data " + blockId + " from remote machine " + address + " received");

      if (!recvMsg.isMessageReady()) {
        LOG.debug("Data " + blockId + " from remote machine is not ready.");
        return null;
      }

      if (recvMsg.getBlockId() < 0) {
        LOG.debug("Data " + recvMsg.getBlockId() + " is not in remote machine.");
        return null;
      }
      return recvMsg.getReadOnlyData();
    } finally {
      socketChannel.close();
    }
  }

  private int readRestFromUnderFS(byte[] b, int off, int len) throws IOException {
    // If we're caching, then we need to make sure the stream is at the page offset rounded down
    // from the block position. Otherwise, we need to make sure it is at the block position.
    long requiredStreamPosition =
        mRecache ? PageUtils.getPageOffset(PageUtils.getPageId(mBlockPos)) : mBlockPos;
    if (mCheckpointPos == -1 || mCheckpointPos > requiredStreamPosition) {
      if (mCheckpointPos != -1) {
        mCheckpointInputStream.close();
      }
      String checkpointPath = mFile.getUfsPath();
      LOG.debug("Opening stream from underlayer fs: " + checkpointPath);
      if (checkpointPath.equals("")) {
        // We can't stream from the UnderFS, so return 0 bytes read
        return 0;
      }
      UnderFileSystem underfsClient = UnderFileSystem.get(checkpointPath, mUFSConf);
      mCheckpointInputStream = underfsClient.open(checkpointPath);
      // We skip to the start of the block, relative to the file
      if (mCheckpointInputStream.skip(mBlockInfo.getOffset()) != mBlockInfo.getOffset()) {
        throw new IOException("Failed to skip to the block offset " + mBlockInfo.getOffset()
            + " in the checkpoint file");
      }
      mCheckpointPos = 0;
    }
    // We skip from mCheckpointPos to requiredStreamPosition
    final long skipAmount = requiredStreamPosition - mCheckpointPos;
    if (mCheckpointInputStream.skip(skipAmount) != skipAmount) {
      throw new IOException("Failed to skip to the block offset " + requiredStreamPosition
          + " (relative to the block) in the checkpoint file");
    }
    mCheckpointPos = requiredStreamPosition;
    // If we are caching, then read at least as many pages as necessary, cache it, then copy the
    // correct parts to the argument array.
    if (mRecache) {
      // We can only read entire pages from the checkpoint stream, because we have to cache it, so
      // we read it into mBuffer. This way, future reads can get the data from mBuffer.
      assert mBufferStartPage == -1 || !mBuffer.hasRemaining();
      int lenToRead =
          (int) (Math.min(mBlockInfo.getLength(), PageUtils.ceilingPageMultiple(mBlockPos + len))
              - mCheckpointPos);
      mBuffer = ByteBuffer.allocate(lenToRead);
      mBufferStartPage = PageUtils.getPageId(mCheckpointPos);
      int bytesRead = 0;
      while (bytesRead < lenToRead) {
        int justRead =
            mCheckpointInputStream.read(mBuffer.array(), bytesRead, lenToRead - bytesRead);
        if (justRead <= 0) {
          // We failed to read as many bytes as needed, so we'll throw an error. Here, just return
          // the number of bytes we did read
          return bytesRead;
        }
        bytesRead += justRead;
      }
      mBlockCacher.writePages(mBlockInfo, PageUtils.getPageId(mCheckpointPos), mBuffer);
      // To read into the argument array, we have to set the position to the relative difference
      // between mBlockPos and mCheckpointPos
      mBuffer.position((int) (mBlockPos - mCheckpointPos));
      mBuffer.get(b, off, len);
      // Now the new checkpoint position is incremented lenToRead bytes
      mCheckpointPos += lenToRead;
    } else {
      // Otherwise, just read in the necessary amount
      int bytesRead = 0;
      while (bytesRead < len) {
        int justRead = mCheckpointInputStream.read(b, off + bytesRead, len);
        if (justRead <= 0) {
          return bytesRead;
        }
        bytesRead += justRead;
      }
      mCheckpointPos += len;
    }
    return len;
  }

  @Override
  public void seek(long pos) throws IOException {
    if (pos < 0) {
      throw new IOException("Seek position is negative: " + pos);
    } else if (pos > mBlockInfo.length) {
      throw new IOException("Seek position is past block size: " + pos + ", Block Size = "
          + mBlockInfo.length);
    }
    mBlockPos = pos;
  }

  @Override
  public long skip(long n) throws IOException {
    long bytesToSkip = Math.min(n, mBlockInfo.getLength() - mBlockPos);
    seek(mBlockPos + bytesToSkip);
    return bytesToSkip;
  }
}
