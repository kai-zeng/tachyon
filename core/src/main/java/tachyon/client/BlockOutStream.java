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
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.conf.UserConf;
import tachyon.worker.BlockHandler;

/**
 * <code>BlockOutStream</code> implementation of TachyonFile. This class is not client facing.
 */
public class BlockOutStream extends OutStream {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  // The index of the block in the file
  private final int mBlockIndex;
  // The maximum size the block can be
  private final long mBlockCapacityByte;
  // The id of the block
  private final long mBlockId;
  // The byte offset of the block in the file
  private final long mBlockOffset;
  // Whether this block is pinned
  private final boolean mPin;
  // The BlockHandler that deals with writing to the block
  private final BlockHandler mBlockHandler;
  // The local directory the block is being written in
  private final String mBlockDir;
  // We buffer writes to a ByteBuffer and periodically flush them to the block handler
  private final ByteBuffer mBuffer;

  // When false, all write operations will fail
  private boolean mCanWrite = false;
  // true when the block has been closed
  private boolean mClosed = false;

  // The number of bytes allocated to the block by its worker
  private long mAvailableBytes = 0;

  /**
   * @param file the file the block belongs to
   * @param opType the OutStream's write type
   * @param blockIndex the index of the block in the file
   * @throws IOException
   */
  BlockOutStream(TachyonFile file, WriteType opType, int blockIndex) throws IOException {
    this(file, opType, blockIndex, UserConf.get().QUOTA_UNIT_BYTES);
  }

  /**
   * @param file the file the block belongs to
   * @param opType the OutStream's write type
   * @param blockIndex the index of the block in the file
   * @param initialBytes the initial size bytes that will be allocated to the block
   * @throws IOException
   */
  BlockOutStream(TachyonFile file, WriteType opType, int blockIndex, long initialBytes)
      throws IOException {
    super(file, opType);

    if (!opType.isCache()) {
      throw new IOException("BlockOutStream only support WriteType.CACHE");
    }

    mBlockIndex = blockIndex;
    mBlockCapacityByte = mFile.getBlockSizeByte();
    mBlockId = mFile.getBlockId(mBlockIndex);
    mBlockOffset = mBlockCapacityByte * blockIndex;
    mPin = mFile.needPin();

    mCanWrite = true;

    if (!mTachyonFS.hasLocalWorker()) {
      mCanWrite = false;
      String msg = "The machine does not have any local worker.";
      throw new IOException(msg);
    }
    mBlockDir = mTachyonFS.getLocalBlockTemporaryPath(mBlockId, initialBytes);
    mBlockHandler = BlockHandler.get(mBlockDir);
    mBuffer = ByteBuffer.allocate(mUserConf.FILE_BUFFER_BYTES + 4);
    mAvailableBytes += initialBytes;
    LOG.info(mBlockDir + " was created!");
  }

  @Override
  public void cancel() throws IOException {
    if (!mClosed) {
      mBlockHandler.close();
      mClosed = true;
      mTachyonFS.cancelBlock(mBlockId);
      LOG.info(String.format("Canceled output of block. blockId(%d) path(%s)", mBlockId,
          mBlockDir));
    }
  }

  /**
   * @return true if the stream can write and is not closed, otherwise false
   */
  public boolean canWrite() {
    return !mClosed && mCanWrite;
  }

  @Override
  public void close() throws IOException {
    if (!mClosed) {
      if (mBuffer.hasRemaining()) {
        mBuffer.flip();
        mBlockHandler.append(mBuffer);
      }
      mBlockHandler.close();
      mTachyonFS.cacheBlock(mBlockId);
      mClosed = true;
    }
  }

  @Override
  public void flush() throws IOException {
    // Since this only writes to memory, this flush is not outside visible.
  }

  /**
   * @return the block id of the block
   */
  public long getBlockId() {
    return mBlockId;
  }

  /**
   * @return the block offset in the file.
   */
  public long getBlockOffset() {
    return mBlockOffset;
  }

  /**
   * @return the remaining space of the block, in bytes
   */
  public long getRemainingSpaceByte() throws IOException {
    return mBlockCapacityByte - mBlockHandler.getLength();
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (!canWrite()) {
      throw new IOException("Can not write cache.");
    }

    if (b == null) {
      throw new NullPointerException();
    } else if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length)
        || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException(String.format("Buffer length (%d), offset(%d), len(%d)",
          b.length, off, len));
    }

    long newLen = mBlockHandler.getLength() + len;
    // If the new length is longer than the block capacity, throw an error. If it is greater than
    // what the worker has allocated for the given block, we need to request more space.
    if (newLen > mBlockCapacityByte) {
      throw new IOException("Out of capacity.");
    } else if (newLen > mAvailableBytes) {
      long bytesRequested = mTachyonFS.requestSpace(mBlockId, newLen - mAvailableBytes);
      if (bytesRequested + mAvailableBytes >= newLen) {
        mAvailableBytes += bytesRequested;
      } else {
        mCanWrite = false;
        throw new IOException(String.format("No enough space on local worker: fileId(%d)"
            + " blockId(%d) requestSize(%d)", mFile.mFileId, mBlockId, newLen - mAvailableBytes));
      }
    }
    mBlockHandler.append(b, off, len);
  }

  @Override
  public void write(int b) throws IOException {
    byte[] barr = new byte[1];
    barr[0] = (byte) (b & 0xFF);
    write(barr);
  }
}
