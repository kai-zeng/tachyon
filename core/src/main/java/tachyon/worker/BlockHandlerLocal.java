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

package tachyon.worker;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import tachyon.Constants;
import tachyon.UnderFileSystem;
import tachyon.conf.UserConf;
import tachyon.util.CommonUtils;
import tachyon.util.PageUtils;

/**
 * BlockHandler for files on LocalFS, such as RamDisk, SSD and HDD. Blocks are actually directories,
 * which contain a file for each page.
 */
public final class BlockHandlerLocal extends BlockHandler {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  // A PageFile combines the File, RandomAccessFile, FileChannel, and boolean objects for each page
  private class PageFile {
    private File mFile;
    private RandomAccessFile mRandomAccessFile;
    private FileChannel mChannel;
    private boolean mPermission;

    public PageFile(File mFile, Closer closer) throws IOException {
      this.mFile = mFile;
      this.mRandomAccessFile = closer.register(new RandomAccessFile(mFile.getAbsolutePath(), "rw"));
      this.mChannel = closer.register(this.mRandomAccessFile.getChannel());
      this.mPermission = false;
    }

    public File getFile() {
      return mFile;
    }

    public void setFile(File mFile) {
      this.mFile = mFile;
    }

    public RandomAccessFile getRandomAccessFile() {
      return mRandomAccessFile;
    }

    public void setRandomAccessFile(RandomAccessFile mRandomAccessFile) {
      this.mRandomAccessFile = mRandomAccessFile;
    }

    public FileChannel getChannel() {
      return mChannel;
    }

    public void setChannel(FileChannel mChannel) {
      this.mChannel = mChannel;
    }

    public boolean getPermission() {
      return mPermission;
    }

    public void setPermission(boolean mPermission) {
      this.mPermission = mPermission;
    }
  }

  // Stores a list of file objects indexed by pageId
  private List<PageFile> mPageFiles;
  // The directory of the block
  private File mBlockDir;
  // A closer to close all the resources opened during the course of the handler
  private final Closer mCloser = Closer.create();
  // If true, the block was deleted, and no further operations can take place
  private boolean mDeleted = false;
  // The length of the block
  private long mLength = 0;

  BlockHandlerLocal(String blockDir) throws IOException {
    mBlockDir = new File(Preconditions.checkNotNull(blockDir));
    mBlockDir.mkdirs();
    LOG.debug("{} is created", blockDir);
    // We examine the current directory and add all the files we see. Page ids should go from 0 to
    // the number of pages in the block, so if we get the number of pages, we can use PageUtils to
    // get the correct page file
    int numPages = mBlockDir.listFiles().length;
    mPageFiles = new ArrayList<PageFile>();
    for (int i = 0; i < numPages; i ++) {
      mPageFiles.add(new PageFile(new File(mBlockDir, PageUtils.getPageFilename(i)), mCloser));
      mLength += mPageFiles.get(mPageFiles.size() - 1).getFile().length();
    }
  }

  /* Adds a new page file after the last one
   */
  private void addNewPageFile() throws IOException {
    int pageInd = mPageFiles.size();
    mPageFiles.add(new PageFile(new File(mBlockDir, PageUtils.getPageFilename(pageInd)), mCloser));
    mPageFiles.get(mPageFiles.size() - 1).getFile().createNewFile();
  }

  @Override
  public int append(ByteBuffer buf) throws IOException {
    checkDeleted();
    // We append to the last file in the list of pages. If we run out of space, we add a new page.
    int bufLen = buf.remaining();
    while (buf.hasRemaining()) {
      // If there are no pages, add a new one
      if (mPageFiles.size() == 0) {
        addNewPageFile();
      }
      // Write as much of our buffer as possible to the last page file
      long lastPageLength = mPageFiles.get(mPageFiles.size() - 1).getFile().length();
      int bytesToWrite =
          Math.min(buf.remaining(), (int) (UserConf.get().PAGE_SIZE_BYTE - lastPageLength));
      ByteBuffer out =
          mPageFiles.get(mPageFiles.size() - 1).getChannel()
              .map(MapMode.READ_WRITE, lastPageLength, bytesToWrite);
      checkPermission();
      out.put((ByteBuffer) buf.slice().limit(bytesToWrite));
      buf.position(buf.position() + bytesToWrite);
      CommonUtils.cleanDirectBuffer(out);
      // If we wrote to the end of the last page file, add a new one
      if (lastPageLength + bytesToWrite == UserConf.get().PAGE_SIZE_BYTE) {
        addNewPageFile();
      }
    }
    mLength += bufLen;
    return bufLen;
  }

  private void checkDeleted() throws IOException {
    if (mDeleted) {
      throw new IOException("Block was deleted, no other operations are valid");
    }
  }

  private void checkPermission() throws IOException {
    PageFile pf = mPageFiles.get(mPageFiles.size() - 1);
    if (!pf.getPermission()) {
      // change the permission of the file and use the sticky bit
      String filePath = pf.getFile().getAbsolutePath();
      CommonUtils.changeLocalFileToFullPermission(filePath);
      CommonUtils.setLocalFileStickyBit(filePath);
      pf.setPermission(true);
    }
  }

  @Override
  public void close() throws IOException {
    mCloser.close();
  }

  @Override
  public boolean delete() throws IOException {
    mDeleted = true;
    // Delete all the files
    for (PageFile pf : mPageFiles) {
      pf.getFile().delete();
    }
    // Delete the directory
    return mBlockDir.delete();
  }

  @Override
  public List<ByteChannel> getChannels(long offset, long length) throws IOException {
    checkDeleted();
    String error = null;
    if (offset > getLength()) {
      error = String.format("offset(%d) is larger than file length(%d)", offset, getLength());
    } else if (length != -1 && offset + length > getLength()) {
      error =
          String.format("offset(%d) plus length(%d) is larger than file length(%d)", offset,
              length, getLength());
    }
    if (error != null) {
      throw new IOException(error);
    }
    if (length == -1) {
      length = getLength() - offset;
    }

    List<ByteChannel> ret = new ArrayList<ByteChannel>();
    long endPos = offset + length;
    while (offset < endPos) {
      // Read the minimum of till the end of the page or till the end of the specified range
      int pageId = PageUtils.getPageId(offset);
      long relativePos = offset - PageUtils.getPageOffset(pageId);
      long bytesToRead = Math.min(UserConf.get().PAGE_SIZE_BYTE - relativePos, endPos - offset);
      // Get the correct channel and seek to the correct starting position
      FileChannel addChannel = mPageFiles.get(pageId).getChannel();
      addChannel.position(relativePos);
      ret.add(addChannel);
      offset += bytesToRead;
    }
    return ret;
  }

  @Override
  public long getLength() throws IOException {
    checkDeleted();
    return mLength;
  }

  @Override
  public ByteBuffer read(long offset, long length) throws IOException {
    List<ByteChannel> channels = getChannels(offset, length);
    if (length == -1) {
      length = getLength() - offset;
    }
    // If there is only one channel, we can simply return its mmapped byte buffer
    if (channels.size() == 1) {
      FileChannel chan = (FileChannel) channels.get(0);
      return chan.map(MapMode.READ_ONLY, chan.position(), length);
    }

    // Otherwise, we have to create a ByteBuffer large enough to hold the requested range and copy
    // the correct pages in.
    // TODO(manugoyal) make this more efficient (we might be able wrap multiple mmaped files into
    // one byte buffer, so we can avoid copying. One idea might be to create a wrapped Netty ByteBuf
    // out of multiple mapped buffers then convert that to an NIO buffer).
    ByteBuffer ret = ByteBuffer.allocate((int) length);
    for (ByteChannel channel : channels) {
      channel.read(ret);
    }
    ret.flip();
    return ret;
  }

  @Override
  public void copy(String path) throws IOException {
    File dstDir = new File(Preconditions.checkNotNull(path));
    dstDir.mkdirs();
    for (PageFile pageFile : mPageFiles) {
      Files.copy(pageFile.getFile().toPath(),
          new File(dstDir, pageFile.getFile().getName()).toPath(),
          StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES);
    }
  }

  @Override
  public void copyToUnderFS(UnderFileSystem underFS, String path) throws IOException {
    underFS.mkdirs(path, true);
    // Copy over each file in blockDirPath to the UFS
    for (int pageId = 0; pageId < PageUtils.getNumPages(mLength); pageId ++) {
      File srcFile = mPageFiles.get(pageId).getFile();
      String orphanPagePath =
          CommonUtils.concat(path, srcFile.getName());
      OutputStream os = underFS.create(orphanPagePath);
      WritableByteChannel outputChannel = Channels.newChannel(os);
      ByteBuffer pageBuf =
          mPageFiles.get(pageId).getChannel().map(MapMode.READ_ONLY, 0, srcFile.length());
      try {
        outputChannel.write(pageBuf);
      } finally {
        outputChannel.close();
        os.close();
        CommonUtils.cleanDirectBuffer(pageBuf);
      }
    }
  }

  @Override
  public void move(String path) throws IOException {
    mDeleted = true;
    File dstDir = new File(Preconditions.checkNotNull(path));
    dstDir.mkdirs();
    for (PageFile pageFile : mPageFiles) {
      Files.move(pageFile.getFile().toPath(),
          new File(dstDir, pageFile.getFile().getName()).toPath(),
          StandardCopyOption.REPLACE_EXISTING);
    }
    mBlockDir.delete();
  }
}
