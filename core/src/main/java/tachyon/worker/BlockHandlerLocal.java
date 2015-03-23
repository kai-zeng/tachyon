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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import tachyon.Constants;
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
  // A buffer to hold the bytes appended to the current page. We flush
  // whenever we reach the end of a page file.
  private final ByteBuffer mBuffer;

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
    mBuffer = ByteBuffer.allocate((int) UserConf.get().PAGE_SIZE_BYTE);
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
      // Append as much of the argument buffer as possible to our buffer
      int bytesToWrite = Math.min(buf.remaining(), mBuffer.remaining());
      mBuffer.put((ByteBuffer) buf.slice().limit(bytesToWrite));
      buf.position(buf.position() + bytesToWrite);
      // If our buffer is full, flush it to the filesystem
      if (!mBuffer.hasRemaining()) {
        flush();
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
    flush();
    mCloser.close();
  }

  @Override
  public boolean delete() throws IOException {
    checkPermission();
    mDeleted = true;
    // Delete all the files
    for (PageFile pf : mPageFiles) {
      pf.getFile().delete();
    }
    // Delete the directory
    return mBlockDir.delete();
  }

  @Override
  public void flush() throws IOException {
    mBuffer.flip();
    while (mBuffer.hasRemaining()) {
      // If there are no pages, add a new one
      if (mPageFiles.size() == 0) {
        addNewPageFile();
      }
      // Write as much of our buffer as possible to the last page file
      long lastPageLength = mPageFiles.get(mPageFiles.size() - 1).getFile().length();
      int bytesToWrite =
          Math.min(mBuffer.remaining(), (int) (UserConf.get().PAGE_SIZE_BYTE - lastPageLength));
      ByteBuffer out =
          mPageFiles.get(mPageFiles.size() - 1).getChannel()
              .map(MapMode.READ_WRITE, lastPageLength, bytesToWrite);
      checkPermission();
      out.put((ByteBuffer) mBuffer.slice().limit(bytesToWrite));
      mBuffer.position(mBuffer.position() + bytesToWrite);
      CommonUtils.cleanDirectBuffer(out);
      // If we wrote to the end of the last page file, add a new one
      if (lastPageLength + bytesToWrite == UserConf.get().PAGE_SIZE_BYTE) {
        addNewPageFile();
      }
    }
    mBuffer.clear();
  }

  @Override
  public ByteChannel getChannel(long offset, long length) throws IOException {
    checkDeleted();
    // If the offset,length range goes past the length of the file, or spans multiple pages,
    // we return null
    if (PageUtils.getPageId(offset + length) >= mPageFiles.size()
        || PageUtils.getPageId(offset) != PageUtils.getPageId(offset + length - 1)) {
      return null;
    }
    return mPageFiles.get(PageUtils.getPageId(offset)).getChannel();
  }

  @Override
  public long getLength() throws IOException {
    checkDeleted();
    return mLength;
  }

  @Override
  public ByteBuffer read(long offset, long length) throws IOException {
    checkDeleted();
    long fileLength = getLength();
    String error = null;
    if (offset > fileLength) {
      error = String.format("offset(%d) is larger than file length(%d)", offset, fileLength);
    } else if (length != -1 && offset + length > fileLength) {
      error =
          String.format("offset(%d) plus length(%d) is larger than file length(%d)", offset,
              length, fileLength);
    }
    if (error != null) {
      throw new IOException(error);
    }
    if (length == -1) {
      length = (int) (fileLength - offset);
    }

    // If the offset-length range maps to exactly one file, we can simply run readPage
    if (PageUtils.getPageId(offset) == PageUtils.getPageId(offset + length - 1)) {
      int id = PageUtils.getPageId(offset);
      long relativeOffset = offset - PageUtils.getPageOffset(id);
      return readPage(id, relativeOffset, length);
    }
    // Otherwise, we have to create a ByteBuffer large enough to hold the requested range and copy
    // the correct pages in.
    // TODO(manugoyal) make this more efficient (we might be able wrap multiple mmaped files into
    // one byte buffer, so we can avoid copying. One idea might be to create a wrapped Netty ByteBuf
    // out of multiple mapped buffers then convert that to an NIO buffer).
    ByteBuffer ret = ByteBuffer.allocate((int) length);
    long bytesWritten = 0;
    while (bytesWritten < length) {
      int id = PageUtils.getPageId(offset + bytesWritten);
      long relativeOffset = (offset + bytesWritten) - PageUtils.getPageOffset(id);
      long bytesToRead =
          Math.min(UserConf.get().PAGE_SIZE_BYTE - relativeOffset, length - bytesWritten);
      ret.put(mPageFiles.get(id).getChannel().map(MapMode.READ_ONLY, relativeOffset, bytesToRead));
      bytesWritten += bytesToRead;
    }
    ret.flip();
    return ret;
  }

  @Override
  public ByteBuffer readPage(int pageId, long offset, long length) throws IOException {
    checkDeleted();
    // We can directly map a file into a byte buffer and return that.
    long fileLength = mPageFiles.get(pageId).getFile().length();
    if (offset > fileLength) {
      throw new IOException(String.format("offset(%d) is larger than file length(%d)", offset,
          fileLength));
    } else if (length != -1 && offset + length > fileLength) {
      throw new IOException(String.format(
          "offset(%d) plus length(%d) is larger than file length(%d)", offset, length, fileLength));
    } else if (length == -1) {
      length = fileLength - offset;
    }
    return mPageFiles.get(pageId).getChannel().map(MapMode.READ_ONLY, offset, length);
  }

  public List<Integer> getPageIds() {
    // Right now we assume blocks are cached in their entirety, so we can simply return all the
    // pages
    return PageUtils.generateAllPages(mLength);
  }
}
