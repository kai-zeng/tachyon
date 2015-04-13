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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;


import com.google.common.base.Preconditions;

import tachyon.conf.UserConf;
import tachyon.util.CommonUtils;
import tachyon.util.PageUtils;

/**
 * A BlockAppender is used to create a new block. This handler only supports appending data to
 * the end of the new block.
 */
public final class BlockAppender implements Closeable {
  // The directory being written to
  private File mBlockDir;
  // A buffer to hold data to be flushed to the next page. This is useful when a lot of small
  // appends are executed in a row. Rather than individually writing each one to the page file, we
  // can flush them at once when the buffer is full.
  private ByteBuffer mPageBuf = null;
  // The id of the page currently being written, -1 if there is no such page
  private int mCurrentPageId = -1;
  // The page file currently being written, null if there is no such page
  private RandomAccessFile mCurrentPageFile = null;
  // The number of bytes that have been written to the block
  private long mWrittenBytes = 0;
  // Whether the blockAppender has been closed or not
  private boolean mClosed = false;

  /**
   * Create a new BlockAppender at the given directory
   * @param blockDir the name of the directory to write the block to
   * @throws java.io.IOException if the directory is non-empty or we cannot create it
   */
  public BlockAppender(String blockDir) throws IOException {
    mBlockDir = new File(Preconditions.checkNotNull(blockDir));
    // We buffer writes up to half the page size, since larger than that, it would probably be
    // better to write directly to the file.
    mPageBuf = ByteBuffer.allocate((int) (UserConf.get().PAGE_SIZE_BYTE / 2));
    if (mBlockDir.exists()) {
      if (mBlockDir.isFile()) {
        throw new IOException(
            "BlockAppender expects the given block path to be a directory if it already exists");
      } else if (mBlockDir.list().length > 0) {
        throw new IOException(
            "BlockAppender expects the given block directory to be empty if it already exists");
      }
    }
    mBlockDir.mkdirs();
  }

  /**
   * Flushes the given buffer to the current page file or files, incrementing mCurrentPageId as
   * necessary.
   *
   * @param buf the ByteBuffer to write
   * @throws IOException
   */
  private void flushBuffer(ByteBuffer buf) throws IOException {
    while (buf.hasRemaining()) {
      if (mCurrentPageFile == null || mCurrentPageFile.length() == UserConf.get().PAGE_SIZE_BYTE) {
        // Either we haven't created a page file yet, or our current one is full
        if (mCurrentPageFile == null) {
          mCurrentPageId = 0;
        } else {
          mCurrentPageFile.close();
          mCurrentPageId++;
        }
        File pageFile = new File(mBlockDir, PageUtils.getPageFilename(mCurrentPageId));
        mCurrentPageFile = new RandomAccessFile(pageFile, "rw");
      }
      // Write the minimum of the remaining bytes in the buffer and the remaining bytes in the file
      int bytesToWrite =
          (int) Math
              .min(buf.remaining(), UserConf.get().PAGE_SIZE_BYTE - mCurrentPageFile.length());
      mCurrentPageFile.getChannel().write((ByteBuffer) buf.slice().limit(bytesToWrite));
      buf.position(buf.position() + bytesToWrite);
    }
  }

  /**
   * Writes the given data to the end of the block
   * @param buf the data to append to the block
   * @throws IOException if an I/O error related to creating and writing files occurs
   */
  public void append(ByteBuffer buf) throws IOException {
    if (mClosed) {
      throw new IOException("Cannot append after closing the BlockAppender");
    }
    mWrittenBytes += buf.remaining();
    if (buf.remaining() <= mPageBuf.remaining()) {
      // The write is small enough to buffer to mPageBuf
      mPageBuf.put(buf);
    } else {
      // The write is too large. Flush mPageBuf, clear it, then flush buf
      mPageBuf.flip();
      flushBuffer(mPageBuf);
      mPageBuf.clear();
      flushBuffer(buf);
    }
  }

  @Override
  public void close() throws IOException {
    if (!mClosed) {
      if (mPageBuf.position() > 0) {
        mPageBuf.flip();
        flushBuffer(mPageBuf);
      }
      if (mCurrentPageFile != null) {
        mCurrentPageFile.close();
      }
      mClosed = true;
    }
  }

  /**
   * Get the number of bytes written to the block
   * @return the number of written bytes
   */
  public long getWrittenBytes() {
    return mWrittenBytes;
  }
}
