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
  // A buffer to hold data to be flushed to the next page
  private ByteBuffer mPageBuf = null;
  // The id of the page currently being written, -1 if there is no such page
  private int mCurrentPageId = -1;
  // The number of bytes that have been written to the block
  private long mWrittenBytes = 0;

  /**
   * Create a new BlockAppender at the given directory
   * @param blockDir the name of the directory to write the block to
   * @throws java.io.IOException if the directory is non-empty or we cannot create it
   */
  public BlockAppender(String blockDir) throws IOException {
    mBlockDir = new File(Preconditions.checkNotNull(blockDir));
    mPageBuf = ByteBuffer.allocate((int) UserConf.get().PAGE_SIZE_BYTE);
    mCurrentPageId = 0;
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
   * Flushes mPageBuf to a new page file and increments mCurrentPageId
   * @throws IOException
   */
  private void flushBuffer() throws IOException {
    File pageFile = new File(mBlockDir, PageUtils.getPageFilename(mCurrentPageId));
    RandomAccessFile raPageFile = new RandomAccessFile(pageFile, "rw");
    raPageFile.write(mPageBuf.array());
    raPageFile.close();
    mPageBuf.clear();
    mCurrentPageId++;
  }

  /**
   * Writes the given data to the end of the block
   * @param buf the data to append to the block
   * @throws IOException if an I/O error related to creating and writing files occurs
   */
  public void append(ByteBuffer buf) throws IOException {
    while (buf.hasRemaining()) {
      // Write as much as possible to mPageBuf, flushing it when it gets full
      int bytesToWrite = Math.min(buf.remaining(), mPageBuf.remaining());
      mPageBuf.put((ByteBuffer) buf.slice().limit(bytesToWrite));
      if (mPageBuf.remaining() == 0) {
        flushBuffer();
      }
      buf.position(buf.position() + bytesToWrite);
      mWrittenBytes += bytesToWrite;
    }
  }

  @Override
  public void close() throws IOException {
    if (mPageBuf.position() > 0) {
      flushBuffer();
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
