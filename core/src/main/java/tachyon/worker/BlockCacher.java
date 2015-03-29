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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Preconditions;

import tachyon.conf.UserConf;
import tachyon.thrift.ClientBlockInfo;
import tachyon.util.PageUtils;

/**
 * A BlockCacher is used to cache an existing block at a given directory. Since the block already
 * exists, the BlockCacher will only write complete pages. The BlockCacher will only write pages
 * that are not already in the given directory.
 */
public final class BlockCacher {
  // The directory being written to
  private File mBlockDir;
  // The metadata of the existing block
  private ClientBlockInfo mBlockInfo;
  // A set of page ids that have already been written to the directory
  Set<Integer> mExistingPages;

  /**
   * Create a BlockCacher at a given directory for the given block
   * 
   * @param blockDir the directory to add pages to, which will be created if it doesn't already
   *        exist
   * @param blockInfo the block metadata for the block being cached
   * @throws IOException if the given directory is invalid or an I/O error occurs
   */
  public BlockCacher(String blockDir, ClientBlockInfo blockInfo) throws IOException {
    mBlockDir = new File(Preconditions.checkNotNull(blockDir));
    mBlockInfo = Preconditions.checkNotNull(blockInfo);
    if (!mBlockDir.exists()) {
      mBlockDir.mkdirs();
    } else if (mBlockDir.isFile()) {
      throw new IOException(
          "BlockCacher expects the given block path to be a directory if it already exists");
    }
    mExistingPages = new HashSet<Integer>();
    // We examine the current directory and add to existingPages all the page files we see
    for (String pageName : mBlockDir.list()) {
      mExistingPages.add(PageUtils.getPageId(pageName));
    }
  }

  /**
   * Write the given contiguous range pages to the directory. The range is determined by the start
   * page id and the number of bytes left in the given ByteBuffer.
   * 
   * @param startPageId the first page in the range
   * @param data the data to write, which will be correctly split up across the page files
   * @throws IOException if the given range is invalid or an I/O
   *         error occurs
   */
  public void writePages(int startPageId, ByteBuffer data) throws IOException {
    int numPages = PageUtils.getNumPages(mBlockInfo.getLength());
    long startOffset = PageUtils.getPageOffset(startPageId);
    long endOffset = startOffset + data.remaining();
    if (startPageId < 0) {
      throw new IOException("Start page id " + startPageId + " cannot be less than 0");
    } else if (endOffset > mBlockInfo.getLength()) {
      throw new IOException(String.format(
          "Start page {} and data length {} is invalid for block of length {}", startPageId,
          data.remaining(), mBlockInfo.getLength()));
    } else if (endOffset != mBlockInfo.getLength()
        && PageUtils.getPageOffset(PageUtils.getPageId(endOffset)) != endOffset) {
      throw new IOException("End position " + endOffset
          + " does not lie at a page boundary or at the end of the block");
    }
    // Write all the pages that haven't already been written
    for (int pageId = startPageId; data.hasRemaining(); pageId ++) {
      // We either write a full page or the remaining bytes left in the buffer
      long bytesToWrite = Math.min(UserConf.get().PAGE_SIZE_BYTE, data.remaining());
      ByteBuffer writeSlice = (ByteBuffer) data.slice().limit((int) bytesToWrite);
      data.position(data.position() + (int) bytesToWrite);
      // Only write the page if we're actually a new one
      if (mExistingPages.add(pageId)) {
        FileChannel channel =
            FileChannel.open(new File(mBlockDir, PageUtils.getPageFilename(pageId)).toPath(),
                StandardOpenOption.WRITE);
        try {
          ByteBuffer outBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, bytesToWrite);
          outBuffer.put(writeSlice);
        } finally {
          channel.close();
        }
      }
    }
  }
}
