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
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;

import tachyon.conf.UserConf;
import tachyon.util.PageUtils;

/**
 * A BlockReader is used for reading content from an existing block, as well as finding the size of
 * the block. It is not thread-safe, as such, it is inadvisable to read and modify a block
 * concurrently with different handler classes.
 */
public final class BlockReader {
  // Stores a mapping from page ids to the file object for that page file
  private Map<Integer, File> mPageFiles;
  // The directory of the block
  private File mBlockDir;
  // The size of the block
  private long mSize = 0;

  /**
   * The getChannels method needs to return a list of channels. This class wraps a list of channels
   * into a closeable class, so the returned channels can easily be closed.
   */
  public class CloseableChannels implements Closeable, Iterable<FileChannel> {
    private List<FileChannel> mChannels;

    private CloseableChannels(List<FileChannel> channels) {
      mChannels = channels;
    }

    public List<FileChannel> getChannels() {
      return mChannels;
    }
    
    @Override
    public void close() throws IOException {
      for (FileChannel channel : mChannels) {
        channel.close();
      }
    }

    @Override
    public Iterator<FileChannel> iterator() {
      return mChannels.iterator();
    }
  }

  /**
   * Creates a BlockReader given the directory of an existing block
   * @param blockDir the directory of the block
   * @throws IOException if the given directory does not exist
   */
  public BlockReader(String blockDir) throws IOException {
    mBlockDir = new File(Preconditions.checkNotNull(blockDir));
    if (!mBlockDir.exists()) {
      throw new IOException("BlockReader expects the given block directory to already exist");
    }
    if (mBlockDir.isFile()) {
      throw new IOException("BlockReader expects the given block path to be a directory");
    }
    // We examine the current directory and create files for all the pages we see
    mPageFiles = new HashMap<Integer, File>();
    for (File pageFile : mBlockDir.listFiles()) {
      mPageFiles.put(PageUtils.getPageId(pageFile.getName()), pageFile);
      mSize += pageFile.length();
    }
  }

  /**
   * Gets a closeable list of channels used to access the block at a given offset and length. The
   * channels must be closed by the caller.
   *
   * @param offset the offset into the block
   * @param length the length of data to read, -1 represents reading the rest of the block
   * @return a sequence of channels that will produce the requested data when read in order, or null
   *         if all the requested data is not in the block directory
   * @throws IOException if the bounds are out of range of the block or some other I/O error
   */
  public CloseableChannels getChannels(long offset, long length) throws IOException {
    String error = null;
    if (offset < 0) {
      throw new IOException("Offset cannot be negative");
    } else if (length < 0) {
      throw new IOException("Length cannot be negative");
    }

    List<FileChannel> ret = new ArrayList<FileChannel>();
    long endPos = offset + length;
    while (offset < endPos) {
      // Read the minimum of till the end of the page or till the end of the specified range
      int pageId = PageUtils.getPageId(offset);
      long relativePos = offset - PageUtils.getPageOffset(pageId);
      long bytesToRead = Math.min(UserConf.get().PAGE_SIZE_BYTE - relativePos, endPos - offset);
      // Get the correct channel and seek to the correct starting position. If the required page is
      // not here, we close all the existing channels and return null
      File pageFile = mPageFiles.get(pageId);
      if (pageFile == null) {
        for (FileChannel chan : ret) {
          chan.close();
        }
        return null;
      }
      FileChannel addChannel = FileChannel.open(pageFile.toPath(), StandardOpenOption.READ);
      addChannel.position(relativePos);
      ret.add(addChannel);
      offset += bytesToRead;
    }
    return new CloseableChannels(ret);
  }

  /**
   * Get the number of bytes stored in the block directory.
   * 
   * @return size of the block in bytes
   */
  public long getSize() {
    return mSize;
  }

  /**
   * Reads the requested data from the block.
   * 
   * @param offset the offset to start reading at
   * @param length the number of bytes to read
   * @return a ByteBuffer containing the requested data, or null if all the requested data is not in
   *         the block directory
   * @throws IOException if the requested bounds are out of range
   */
  public ByteBuffer read(long offset, long length) throws IOException {
    CloseableChannels channels = getChannels(offset, length);
    if (channels == null) {
      return null;
    }
    // We create a ByteBuffer large enough to hold the requested range and copy
    // the correct pages in.
    // TODO There might be a way to wrap multiple mapped byte buffers into a larger one, so that we
    // can avoid copying.
    ByteBuffer ret = ByteBuffer.allocate((int) length);
    try {
      for (FileChannel channel : channels) {
        channel.read(ret);
      }
    } finally {
      channels.close();
    }
    ret.flip();
    return ret;
  }

  /**
   * Returns a mapping for all pages in the block of the page id to the memory-mapped page file
   * @return a map from page id to the mapped page buffer
   * @throws IOException
   */
  public Map<Integer, MappedByteBuffer> getMappedPages() throws IOException {
    Map<Integer, MappedByteBuffer> ret = new HashMap<Integer, MappedByteBuffer>();
    for (Map.Entry<Integer, File> entry : mPageFiles.entrySet()) {
      FileChannel channel = FileChannel.open(entry.getValue().toPath(), StandardOpenOption.READ);
      try {
        ret.put(entry.getKey(), channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size()));
      } finally {
        channel.close();
      }
    }
    return ret;
  }
}