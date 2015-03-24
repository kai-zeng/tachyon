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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.util.List;

import tachyon.TachyonURI;
import tachyon.UnderFileSystem;

/**
 * Base class for handling block I/O. Block handlers for different under file systems can be
 * implemented by extending this class. The block is append-only. It is not possible to rewrite at a
 * position that has already been written. It is not thread safe, the caller must guarantee thread
 * safe. This class is internal and subject to changes.
 */
public abstract class BlockHandler implements Closeable {

  /**
   * Create a block handler according to path scheme
   * 
   * @param path the block path
   * @return the handler of the block
   * @throws IOException
   * @throws IllegalArgumentException
   */
  public static BlockHandler get(String path) throws IOException, IllegalArgumentException {
    if (path.startsWith(TachyonURI.SEPARATOR) || path.startsWith("file://")) {
      return new BlockHandlerLocal(path);
    }
    throw new IllegalArgumentException("Unsupported block file path: " + path);
  }

  /**
   * Append data to the block from a byte array. The write may be buffered, so the written bytes may
   * not appear immediately on the filesystem. The flush method will flush anything in the buffer to
   * the filesystem.
   *
   * @param buf the data buffer
   * @param offset the offset of the buffer
   * @param length the length of the data
   * @return the size of data that was written
   * @throws IOException
   */
  public int append(byte[] buf, int offset, int length) throws IOException {
    return append(ByteBuffer.wrap(buf, offset, length));
  }

  /**
   * Appends data to the block from a ByteBuffer
   * 
   * @param srcBuf ByteBuffer that data is stored in
   * @return the size of data that was written
   * @throws IOException
   */
  public abstract int append(ByteBuffer srcBuf) throws IOException;

  /**
   * Deletes the block
   * 
   * @return true if success, otherwise false
   * @throws IOException
   */
  public abstract boolean delete() throws IOException;

  /**
   * Flushes the block to the filesystem
   *
   * @throws java.io.IOException
   */
  public abstract void flush() throws IOException;

  /**
   * Gets channel used to access block at a given offset and length. If there is no single
   * ByteChannel that covers the given range of the block, it returns null.
   *
   * @param offset the offset into the block
   * @param length the number of bytes the channel should span
   * @return the channel bounded with the block file
   */
  public abstract ByteChannel getChannel(long offset, long length) throws IOException;

  /**
   * Gets the length of the block
   * 
   * @return the length of the block
   * @throws IOException
   */
  public abstract long getLength() throws IOException;

  /**
   * Reads data from block
   * 
   * @param offset the offset from starting of the block file
   * @param length the length of data to read, -1 represents reading the rest of the block
   * @return ByteBuffer containing the data that was read
   * @throws IOException
   */
  public abstract ByteBuffer read(long offset, long length) throws IOException;

  /**
   * Reads the data from the given page
   *
   * @param pageId the id of the page to read
   * @param offset the offset in bytes relative to the page
   * @param length the number of bytes to read, -1 represent reading until the end of the page
   * @return ByteBuffer containing the page data, or null if the page is not in the block directory
   */
  public abstract ByteBuffer readPage(int pageId, long offset, long length) throws IOException;

  /**
   * Return a list of page ids contained in the current block. Since blocks can be partially cached,
   * these pages do not have to be contiguous
   *
   * @return A list of page Ids
   */
  public abstract List<Integer> getPageIds();

  /**
   * Copies the block to the given block directory. The given directory should be in another
   * StorageDir on the same worker. It is safe to concurrently copy two BlockHandlers to the same
   * destination directory, regardless of how much data overlaps.
   *
   * @param path The destination directory of the block
   */
  public abstract void copy(String path) throws IOException;

  /**
   * Copies the block to the given UnderFS directory.
   *
   * @param underFS The UnderFS object that we are copying to
   * @param path The destination directory of the block
   */
  public abstract void copyToUnderFS(UnderFileSystem underFS, String path) throws IOException;

  /**
   * Moves the block to the given block directory. The given directory should be in another
   * StorageDir on the same worker. It is safe to concurrently move two BlockHandlers to the same
   * destination directory, regardless of how much data overlaps. This will put the BlockHandler in
   * the deleted state, so no other operations can be performed on it.
   */
  public abstract void move(String path) throws IOException;
}
