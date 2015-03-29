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
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import com.google.common.base.Preconditions;

import tachyon.UnderFileSystem;
import tachyon.util.CommonUtils;

/**
 * A BlockOperator is used to execute whole-block operations on the block.
 */
public final class BlockOperator {
  // Stores the file object for the block
  private File mBlockDir;

  // Stores whether the block has been deleted, which will invalidate all future block operations
  private boolean mDeleted;

  /**
   * Creates a BlockOperator for the block at the given directory
   * @param blockDir the directory of the block
   * @throws IOException if the given directory is invalid
   */
  public BlockOperator(String blockDir) throws IOException {
    Preconditions.checkNotNull(blockDir);
    mBlockDir = new File(blockDir);
    if (!mBlockDir.exists()) {
      throw new IOException("BlockOperator expects the given directory to exist");
    }
    if (mBlockDir.isFile()) {
      throw new IOException("BlockOperator expects the given path to be a directory");
    }
    mDeleted = false;
  }

  /**
   * Copies the block to the given destination directory
   * 
   * @param path the path of the directory to copy the block to, which must be on the same machine
   * @throws IOException if the given path is invalid or a copy operation fails
   */
  public void copy(String path) throws IOException {
    checkDeleted();
    File dstDir = new File(Preconditions.checkNotNull(path));
    if (dstDir.exists() && dstDir.isFile()) {
      throwExistingPathNotDirectory(path);
    }
    dstDir.mkdirs();
    for (File pageFile : mBlockDir.listFiles()) {
      Files.copy(pageFile.toPath(), new File(dstDir, pageFile.getName()).toPath(),
          StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES);
    }
  }

  /**
   * Copies the block to the given destination directory in the given UnderFileSystem
   *
   * @param underFS the UnderFileSystem to copy to
   * @param dstDir the directory in the underFS to write to
   * @throws IOException if the directory is invalid or a copy operation fails
   */
  public void copyToUnderFS(UnderFileSystem underFS, String dstDir) throws IOException {
    checkDeleted();
    if (underFS.exists(dstDir) && underFS.isFile(dstDir)) {
      throwExistingPathNotDirectory(dstDir);
    }
    underFS.mkdirs(dstDir, true);
    for (File pageFile : mBlockDir.listFiles()) {
      FileChannel readChannel = new RandomAccessFile(pageFile.getAbsolutePath(), "r").getChannel();
      ByteBuffer readBuf = readChannel.map(FileChannel.MapMode.READ_ONLY, 0, readChannel.size());
      String dstPath = CommonUtils.concat(dstDir, pageFile.getName());
      OutputStream os = underFS.create(dstPath);
      WritableByteChannel outputChannel = Channels.newChannel(os);
      try {
        outputChannel.write(readBuf);
      } finally {
        outputChannel.close();
        os.close();
        CommonUtils.cleanDirectBuffer(readBuf);
      }
    }
  }

  /**
   * Deletes the block, all future operations on the block will be invalid
   * @throws IOException if a delete operation fails
   */
  public void delete() throws IOException {
    checkDeleted();
    // Delete all the files
    for (File pageFile : mBlockDir.listFiles()) {
      pageFile.delete();
    }
    mBlockDir.delete();
    mDeleted = true;
  }

  /**
   * Moves the block to the given destination directory. The block will be marked as deleted and no
   * future operations will be possible on it.
   *
   * @param path the path of the directory to move the block to, which must be on the same machine
   * @throws IOException if the given path is invalid or a move operation fails
   */
  public void move(String path) throws IOException {
    // This has to be implemented by moving each file in the block individually. It's hard to just
    // rename the source directory, even if the destination doesn't exist, because if two threads
    // are caching to the same block destination, then it's difficult to atomicize the directory
    // existence and rename check, so one move could succeed and the other fail. One possibility is
    // to try and rename the entire directory and do each file if that
    // throws an exception, but that will need some thinking about.
    checkDeleted();
    File dstDir = new File(Preconditions.checkNotNull(path));
    if (dstDir.exists() && dstDir.isFile()) {
      throwExistingPathNotDirectory(path);
    }
    dstDir.mkdirs();
    for (File pageFile : mBlockDir.listFiles()) {
      Files.move(pageFile.toPath(), new File(dstDir, pageFile.getName()).toPath(),
          StandardCopyOption.REPLACE_EXISTING);
    }
    mBlockDir.delete();
    mDeleted = true;
  }

  /**
   * Check if the block was deleted
   * @throws IOException if the block is deleted
   */
  private void checkDeleted() throws IOException {
    if (mDeleted) {
      throw new IOException("Block has been deleted. No more operations are possible");
    }
  }

  /**
   * Throw an exception for when the given path exists but is not a directory
   * @param dstPath the invalid destination path
   * @throws IOException explaining the situation
   */
  private void throwExistingPathNotDirectory(String dstPath) throws IOException {
    throw new IOException("The destination path {" + dstPath
        + "} must be a directory if it already exists");
  }
}
