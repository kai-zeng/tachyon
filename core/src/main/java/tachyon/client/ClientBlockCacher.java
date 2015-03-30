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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.conf.UserConf;
import tachyon.thrift.ClientBlockInfo;
import tachyon.worker.BlockCacher;

/**
 * The ClientBlockCacher is used by RemoteBlockInStream to to re-cache a block on the local worker.
 * It writes full pages in any order at any part of the block. Partial pages cannot be written since
 * pages cannot themselves be partially cached.
 */
public class ClientBlockCacher implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  // The file containing the block we are re-caching
  private TachyonFile mFile;
  // The id of the block
  private long mBlockId;
  // The BlockCacher that we delegate all the writing work to
  private final BlockCacher mBlockCacher;
  // The amount of space allotted by the worker for this cacher
  private long mAvailableBytes = 0;
  // The number of bytes written by this cacher
  private long mWrittenBytes = 0;
  // Whether the block was closed
  private boolean mClosed = false;

  ClientBlockCacher(TachyonFile file, int blockIndex) throws IOException {
    this(file, blockIndex, UserConf.get().QUOTA_UNIT_BYTES);
  }

  ClientBlockCacher(TachyonFile file, int blockIndex, long initialBytes) throws IOException {
    mFile = file;
    ClientBlockInfo blockInfo = mFile.getClientBlockInfo(blockIndex);
    mBlockId = blockInfo.getBlockId();
    // Get a temporary worker directory to write to
    if (!mFile.mTachyonFS.hasLocalWorker()) {
      throw new IOException("The machine does not have any local worker.");
    }
    String blockDir = mFile.mTachyonFS.getLocalBlockTemporaryPath(mBlockId, initialBytes);
    mAvailableBytes += initialBytes;
    mBlockCacher = new BlockCacher(blockDir, blockInfo);
  }

  public void writePages(int startPageId, ByteBuffer data) throws IOException {
    // We leave it to the BlockCacher to verify the bounds, we just request more space from the
    // worker if necessary
    if (mClosed) {
      throw new IOException("Cannot write more pages, ClientBlockCacher is closed");
    }
    long bytesToWrite = data.remaining();
    long newLen = mWrittenBytes + bytesToWrite;
    if (newLen > mAvailableBytes) {
      long bytesRequested = mFile.mTachyonFS.requestSpace(mBlockId, newLen - mAvailableBytes);
      if (bytesRequested + mAvailableBytes >= newLen) {
        mAvailableBytes += bytesRequested;
      } else {
        throw new IOException(String.format("No enough space on local worker: fileId(%d)"
            + " blockId(%d) requestSize(%d)", mFile.mFileId, mBlockId, newLen - mAvailableBytes));
      }
    }

    mBlockCacher.writePages(startPageId, data);
    mWrittenBytes += bytesToWrite;
  }

  @Override
  public void close() throws IOException {
    if (!mClosed) {
      mFile.mTachyonFS.cacheBlock(mBlockId);
      mClosed = true;
    }
  }

  public void cancel() throws IOException {
    if (!mClosed) {
      mFile.mTachyonFS.cancelBlock(mBlockId);
      LOG.info(String.format("Canceled output of block. blockId(%d)", mBlockId));
      mClosed = true;
    }
  }
}
