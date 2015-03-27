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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.TachyonURI;
import tachyon.TestUtils;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.master.LocalTachyonCluster;

/**
 * Unit tests for <code>tachyon.client.BlockHandlerLocal</code>.
 */
public class BlockHandlerLocalTest {
  private static LocalTachyonCluster mLocalTachyonCluster = null;
  private static TachyonFS mTfs = null;
  private static final int PAGE_SIZE = 20;

  @AfterClass
  public static final void afterClass() throws Exception {
    mLocalTachyonCluster.stop();
  }

  @BeforeClass
  public static final void beforeClass() throws IOException {
    mLocalTachyonCluster = new LocalTachyonCluster(10000, 1000, Constants.GB, PAGE_SIZE);
    mLocalTachyonCluster.start();
    mTfs = mLocalTachyonCluster.getClient();
  }

  @Test
  public void directByteBufferWriteTest() throws IOException {
    ByteBuffer buf = ByteBuffer.allocateDirect(100);
    buf.put(TestUtils.getIncreasingByteArray(100));
    buf.position(0);

    int fileId = mTfs.createFile(new TachyonURI(TestUtils.uniqPath()));
    long blockId = mTfs.getBlockId(fileId, 0);
    String blockdir = mTfs.getLocalBlockTemporaryPath(blockId, 100);
    BlockHandler handler = BlockHandler.get(mLocalTachyonCluster.getWorkerTachyonConf(), blockdir);
    try {
      handler.append(buf);
      mTfs.cacheBlock(blockId);
      TachyonFile file = mTfs.getFile(fileId);
      long fileLen = file.length();
      Assert.assertEquals(100, fileLen);
    } finally {
      handler.close();
    }
  }

  @Test
  public void heapByteBufferwriteTest() throws IOException {
    int fileId = mTfs.createFile(new TachyonURI(TestUtils.uniqPath()));
    long blockId = mTfs.getBlockId(fileId, 0);
    String filename = mTfs.getLocalBlockTemporaryPath(blockId, 100);
    BlockHandler handler = BlockHandler.get(mLocalTachyonCluster.getWorkerTachyonConf(), filename);
    byte[] buf = TestUtils.getIncreasingByteArray(100);
    try {
      handler.append(ByteBuffer.wrap(buf));
      mTfs.cacheBlock(blockId);
      TachyonFile file = mTfs.getFile(fileId);
      long fileLen = file.length();
      Assert.assertEquals(100, fileLen);
    } finally {
      handler.close();
    }
  }

  @Test
  public void readExceptionTest() throws IOException {
    int fileId = TestUtils.createByteFile(mTfs, TestUtils.uniqPath(), WriteType.MUST_CACHE, 100);
    TachyonFile file = mTfs.getFile(fileId);
    String blockDir = file.getLocalDirectory(0);
    BlockHandler handler = BlockHandler.get(mLocalTachyonCluster.getWorkerTachyonConf(), blockDir);
    try {
      Exception exception = null;
      try {
        handler.read(101, 10);
      } catch (IOException e) {
        exception = e;
      }
      Assert.assertEquals("offset(101) is larger than file length(100)",
          exception.getMessage());
      try {
        handler.read(10, 100);
      } catch (IOException e) {
        exception = e;
      }
      Assert.assertEquals("offset(10) plus length(100) is larger than file length(100)",
          exception.getMessage());
    } finally {
      handler.close();
    }
  }

  /**
   * To test reading, we make sure we get the correct results for the given bounds
   * 
   * @param fileLength the length of the file
   * @return A list of pairs, where the first item in each pair is a pair of offset-length bounds,
   *         and the second is the expected result as a ByteBuffer
   */
  private List<Pair<Pair<Long, Long>, ByteBuffer>> generateReadTestBounds(long fileLength) {
    List<Pair<Pair<Long, Long>, ByteBuffer>> ret = new ArrayList<Pair<Pair<Long, Long>, ByteBuffer>>();
    // Read the whole file with and without -1 as the length
    ret.add(new Pair<Pair<Long, Long>, ByteBuffer>(new Pair<Long, Long>((long) 0, fileLength), TestUtils
        .getIncreasingByteBuffer((int) fileLength)));
    ret.add(new Pair<Pair<Long, Long>, ByteBuffer>(new Pair<Long, Long>((long) 0, (long) -1), TestUtils
        .getIncreasingByteBuffer((int) fileLength)));
    // Read an entire page
    long offset = 0;
    long length = PAGE_SIZE;
    ret.add(new Pair<Pair<Long, Long>, ByteBuffer>(new Pair<Long, Long>(offset, length), TestUtils
        .getIncreasingByteBuffer((int) offset, (int) length)));
    // Read an entire page offset by half
    offset = PAGE_SIZE / 2;
    length = PAGE_SIZE;
    ret.add(new Pair<Pair<Long, Long>, ByteBuffer>(new Pair<Long, Long>(offset, length), TestUtils
        .getIncreasingByteBuffer((int) offset, (int) length)));
    // Read half a page offset by 1/4 of a page
    offset = PAGE_SIZE / 4;
    length = PAGE_SIZE / 2;
    ret.add(new Pair<Pair<Long, Long>, ByteBuffer>(new Pair<Long, Long>(offset, length), TestUtils
        .getIncreasingByteBuffer((int) offset, (int) length)));
    return ret;
  }

  @Test
  public void readTest() throws IOException {
    int fileId = TestUtils.createByteFile(mTfs, TestUtils.uniqPath(), WriteType.MUST_CACHE, 100);
    TachyonFile file = mTfs.getFile(fileId);
    String blockDir = file.getLocalDirectory(0);
    BlockHandler handler = BlockHandler.get(mLocalTachyonCluster.getWorkerTachyonConf(), blockDir);
    List<Pair<Pair<Long, Long>, ByteBuffer>> readBounds = generateReadTestBounds(100);
    try {
      for (Pair<Pair<Long, Long>, ByteBuffer> bound : readBounds) {
        Assert.assertEquals(bound.getSecond(),
            handler.read(bound.getFirst().getFirst(), bound.getFirst().getSecond()));
      }
    } finally {
      handler.close();
    }
  }

  @Test
  public void getChannelsTest() throws IOException {
    int fileId = TestUtils.createByteFile(mTfs, TestUtils.uniqPath(), WriteType.MUST_CACHE, 100);
    TachyonFile file = mTfs.getFile(fileId);
    String blockDir = file.getLocalDirectory(0);
    BlockHandler handler = BlockHandler.get(mLocalTachyonCluster.getWorkerTachyonConf(), blockDir);
    List<Pair<Pair<Long, Long>, ByteBuffer>> readBounds = generateReadTestBounds(100);
    try {
      for (Pair<Pair<Long, Long>, ByteBuffer> bound : readBounds) {
        List<ByteChannel> channels =
            handler.getChannels(bound.getFirst().getFirst(), bound.getFirst().getSecond());
        ByteBuffer buf = ByteBuffer.allocate(bound.getSecond().remaining());
        for (ByteChannel chan : channels) {
          chan.read(buf);
        }
        buf.flip();
        Assert.assertEquals(bound.getSecond(), buf);
      }
    } finally {
      handler.close();
    }
  }
}
