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

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import tachyon.TestUtils;
import tachyon.conf.WorkerConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.util.CommonUtils;
import tachyon.util.PageUtils;
import tachyon.worker.BlockReader;

/**
 * Unit tests for <code>tachyon.client.BlockInStream</code>.
 */
public class BlockInStreamTest {
  private static final int MIN_LEN = 0;
  private static final int MAX_LEN = 255;
  private static final int DELTA = 33;

  private static final int CLUSTER_SIZE = 50000;
  private static final int BLOCK_SIZE = 50;
  private static final int PAGE_SIZE = 10;

  private static LocalTachyonCluster sLocalTachyonCluster = null;
  private static TachyonFS sTfs = null;

  @AfterClass
  public static final void afterClass() throws Exception {
    sLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
    System.clearProperty("tachyon.user.default.block.size.byte");
    System.clearProperty("tachyon.user.page.size.byte");
    System.clearProperty("tachyon.user.remote.read.buffer.size.byte");

  }

  @BeforeClass
  public static final void beforeClass() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", String.valueOf(DELTA));
    System.setProperty("tachyon.user.default.block.size.byte", String.valueOf(BLOCK_SIZE));
    System.setProperty("tachyon.user.page.size.byte", String.valueOf(PAGE_SIZE));
    System.setProperty("tachyon.user.remote.read.buffer.size.byte", String.valueOf(PAGE_SIZE));
    sLocalTachyonCluster = new LocalTachyonCluster(CLUSTER_SIZE);
    sLocalTachyonCluster.start();
    sTfs = sLocalTachyonCluster.getClient();
  }

  /**
   * Reading the entire file should return correct results. If CACHE, the file should always be in
   * memory afterwards. If NO_CACHE, it shouldn't change the in-memoryness. We read it in every
   * possible way, (with read(), read(byte[]), and read(byte[], off, len)).
   */
  @Test
  public void readWholeFileTest() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (WriteType writeOp : WriteType.values()) {
        for (ReadType readOp : ReadType.values()) {
          for (int readMechanism = 0; readMechanism < 3; readMechanism ++) {
            int fileId =
                TestUtils.createByteFile(sTfs, String.format("%s/file_%s_%s_%s_%s", uniqPath, k,
                    writeOp, readOp, readMechanism), writeOp, k);
            TachyonFile file = sTfs.getFile(fileId);
            InStream is = file.getInStream(readOp);
            byte[] ret = new byte[k];
            if (readMechanism == 0) {
              int value = is.read();
              int cnt = 0;
              while (value != -1) {
                Assert.assertTrue(value >= 0);
                Assert.assertTrue(value < 256);
                ret[cnt ++] = (byte) value;
                value = is.read();
              }
              Assert.assertEquals(k, cnt);
            } else if (readMechanism == 1) {
              is.read(ret);
            } else {
              // Try reading in 2 parts just to exercise the offset feature
              is.read(ret, 0, k / 2);
              is.read(ret, k / 2, k - k / 2);
            }
            is.close();
            Assert.assertTrue(String.format(
                "Failed read with k = %s, writeOp = %s, readOp = %s, readMechanism = %s", k,
                writeOp, readOp, readMechanism), TestUtils.equalIncreasingByteArray(k, ret));
            if (k == 0) {
              Assert.assertTrue(file.isInMemory());
            } else if (readOp.isCache()) {
              Assert.assertTrue(file.isInMemory());
            } else {
              Assert.assertEquals(String.format("Failed with writeOp = %s", writeOp),
                  writeOp.isCache(), file.isInMemory());
            }
          }
        }
      }
    }
  }

  /**
   * Read first with NO_CACHE and then with CACHE. Both reads should be valid, and the first read
   * shouldn't change the inMemory status while the second should.
   */
  @Test
  public void readTwiceTest() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    int fileId = TestUtils.createByteFile(sTfs, uniqPath + "/file", WriteType.THROUGH, BLOCK_SIZE);
    TachyonFile file = sTfs.getFile(fileId);

    InStream is = file.getInStream(ReadType.NO_CACHE);
    byte[] ret = new byte[BLOCK_SIZE];
    is.read(ret);
    is.close();
    Assert.assertTrue(TestUtils.equalIncreasingByteArray(BLOCK_SIZE, ret));
    Assert.assertFalse(file.isInMemory());

    is = file.getInStream(ReadType.CACHE);
    ret = new byte[BLOCK_SIZE];
    is.read(ret);
    is.close();
    Assert.assertTrue(TestUtils.equalIncreasingByteArray(BLOCK_SIZE, ret));
    Assert.assertTrue(file.isInMemory());
  }

  /**
   * Reading only part of a block should only cache the pages that were read.
   */
  @Test
  public void readPartialBlockTest() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    int fileId = TestUtils.createByteFile(sTfs, uniqPath + "/file", WriteType.THROUGH, BLOCK_SIZE);
    TachyonFile file = sTfs.getFile(fileId);

    InStream is = file.getInStream(ReadType.CACHE);
    int ret = is.read();
    is.close();
    Assert.assertEquals(0, ret);

    int lockBlockId = sTfs.getBlockLockId();
    String blockDir = sTfs.lockBlock(file.getBlockId(0), lockBlockId);
    Assert.assertNotNull(blockDir);
    BlockReader blockReader = new BlockReader(blockDir);
    Assert.assertEquals(PAGE_SIZE, blockReader.getSize());
    Assert.assertEquals(TestUtils.getIncreasingByteBuffer(PAGE_SIZE),
        blockReader.read(0, PAGE_SIZE));
  }

  // TODO Figure out a way to test reading from remote workers (Seems like the local cluster can't
  // have multiple workers, and if we cache it locally, the localBlockReader will pick up the read).

  /**
   * Test seeking, making sure the proper pages are correctly cached.
   */
  @Test
  public void seekCachingTest() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    int fileId = TestUtils.createByteFile(sTfs, uniqPath + "/file", WriteType.THROUGH, BLOCK_SIZE);
    TachyonFile file = sTfs.getFile(fileId);

    InStream is = file.getInStream(ReadType.CACHE);
    int ret1 = is.read();
    int seekPos = BLOCK_SIZE - PAGE_SIZE - 1;
    is.seek(seekPos);
    int ret2 = is.read();
    is.close();
    Assert.assertEquals(0, ret1);
    Assert.assertEquals(seekPos, ret2);

    int lockBlockId = sTfs.getBlockLockId();
    String blockDir = sTfs.lockBlock(file.getBlockId(0), lockBlockId);
    Assert.assertNotNull(blockDir);
    BlockReader blockReader = new BlockReader(blockDir);
    Assert.assertEquals(PAGE_SIZE * 2, blockReader.getSize());
    Assert.assertEquals(TestUtils.getIncreasingByteBuffer(PAGE_SIZE),
        blockReader.read(0, PAGE_SIZE));
    Assert.assertEquals(TestUtils.getIncreasingByteBuffer(seekPos - PAGE_SIZE + 1, PAGE_SIZE),
        blockReader.read(seekPos - PAGE_SIZE + 1, PAGE_SIZE));
  }

  /**
   * Test caching the same file with two different input streams, making sure the proper pages are
   * correctly cached.
   */
  @Test
  public void multipleInputStreamTest() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    int fileId = TestUtils.createByteFile(sTfs, uniqPath + "/file", WriteType.THROUGH, BLOCK_SIZE);
    TachyonFile file = sTfs.getFile(fileId);

    InStream is1 = file.getInStream(ReadType.CACHE);
    int ret1 = is1.read();
    int skipPos = BLOCK_SIZE - PAGE_SIZE - 1;
    is1.close();
    InStream is2 = file.getInStream(ReadType.CACHE);
    is2.skip(skipPos);
    int ret2 = is2.read();
    is2.close();
    Assert.assertEquals(0, ret1);
    Assert.assertEquals(skipPos, ret2);

    int lockBlockId = sTfs.getBlockLockId();
    String blockDir = sTfs.lockBlock(file.getBlockId(0), lockBlockId);
    Assert.assertNotNull(blockDir);
    BlockReader blockReader = new BlockReader(blockDir);
    Assert.assertEquals(PAGE_SIZE * 2, blockReader.getSize());
    Assert.assertEquals(TestUtils.getIncreasingByteBuffer(PAGE_SIZE),
        blockReader.read(0, PAGE_SIZE));
    Assert.assertEquals(TestUtils.getIncreasingByteBuffer(skipPos - PAGE_SIZE + 1, PAGE_SIZE),
        blockReader.read(skipPos - PAGE_SIZE + 1, PAGE_SIZE));
  }

  /**
   * Test reading the file with CACHE twice. It should still work even though its caching nothing in
   * the end.
   */
  @Test
  public void readCacheTwiceTest() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    int fileId = TestUtils.createByteFile(sTfs, uniqPath + "/file", WriteType.THROUGH, BLOCK_SIZE);
    TachyonFile file = sTfs.getFile(fileId);

    InStream is = file.getInStream(ReadType.CACHE);
    byte[] results = new byte[(int) file.length()];
    int ret = is.read(results, 0, results.length);
    Assert.assertEquals(BLOCK_SIZE, ret);
    is.close();
    Assert.assertTrue(TestUtils.equalIncreasingByteArray(BLOCK_SIZE, results));

    is = file.getInStream(ReadType.CACHE);
    results = new byte[(int) file.length()];
    ret = is.read(results, 0, results.length);
    Assert.assertEquals(BLOCK_SIZE, ret);
    is.close();
    Assert.assertTrue(TestUtils.equalIncreasingByteArray(BLOCK_SIZE, results));
  }
}
