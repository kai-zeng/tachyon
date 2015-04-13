package tachyon.worker;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import tachyon.TachyonURI;
import tachyon.TestUtils;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.master.LocalTachyonCluster;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BlockAppenderTest {
  private static LocalTachyonCluster mLocalTachyonCluster = null;
  private static TachyonFS mTfs = null;
  private static final int PAGE_SIZE = 20;

  @AfterClass
  public static final void afterClass() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
    System.clearProperty("tachyon.user.page.size.byte");
  }

  @BeforeClass
  public static final void beforeClass() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
    System.setProperty("tachyon.user.page.size.byte", String.valueOf(PAGE_SIZE));
    mLocalTachyonCluster = new LocalTachyonCluster(10000);
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
    BlockAppender blockAppender = new BlockAppender(blockdir);
    blockAppender.append(buf);
    Assert.assertEquals(100, blockAppender.getWrittenBytes());
    blockAppender.close();
    mTfs.cacheBlock(blockId);
    TachyonFile file = mTfs.getFile(fileId);
    long fileLen = file.length();
    Assert.assertEquals(100, fileLen);
    Assert.assertEquals(100, blockAppender.getWrittenBytes());
  }

  @Test
  public void heapByteBufferwriteTest() throws IOException {
    int fileId = mTfs.createFile(new TachyonURI(TestUtils.uniqPath()));
    long blockId = mTfs.getBlockId(fileId, 0);
    String dirname = mTfs.getLocalBlockTemporaryPath(blockId, 100);
    BlockAppender blockAppender = new BlockAppender(dirname);
    byte[] buf = TestUtils.getIncreasingByteArray(100);
    blockAppender.append(ByteBuffer.wrap(buf));
    blockAppender.close();
    mTfs.cacheBlock(blockId);
    TachyonFile file = mTfs.getFile(fileId);
    long fileLen = file.length();
    Assert.assertEquals(100, fileLen);
    Assert.assertEquals(100, blockAppender.getWrittenBytes());
  }
}

