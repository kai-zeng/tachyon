package tachyon.worker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import tachyon.Pair;
import tachyon.TestUtils;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.master.LocalTachyonCluster;

/**
 * Unit tests for BlockReader
 */
public class BlockReaderTest {
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
  public void readExceptionTest() throws IOException {
    int fileId = TestUtils.createByteFile(mTfs, TestUtils.uniqPath(), WriteType.MUST_CACHE, 100);
    TachyonFile file = mTfs.getFile(fileId);
    String blockDir = file.getLocalDirectory(0);
    BlockReader blockReader = new BlockReader(blockDir);
    try {
      Exception exception = null;
      try {
        blockReader.read(101, 10);
      } catch (IOException e) {
        exception = e;
      }
      Assert.assertEquals("offset(101) is larger than file length(100)", exception.getMessage());
      try {
        blockReader.read(10, 100);
      } catch (IOException e) {
        exception = e;
      }
      Assert.assertEquals("offset(10) plus length(100) is larger than file length(100)",
          exception.getMessage());
    } finally {
      blockReader.close();
    }
  }

  @Test
  public void readTest() throws IOException {
    int fileId = TestUtils.createByteFile(mTfs, TestUtils.uniqPath(), WriteType.MUST_CACHE, 100);
    TachyonFile file = mTfs.getFile(fileId);
    String blockDir = file.getLocalDirectory(0);
    List<Pair<Pair<Long, Long>, ByteBuffer>> readBounds = generateReadTestBounds(100);
    BlockReader blockReader = new BlockReader(blockDir);
    try {
      for (Pair<Pair<Long, Long>, ByteBuffer> bound : readBounds) {
        Assert.assertEquals(bound.getSecond(),
            blockReader.read(bound.getFirst().getFirst(), bound.getFirst().getSecond()));
      }
    } finally {
      blockReader.close();
    }
  }

  @Test
  public void getChannelsTest() throws IOException {
    int fileId = TestUtils.createByteFile(mTfs, TestUtils.uniqPath(), WriteType.MUST_CACHE, 100);
    TachyonFile file = mTfs.getFile(fileId);
    String blockDir = file.getLocalDirectory(0);
    BlockReader blockReader = new BlockReader(blockDir);
    List<Pair<Pair<Long, Long>, ByteBuffer>> readBounds = generateReadTestBounds(100);
    try {
      for (Pair<Pair<Long, Long>, ByteBuffer> bound : readBounds) {
        List<FileChannel> channels =
            blockReader.getChannels(bound.getFirst().getFirst(), bound.getFirst().getSecond());
        ByteBuffer buf = ByteBuffer.allocate(bound.getSecond().remaining());
        for (FileChannel chan : channels) {
          chan.read(buf);
        }
        buf.flip();
        Assert.assertEquals(bound.getSecond(), buf);
      }
    } finally {
      blockReader.close();
    }
  }
}