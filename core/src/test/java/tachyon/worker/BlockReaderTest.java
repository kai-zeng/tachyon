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
    // Read the whole file
    ret.add(new Pair<Pair<Long, Long>, ByteBuffer>(new Pair<Long, Long>(0L, fileLength), TestUtils
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
    // Read past the end of the file
    ret.add(new Pair<Pair<Long, Long>, ByteBuffer>(new Pair<Long, Long>(0L, fileLength + 1), null));
    return ret;
  }

  @Test
  public void readExceptionTest() throws IOException {
    int fileId = TestUtils.createByteFile(mTfs, TestUtils.uniqPath(), WriteType.MUST_CACHE, 100);
    TachyonFile file = mTfs.getFile(fileId);
    String blockDir = file.getLocalDirectory(0);
    BlockReader blockReader = new BlockReader(blockDir);
    Exception exception = null;
    try {
      blockReader.read(-1, 10);
    } catch (IOException e) {
      exception = e;
    }
    Assert.assertEquals("Offset cannot be negative", exception.getMessage());
    try {
      blockReader.read(10, -1);
    } catch (IOException e) {
      exception = e;
    }
    Assert.assertEquals("Length cannot be negative", exception.getMessage());
  }

  @Test
  public void readTest() throws IOException {
    int fileId = TestUtils.createByteFile(mTfs, TestUtils.uniqPath(), WriteType.MUST_CACHE, 100);
    TachyonFile file = mTfs.getFile(fileId);
    String blockDir = file.getLocalDirectory(0);
    List<Pair<Pair<Long, Long>, ByteBuffer>> readBounds = generateReadTestBounds(100);
    BlockReader blockReader = new BlockReader(blockDir);
    for (Pair<Pair<Long, Long>, ByteBuffer> bound : readBounds) {
      Assert.assertEquals(bound.getSecond(),
          blockReader.read(bound.getFirst().getFirst(), bound.getFirst().getSecond()));
    }
  }

  @Test
  public void getChannelsTest() throws IOException {
    int fileId = TestUtils.createByteFile(mTfs, TestUtils.uniqPath(), WriteType.MUST_CACHE, 100);
    TachyonFile file = mTfs.getFile(fileId);
    String blockDir = file.getLocalDirectory(0);
    BlockReader blockReader = new BlockReader(blockDir);
    List<Pair<Pair<Long, Long>, ByteBuffer>> readBounds = generateReadTestBounds(100);
    for (Pair<Pair<Long, Long>, ByteBuffer> bound : readBounds) {
      BlockReader.CloseableChannels channels =
          blockReader.getChannels(bound.getFirst().getFirst(), bound.getFirst().getSecond());
      ByteBuffer buf = null;
      if (channels != null) {
        try {
          buf = ByteBuffer.allocate(bound.getSecond().remaining());
          for (FileChannel chan : channels) {
            chan.read(buf);
          }
          buf.flip();
        } finally {
          channels.close();
        }
      }
      Assert.assertEquals(bound.getSecond(), buf);
    }
  }

  // If we write a page smaller than the page length and request a channel between the length of the
  // file we wrote and the maximum page size, getChannels should return NULL
  @Test
  public void getChannelsPastPageLength() throws IOException {
    int fileId = TestUtils.createByteFile(mTfs, TestUtils.uniqPath(), WriteType.MUST_CACHE, PAGE_SIZE / 2);
    TachyonFile file = mTfs.getFile(fileId);
    String blockDir = file.getLocalDirectory(0);
    BlockReader blockReader = new BlockReader(blockDir);
    Assert.assertEquals(null, blockReader.getChannels(0, PAGE_SIZE));
  }

}