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

package tachyon.hadoop;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.client.InStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFile;
import tachyon.client.TachyonFS;

public class HdfsFileInputStream extends InputStream implements Seekable, PositionedReadable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  // The current position in the file
  private long mCurrentPosition;
  // The TachyonFile containing the file metadata
  private TachyonFile mTachyonFile;
  // The TachyonFS used to execute operations. This object owns the TachyonFS
  // and will close it when closed.
  private TachyonFS mTFS;

  // An input stream for the file stored in Tachyon. It will always be ready to read at
  // mCurrentPosition.
  private InStream mTachyonFileInputStream = null;

  // The path of the file in HDFS
  private Path mHdfsPath;
  // The buffer size to create the stream with
  private int mHadoopBufferSize;
  // The hadoop configuration for the file system
  private Configuration mHadoopConf;
  // An input stream for the HDFS file. It will always be ready to read at mCurrentPosition if it
  // isn't null.
  private FSDataInputStream mHdfsInputStream = null;

  public HdfsFileInputStream(TachyonFS tfs, int fileId, Path hdfsPath, Configuration conf,
      int bufferSize) throws IOException {
    LOG.debug("PartitionInputStreamHdfs({}, {}, {}, {}, {})", tfs, fileId, hdfsPath, conf,
        bufferSize);
    mCurrentPosition = 0;
    mHdfsPath = hdfsPath;
    mHadoopConf = conf;
    mHadoopBufferSize = bufferSize;
    mTFS = tfs;
    mTachyonFile = mTFS.getFile(fileId);
    if (mTachyonFile == null) {
      throw new FileNotFoundException("File " + hdfsPath + " with FID " + fileId
          + " is not found.");
    }
    mTachyonFile.setUFSConf(mHadoopConf);
    try {
      mTachyonFileInputStream = mTachyonFile.getInStream(ReadType.CACHE);
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (mTachyonFileInputStream != null) {
      mTachyonFileInputStream.close();
    }
    if (mHdfsInputStream != null) {
      mHdfsInputStream.close();
    }
  }

  private void getHdfsInputStream(long seekPosition) throws IOException {
    if (mHdfsInputStream == null) {
      FileSystem fs = mHdfsPath.getFileSystem(mHadoopConf);
      mHdfsInputStream = fs.open(mHdfsPath, mHadoopBufferSize);
    }
    mHdfsInputStream.seek(seekPosition);
  }

  /**
   * Return the current offset from the start of the file
   */
  @Override
  public synchronized long getPos() throws IOException {
    return mCurrentPosition;
  }

  @Override
  public synchronized int read() throws IOException {
    byte[] b = new byte[1];
    int ret = read(b);
    if (ret == -1) {
      return -1;
    } else {
      return (int) (b[0] & 0xFF);
    }
  }

  @Override
  public synchronized int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public synchronized int read(byte[] b, int off, int len) throws IOException {
    if (mTachyonFileInputStream != null) {
      try {
        int ret = mTachyonFileInputStream.read(b, off, len);
        if (ret != -1) {
          mCurrentPosition += ret;
        }
        return ret;
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
        mTachyonFileInputStream = null;
      }
    }
    getHdfsInputStream(mCurrentPosition);
    int ret = mHdfsInputStream.read(b, off, len);
    if (ret != -1) {
      mCurrentPosition += ret;
    }
    return ret;
  }

  /**
   * Read upto the specified number of bytes, from a given position within a file, and return the
   * number of bytes read. This does not change the current offset of a file, and is thread-safe.
   */
  @Override
  public synchronized int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    if ((position < 0) || (position >= mTachyonFile.length())) {
      return -1;
    }
    long oldPos = getPos();
    if (mTachyonFileInputStream != null) {
      try {
        mTachyonFileInputStream.seek(position);
        int ret = mTachyonFileInputStream.read(buffer, offset, length);
        mTachyonFileInputStream.seek(oldPos);
        return ret;
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
        mTachyonFileInputStream = null;
      }
    }
    getHdfsInputStream(position);
    int ret = mHdfsInputStream.read(buffer, offset, length);
    mHdfsInputStream.seek(oldPos);
    return ret;
  }

  /**
   * Read number of bytes equalt to the length of the buffer, from a given position within a file.
   * This does not change the current offset of a file, and is thread-safe.
   */
  @Override
  public synchronized void readFully(long position, byte[] buffer) throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }

  /**
   * Read the specified number of bytes, from a given position within a file. This does not change
   * the current offset of a file, and is thread-safe.
   */
  @Override
  public synchronized void readFully(long position, byte[] buffer, int offset, int length)
      throws IOException {
    int ret = read(position, buffer, offset, length);
    if (ret != offset) {
      throw new IOException("Only read " + ret + " bytes, but " + length + " requested");
    }
  }

  /**
   * Seek to the given offset from the start of the file. The next read() will be from that
   * location. Can't seek past the end of the file.
   */
  @Override
  public synchronized void seek(long pos) throws IOException {
    if (pos < 0) {
      throw new IllegalArgumentException("Seek position is negative: " + pos);
    } else if (pos > mTachyonFile.length()) {
      throw new IllegalArgumentException("Seek position is past EOF: " + pos + ", fileSize = "
          + mTachyonFile.length());
    }
    mCurrentPosition = pos;
    if (mTachyonFileInputStream != null) {
      mTachyonFileInputStream.seek(mCurrentPosition);
    }
    if (mHdfsInputStream != null) {
      mHdfsInputStream.seek(mCurrentPosition);
    }
  }

  /**
   * Seeks a different copy of the data. Returns true if found a new source, false otherwise.
   */
  @Override
  public synchronized boolean seekToNewSource(long targetPos) throws IOException {
    throw new IOException("Not supported");
  }
}
