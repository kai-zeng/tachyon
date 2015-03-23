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

package tachyon.util;

import java.util.ArrayList;
import java.util.List;

import tachyon.conf.UserConf;
import tachyon.conf.WorkerConf;

/**
 * PageUtils provides methods to work with pages in Tachyon. Each block in Tachyon is split into a
 * number of pages, and can be uniquely identified within the block by an ID.
 *
 */
public class PageUtils {

  /**
   * Gets the number of pages in a block of the given size
   * 
   * @param blockLength the number of bytes in the block
   * @return the number of pages that the block is split into
   */
  public static int getNumPages(long blockLength) {
    return (int) ((blockLength + UserConf.get().PAGE_SIZE_BYTE - 1)
        / UserConf.get().PAGE_SIZE_BYTE);
  }

  /**
   * Generates a list of all the pages for the given block size. This should only be used in the
   * intermediate stages of implementing partial-block, when we aren't actually storing pages, so
   * for methods that expect a list of pages that were cached, we return all the pages
   * automatically. Once partial-block is implemented, this method should not be used anywhere,
   * except perhaps for testing.
   * 
   * @param blockLength the number of bytes in the block
   * @return a list of all pages in the block
   */
  public static List<Integer> generateAllPages(long blockLength) {
    List<Integer> ret = new ArrayList<Integer>();
    int numPages = getNumPages(blockLength);
    for (int i = 0; i < numPages; i ++) {
      ret.add(i);
    }
    return ret;
  }

  /**
   * Gets the data folder for the worker. To avoid storing pages with different sizes in the same
   * directory, we put all the pages in a sub-directory according to the page size, so now the
   * worker data folder depends on the page size.
   * 
   * @return the data folder
   */
  public static String getWorkerDataFolder() {
    String pageString = "pagesize_" + UserConf.get().PAGE_SIZE_BYTE;
    return CommonUtils.concat(WorkerConf.get().DATA_FOLDER, pageString);
  }

  /* Gets the file name of the page with the given id
   *
   * @param id the id of the page
   * @return the file name for the page
   */
  public static String getPageFilename(int pageId) {
    return String.valueOf(pageId);
  }

  /**
   * Gets the id of the page containing the given block offset
   *
   * @param offset the offset in bytes from the block
   * @return page id containing the offset
   */
  public static int getPageId(long offset) {
    return (int) (offset / UserConf.get().PAGE_SIZE_BYTE);
  }

  /**
   * Gets the byte offset of the page in the block
   *
   * @param pageId the id of the page
   * @return the byte offset
   */
  public static long getPageOffset(int pageId) {
    return pageId * UserConf.get().PAGE_SIZE_BYTE;
  }

  /**
   * Returns a list of page ids spanning the given offset and length
   *
   * @param offset the offset in bytes from the block
   * @param length the length of the range of bytes
   * @return a list of page ids covering the given range
   */
  public static List<Integer> getPageIdsOverRange(long offset, long length) {
    int startId = getPageId(offset);
    int endId = getPageId(offset + length - 1);
    List<Integer> ret = new ArrayList<Integer>((int) (endId - startId + 1));
    for (int i = startId; i <= endId; i ++) {
      ret.add(i);
    }
    return ret;
  }
}
