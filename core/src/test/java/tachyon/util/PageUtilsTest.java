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

import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.conf.UserConf;
import tachyon.conf.WorkerConf;

public class PageUtilsTest {
  private static long mPageSize;
  
  @Before
  public final void before() throws IOException {
    mPageSize = UserConf.get().PAGE_SIZE_BYTE;
  }

  @Test
  public void getNumPagesTest() {
    Assert.assertEquals(0, PageUtils.getNumPages(0));
    
    Assert.assertEquals(1, PageUtils.getNumPages(mPageSize-1));
    Assert.assertEquals(1, PageUtils.getNumPages(mPageSize));
    
    Assert.assertEquals(2, PageUtils.getNumPages(mPageSize+1));
    Assert.assertEquals(2, PageUtils.getNumPages(2*mPageSize));
  }
  
  @Test
  public void generateAllPagesTest() {
    List<Integer> pages = PageUtils.generateAllPages(0);
    Assert.assertEquals(0, pages.size());
    
    pages = PageUtils.generateAllPages(mPageSize-1);
    for (int i = 0; i < 1; i++) {
      Assert.assertEquals(Integer.valueOf(i), pages.get(i));
    }
    
    pages = PageUtils.generateAllPages(mPageSize+1);
    for (int i = 0; i < 2; i++) {
      Assert.assertEquals(Integer.valueOf(i), pages.get(i));
    }
  }
  
  @Test
  public void getWorkerDataFolderTest() {
    Assert.assertEquals(CommonUtils.concat(WorkerConf.get().DATA_FOLDER, "pagesize_" + mPageSize),
        PageUtils.getWorkerDataFolder());
  }

  @Test
  public void getPageFilenameTest() {
    Assert.assertEquals("0", PageUtils.getPageFilename(0));
    Assert.assertEquals("1", PageUtils.getPageFilename(1));
    Assert.assertEquals("2147483647", PageUtils.getPageFilename(2147483647));
  }

  @Test
  public void getPageIdTest() {
    Assert.assertEquals(0, PageUtils.getPageId(0));
    Assert.assertEquals(0, PageUtils.getPageId(mPageSize-1));
    Assert.assertEquals(1, PageUtils.getPageId(mPageSize));
    Assert.assertEquals(2, PageUtils.getPageId(2*mPageSize));
  }
  @Test
  public void getPageIdsOverRangeTest() {
    List<Integer> pages = PageUtils.getPageIdsOverRange(0, mPageSize);
    Assert.assertEquals(1, pages.size());
    Assert.assertEquals(0, pages.get(0).intValue());
    pages = PageUtils.getPageIdsOverRange(0, mPageSize+1);
    Assert.assertEquals(2, pages.size());
    Assert.assertEquals(0, pages.get(0).intValue());
    Assert.assertEquals(1, pages.get(1).intValue());
    pages = PageUtils.getPageIdsOverRange(mPageSize, 2*mPageSize);
    Assert.assertEquals(2, pages.size());
    Assert.assertEquals(1, pages.get(0).intValue());
    Assert.assertEquals(2, pages.get(1).intValue());
    pages = PageUtils.getPageIdsOverRange(mPageSize, 2*mPageSize+1);
    Assert.assertEquals(3, pages.size());
    Assert.assertEquals(1, pages.get(0).intValue());
    Assert.assertEquals(2, pages.get(1).intValue());
    Assert.assertEquals(3, pages.get(2).intValue());
  }

  @Test
  public void roundToPageMultipleTest() {
    Assert.assertEquals(0, PageUtils.ceilingPageMultiple(0));

    Assert.assertEquals(mPageSize, PageUtils.ceilingPageMultiple(mPageSize));
    Assert.assertEquals(mPageSize, PageUtils.ceilingPageMultiple(mPageSize - 1));
    Assert.assertEquals(2 * mPageSize, PageUtils.ceilingPageMultiple(mPageSize + 1));

    Assert.assertEquals(10 * mPageSize, PageUtils.ceilingPageMultiple(10 * mPageSize));
    Assert.assertEquals(10 * mPageSize, PageUtils.ceilingPageMultiple(10 * mPageSize - 1));
    Assert.assertEquals(11 * mPageSize, PageUtils.ceilingPageMultiple(10 * mPageSize + 1));
  }
}
