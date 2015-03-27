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

import tachyon.conf.TachyonConf;

public class PageUtilsTest {
  private static PageUtils mPageUtils;
  private static long mPageSize;

  @Before
  public final void beforeClass() throws IOException {
    TachyonConf tachyonConf = new TachyonConf();
    mPageUtils = new PageUtils(tachyonConf);
    mPageSize = mPageUtils.getPageSize();
  }

  @Test
  public void getNumPagesTest() {
    Assert.assertEquals(0, mPageUtils.getNumPages(0));
    
    Assert.assertEquals(1, mPageUtils.getNumPages(mPageSize-1));
    Assert.assertEquals(1, mPageUtils.getNumPages(mPageSize));
    
    Assert.assertEquals(2, mPageUtils.getNumPages(mPageSize+1));
    Assert.assertEquals(2, mPageUtils.getNumPages(2*mPageSize));
  }
  
  @Test
  public void generateAllPagesTest() {
    List<Integer> pages = mPageUtils.generateAllPages(0);
    Assert.assertEquals(0, pages.size());
    
    pages = mPageUtils.generateAllPages(mPageSize-1);
    for (int i = 0; i < 1; i++) {
      Assert.assertEquals(Integer.valueOf(i), pages.get(i));
    }
    
    pages = mPageUtils.generateAllPages(mPageSize+1);
    for (int i = 0; i < 2; i++) {
      Assert.assertEquals(Integer.valueOf(i), pages.get(i));
    }
  }

  @Test
  public void getPageFilenameTest() {
    Assert.assertEquals("0", PageUtils.getPageFilename(0));
    Assert.assertEquals("1", PageUtils.getPageFilename(1));
    Assert.assertEquals("2147483647", PageUtils.getPageFilename(2147483647));
  }

  @Test
  public void getPageIdTest() {
    Assert.assertEquals(0, mPageUtils.getPageId(0));
    Assert.assertEquals(0, mPageUtils.getPageId(mPageSize-1));
    Assert.assertEquals(1, mPageUtils.getPageId(mPageSize));
    Assert.assertEquals(2, mPageUtils.getPageId(2*mPageSize));
  }
  @Test
  public void getPageIdsOverRangeTest() {
    List<Integer> pages = mPageUtils.getPageIdsOverRange(0, mPageSize);
    Assert.assertEquals(1, pages.size());
    Assert.assertEquals(0, pages.get(0).intValue());
    pages = mPageUtils.getPageIdsOverRange(0, mPageSize+1);
    Assert.assertEquals(2, pages.size());
    Assert.assertEquals(0, pages.get(0).intValue());
    Assert.assertEquals(1, pages.get(1).intValue());
    pages = mPageUtils.getPageIdsOverRange(mPageSize, 2*mPageSize);
    Assert.assertEquals(2, pages.size());
    Assert.assertEquals(1, pages.get(0).intValue());
    Assert.assertEquals(2, pages.get(1).intValue());
    pages = mPageUtils.getPageIdsOverRange(mPageSize, 2*mPageSize+1);
    Assert.assertEquals(3, pages.size());
    Assert.assertEquals(1, pages.get(0).intValue());
    Assert.assertEquals(2, pages.get(1).intValue());
    Assert.assertEquals(3, pages.get(2).intValue());
  }
}
