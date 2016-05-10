/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.viewfs;

import java.io.File;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class TestCacheFileSystem {
  private Configuration conf;
  private MiniDFSCluster pCluster;
  private FileSystem pFS;
  private Path pFSTargetRoot;
  private FileSystem fsView;
  private static final File TEST_DIR = GenericTestUtils.getTestDir(
      TestCacheFileSystem.class.getSimpleName());
  
  @Before
  public void setUp() throws Exception {
    conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024);
    
    pCluster = new MiniDFSCluster.Builder(conf).
        numDataNodes(3).build();
    pCluster.waitActive();
    pFS = pCluster.getFileSystem();

    pFS.mkdirs(new Path("/user"));
    pFS.mkdirs(new Path("/data"));
    pFS.mkdirs(new Path("/data/dir1"));
    DFSTestUtil.createFile(pFS, new Path("/data/dir1/pFile"), 1024, (short)3, 0xBEEFBEEF);
    pFSTargetRoot = pFS.makeQualified(new Path("/data"));

    TEST_DIR.mkdirs();
    FileSystem.getLocal(new Configuration()).mkdirs(new Path(TEST_DIR.getPath(), "/data"));
    FileSystem.getLocal(new Configuration()).mkdirs(new Path(TEST_DIR.getPath(), "/data/data2"));
    
    ConfigUtil.addCacheLink(conf, "/data", TEST_DIR.toURI(),
        pFSTargetRoot.toUri());
    fsView = FileSystem.get(FsConstants.VIEWFS_URI, conf);
  }
  
  @Test
  public void testList() throws Exception {
    fsView.mkdirs(new Path("/data/dir3"));
    System.out.println();
    FileStatus[] statuses = fsView.listStatus(new Path("/data"));
    System.out.println(statuses);
    fsView.open(new Path("/data/dir1/pFile"));
    System.out.println();
  }

  @After
  public void tearDown() throws Exception {
    if (pCluster != null) {
      pCluster.shutdown();
    }
    TEST_DIR.delete();
  }
}
