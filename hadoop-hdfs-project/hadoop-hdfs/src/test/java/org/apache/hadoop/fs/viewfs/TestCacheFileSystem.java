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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Before;
import org.junit.Test;


public class TestCacheFileSystem {
  private Configuration conf;
  private MiniDFSCluster pCluster;
  private MiniDFSCluster cacheCluster;
  private FileSystem pFS;
  private FileSystem cacheFS;
  private Path pFSTargetRoot;
  private Path cacheFSTargetRoot;
  private FileSystem fsView;
  
  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    
    pCluster = new MiniDFSCluster.Builder(new Configuration()).
        numDataNodes(3).build();
    pCluster.waitClusterUp();
    pFS = pCluster.getFileSystem();
    
    cacheCluster = new MiniDFSCluster.Builder(new Configuration()).
        numDataNodes(3).build();
    cacheCluster.waitClusterUp();
    cacheFS = cacheCluster.getFileSystem();

    pFS.mkdirs(new Path("/user"));
    pFS.mkdirs(new Path("/data"));
    pFS.mkdirs(new Path("/data/dir1"));
//    DFSTestUtil.createFile(pFS, new Path("/data/pFile"), 1024, (short)1, 0xBEEFBEEF);
    pFSTargetRoot = pFS.makeQualified(new Path("/data"));
    
    cacheFS.mkdirs(new Path("/data"));
    cacheFS.mkdirs(new Path("/data/dir2"));
    FileStatus[] statuses = cacheFS.listStatus(new Path("/data"));
    cacheFSTargetRoot = cacheFS.makeQualified(new Path("/data"));
    
    ConfigUtil.addCacheLink(conf, "/data", cacheFSTargetRoot.toUri(),
        pFSTargetRoot.toUri());
    fsView = FileSystem.get(FsConstants.VIEWFS_URI, conf);
  }
  
  @Test
  public void testList() throws Exception {
    fsView.mkdirs(new Path("/data/dir3"));
    FileStatus[] statuses = cacheFS.listStatus(new Path("/data"));
    System.out.println();
    statuses = fsView.listStatus(new Path("/data"));
    System.out.println(statuses);
  }
}
