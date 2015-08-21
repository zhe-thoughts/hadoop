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
package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.ErasureCodingPolicyManager;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.junit.Assert.*;

public class TestErasureCodingPolicies {
  private Configuration conf;
  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private static final int BLOCK_SIZE = 1024;
  private FSNamesystem namesystem;

  @Before
  public void setupCluster() throws IOException {
    conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    cluster = new MiniDFSCluster.Builder(conf).
        numDataNodes(1).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    namesystem = cluster.getNamesystem();
  }

  @After
  public void shutdownCluster() throws IOException {
    cluster.shutdown();
  }

  @Test
  public void testBasicSetECPolicy()
      throws IOException, InterruptedException {
    final Path testDir = new Path("/ec");
    fs.mkdir(testDir, FsPermission.getDirDefault());

    /* Normal creation of an erasure coding directory */
    fs.getClient().setErasureCodingPolicy(testDir.toString(), null);

    /* Verify files under the directory are striped */
    final Path ECFilePath = new Path(testDir, "foo");
    fs.create(ECFilePath);
    INode inode = namesystem.getFSDirectory().getINode(ECFilePath.toString());
    assertTrue(inode.asFile().isStriped());

    /* Verify that EC policy cannot be set on non-empty dir
    TODO: reach a conclusion on non-empty dirs
    final Path notEmpty = new Path("/nonEmpty");
    fs.mkdir(notEmpty, FsPermission.getDirDefault());
    fs.create(new Path(notEmpty, "foo"));
    try {
      fs.getClient().setErasureCodingPolicy(notEmpty.toString(), null);
      fail("Erasure coding zone on non-empty dir");
    } catch (IOException e) {
      assertExceptionContains("erasure coding zone for a non-empty directory", e);
    }
    */

    /* Verify that nested EC policies not supported */
    final Path dir1 = new Path("/dir1");
    final Path dir2 = new Path(dir1, "dir2");
    fs.mkdir(dir1, FsPermission.getDirDefault());
    fs.getClient().setErasureCodingPolicy(dir1.toString(), null);
    fs.mkdir(dir2, FsPermission.getDirDefault());
    try {
      fs.getClient().setErasureCodingPolicy(dir2.toString(), null);
      fail("Nested erasure coding policies");
    } catch (IOException e) {
      assertExceptionContains("already has an erasure coding policy", e);
    }

    /* Verify that EC policy cannot be set on a file */
    final Path fPath = new Path("/file");
    fs.create(fPath);
    try {
      fs.getClient().setErasureCodingPolicy(fPath.toString(), null);
      fail("Erasure coding policy on file");
    } catch (IOException e) {
      assertExceptionContains("erasure coding policy for a file", e);
    }
  }

  @Test
  public void testMoveValidity() throws IOException, InterruptedException {
    final Path srcECDir = new Path("/srcEC");
    final Path dstECDir = new Path("/dstEC");
    fs.mkdir(srcECDir, FsPermission.getDirDefault());
    fs.mkdir(dstECDir, FsPermission.getDirDefault());
    fs.getClient().setErasureCodingPolicy(srcECDir.toString(), null);
    fs.getClient().setErasureCodingPolicy(dstECDir.toString(), null);
    final Path srcFile = new Path(srcECDir, "foo");
    fs.create(srcFile);

    // Test move dir
    // Move EC dir under non-EC dir
    final Path newDir = new Path("/srcEC_new");
    fs.rename(srcECDir, newDir);
    fs.rename(newDir, srcECDir); // move back

    // Move EC dir under another EC dir
    fs.rename(srcECDir, dstECDir);
    fs.rename(new Path("/dstEC/srcEC"), srcECDir); // move back

    // Test move file
    /* Verify that a file can be moved between 2 EC dirs */
    fs.rename(srcFile, dstECDir);
    fs.rename(new Path(dstECDir, "foo"), srcECDir); // move back

    /* Verify that a file can be moved from a non-EC dir to an EC dir */
    final Path nonECDir = new Path("/nonEC");
    fs.mkdir(nonECDir, FsPermission.getDirDefault());
    fs.rename(srcFile, nonECDir);

    /* Verify that a file can be moved from an EC dir to a non-EC dir */
    final Path nonECFile = new Path(nonECDir, "nonECFile");
    fs.create(nonECFile);
    fs.rename(nonECFile, dstECDir);
  }

  @Test
  public void testReplication() throws IOException {
    final Path testDir = new Path("/ec");
    fs.mkdir(testDir, FsPermission.getDirDefault());
    fs.setErasureCodingPolicy(testDir, null);
    final Path fooFile = new Path(testDir, "foo");
    // create ec file with replication=0
    fs.create(fooFile, FsPermission.getFileDefault(), true,
        conf.getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
        (short)0, fs.getDefaultBlockSize(fooFile), null);

    try {
      fs.setReplication(fooFile, (short) 3);
      fail("Shouldn't allow to set replication to a file with striped blocks");
    } catch (IOException e) {
      assertExceptionContains(
          "Cannot set replication to a file with striped blocks", e);
    }
  }

  @Test
  public void testGetErasureCodingPolicyWithSystemDefaultECPolicy() throws Exception {
    String src = "/ec";
    final Path ecDir = new Path(src);
    fs.mkdir(ecDir, FsPermission.getDirDefault());
    // dir EC policy should be null
    assertNull(fs.getClient().getFileInfo(src).getErasureCodingPolicy());
    // dir EC policy after setting
    fs.getClient().setErasureCodingPolicy(src, null); //Default one will be used.
    ErasureCodingPolicy sysDefaultECPolicy = ErasureCodingPolicyManager.getSystemDefaultPolicy();
    verifyErasureCodingInfo(src, sysDefaultECPolicy);
    fs.create(new Path(ecDir, "child1")).close();
    // verify for the files in ec dir
    verifyErasureCodingInfo(src + "/child1", sysDefaultECPolicy);
  }

  @Test
  public void testGetErasureCodingPolicy() throws Exception {
    ErasureCodingPolicy[] sysECPolicies = ErasureCodingPolicyManager.getSystemPolices();
    assertTrue("System ecPolicies should be of only 1 for now",
        sysECPolicies.length == 1);

    ErasureCodingPolicy usingECPolicy = sysECPolicies[0];
    String src = "/ec2";
    final Path ecDir = new Path(src);
    fs.mkdir(ecDir, FsPermission.getDirDefault());
    // dir ECInfo before being set
    assertNull(fs.getClient().getFileInfo(src).getErasureCodingPolicy());
    // dir ECInfo after set
    fs.getClient().setErasureCodingPolicy(src, usingECPolicy);
    verifyErasureCodingInfo(src, usingECPolicy);
    fs.create(new Path(ecDir, "child1")).close();
    // verify for the files in ec dir
    verifyErasureCodingInfo(src + "/child1", usingECPolicy);
  }

  private void verifyErasureCodingInfo(
      String src, ErasureCodingPolicy usingECPolicy) throws IOException {
    HdfsFileStatus hdfsFileStatus = fs.getClient().getFileInfo(src);
    ErasureCodingPolicy ecPolicy = hdfsFileStatus.getErasureCodingPolicy();
    assertNotNull(ecPolicy);
    assertEquals("Actually used ecPolicy should be equal with target ecPolicy",
        usingECPolicy, ecPolicy);
  }
}
