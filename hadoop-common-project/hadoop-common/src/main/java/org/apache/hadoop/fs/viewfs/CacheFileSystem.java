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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;


public class CacheFileSystem extends FileSystem {

  static final class CacheFileStatus extends FileStatus {
    private final ChRootedFileSystem realFs;
    private CacheFileStatus(ChRootedFileSystem realFs, FileStatus other) 
        throws IOException {
      super(other);
      this.realFs = realFs; 
    }

    String stripRoot() throws IOException {
      return realFs.stripOutRoot(this.getPath());
    }
  }
  private final ChRootedFileSystem cacheFS;
  private final ChRootedFileSystem pFS;
  private static URI myUri = URI.create("cache:///");

  public CacheFileSystem(URI cacheURI, URI pURI, Configuration conf) throws IOException {
    setConf(conf);
    cacheFS = new ChRootedFileSystem(cacheURI, conf);
    pFS = new ChRootedFileSystem(pURI, conf);
  }
  @Override
  public void initialize(final URI theUri, final Configuration conf)
      throws IOException{

  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    try {
      return cacheFS.getFileStatus(f);
    } catch (FileNotFoundException e) {
      return pFS.getFileStatus(f);
    }
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    cacheFS.setWorkingDirectory(new_dir);
  }

  @Override
  public Path getWorkingDirectory() {
    return cacheFS.getWorkingDirectory();
  }
  @Override
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    return cacheFS.append(f, bufferSize, progress);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return cacheFS.rename(src, dst);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return cacheFS.delete(f, recursive);
  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException,
                                                IOException {
    FileStatus[] cacheStatuses = cacheFS.listStatus(f);
    FileStatus[] pStatuses = pFS.listStatus(f);
    FileStatus[] combinedFileStatus = 
        new FileStatus[cacheStatuses.length + pStatuses.length];
    int i = 0;
    for (FileStatus s : cacheStatuses) {
      combinedFileStatus[i++] = new CacheFileStatus(cacheFS, s);
    }
    for (FileStatus s : pStatuses) {
      combinedFileStatus[i++] = new CacheFileStatus(pFS, s);
    }
    return combinedFileStatus;
  }

  public boolean mkdirs(Path f, FsPermission permission
  ) throws IOException {
    return cacheFS.mkdirs(f, permission);
  }

  @Override
  public URI getUri() {
    return myUri;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize)
      throws IOException {
    try {
      return cacheFS.open(f, bufferSize);
    } catch (FileNotFoundException e) {
      FSDataOutputStream out = cacheFS.create(f);
      FSDataInputStream in = pFS.open(f);
      /**
       * Option 1: Direct transfer (via WebHDFS)
       */
      IOUtils.copyBytes(in, out, getConf());
      /**
       * Option 2: DistCP
       */
      return cacheFS.open(f, bufferSize);
    }
  }

  @Override
  public FSDataOutputStream create(Path f,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress) throws IOException {
    return cacheFS.create(f, permission, overwrite,
        bufferSize, replication, blockSize, progress);
  }
}
