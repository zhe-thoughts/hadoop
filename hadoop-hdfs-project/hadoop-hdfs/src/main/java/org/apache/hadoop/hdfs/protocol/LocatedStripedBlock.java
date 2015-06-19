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
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;

import java.util.Arrays;

/**
 * {@link LocatedBlock} with striped block support. For a striped block, each
 * datanode storage is associated with a block in the block group. We need to
 * record the index (in the striped block group) for each of them.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class LocatedStripedBlock extends LocatedBlock {
  private int[] blockIndices;

  public LocatedStripedBlock(ExtendedBlock b, DatanodeInfo[] locs,
      String[] storageIDs, StorageType[] storageTypes, int[] indices,
      long startOffset, boolean corrupt, DatanodeInfo[] cachedLocs) {
    super(b, locs, storageIDs, storageTypes, startOffset, corrupt, cachedLocs);
    assert indices != null && indices.length == locs.length;
    this.blockIndices = new int[indices.length];
    System.arraycopy(indices, 0, blockIndices, 0, indices.length);
  }

  public LocatedStripedBlock(ExtendedBlock b, DatanodeStorageInfo[] storages,
      int[] indices, long startOffset, boolean corrupt) {
    this(b, DatanodeStorageInfo.toDatanodeInfos(storages),
        DatanodeStorageInfo.toStorageIDs(storages),
        DatanodeStorageInfo.toStorageTypes(storages), indices,
        startOffset, corrupt, EMPTY_LOCS);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{" + getBlock()
        + "; getBlockSize()=" + getBlockSize()
        + "; corrupt=" + isCorrupt()
        + "; offset=" + getStartOffset()
        + "; locs=" + Arrays.asList(getLocations())
        + "; indices=" + Arrays.asList(blockIndices)
        + "}";
  }

  public int[] getBlockIndices() {
    return this.blockIndices;
  }
}
