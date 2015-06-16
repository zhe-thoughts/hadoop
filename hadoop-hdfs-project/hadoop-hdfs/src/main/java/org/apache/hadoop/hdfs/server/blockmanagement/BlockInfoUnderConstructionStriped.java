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
package org.apache.hadoop.hdfs.server.blockmanagement;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.io.erasurecode.ECSchema;

import java.util.ArrayList;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState.COMPLETE;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION;

public class BlockInfoUnderConstructionStriped extends
    BlockInfoUnderConstruction {
  private final StripedBlockStorageOp storageOp;

  /**
   * Constructor with null storage targets.
   */
  public BlockInfoUnderConstructionStriped(Block blk, ECSchema schema) {
    this(blk, schema, UNDER_CONSTRUCTION, null);
  }

  /**
   * Create a striped block that is currently being constructed.
   */
  public BlockInfoUnderConstructionStriped(Block blk, ECSchema schema,
      HdfsServerConstants.BlockUCState state, DatanodeStorageInfo[] targets) {
    super(blk, (short) (schema.getNumDataUnits() + schema.getNumParityUnits()));
    storageOp = new StripedBlockStorageOp(this, schema);
    assert getBlockUCState() != COMPLETE :
        "BlockInfoStripedUnderConstruction cannot be in COMPLETE state";
    this.blockUCState = state;
    setExpectedLocations(targets);
  }

  /**
   * Convert an under construction block to a complete block.
   *
   * @return BlockInfo - a complete block.
   * @throws IOException if the state of the block
   * (the generation stamp and the length) has not been committed by
   * the client or it does not have at least a minimal number of replicas
   * reported from data-nodes.
   */
  @Override
  public BlockInfoStriped convertToCompleteBlock() {
    Preconditions.checkState(getBlockUCState() !=
            HdfsServerConstants.BlockUCState.COMPLETE,
        "Trying to convert a COMPLETE block");
    return new BlockInfoStriped(this);
  }

  public boolean isStriped() {
    return true;
  }

  public StripedBlockStorageOp getStripedBlockStorageOp() {
    return storageOp;
  }

  @Override
  boolean addStorage(DatanodeStorageInfo storage, Block reportedBlock) {
    return storageOp.addStorage(storage, reportedBlock);
  }

  @Override
  boolean removeStorage(DatanodeStorageInfo storage) {
    return storageOp.removeStorage(storage);
  }

  @Override
  public int numNodes() {
    return storageOp.numNodes();
  }

  @Override
  void replaceBlock(BlockInfo newBlock) {
    storageOp.replaceBlock(newBlock);
  }

  @Override
  public void setExpectedLocations(DatanodeStorageInfo[] targets) {
    int numLocations = targets == null ? 0 : targets.length;
    this.replicas = new ArrayList<>(numLocations);
    for(int i = 0; i < numLocations; i++) {
      // when creating a new block we simply sequentially assign block index to
      // each storage
      Block blk = new Block(this.getBlockId() + i, 0, this.getGenerationStamp());
      replicas.add(new ReplicaUnderConstruction(blk, targets[i],
          ReplicaState.RBW));
    }
  }

  @Override
  public Block getTruncateBlock() {
    return null;
  }

  @Override
  public void setTruncateBlock(Block recoveryBlock) {
    BlockManager.LOG.warn("Truncate not supported on striped blocks.");
  }
}
