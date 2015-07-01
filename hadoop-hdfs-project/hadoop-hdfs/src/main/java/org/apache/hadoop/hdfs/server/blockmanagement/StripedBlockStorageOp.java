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
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.erasurecode.ECSchema;

import static org.apache.hadoop.hdfs.protocol.HdfsConstants.BLOCK_STRIPED_CELL_SIZE;

public class StripedBlockStorageOp {
  private final ECSchema schema;
  private final int cellSize;

  /**
   * Always the same size with triplets. Record the block index for each triplet
   * TODO: actually this is only necessary for over-replicated block. Thus can
   * be further optimized to save memory usage.
   */
  private byte[] indices;
  private final BlockInfo b;

  public StripedBlockStorageOp(BlockInfo b, ECSchema schema, int cellSize) {
    this.b = b;
    indices = new byte[schema.getNumDataUnits() + schema.getNumParityUnits()];
    initIndices();
    this.schema = schema;
    this.cellSize = cellSize;
  }

  public StripedBlockStorageOp(StripedBlockStorageOp op) {
    this(op.b, op.schema, op.cellSize);
  }

  public ECSchema getSchema() {
    return schema;
  }

  public short getTotalBlockNum() {
    return (short) (schema.getNumDataUnits()
        + schema.getNumParityUnits());
  }

  public short getDataBlockNum() {
    return (short) this.schema.getNumDataUnits();
  }

  public short getParityBlockNum() {
    return (short) this.schema.getNumParityUnits();
  }

  public long spaceConsumed() {
    // In case striped blocks, total usage by this striped blocks should
    // be the total of data blocks and parity blocks because
    // `getNumBytes` is the total of actual data block size.
    return StripedBlockUtil.spaceConsumedByStripedBlock(b.getNumBytes(),
        this.schema.getNumDataUnits(), this.schema.getNumParityUnits(),
        BLOCK_STRIPED_CELL_SIZE);
  }

  public int getCellSize() {
    return cellSize;
  }

  private void initIndices() {
    for (int i = 0; i < indices.length; i++) {
      indices[i] = -1;
    }
  }

  private void ensureCapacity(int totalSize, boolean keepOld) {
    if (b.getCapacity() < totalSize) {
      Object[] old = b.triplets;
      byte[] oldIndices = indices;
      b.triplets = new Object[totalSize * 3];
      indices = new byte[totalSize];
      initIndices();

      if (keepOld) {
        System.arraycopy(old, 0, b.triplets, 0, old.length);
        System.arraycopy(oldIndices, 0, indices, 0, oldIndices.length);
      }
    }
  }

  private int findSlot() {
    int i = getTotalBlockNum();
    for (; i < b.getCapacity(); i++) {
      if (b.getStorageInfo(i) == null) {
        return i;
      }
    }
    // need to expand the triplet size
    ensureCapacity(i + 1, true);
    return i;
  }

  boolean addStorage(DatanodeStorageInfo storage, Block reportedBlock) {
    int blockIndex = BlockIdManager.getBlockIndex(reportedBlock);
    int index = blockIndex;
    DatanodeStorageInfo old = b.getStorageInfo(index);
    if (old != null && !old.equals(storage)) { // over replicated
      // check if the storage has been stored
      int i = b.findStorageInfo(storage);
      if (i == -1) {
        index = findSlot();
      } else {
        return true;
      }
    }
    addStorage(storage, index, blockIndex);
    return true;
  }

  private void addStorage(DatanodeStorageInfo storage, int index,
      int blockIndex) {
    b.setStorageInfo(index, storage);
    b.setNext(index, null);
    b.setPrevious(index, null);
    indices[index] = (byte) blockIndex;
  }

  private int findStorageInfoFromEnd(DatanodeStorageInfo storage) {
    final int len = b.getCapacity();
    for (int idx = len - 1; idx >= 0; idx--) {
      DatanodeStorageInfo cur = b.getStorageInfo(idx);
      if (storage.equals(cur)) {
        return idx;
      }
    }
    return -1;
  }

  int getStorageBlockIndex(DatanodeStorageInfo storage) {
    int i = b.findStorageInfo(storage);
    return i == -1 ? -1 : indices[i];
  }

  /**
   * Identify the block stored in the given datanode storage. Note that
   * the returned block has the same block Id with the one seen/reported by the
   * DataNode.
   */
  Block getBlockOnStorage(DatanodeStorageInfo storage) {
    int index = getStorageBlockIndex(storage);
    if (index < 0) {
      return null;
    } else {
      Block block = new Block(b);
      block.setBlockId(b.getBlockId() + index);
      return block;
    }
  }

  boolean removeStorage(DatanodeStorageInfo storage) {
    int dnIndex = findStorageInfoFromEnd(storage);
    if (dnIndex < 0) { // the node is not found
      return false;
    }
    Preconditions.checkArgument(b.getPrevious(dnIndex) == null &&
            b.getNext(dnIndex) == null,
        "Block is still in the list and must be removed first.");
    // set the triplet to null
    b.setStorageInfo(dnIndex, null);
    b.setNext(dnIndex, null);
    b.setPrevious(dnIndex, null);
    indices[dnIndex] = -1;
    return true;
  }

  int numNodes() {
    Preconditions.checkArgument(b.triplets != null,
        "BlockInfo is not initialized");
    Preconditions.checkState(b.triplets.length % 3 == 0,
        "Malformed BlockInfo");
    int num = 0;
    for (int idx = b.getCapacity() - 1; idx >= 0; idx--) {
      if (b.getStorageInfo(idx) != null) {
        num++;
      }
    }
    return num;
  }

  void replaceBlock(BlockInfo newBlock) {
    Preconditions.checkArgument(newBlock instanceof BlockInfoStriped);
    StripedBlockStorageOp newStorageOp = newBlock.getStripedBlockStorageOp();
    final int size = b.getCapacity();
    newStorageOp.ensureCapacity(size, false);
    for (int i = 0; i < size; i++) {
      final DatanodeStorageInfo storage = b.getStorageInfo(i);
      if (storage != null) {
        final int blockIndex = indices[i];
        final boolean removed = storage.removeBlock(b);
        assert removed : "currentBlock not found.";

        newStorageOp.addStorage(storage, i, blockIndex);
        storage.insertToList(newBlock);
      }
    }
  }

  /**
   * If the block is committed/completed and its length is less than a full
   * stripe, it returns the the number of actual data blocks.
   * Otherwise it returns the number of data units specified by schema.
   */
  public short getRealDataBlockNum() {
    if (b.isComplete() || b.getBlockUCState() == BlockUCState.COMMITTED) {
      return (short) Math.min(getDataBlockNum(),
          (b.getNumBytes() - 1) / BLOCK_STRIPED_CELL_SIZE + 1);
    } else {
      return getDataBlockNum();
    }
  }

  public short getRealTotalBlockNum() {
    return (short) (getRealDataBlockNum() + getParityBlockNum());
  }

}
