package org.apache.hadoop.hdfs.server.blockmanagement;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.io.erasurecode.ECSchema;

public class StripedBlockStorageOp {
  private final ECSchema schema;
  /**
   * Always the same size with triplets. Record the block index for each triplet
   * TODO: actually this is only necessary for over-replicated block. Thus can
   * be further optimized to save memory usage.
   */
  private byte[] indices;
  private final BlockInfo b;

  public StripedBlockStorageOp(BlockInfo b, ECSchema schema) {
    this.b = b;
    indices = new byte[schema.getNumDataUnits() + schema.getNumParityUnits()];
    initIndices();
    this.schema = schema;
  }

  public StripedBlockStorageOp(StripedBlockStorageOp op) {
    this(op.b, op.schema);
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
}
