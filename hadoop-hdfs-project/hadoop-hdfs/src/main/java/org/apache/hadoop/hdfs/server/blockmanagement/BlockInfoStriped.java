package org.apache.hadoop.hdfs.server.blockmanagement;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.io.erasurecode.ECSchema;

/**
 * Subclass of {@link BlockInfo}, used for a block with striping format.
 */
@InterfaceAudience.Private
public class BlockInfoStriped extends BlockInfo {
  private final StripedBlockStorageOp storageOp;

  public BlockInfoStriped(Block blk, ECSchema schema, int cellSize) {
    super(blk, (short) (schema.getNumDataUnits() + schema.getNumParityUnits()));
    storageOp = new StripedBlockStorageOp(this, schema, cellSize);
  }

  BlockInfoStriped(BlockInfo b) {
    super(b);
    Preconditions.checkState(b.isStriped());
    this.storageOp = new StripedBlockStorageOp(this,
        b.getStripedBlockStorageOp());
    this.setBlockCollection(b.getBlockCollection());
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
  BlockInfoUnderConstructionStriped convertCompleteBlockToUC(
      HdfsServerConstants.BlockUCState s, DatanodeStorageInfo[] targets) {
    BlockInfoUnderConstructionStriped ucBlock =
        new BlockInfoUnderConstructionStriped(this,
            storageOp.getSchema(), storageOp.getCellSize(), s, targets);
    ucBlock.setBlockCollection(getBlockCollection());
    return ucBlock;
  }
}
