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
package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstructionStriped;

/**
 * Feature for file with striped blocks
 */
class FileWithStripedBlocksFeature implements INode.Feature {
  private BlockInfo[] blocks;

  FileWithStripedBlocksFeature() {
    blocks = new BlockInfo[0];
  }

  FileWithStripedBlocksFeature(BlockInfo[] blocks) {
    Preconditions.checkArgument(blocks != null);
    this.blocks = blocks;
  }

  BlockInfo[] getBlocks() {
    return this.blocks;
  }

  void setBlock(int index, BlockInfo blk) {
    Preconditions.checkArgument(blk.isStriped());
    blocks[index] = blk;
  }

  private void setBlocks(BlockInfo[] blocks) {
    this.blocks = blocks;
  }

  void addBlock(BlockInfo newBlock) {
    Preconditions.checkArgument(newBlock.isStriped());
    if (this.blocks == null) {
      this.setBlocks(new BlockInfo[]{newBlock});
    } else {
      int size = this.blocks.length;
      BlockInfo[] newlist = new BlockInfo[size + 1];
      System.arraycopy(this.blocks, 0, newlist, 0, size);
      newlist[size] = newBlock;
      this.setBlocks(newlist);
    }
  }

  BlockInfoUnderConstructionStriped removeLastBlock(
      Block oldblock) {
    if (blocks == null || blocks.length == 0) {
      return null;
    }
    int newSize = blocks.length - 1;
    if (!blocks[newSize].equals(oldblock)) {
      return null;
    }

    Preconditions.checkState(blocks[newSize].isStriped());
    BlockInfoUnderConstructionStriped uc =
        (BlockInfoUnderConstructionStriped) blocks[newSize];
    //copy to a new list
    BlockInfo[] newlist = new BlockInfo[newSize];
    System.arraycopy(blocks, 0, newlist, 0, newSize);
    setBlocks(newlist);
    return uc;
  }

  void truncateStripedBlocks(int n) {
    final BlockInfo[] newBlocks;
    if (n == 0) {
      newBlocks = new BlockInfo[0];
    } else {
      newBlocks = new BlockInfo[n];
      System.arraycopy(getBlocks(), 0, newBlocks, 0, n);
    }
    // set new blocks
    setBlocks(newBlocks);
  }

  void clear() {
    this.blocks = null;
  }
}
