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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.classification.InterfaceAudience;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.util.ByteArrayManager;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.util.Time;

/**
 * This class extends {@link DataStreamer} to support writing striped blocks
 * to DataNodes.
 *
 * Main differences compared with {@link DataStreamer}:
 *  1) A striped streamer's lifespan is limited within a single block. After
 *      finishing writing to a block the striped streamer will stop.
 *  2) A striped streamer doesn't communicate with NameNode. Instead, it adds
 *      events to {@link BlockMetadataCoordinator}, which will coordinate
 *      updates from all streamers before communicating to NameNode.
 */
@InterfaceAudience.Private
public class StripedDataStreamer extends DataStreamer {
  private final BlockMetadataCoordinator coordinator;
  private final int index;
  private LocatedBlock locatedBlock;

  StripedDataStreamer(HdfsFileStatus stat,
      DFSClient dfsClient, String src,
      Progressable progress, DataChecksum checksum,
      AtomicReference<CachingStrategy> cachingStrategy,
      ByteArrayManager byteArrayManage, String[] favoredNodes,
      short index, BlockMetadataCoordinator coordinator,
      LocatedBlock lb) {
    super(stat, lb == null ? null : lb.getBlock(), dfsClient, src, progress,
        checksum, cachingStrategy, byteArrayManage, favoredNodes);
    this.index = index;
    this.coordinator = coordinator;
    this.locatedBlock = lb;
    LOG.debug("Creating new StripedDataStreamer " + this);
  }

  int getIndex() {
    return index;
  }

  boolean isHealthy() {
    return !streamerClosed() && !getErrorState().hasInternalError();
  }

  @Override
  protected void endBlock() {
    LOG.info("StripedDataStreamer " + getIndex() + " ending block");
    super.endBlock();
    coordinator.addEvent(new BlockMetadataCoordinator.
        EndBlockEvent(getIndex()));
    closeInternal();
  }

  /**
   * The upper level DFSStripedOutputStream will allocate the new block group.
   * All the striped data streamer only needs to fetch from the queue, which
   * should be already be ready.
   */
  @Override
  protected LocatedBlock locateFollowingBlock(DatanodeInfo[] excludedNodes)
      throws IOException {
    if (!this.isHealthy()) {
      // No internal block for this streamer, maybe no enough healthy DN.
      // Throw the exception which has been set by the StripedOutputStream.
      this.getLastException().check(false);
    }
    return locatedBlock;
  }

  @VisibleForTesting
  LocatedBlock peekFollowingBlock() {
    return locatedBlock;
  }

  @Override
  protected void setupPipelineInternal(DatanodeInfo[] nodes,
      StorageType[] nodeStorageTypes) throws IOException {
    boolean success = false;
    while (!success && !streamerClosed() && dfsClient.clientRunning) {
      if (!handleRestartingDatanode()) {
        return;
      }
      handleBadDatanode();

      if (getErrorState().hasInternalError()) {
        LOG.debug("Streamer #" + getIndex() + " updating block for pipeline");
        // If the streamer itself has encountered an error, bump GS
        coordinator.addEvent(new BlockMetadataCoordinator.DNFailureEvent(getIndex()));
        Preconditions.checkState(coordinator.getProposedGenStamp()
            >= block.getGenerationStamp());
        closeInternal();
        setStreamerAsClosed();
      } else {
        long newGS = coordinator.getProposedGenStamp();
        Preconditions.checkState(newGS >
            locatedBlock.getBlock().getGenerationStamp());
        LOG.debug("Streamer " + getIndex() + " processing external error, " +
            "updating DN with new GS " + newGS + " (old GS = " +
            locatedBlock.getBlock().getGenerationStamp() + ")");

        while (accessToken != null && accessToken.decodeIdentifier() != null &&
            accessToken.decodeIdentifier().getExpiryDate() <
            Time.monotonicNow()) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            throw new IOException(
                this + "interrupted while waiting for updated access token.");
          }
        }
        success = createBlockOutputStream(nodes, nodeStorageTypes, newGS, true);
        failPacket4Testing();
        getErrorState().checkRestartingNodeDeadline(nodes);

        if (success) {
          // Notify coordinator the event of DN accepting new GS
          coordinator.addEvent(new BlockMetadataCoordinator.DNAcceptedGSEvent(
              index, newGS));
          locatedBlock.getBlock().setGenerationStamp(newGS);
          synchronized (this.getErrorState()) {
            if (newGS == coordinator.getProposedGenStamp()) {
              this.getErrorState().reset();
            }
          }
        } else {
          // if fail, close the stream. The internal error state and last
          // exception have already been set in createBlockOutputStream
          // TODO: wait for restarting DataNodes during RollingUpgrade
          LOG.debug("Finding failure of DN #" + getIndex() + " during GS bumping.");
          coordinator.addEvent(new BlockMetadataCoordinator.DNFailureEvent(getIndex()));
          closeInternal();
          setStreamerAsClosed();
        }
      }
    } // while
  }

  @Override
  protected LocatedBlock nextBlockOutputStream() throws IOException {
    try {
      LocatedBlock lb = super.nextBlockOutputStream();
      coordinator.addEvent(new BlockMetadataCoordinator.
          StreamerStreamingEvent(getIndex()));
      return lb;
    } catch (IOException e) {
      closeInternal();
      BlockMetadataCoordinator.StreamerStatus status =
          coordinator.getStreamerStatus(getIndex());
      if (status == BlockMetadataCoordinator.StreamerStatus.RUNNING ||
          status == BlockMetadataCoordinator.StreamerStatus.INITIATED){
        coordinator.addEvent(
            new BlockMetadataCoordinator.DNFailureEvent(getIndex()));
      }
      throw e;
    }
  }

  public void setExternalError() {
    LOG.debug("Setting external error for streamer #" + getIndex() + ", currently has " +
        getErrorState().hasInternalError() + "-" + getErrorState().hasDatanodeError());
    synchronized (this.getErrorState()) {
      getErrorState().setExternalError();
    }
    synchronized (dataQueue) {
      dataQueue.notifyAll();
    }
  }

  LocatedBlock getLocatedBlock() {
    return locatedBlock;
  }

  @Override
  public String toString() {
    return "#" + index + ": " + (!isHealthy() ? "failed, ": "") + super.toString();
  }
}
