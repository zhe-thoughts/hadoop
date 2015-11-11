/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * When writing to each striped block group, a coordinator thread is started
 * to serialize events from all streamers.
 */
public class BlockMetadataCoordinator extends Daemon {
  static final Logger LOG =
      LoggerFactory.getLogger(BlockMetadataCoordinator.class);

  enum BlockMetadataEventTypes {
    /** Streamer enters {@link BlockConstructionStage#DATA_STREAMING} stage */
    STREAMER_STREAMING,

    /** A streamer has finished writing its block. */
    END_BLOCK,

    /** A streamer has encountered a DN failure. */
    DN_FAILURE,

    /** A streamer has successfully updated genStamp on its DN. */
    DN_ACCEPT_GS
  }

  /** Status of each streamer.
   * TODO: consolidate with {@link BlockConstructionStage}
   */
  enum StreamerStatus {
    INITIATED,
    RUNNING,
    FINISHED,
    FAILED,
    NULL // When cluster doesn't have enough DNs for all parity blocks
  }

  /**
   * An event to be sent by a streamer and processed by this coordinator
   */
  public static class BlockMetadataEvent {
    private int streamerIdx;
    private BlockMetadataEventTypes type;

    public BlockMetadataEvent(int index, BlockMetadataEventTypes type) {
      this.streamerIdx = index;
      this.type = type;
    }

    public int getStreamerIdx() {
      return streamerIdx;
    }

    public BlockMetadataEventTypes getType() {
      return type;
    }

    @Override
    public String toString() {
      return "BlockMetadataEvent " + type + " for streamer " + streamerIdx;
    }
  }

  public static class EndBlockEvent extends BlockMetadataEvent {
    public EndBlockEvent(int index) {
      super(index, BlockMetadataEventTypes.END_BLOCK);
    }
  }

  public static class DNFailureEvent extends BlockMetadataEvent {
    public DNFailureEvent(int index) {
      super(index, BlockMetadataEventTypes.DN_FAILURE);
    }
  }

  public static class DNAcceptedGSEvent extends BlockMetadataEvent {
    private long acceptedGS;

    public DNAcceptedGSEvent(int index, long acceptedGS) {
      super(index, BlockMetadataEventTypes.DN_ACCEPT_GS);
      this.acceptedGS = acceptedGS;
    }
  }

  public static class StreamerStreamingEvent extends BlockMetadataEvent {
    public StreamerStreamingEvent(int index) {
      super(index, BlockMetadataEventTypes.STREAMER_STREAMING);
    }
  }

  final BlockingQueue<BlockMetadataEvent> events = new LinkedBlockingQueue<>();

  private DFSStripedOutputStream outputStream;
  private AtomicLong proposedGenStamp;
  private long tokenExpiryDate;
  private final long[] dnAcceptedGenStamps;
  private final StreamerStatus[] streamerStatus;
  private final int numAllBlocks;
  private AtomicBoolean allStreamersEndedBlock = new AtomicBoolean(false);
  private volatile boolean coordinatorClosed = false;

  /**
   *
   * @param outputStream The {@link DFSStripedOutputStream} with all streamers
   * @param blkGroup The striped block group
   * @param internalBlks The array of internal blocks. Used to initialize
   *                     the status of each streamer
   */
  public BlockMetadataCoordinator(DFSStripedOutputStream outputStream,
      final int numAllBlocks, final LocatedStripedBlock blkGroup,
      final LocatedBlock[] internalBlks) {
    this.outputStream = outputStream;
    this.numAllBlocks = numAllBlocks;
    dnAcceptedGenStamps = new long[numAllBlocks];
    streamerStatus = new StreamerStatus[numAllBlocks];
    for (int i = 0; i < numAllBlocks; i++) {
      dnAcceptedGenStamps[i] = blkGroup.getBlock().getGenerationStamp();
      streamerStatus[i] = internalBlks[i] == null ?
          StreamerStatus.NULL : StreamerStatus.INITIATED;
    }
    proposedGenStamp = new AtomicLong(blkGroup.getBlock().getGenerationStamp());
    tokenExpiryDate = getTokenExpiryDate(blkGroup);
    LOG.debug("Initial tokenExpiryDate is " +
        tokenExpiryDate + ", time is " + Time.monotonicNow());
  }

  public void addEvent(BlockMetadataEvent e) {
    try {
      LOG.debug("Adding event " + e);
      events.put(e);
    } catch (InterruptedException ie) {
    }
  }

  public synchronized long getProposedGenStamp() {
    return proposedGenStamp.get();
  }

  public synchronized StreamerStatus getStreamerStatus(int i) {
    return streamerStatus[i];
  }

  public synchronized boolean processedAllEvents() {
    return events.isEmpty() && allStreamersHaveSyncedGS();
  }
  private synchronized boolean allStreamersHaveSyncedGS() {
    boolean gsInSync = true;
    for (int i = 0; i < numAllBlocks; i++) {
      if (streamerStatus[i] == StreamerStatus.FAILED ||
          streamerStatus[i] == StreamerStatus.NULL ||
          streamerStatus[i] == StreamerStatus.INITIATED) {
        continue;
      }
      if (streamerStatus[i] == StreamerStatus.FINISHED) {
        LOG.error("Streamer #" + i + " has already finished.");
      }
      long gs = dnAcceptedGenStamps[i];
      Preconditions.checkState(gs <= getProposedGenStamp());
      if (gs < getProposedGenStamp()) {
        gsInSync = false;
        break;
      }
    }
    return gsInSync;
  }

  public synchronized boolean getAllStreamersEndedBlock() {
    return allStreamersEndedBlock.get();
  }

  private void updatePipeline(long newGS) throws IOException {
    final DatanodeInfo[] newNodes = new DatanodeInfo[numAllBlocks];
    final String[] newStorageIDs = new String[numAllBlocks];
    for (int i = 0; i < numAllBlocks; i++) {
      final StripedDataStreamer streamer =
          outputStream.getStripedDataStreamer(i);
      final DatanodeInfo[] nodes = streamer.getNodes();
      final String[] storageIDs = streamer.getStorageIDs();
      if (streamerStatus[i] == StreamerStatus.RUNNING
          && nodes != null && storageIDs != null) {
        newNodes[i] = nodes[0];
        newStorageIDs[i] = storageIDs[0];
      } else {
        newNodes[i] = new DatanodeInfo(DatanodeID.EMPTY_DATANODE_ID);
        newStorageIDs[i] = "";
      }
    }
    ExtendedBlock newBG = DataStreamer.newBlock(outputStream.getBlock(), newGS);
    LOG.debug("updatePipeline with new GS = " + newGS);
    outputStream.dfsClient.namenode.updatePipeline(
        outputStream.dfsClient.clientName,
        outputStream.getBlock(), newBG, newNodes, newStorageIDs);
  }

  static long getTokenExpiryDate(LocatedStripedBlock lb) {
    if (lb.getBlockTokens() == null || lb.getBlockTokens().length == 0) {
      return Long.MAX_VALUE;
    } else {
      try {
        BlockTokenIdentifier id = lb.getBlockTokens()[0].decodeIdentifier();
        return id.getExpiryDate();
      } catch (IOException | NullPointerException e) {
        return Long.MAX_VALUE;
      }
    }
  }

  private void close(boolean force) {
    coordinatorClosed = true;
    if (force) {
      this.interrupt();
    }
  }

  boolean isCoordinatorClosed() {
    return coordinatorClosed;
  }

  @Override
  public void run() {
    try {
      while (!coordinatorClosed || !events.isEmpty()) {
        // TODO: more elegant way to renew lease ahead of expiry
        if (Time.monotonicNow() + 500L > tokenExpiryDate) {
          try {
            LocatedBlock updatedBlock =
                outputStream.dfsClient.namenode.updateBlockForPipeline(
                    outputStream.getBlock(), outputStream.dfsClient.getClientName());
            Preconditions.checkState(updatedBlock.isStriped());
            LocatedStripedBlock updatedBlockGroup =
                (LocatedStripedBlock) updatedBlock;
            int[] internalBlkIndices = updatedBlockGroup.getBlockIndices();
            for (int i = 0; i < internalBlkIndices.length; i++) {
              if (i >= outputStream.getStreamers().size()) {
                break;
              }
              outputStream.getStreamers().get(internalBlkIndices[i]).
                  setAccessToken(updatedBlockGroup.getBlockTokens()[i]);

              LOG.debug("Renewed block token for " +
                  outputStream.getStreamers().get(i) + " from " +
                  tokenExpiryDate + " to " + outputStream.getStreamers().
                  get(i).accessToken.decodeIdentifier().getExpiryDate());
            }
            tokenExpiryDate = getTokenExpiryDate(updatedBlockGroup);
          } catch (IOException e) {
            LOG.error("Failed renewing block tokens.");
          }
        }
        BlockMetadataEvent event = events.take();
        int eventStreamerIdx = event.getStreamerIdx();
        StripedDataStreamer eventStreamer =
            outputStream.getStripedDataStreamer(eventStreamerIdx);
        LOG.debug("Got event " + event);
        switch (event.getType()) {
          case STREAMER_STREAMING:
            streamerStatus[eventStreamerIdx] = StreamerStatus.RUNNING;
            if (dnAcceptedGenStamps[eventStreamerIdx] <
                getProposedGenStamp()) {
              eventStreamer.setExternalError();
            }
            break;
          case END_BLOCK:
            streamerStatus[eventStreamerIdx] = StreamerStatus.FINISHED;
            LOG.debug("Streamer status: " + Arrays.asList(streamerStatus).toString());
            boolean allStreamersEnded = true;
            for (StreamerStatus status : streamerStatus) {
              if (status == StreamerStatus.RUNNING ||
                  status == StreamerStatus.INITIATED) {
                allStreamersEnded = false;
                break;
              }
            }
            if (allStreamersEnded) {
              // END_BLOCK should be the last event a streamer can ever send
              if (!events.isEmpty()) {
                LOG.debug("Still has events " + events);
              }
              allStreamersEndedBlock.set(true);
              LOG.debug("All streamers have ended block.");
              close(false);
            }
            break;
          case DN_FAILURE:
            if (streamerStatus[eventStreamerIdx] == StreamerStatus.FAILED ||
                streamerStatus[eventStreamerIdx] == StreamerStatus.NULL) {
              // Already reported as failed
              break;
            }
            LOG.debug("Bumping GS for failure of DN #" + eventStreamerIdx);
            streamerStatus[eventStreamerIdx] = StreamerStatus.FAILED;
            LOG.debug("Streamer status: " + Arrays.asList(streamerStatus).toString());
            proposedGenStamp.getAndIncrement();
            outputStream.getBlock().setGenerationStamp(
                getProposedGenStamp());
            for (int i = 0; i < numAllBlocks; i++) {
              if (streamerStatus[i] == StreamerStatus.RUNNING) {
                outputStream.getStreamers().get(i).setExternalError();
              }
            }
            break;
          case DN_ACCEPT_GS:
            long acceptedGS = ((DNAcceptedGSEvent) event).acceptedGS;
            LOG.debug("Streamer #" + eventStreamerIdx +
                " has accepted GS " + acceptedGS);
            dnAcceptedGenStamps[eventStreamerIdx] = acceptedGS;
            boolean gsInSync = allStreamersHaveSyncedGS();
            if (gsInSync) {
              try {
                LOG.debug("All healthy DNs have accepted proposed GS " +
                    proposedGenStamp);
                updatePipeline(getProposedGenStamp());
              } catch (IOException e) {
              }
            }
            break;
          default:
            LOG.error("Unrecognizable event " + event);
            break;
        }
      }
    } catch (InterruptedException e) {
      DFSClient.LOG.warn("BlockMetadataCoordinator interrupted");
    }
  }
}
