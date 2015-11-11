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
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;

import com.google.common.base.Preconditions;
import org.apache.htrace.core.TraceScope;


/**
 * This class supports writing files in striped layout and erasure coded format.
 * Each stripe contains a sequence of cells.
 *
 * It mainly adds the following logic on top of {@link DFSOutputStream}:
 *  1) Writes to a set of queues in round-robin pattern, in {@link #writeChunk}
 *  2) Generates and queues parity packets
 *  3) Works with NameNode to allocate new blocks
 *  4) When needed, cleanup a set of streamers instead of one
 */
@InterfaceAudience.Private
public class DFSStripedOutputStream extends DFSOutputStream {

  /** Buffers for writing the data and parity cells of a stripe. */
  class CellBuffers {
    private final ByteBuffer[] buffers;
    private final byte[][] checksumArrays;

    CellBuffers(int numParityBlocks) throws InterruptedException{
      if (cellSize % bytesPerChecksum != 0) {
        throw new HadoopIllegalArgumentException("Invalid values: "
            + HdfsClientConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY + " (="
            + bytesPerChecksum + ") must divide cell size (=" + cellSize + ").");
      }

      checksumArrays = new byte[numParityBlocks][];
      final int size = getChecksumSize() * (cellSize / bytesPerChecksum);
      for (int i = 0; i < checksumArrays.length; i++) {
        checksumArrays[i] = new byte[size];
      }

      buffers = new ByteBuffer[numAllBlocks];
      for (int i = 0; i < buffers.length; i++) {
        buffers[i] = ByteBuffer.wrap(byteArrayManager.newByteArray(cellSize));
      }
    }

    private ByteBuffer[] getBuffers() {
      return buffers;
    }

    byte[] getChecksumArray(int i) {
      return checksumArrays[i - numDataBlocks];
    }

    private int addTo(int i, byte[] b, int off, int len) {
      final ByteBuffer buf = buffers[i];
      final int pos = buf.position() + len;
      Preconditions.checkState(pos <= cellSize);
      buf.put(b, off, len);
      return pos;
    }

    private void clear() {
      for (int i = 0; i< numAllBlocks; i++) {
        buffers[i].clear();
      }
    }

    private void release() {
      for (int i = 0; i < numAllBlocks; i++) {
        byteArrayManager.release(buffers[i].array());
      }
    }

    private void flipDataBuffers() {
      for (int i = 0; i < numDataBlocks; i++) {
        buffers[i].flip();
      }
    }
  }

  private BlockMetadataCoordinator coordinator;
  private final CellBuffers cellBuffers;
  private final RawErasureEncoder encoder;
  private final List<StripedDataStreamer> streamers;
  private final DFSPacket[] currentPackets; // current Packet of each streamer
  private final HdfsFileStatus stat;
  private final Progressable progress;
  private final DataChecksum checksum;

  /** Size of each striping cell, must be a multiple of bytesPerChecksum */
  private final int cellSize;
  private final int numAllBlocks;

  private final int numDataBlocks;
  private ExtendedBlock currentBlockGroup = null;
  private final String[] favoredNodes;

  /** Construct a new output stream for creating a file. */
  DFSStripedOutputStream(DFSClient dfsClient, String src, HdfsFileStatus stat,
                         EnumSet<CreateFlag> flag, Progressable progress,
                         DataChecksum checksum, String[] favoredNodes)
                         throws IOException {
    super(dfsClient, src, stat, flag, progress, checksum, favoredNodes, false);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating DFSStripedOutputStream for " + src);
    }

    final ErasureCodingPolicy ecPolicy = stat.getErasureCodingPolicy();
    final int numParityBlocks = ecPolicy.getNumParityUnits();
    cellSize = ecPolicy.getCellSize();
    numDataBlocks = ecPolicy.getNumDataUnits();
    numAllBlocks = numDataBlocks + numParityBlocks;
    this.favoredNodes = favoredNodes;

    encoder = CodecUtil.createRSRawEncoder(dfsClient.getConfiguration(),
        numDataBlocks, numParityBlocks);

    try {
      cellBuffers = new CellBuffers(numParityBlocks);
    } catch (InterruptedException ie) {
      throw DFSUtilClient.toInterruptedIOException(
          "Failed to create cell buffers", ie);
    }

    streamers = new ArrayList<>(numAllBlocks);
    for (short i = 0; i < numAllBlocks; i++) {
      StripedDataStreamer streamer = new StripedDataStreamer(stat,
          dfsClient, src, progress, checksum, cachingStrategy, byteArrayManager,
          favoredNodes, i, coordinator, null);
      streamers.add(streamer);
    }
    currentPackets = new DFSPacket[streamers.size()];
    setCurrentStreamer(0);
    this.stat = stat;
    this.progress = progress;
    this.checksum = checksum;
  }

  StripedDataStreamer getStripedDataStreamer(int i) {
    return streamers.get(i);
  }

  int getCurrentIndex() {
    return getCurrentStreamer().getIndex();
  }

  private synchronized StripedDataStreamer getCurrentStreamer() {
    return (StripedDataStreamer) streamer;
  }

  private synchronized StripedDataStreamer setCurrentStreamer(int newIdx) {
    // backup currentPacket for current streamer
    if (streamer != null) {
      int oldIdx = streamers.indexOf(getCurrentStreamer());
      if (oldIdx >= 0) {
        currentPackets[oldIdx] = currentPacket;
      }
    }

    streamer = getStripedDataStreamer(newIdx);
    currentPacket = currentPackets[newIdx];
    adjustChunkBoundary();

    return getCurrentStreamer();
  }

  /**
   * Encode the buffers, i.e. compute parities.
   *
   * @param buffers data buffers + parity buffers
   */
  private static void encode(RawErasureEncoder encoder, int numData,
      ByteBuffer[] buffers) {
    final ByteBuffer[] dataBuffers = new ByteBuffer[numData];
    final ByteBuffer[] parityBuffers = new ByteBuffer[buffers.length - numData];
    System.arraycopy(buffers, 0, dataBuffers, 0, dataBuffers.length);
    System.arraycopy(buffers, numData, parityBuffers, 0, parityBuffers.length);

    encoder.encode(dataBuffers, parityBuffers);
  }

  private void handleStreamerFailure(String err, Exception e)
      throws IOException {
    LOG.warn("Failed: " + err + ", " + this, e);
    getCurrentStreamer().getErrorState().setInternalError();
    getCurrentStreamer().close(true);
    currentPacket = null;
  }

  private DatanodeInfo[] getExcludedNodes() {
    List<DatanodeInfo> excluded = new ArrayList<>();
    for (StripedDataStreamer streamer : streamers) {
      for (DatanodeInfo e : streamer.getExcludedNodes()) {
        if (e != null) {
          excluded.add(e);
        }
      }
    }
    return excluded.toArray(new DatanodeInfo[excluded.size()]);
  }

  /**
   * For striped files, we limit the lifespan of a {@link StripedDataStreamer}
   * to a single block. Each streamer thread stops after finishing its
   * assigned block ({@link StripedDataStreamer#getLocatedBlock()}).
   *
   * So instead of the streamer, the OutputStream takes charge of allocating
   * new blocks, when:
   *  1) starting to write file ({@link #currentBlockGroup} is still null)
   *  2) just finished the previous block
   * @throws IOException
   * @throws InterruptedException
   */
  private void allocateNewBlock() throws IOException,InterruptedException {
    // If allocating the next block (instead of first block in file), wait til
    // the current block is finished.
    if (currentBlockGroup != null) {
      Preconditions.checkNotNull(coordinator);
      while (!coordinator.getAllStreamersEndedBlock()) {
        Thread.sleep(100);
      }

      // Cleanup old coordinator and streamer threads
      Preconditions.checkState(coordinator.isCoordinatorClosed());
      coordinator.join();

      for (StripedDataStreamer oldStreamer : streamers) {
        if (oldStreamer != null) {
          oldStreamer.close(false);
          oldStreamer.join();
          oldStreamer.closeSocket();
        }
      }
    }

    DatanodeInfo[] excludedNodes = getExcludedNodes();
    LOG.debug("Excluding DataNodes when allocating new block: "
        + Arrays.asList(excludedNodes));

    LOG.debug("Allocating new block group. The previous block group: "
        + currentBlockGroup);
    final LocatedBlock lb = addBlock(excludedNodes, dfsClient, src,
        currentBlockGroup, fileId, favoredNodes);
    assert lb.isStriped();
    LocatedStripedBlock blockGroup = (LocatedStripedBlock)lb;
    if (blockGroup.getLocations().length < numDataBlocks) {
      throw new IOException("Failed to get " + numDataBlocks
          + " nodes from namenode: blockGroupSize= " + numAllBlocks
          + ", blocks.length= " + blockGroup.getLocations().length);
    }
    // assign the new block to the current block group
    currentBlockGroup = blockGroup.getBlock();
    currentPacket = null;

    final LocatedBlock[] blocks = StripedBlockUtil.parseStripedBlockGroup(
        blockGroup, cellSize, numDataBlocks,
        numAllBlocks - numDataBlocks);

    // Replace the coordinator and all streamers
    coordinator = new BlockMetadataCoordinator(this, numAllBlocks, blockGroup,
        blocks);
    coordinator.start();
    for (short i = 0; i < numAllBlocks; i++) {
      currentPackets[i] = null;

      StripedDataStreamer streamer = new StripedDataStreamer(stat,
          dfsClient, src, progress, checksum, cachingStrategy, byteArrayManager,
          favoredNodes, i, coordinator, blocks[i]);
      streamers.set(i, streamer);
      if (blocks[i] != null) {
        streamer.start();
      } else {
        streamer.close(false);
      }
    }
    setCurrentStreamer(0);
  }

  private boolean shouldEndBlockGroup() {
    return currentBlockGroup != null &&
        currentBlockGroup.getNumBytes() == blockSize * numDataBlocks;
  }

  @Override
  protected synchronized void writeChunk(byte[] bytes, int offset, int len,
      byte[] checksum, int ckoff, int cklen) throws IOException {
    final int index = getCurrentIndex();
    final int pos = cellBuffers.addTo(index, bytes, offset, len);
    final boolean cellFull = pos == cellSize;

    if (currentBlockGroup == null || shouldEndBlockGroup()) {
      // the incoming data should belong to a new block. Allocate a new block.
      try {
        allocateNewBlock();
      } catch (InterruptedException e) {
        LOG.info("Interrupted during allocateNewBlock.");
      }
    }

    currentBlockGroup.setNumBytes(currentBlockGroup.getNumBytes() + len);
    // note: the current streamer can be refreshed after allocating a new block
    final StripedDataStreamer current = getCurrentStreamer();
    if (current.isHealthy()) {
      try {
        super.writeChunk(bytes, offset, len, checksum, ckoff, cklen);
      } catch(Exception e) {
        handleStreamerFailure("offset=" + offset + ", length=" + len, e);
      }
    } else{
      LOG.debug("Streamer " + current + " has an error.");
      // If coordinator still thinks the streamer is healthy, send an update
      if (coordinator.getStreamerStatus(current.getIndex()) ==
          BlockMetadataCoordinator.StreamerStatus.RUNNING){
        coordinator.addEvent(
            new BlockMetadataCoordinator.DNFailureEvent(current.getIndex()));
      }
      currentPacket = null;
    }

    // Two extra steps are needed when a striping cell is full:
    // 1. Forward the current index pointer
    // 2. Generate parity packets if a full stripe of data cells are present
    if (cellFull) {
      int next = index + 1;
      //When all data cells in a stripe are ready, we need to encode
      //them and generate some parity cells. These cells will be
      //converted to packets and put to their DataStreamer's queue.
      if (next == numDataBlocks) {
        cellBuffers.flipDataBuffers();
        writeParityCells();
        next = 0;

        // if this is the end of the block group, end each internal block
        if (shouldEndBlockGroup()) {
          flushAllInternals();
          while (!coordinator.processedAllEvents()) {
            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
            }
          }
          for (int i = 0; i < numAllBlocks; i++) {
            final StripedDataStreamer s = setCurrentStreamer(i);
            if (s.isHealthy()) {
              try {
                endBlock();
              } catch (IOException ignored) {}
            }
          }
        }
      }
      setCurrentStreamer(next);
    }
  }

  @Override
  synchronized void enqueueCurrentPacketFull() throws IOException {
    LOG.debug("enqueue full {}, src={}, bytesCurBlock={}, blockSize={},"
            + " appendChunk={}, {}", currentPacket, src, getStreamer()
            .getBytesCurBlock(), blockSize, getStreamer().getAppendChunk(),
        getStreamer());
    enqueueCurrentPacket();
    adjustChunkBoundary();
    // no need to end block here
  }

  private int stripeDataSize() {
    return numDataBlocks * cellSize;
  }

  @Override
  public void hflush() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void hsync() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected synchronized void start() {
    for (StripedDataStreamer streamer : streamers) {
      if (streamer.getLocatedBlock() != null) {
        streamer.start();
      }
    }
  }

  @Override
  synchronized void abort() throws IOException {
    if (isClosed()) {
      return;
    }
    for (StripedDataStreamer streamer : streamers) {
      streamer.getLastException().set(new IOException("Lease timeout of "
          + (dfsClient.getConf().getHdfsTimeout()/1000) +
          " seconds expired."));
    }
    closeThreads(true);
    dfsClient.endFileLease(fileId);
  }

  @Override
  boolean isClosed() {
    if (closed) {
      return true;
    }
    for(StripedDataStreamer s : streamers) {
      if (!s.streamerClosed()) {
        return false;
      }
    }
    return true;
  }

  @Override
  protected void closeThreads(boolean force) throws IOException {
    final MultipleIOException.Builder b = new MultipleIOException.Builder();
    try {
      for (StripedDataStreamer streamer : streamers) {
        try {
          streamer.close(force);
          streamer.join();
          streamer.closeSocket();
        } catch (Exception e) {
          try {
            handleStreamerFailure("force=" + force, e);
          } catch (IOException ioe) {
            b.add(ioe);
          }
        } finally {
          streamer.setSocketToNull();
        }
      }
    } finally {
      setClosed();
    }
    final IOException ioe = b.build();
    if (ioe != null) {
      throw ioe;
    }
  }

  private boolean generateParityCellsForLastStripe() {
    final long currentBlockGroupBytes = currentBlockGroup == null ?
        0 : currentBlockGroup.getNumBytes();
    final long lastStripeSize = currentBlockGroupBytes % stripeDataSize();
    if (lastStripeSize == 0) {
      return false;
    }

    final long parityCellSize = lastStripeSize < cellSize?
        lastStripeSize : cellSize;
    final ByteBuffer[] buffers = cellBuffers.getBuffers();

    for (int i = 0; i < numAllBlocks; i++) {
      // Pad zero bytes to make all cells exactly the size of parityCellSize
      // If internal block is smaller than parity block, pad zero bytes.
      // Also pad zero bytes to all parity cells
      final int position = buffers[i].position();
      assert position <= parityCellSize : "If an internal block is smaller" +
          " than parity block, then its last cell should be small than last" +
          " parity cell";
      for (int j = 0; j < parityCellSize - position; j++) {
        buffers[i].put((byte) 0);
      }
      buffers[i].flip();
    }
    return true;
  }

  void writeParityCells() throws IOException {
    final ByteBuffer[] buffers = cellBuffers.getBuffers();
    // Skips encoding and writing parity cells if there are no healthy parity
    // data streamers
    if (!checkAnyParityStreamerIsHealthy()) {
      return;
    }
    //encode the data cells
    encode(encoder, numDataBlocks, buffers);
    for (int i = numDataBlocks; i < numAllBlocks; i++) {
      writeParity(i, buffers[i], cellBuffers.getChecksumArray(i));
    }
    cellBuffers.clear();
  }

  private boolean checkAnyParityStreamerIsHealthy() {
    for (int i = numDataBlocks; i < numAllBlocks; i++) {
      if (streamers.get(i).isHealthy()) {
        return true;
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Skips encoding and writing parity cells as there are "
          + "no healthy parity data streamers: " + streamers);
    }
    return false;
  }

  void writeParity(int index, ByteBuffer buffer, byte[] checksumBuf)
      throws IOException {
    final StripedDataStreamer current = setCurrentStreamer(index);
    final int len = buffer.limit();

    final long oldBytes = current.getBytesCurBlock();
    if (current.isHealthy()) {
      try {
        DataChecksum sum = getDataChecksum();
        sum.calculateChunkedSums(buffer.array(), 0, len, checksumBuf, 0);
        for (int i = 0; i < len; i += sum.getBytesPerChecksum()) {
          int chunkLen = Math.min(sum.getBytesPerChecksum(), len - i);
          int ckOffset = i / sum.getBytesPerChecksum() * getChecksumSize();
          super.writeChunk(buffer.array(), i, chunkLen, checksumBuf, ckOffset,
              getChecksumSize());
        }
      } catch(Exception e) {
        handleStreamerFailure("oldBytes=" + oldBytes + ", len=" + len, e);
      }
    }
  }

  @Override
  void setClosed() {
    super.setClosed();
    for (int i = 0; i < numAllBlocks; i++) {
      getStripedDataStreamer(i).release();
    }
    cellBuffers.release();
  }

  @Override
  protected synchronized void closeImpl() throws IOException {
    if (isClosed()) {
      final MultipleIOException.Builder b = new MultipleIOException.Builder();
      for(int i = 0; i < streamers.size(); i++) {
        final StripedDataStreamer si = getStripedDataStreamer(i);
        try {
          si.getLastException().check(true);
        } catch (IOException e) {
          b.add(e);
        }
      }
      final IOException ioe = b.build();
      if (ioe != null) {
        throw ioe;
      }
      return;
    }

    try {
      // flush from all upper layers
      flushBuffer();
      // if the last stripe is incomplete, generate and write parity cells
      if (generateParityCellsForLastStripe()) {
        writeParityCells();
      }
      enqueueAllCurrentPackets();

      // flush all the data packets
      flushAllInternals();
      while (!coordinator.processedAllEvents()) {
        sleep(100, "Waiting for coordinator to process all events.");
      }

      for (int i = 0; i < numAllBlocks; i++) {
        final StripedDataStreamer s = setCurrentStreamer(i);
        if (s.isHealthy()) {
          try {
            if (s.getBytesCurBlock() > 0) {
              setCurrentPacketToEmpty();
            }
            if (s.getBlock().getNumBytes() == 0) {
              LOG.info("closeImpl ending block for streamer #" + s.getIndex());
              coordinator.addEvent(new BlockMetadataCoordinator.
                  EndBlockEvent(s.getIndex()));
            }
            // flush the last "close" packet to Datanode
            flushInternal();
          } catch(Exception e) {
            // TODO for both close and endBlock, we currently do not handle
            // failures when sending the last packet. We actually do not need to
            // bump GS for this kind of failure. Thus counting the total number
            // of failures may be good enough.
          }
        }
      }

      while (coordinator != null && !coordinator.getAllStreamersEndedBlock()) {
        try{
          Thread.sleep(100);
        } catch (InterruptedException e){
        }
      }

      closeThreads(false);
      try (TraceScope ignored =
               dfsClient.getTracer().newScope("completeFile")) {
        completeFile(currentBlockGroup);
      }
      dfsClient.endFileLease(fileId);
    } catch (ClosedChannelException ignored) {
    } finally {
      setClosed();
    }
  }

  private void enqueueAllCurrentPackets() throws IOException {
    int idx = streamers.indexOf(getCurrentStreamer());
    for(int i = 0; i < streamers.size(); i++) {
      final StripedDataStreamer si = setCurrentStreamer(i);
      if (si.isHealthy() && currentPacket != null) {
        try {
          enqueueCurrentPacket();
        } catch (IOException e) {
          handleStreamerFailure("enqueueAllCurrentPackets, i=" + i, e);
        }
      }
    }
    setCurrentStreamer(idx);
  }

  void flushAllInternals() throws IOException {
    int current = getCurrentIndex();

    for (int i = 0; i < numAllBlocks; i++) {
      final StripedDataStreamer s = setCurrentStreamer(i);
      if (s.isHealthy()) {
        try {
          // flush all data to Datanode
          flushInternal();
        } catch(Exception e) {
          handleStreamerFailure("flushInternal " + s, e);
        }
      }
    }
    setCurrentStreamer(current);
  }

  static void sleep(long ms, String op) throws InterruptedIOException {
    try {
      Thread.sleep(ms);
    } catch(InterruptedException ie) {
      throw DFSUtilClient.toInterruptedIOException(
          "Sleep interrupted during " + op, ie);
    }
  }

  @Override
  ExtendedBlock getBlock() {
    return currentBlockGroup;
  }

  public List<StripedDataStreamer> getStreamers() {
    return streamers;
  }
}
