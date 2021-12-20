/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.realtime.appenderator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.data.input.Committer;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.appenderator.SegmentWithState.SegmentState;
import org.apache.druid.segment.realtime.plumber.CoordinatorBasedSegmentHandoffNotifier;
import org.apache.druid.segment.realtime.plumber.SegmentHandoffNotifier;
import org.apache.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * This class is specialized for streaming ingestion. In streaming ingestion, the segment lifecycle is like:
 *
 * <pre>
 * APPENDING -> APPEND_FINISHED -> PUBLISHED
 * </pre>
 *
 * <ul>
 * <li>APPENDING: Segment is available for appending.</li>
 * <li>APPEND_FINISHED: Segment cannot be updated (data cannot be added anymore) and is waiting for being published.</li>
 * <li>PUBLISHED: Segment is pushed to deep storage, its metadata is published to metastore, and finally the segment is
 * dropped from local storage</li>
 * </ul>
 */
public class StreamAppenderatorDriver extends BaseAppenderatorDriver
{
  private static final Logger log = new Logger(StreamAppenderatorDriver.class);

  private final SegmentHandoffNotifier handoffNotifier;
  private final FireDepartmentMetrics metrics;
  private final ObjectMapper objectMapper;

  /**
   * Create a driver.
   *
   * @param appenderator           appenderator
   * @param segmentAllocator       segment allocator
   * @param handoffNotifierFactory handoff notifier factory
   * @param usedSegmentChecker     used segment checker
   * @param objectMapper           object mapper, used for serde of commit metadata
   * @param metrics                Firedepartment metrics
   */
  public StreamAppenderatorDriver(
      Appenderator appenderator,
      SegmentAllocator segmentAllocator,
      SegmentHandoffNotifierFactory handoffNotifierFactory,
      UsedSegmentChecker usedSegmentChecker,
      DataSegmentKiller dataSegmentKiller,
      ObjectMapper objectMapper,
      FireDepartmentMetrics metrics
  )
  {
    super(appenderator, segmentAllocator, usedSegmentChecker, dataSegmentKiller);

    this.handoffNotifier = Preconditions.checkNotNull(handoffNotifierFactory, "handoffNotifierFactory")
                                        .createSegmentHandoffNotifier(appenderator.getDataSource());
    this.metrics = Preconditions.checkNotNull(metrics, "metrics");
    this.objectMapper = Preconditions.checkNotNull(objectMapper, "objectMapper");
  }

  @Override
  @Nullable
  public Object startJob(AppenderatorDriverSegmentLockHelper lockHelper)
  {
    /**
     * 开启定时任务不断调用{@link CoordinatorBasedSegmentHandoffNotifier#checkForSegmentHandoffs()}
     * 每次定时检查实时数据流所属的segment是否有已经完毕成功发布到深度存储的，
     * 如果已经发布到深度存储，则将该segment从appenderator中删除，
     * （appenderator主要负责查询实时数据（这部分数据还未分发入深度存储））
     */
    handoffNotifier.start();

    /**
     * 获取appenderator的元数据
     */
    final AppenderatorDriverMetadata metadata = objectMapper.convertValue(
        appenderator.startJob(),
        AppenderatorDriverMetadata.class
    );

    if (metadata != null) {
      synchronized (segments) {
        final Map<String, String> lastSegmentIds = metadata.getLastSegmentIds();
        Preconditions.checkState(
            metadata.getSegments().keySet().equals(lastSegmentIds.keySet()),
            "Sequences for segment states and last segment IDs are not same"
        );

        final Map<String, SegmentsForSequenceBuilder> builders = new TreeMap<>();

        for (Entry<String, List<SegmentWithState>> entry : metadata.getSegments().entrySet()) {
          final String sequenceName = entry.getKey();
          final SegmentsForSequenceBuilder builder = new SegmentsForSequenceBuilder(lastSegmentIds.get(sequenceName));
          builders.put(sequenceName, builder);
          entry.getValue().forEach(builder::add);
          if (lockHelper != null) {
            for (SegmentWithState segmentWithState : entry.getValue()) {
              if (segmentWithState.getState() != SegmentState.PUSHED_AND_DROPPED
                  && !lockHelper.lock(segmentWithState.getSegmentIdentifier())) {
                throw new ISE("Failed to lock segment[%s]", segmentWithState.getSegmentIdentifier());
              }
            }
          }
        }

        builders.forEach((sequence, builder) -> segments.put(sequence, builder.build()));
      }

      return metadata.getCallerMetadata();
    } else {
      return null;
    }
  }

  public AppenderatorDriverAddResult add(
      InputRow row,
      String sequenceName,
      final Supplier<Committer> committerSupplier
  ) throws IOException
  {
    return append(row, sequenceName, committerSupplier, false, true);
  }

  /**
   * Add a row. Must not be called concurrently from multiple threads.
   *
   * @param row                      the row to add
   * @param sequenceName             sequenceName for this row's segment
   * @param committerSupplier        supplier of a committer associated with all data that has been added, including this row
   *                                 if {@param allowIncrementalPersists} is set to false then this will not be used
   * @param skipSegmentLineageCheck  if true, perform lineage validation using previousSegmentId for this sequence.
   *                                 Should be set to false if replica tasks would index events in same order
   * @param allowIncrementalPersists whether to allow persist to happen when maxRowsInMemory or intermediate persist period
   *                                 threshold is hit
   *
   * @return {@link AppenderatorDriverAddResult}
   *
   * @throws IOException if there is an I/O error while allocating or writing to a segment
   */
  // 提供存储实时流数据，使其可查询，并定期作为segment存储进深度存储中
  public AppenderatorDriverAddResult add(
      final InputRow row,
      final String sequenceName,
      final Supplier<Committer> committerSupplier,
      final boolean skipSegmentLineageCheck,
      final boolean allowIncrementalPersists
  ) throws IOException
  {
    return append(row, sequenceName, committerSupplier, skipSegmentLineageCheck, allowIncrementalPersists);
  }

  /**
   * Move a set of identifiers out from "active", making way for newer segments.
   * This method is to support KafkaIndexTask's legacy mode and will be removed in the future.
   * See KakfaIndexTask.runLegacy().
   */
  public void moveSegmentOut(final String sequenceName, final List<SegmentIdWithShardSpec> identifiers)
  {
    synchronized (segments) {
      final SegmentsForSequence activeSegmentsForSequence = segments.get(sequenceName);
      if (activeSegmentsForSequence == null) {
        throw new ISE("Asked to remove segments for sequenceName[%s], which doesn't exist", sequenceName);
      }

      for (final SegmentIdWithShardSpec identifier : identifiers) {
        log.info("Moving segment[%s] out of active list.", identifier);
        final long key = identifier.getInterval().getStartMillis();
        final SegmentsOfInterval segmentsOfInterval = activeSegmentsForSequence.get(key);
        if (segmentsOfInterval == null ||
            segmentsOfInterval.getAppendingSegment() == null ||
            !segmentsOfInterval.getAppendingSegment().getSegmentIdentifier().equals(identifier)) {
          throw new ISE("Asked to remove segment[%s], which doesn't exist", identifier);
        }
        segmentsOfInterval.finishAppendingToCurrentActiveSegment(SegmentWithState::finishAppending);
      }
    }
  }

  /**
   * Persist all data indexed through this driver so far. Blocks until complete.
   * <p>
   * Should be called after all data has been added through {@link #add(InputRow, String, Supplier, boolean, boolean)}.
   *
   * @param committer committer representing all data that has been added so far
   *
   * @return commitMetadata persisted
   */
  // 持久化报错driver迄今为止收到的所有流数据（kafka task的模式下，应该在一个task消费完所有数据并都通过add方法加入driver后再调用此方法）
  public Object persist(final Committer committer) throws InterruptedException
  {
    try {
      log.debug("Persisting pending data.");
      final long start = System.currentTimeMillis();
      final Object commitMetadata = appenderator.persistAll(wrapCommitter(committer)).get();
      log.debug("Persisted pending data in %,dms.", System.currentTimeMillis() - start);
      return commitMetadata;
    }
    catch (InterruptedException e) {
      throw e;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Persist all data indexed through this driver so far. Returns a future of persisted commitMetadata.
   * <p>
   * Should be called after all data has been added through {@link #add(InputRow, String, Supplier, boolean, boolean)}.
   *
   * @param committer committer representing all data that has been added so far
   *
   * @return future containing commitMetadata persisted
   */
  public ListenableFuture<Object> persistAsync(final Committer committer)
  {
    return appenderator.persistAll(wrapCommitter(committer));
  }

  /**
   * Execute a task in background to publish all segments corresponding to the given sequence names.  The task
   * internally pushes the segments to the deep storage first, and then publishes the metadata to the metadata storage.
   *
   * @param publisher     segment publisher
   * @param committer     committer
   * @param sequenceNames a collection of sequence names to be published
   *
   * @return a {@link ListenableFuture} for the submitted task which removes published {@code sequenceNames} from
   * {@code activeSegments} and {@code publishPendingSegments}
   */
  /**
   * 将segment发布到深度存储，然后储存segment的元数据信息
   */
  public ListenableFuture<SegmentsAndCommitMetadata> publish(
      final TransactionalSegmentPublisher publisher,
      final Committer committer,
      final Collection<String> sequenceNames
  )
  {
    final List<SegmentIdWithShardSpec> theSegments = getSegmentIdsWithShardSpecs(sequenceNames);

    final ListenableFuture<SegmentsAndCommitMetadata> publishFuture = Futures.transform(
        // useUniquePath=true prevents inconsistencies in segment data when task failures or replicas leads to a second
        // version of a segment with the same identifier containing different data; see DataSegmentPusher.push() docs
        pushInBackground(wrapCommitter(committer), theSegments, true),
        (AsyncFunction<SegmentsAndCommitMetadata, SegmentsAndCommitMetadata>) sam -> publishInBackground(
            null,
            sam,
            publisher,
            java.util.function.Function.identity()
        )
    );
    return Futures.transform(
        publishFuture,
        (Function<? super SegmentsAndCommitMetadata, ? extends SegmentsAndCommitMetadata>) sam -> {
          synchronized (segments) {
            sequenceNames.forEach(segments::remove);
          }
          return sam;
        }
    );
  }

  /**
   * Register the segments in the given {@link SegmentsAndCommitMetadata} to be handed off and execute a background task which
   * waits until the hand off completes.
   *
   * @param segmentsAndCommitMetadata the result segments and metadata of
   *                            {@link #publish(TransactionalSegmentPublisher, Committer, Collection)}
   *
   * @return null if the input segmentsAndMetadata is null. Otherwise, a {@link ListenableFuture} for the submitted task
   * which returns {@link SegmentsAndCommitMetadata} containing the segments successfully handed off and the metadata
   * of the caller of {@link AppenderatorDriverMetadata}
   */
  public ListenableFuture<SegmentsAndCommitMetadata> registerHandoff(SegmentsAndCommitMetadata segmentsAndCommitMetadata)
  {
    if (segmentsAndCommitMetadata == null) {
      return Futures.immediateFuture(null);

    } else {
      final List<SegmentIdWithShardSpec> waitingSegmentIdList = segmentsAndCommitMetadata.getSegments().stream()
                                                                                         .map(
                                                                                       SegmentIdWithShardSpec::fromDataSegment)
                                                                                         .collect(Collectors.toList());
      final Object metadata = Preconditions.checkNotNull(segmentsAndCommitMetadata.getCommitMetadata(), "commitMetadata");

      if (waitingSegmentIdList.isEmpty()) {
        return Futures.immediateFuture(
            new SegmentsAndCommitMetadata(
                segmentsAndCommitMetadata.getSegments(),
                ((AppenderatorDriverMetadata) metadata).getCallerMetadata()
            )
        );
      }

      log.debug("Register handoff of segments: [%s]", waitingSegmentIdList);

      final SettableFuture<SegmentsAndCommitMetadata> resultFuture = SettableFuture.create();
      final AtomicInteger numRemainingHandoffSegments = new AtomicInteger(waitingSegmentIdList.size());

      for (final SegmentIdWithShardSpec segmentIdentifier : waitingSegmentIdList) {
        // 每个segment在发布完后执行的回调逻辑
        handoffNotifier.registerSegmentHandoffCallback(
            // segment信息
            new SegmentDescriptor(
                segmentIdentifier.getInterval(),
                segmentIdentifier.getVersion(),
                segmentIdentifier.getShardSpec().getPartitionNum()
            ),
            // 执行以下回调逻辑的线程池
            Execs.directExecutor(),
            // segment成功“分发”后的回调程序
            () -> {
              log.debug("Segment[%s] successfully handed off, dropping.", segmentIdentifier);
              metrics.incrementHandOffCount();

              // 从appenderator中删掉该segment的信息？（appenderator主要负责查询实时数据（这部分数据还未分发入深度存储））
              final ListenableFuture<?> dropFuture = appenderator.drop(segmentIdentifier);
              Futures.addCallback(
                  dropFuture,
                  new FutureCallback<Object>()
                  {
                    @Override
                    public void onSuccess(Object result)
                    {
                      if (numRemainingHandoffSegments.decrementAndGet() == 0) {
                        List<DataSegment> segments = segmentsAndCommitMetadata.getSegments();
                        log.debug("Successfully handed off [%d] segments.", segments.size());
                        resultFuture.set(
                            new SegmentsAndCommitMetadata(
                                segments,
                                ((AppenderatorDriverMetadata) metadata).getCallerMetadata()
                            )
                        );
                      }
                    }

                    @Override
                    public void onFailure(Throwable e)
                    {
                      log.warn(e, "Failed to drop segment[%s]?!", segmentIdentifier);
                      numRemainingHandoffSegments.decrementAndGet();
                      resultFuture.setException(e);
                    }
                  }
              );
            }
        );
      }

      return resultFuture;
    }
  }

  public ListenableFuture<SegmentsAndCommitMetadata> publishAndRegisterHandoff(
      final TransactionalSegmentPublisher publisher,
      final Committer committer,
      final Collection<String> sequenceNames
  )
  {
    return Futures.transform(
        publish(publisher, committer, sequenceNames),
        (AsyncFunction<SegmentsAndCommitMetadata, SegmentsAndCommitMetadata>) this::registerHandoff
    );
  }

  @Override
  public void close()
  {
    super.close();
    handoffNotifier.close();
  }

  private static class SegmentsForSequenceBuilder
  {
    // segmentId -> (appendingSegment, appendFinishedSegments)
    private final NavigableMap<SegmentIdWithShardSpec, Pair<SegmentWithState, List<SegmentWithState>>> intervalToSegments =
        new TreeMap<>(Comparator.comparing(SegmentIdWithShardSpec::getInterval, Comparators.intervalsByStartThenEnd()));
    private final String lastSegmentId;

    SegmentsForSequenceBuilder(String lastSegmentId)
    {
      this.lastSegmentId = lastSegmentId;
    }

    void add(SegmentWithState segmentWithState)
    {
      final SegmentIdWithShardSpec identifier = segmentWithState.getSegmentIdentifier();
      final Pair<SegmentWithState, List<SegmentWithState>> pair = intervalToSegments.get(identifier);
      final List<SegmentWithState> appendFinishedSegments = pair == null || pair.rhs == null ?
                                                            new ArrayList<>() :
                                                            pair.rhs;

      // always keep APPENDING segments for an interval start millis in the front
      if (segmentWithState.getState() == SegmentState.APPENDING) {
        if (pair != null && pair.lhs != null) {
          throw new ISE(
              "appendingSegment[%s] existed before adding an appendingSegment[%s]",
              pair.lhs,
              segmentWithState
          );
        }

        intervalToSegments.put(identifier, Pair.of(segmentWithState, appendFinishedSegments));
      } else {
        final SegmentWithState appendingSegment = pair == null ? null : pair.lhs;
        appendFinishedSegments.add(segmentWithState);
        intervalToSegments.put(identifier, Pair.of(appendingSegment, appendFinishedSegments));
      }
    }

    SegmentsForSequence build()
    {
      final NavigableMap<Long, SegmentsOfInterval> map = new TreeMap<>();
      for (Entry<SegmentIdWithShardSpec, Pair<SegmentWithState, List<SegmentWithState>>> entry :
          intervalToSegments.entrySet()) {
        map.put(
            entry.getKey().getInterval().getStartMillis(),
            new SegmentsOfInterval(entry.getKey().getInterval(), entry.getValue().lhs, entry.getValue().rhs)
        );
      }
      return new SegmentsForSequence(map, lastSegmentId);
    }
  }
}
