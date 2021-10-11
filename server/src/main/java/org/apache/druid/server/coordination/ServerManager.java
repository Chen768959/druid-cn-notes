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

package org.apache.druid.server.coordination;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.druid.client.CachingQueryRunner;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulator;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.guice.annotations.Processing;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.FunctionalIterable;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.BySegmentQueryRunner;
import org.apache.druid.query.CPUTimeMetricQueryRunner;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.MetricsEmittingQueryRunner;
import org.apache.druid.query.NoopQueryRunner;
import org.apache.druid.query.PerSegmentOptimizingQueryRunner;
import org.apache.druid.query.PerSegmentQueryOptimizationContext;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.ReferenceCountingSegmentQueryRunner;
import org.apache.druid.query.ReportTimelineMissingSegmentQueryRunner;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.query.spec.SpecificSegmentQueryRunner;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.join.Joinables;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.SetAndVerifyContextQueryRunner;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Query handler for Historical processes (see CliHistorical).
 *
 * In tests, this class's behavior is partially mimicked by TestClusterQuerySegmentWalker.
 */
public class ServerManager implements QuerySegmentWalker
{
  private static final EmittingLogger log = new EmittingLogger(DiscoveryDruidNode.class);
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final ServiceEmitter emitter;
  private final ExecutorService exec;
  private final CachePopulator cachePopulator;
  private final Cache cache;
  private final ObjectMapper objectMapper;
  private final CacheConfig cacheConfig;
  // 管理这当前his节点负责加载的segment信息
  private final SegmentManager segmentManager;
  private final JoinableFactory joinableFactory;
  private final ServerConfig serverConfig;

  @Inject
  public ServerManager(
      QueryRunnerFactoryConglomerate conglomerate,
      ServiceEmitter emitter,
      @Processing ExecutorService exec,
      CachePopulator cachePopulator,
      @Smile ObjectMapper objectMapper,
      Cache cache,
      CacheConfig cacheConfig,
      SegmentManager segmentManager,
      JoinableFactory joinableFactory,
      ServerConfig serverConfig
  )
  {
    this.conglomerate = conglomerate;
    this.emitter = emitter;

    this.exec = exec;
    this.cachePopulator = cachePopulator;
    this.cache = cache;
    this.objectMapper = objectMapper;

    this.cacheConfig = cacheConfig;
    this.segmentManager = segmentManager;
    this.joinableFactory = joinableFactory;
    this.serverConfig = serverConfig;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(query.getDataSource());
    final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline;
    final Optional<VersionedIntervalTimeline<String, ReferenceCountingSegment>> maybeTimeline =
        segmentManager.getTimeline(analysis);

    if (maybeTimeline.isPresent()) {
      timeline = maybeTimeline.get();
    } else {
      // Even though we didn't find a timeline for the query datasource, we simply returns a noopQueryRunner
      // instead of reporting missing intervals because the query intervals are a filter rather than something
      // we must find.
      return new NoopQueryRunner<>();
    }

    FunctionalIterable<SegmentDescriptor> segmentDescriptors = FunctionalIterable
        .create(intervals)
        .transformCat(timeline::lookup)
        .transformCat(
            holder -> {
              if (holder == null) {
                return null;
              }

              return FunctionalIterable
                  .create(holder.getObject())
                  .transform(
                      partitionChunk ->
                          new SegmentDescriptor(
                              holder.getInterval(),
                              holder.getVersion(),
                              partitionChunk.getChunkNumber()
                          )
                  );
            }
        );

    return getQueryRunnerForSegments(query, segmentDescriptors);
  }

  /**
   * 为每一次查询请求创建queryrunner
   * 主要是由{@link org.apache.druid.query.QueryPlus#run(QuerySegmentWalker, ResponseContext)}调用
   *
   *
   *
   * @param query 此次查询请求对象
   * @param specs
   * 也是由请求对象中得来，包含此次查询涉及的所有segment的信息，包含三点“时间区间、版本、分片号”，
   * 相当于根据时间区间+版本，可以在当前节点通过segmentManager，找到涉及哪些segment，
   * 再查看这些segment数据对象有哪些分片，把每一个分片信息都传入进来，
   *
   * 在以下方法逻辑中，“会为每一个分片对象，都创建一条queryrunner查询链”，然后再从线程池分配一个线程进行查询处理，
   * 最后交由当前主线程进行合并
   * @return org.apache.druid.query.QueryRunner<T>
   */
  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    // 根据query的class类型来获取QueryRunnerFactory对象
    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    if (factory == null) {
      final QueryUnsupportedException e = new QueryUnsupportedException(
          StringUtils.format("Unknown query type, [%s]", query.getClass())
      );
      log.makeAlert(e, "Error while executing a query[%s]", query.getId())
         .addData("dataSource", query.getDataSource())
         .emit();
      throw e;
    }

    final QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();
    // 可以简单的把DataSourceAnalysis理解为dataSource数据源的封装
    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(query.getDataSource());
    final AtomicLong cpuTimeAccumulator = new AtomicLong(0L);
    // 此次查询的数据源“时间轴上的所有segment对象”
    final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline;
    /**
     * ！！！
     * 该segmentManager在启动时用于存放“以时间轴为单位，被成功加载的segment对象”。
     * 此处返回数据源的“时间轴上的所有segment对象”
     *
     * maybeTimeline：用来储存该数据源的各时间轴以及其上的各个ReferenceCountingSegment对象(segement真正的本体)
     */
    final Optional<VersionedIntervalTimeline<String, ReferenceCountingSegment>> maybeTimeline =
        segmentManager.getTimeline(analysis);

    // Make sure this query type can handle the subquery, if present.
    if (analysis.isQuery() && !toolChest.canPerformSubquery(((QueryDataSource) analysis.getDataSource()).getQuery())) {
      throw new ISE("Cannot handle subquery: %s", analysis.getDataSource());
    }

    // 验证此数据源是否存在时间轴及其上的segment
    if (maybeTimeline.isPresent()) {
      timeline = maybeTimeline.get();
    } else {// 如果数据源没有对应时间轴信息，则直接认为数据源是空
      return new ReportTimelineMissingSegmentQueryRunner<>(Lists.newArrayList(specs));
    }

    // segmentMapFn maps each base Segment into a joined Segment if necessary.
    // 该函数会处理每一个入参segment，然后返回一个处理后的segment对象。
    // 但是只有涉及到有子查询时才会执行处理逻辑，否则直接返回入参segment，不做任何处理
    final Function<SegmentReference, SegmentReference> segmentMapFn = Joinables.createSegmentMapFn(
        analysis.getPreJoinableClauses(),
        joinableFactory,
        cpuTimeAccumulator,
        analysis.getBaseQuery().orElse(query)
    );

    specs.forEach(sp->{
      try {
        log.info("!!!：his节点合并runner，正在创建runner，spec："+new ObjectMapper().writeValueAsString(sp));
      } catch (JsonProcessingException e) {
        log.info("!!!：his节点合并runner，正在创建runner，spec解析失败");
      }
    });
    /**
     * 该runners，其每一个runner都是用来处理一个segment分片的查询
     */
    final FunctionalIterable<QueryRunner<T>> queryRunners = FunctionalIterable
        /**
         * 创建函数式工具类{@link FunctionalIterable}
         * 并传入“待处理segment分片列表”
         * （specs：segment信息列表）
         */
        .create(specs)
        /**
         * transformCat函数：使用入参方法处理当前待处理列表中的每一个对象，
         * 并将各个返回结果合并为一个列表返回。
         *
         * 此处为使用
         * {@link this#buildQueryRunnerForSegment(Query, SegmentDescriptor, QueryRunnerFactory, QueryToolChest, VersionedIntervalTimeline, Function, AtomicLong)}
         * 方法处理每一个segment信息，每一个处理完后都会返回一个queryrunner，
         * 最后再将所有queryrunner装进一个list并返回
         */
        .transformCat(
            descriptor -> Collections.singletonList(
                buildQueryRunnerForSegment(
                    query,
                    descriptor,
                    factory,
                    toolChest,
                    timeline,
                    segmentMapFn,
                    cpuTimeAccumulator
                )
            )
        );

    /**
     * 上一步已经获得了多个queryRunner：
     * 每一个时间片段的segment都有一个对应的QueryRunner。
     *
     * 此处的exec是线程池，用来并发执行queryRunners中的各个runner，
     * 即并发的从各segment中查询数据。
     */
    log.info("!!!：his节点合并runner，正在创建runner，factory："+factory.getClass());
    QueryRunner<T> queryRunner = factory.mergeRunners(exec, queryRunners);
    log.info("!!!：his节点合并runner，正在创建runner，toolChest："+toolChest.getClass());
    return CPUTimeMetricQueryRunner.safeBuild(
        new FinalizeResultsQueryRunner<>(
            toolChest.mergeResults(queryRunner),
            toolChest
        ),
        toolChest,
        emitter,
        cpuTimeAccumulator,
        true
    );
  }

  /**
   *
   * @param query 此次查询对象
   * @param descriptor 此次查询涉及到的segment信息“之一”
   * @param factory 根据query的class类型来获取QueryRunnerFactory对象
   * @param toolChest
   * @param timeline 此次查询的数据源“时间轴上的所有segment对象”
   * @param segmentMapFn
   * @param cpuTimeAccumulator
   */
  <T> QueryRunner<T> buildQueryRunnerForSegment(
      final Query<T> query,
      final SegmentDescriptor descriptor,
      final QueryRunnerFactory<T, Query<T>> factory,
      final QueryToolChest<T, Query<T>> toolChest,
      final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline,
      final Function<SegmentReference, SegmentReference> segmentMapFn,
      final AtomicLong cpuTimeAccumulator
  )
  {
    // 找到segment信息对应的真正“segment数据”对象
    final PartitionHolder<ReferenceCountingSegment> entry = timeline.findEntry(
        descriptor.getInterval(),
        descriptor.getVersion()
    );

    if (entry == null) {
      return new ReportTimelineMissingSegmentQueryRunner<>(descriptor);
    }

    // 获取此segment的实际分区数据
    // （如apache-druid-0.20.1/var/druid/segment-cache/test-file/2020-03-01T00:00:00.000Z_2020-04-01T00:00:00.000Z/2021-03-11T12:41:03.686Z/0）结尾的0号分区
    final PartitionChunk<ReferenceCountingSegment> chunk = entry.getChunk(descriptor.getPartitionNumber());
    if (chunk == null) {
      return new ReportTimelineMissingSegmentQueryRunner<>(descriptor);
    }

    // 返回分区上的segment数据对象
    final ReferenceCountingSegment segment = chunk.getObject();
    return buildAndDecorateQueryRunner(
        factory,
        toolChest,
        segmentMapFn.apply(segment),
        descriptor,
        cpuTimeAccumulator
    );
  }

  /**
   *
   * @param factory 根据query的class类型来获取QueryRunnerFactory对象
   * @param toolChest
   * @param segment  如果此次查询不涉及子查询，则此segment数据就是“请求对象对应分区上的segment数据对象”
   * @param segmentDescriptor segment描述信息
   * @param cpuTimeAccumulator
   */
  private <T> QueryRunner<T> buildAndDecorateQueryRunner(
      final QueryRunnerFactory<T, Query<T>> factory,
      final QueryToolChest<T, Query<T>> toolChest,
      final SegmentReference segment,
      final SegmentDescriptor segmentDescriptor,
      final AtomicLong cpuTimeAccumulator
  )
  {
    /**{@link org.apache.druid.segment.ReferenceCountingSegment}*/
//    log.info("!!!runner：创建segment对应runner，segment类型："+segment.getClass());

    // segment描述信息的包装对象，主要是要利用其lookup（查找）方法
    final SpecificSegmentSpec segmentSpec = new SpecificSegmentSpec(segmentDescriptor);
    // {@link DataSegment}已加载信息的标识
    final SegmentId segmentId = segment.getId();
    // segment时间区间
    final Interval segmentInterval = segment.getDataInterval();
    // ReferenceCountingSegment can return null for ID or interval if it's already closed.
    // Here, we check one more time if the segment is closed.
    // If the segment is closed after this line, ReferenceCountingSegmentQueryRunner will handle and do the right thing.
    if (segmentId == null || segmentInterval == null) {
      return new ReportTimelineMissingSegmentQueryRunner<>(segmentDescriptor);
    }
    String segmentIdString = segmentId.toString();

    // 此runner用于计算“内部runner.run”的耗时
    MetricsEmittingQueryRunner<T> metricsEmittingQueryRunnerInner = new MetricsEmittingQueryRunner<>(
        emitter,
        toolChest,
        /**
         * 此runner内部通过factory创建真正的runner用于查询.
         * {@link org.apache.druid.query.groupby.GroupByQueryRunnerFactory.GroupByQueryRunner}
         */
        new ReferenceCountingSegmentQueryRunner<>(factory, segment, segmentDescriptor),
        QueryMetrics::reportSegmentTime,
        queryMetrics -> queryMetrics.segment(segmentIdString)
    );

    // 此runner与缓存相关，判断是否使用缓存结果
    CachingQueryRunner<T> cachingQueryRunner = new CachingQueryRunner<>(
        segmentIdString,
        segmentDescriptor,
        objectMapper,
        cache,
        toolChest,
        metricsEmittingQueryRunnerInner,
        cachePopulator,
        cacheConfig
    );

    // 判断请求中是否设置了“bySegment”，设置了的话会对请求结果进行包装
    BySegmentQueryRunner<T> bySegmentQueryRunner = new BySegmentQueryRunner<>(
        segmentId,
        segmentInterval.getStart(),
        cachingQueryRunner
    );

    // 此runner用于计算“内部runner.run”的耗时
    MetricsEmittingQueryRunner<T> metricsEmittingQueryRunnerOuter = new MetricsEmittingQueryRunner<>(
        emitter,
        toolChest,
        bySegmentQueryRunner,
        QueryMetrics::reportSegmentAndCacheTime,
        queryMetrics -> queryMetrics.segment(segmentIdString)
    ).withWaitMeasuredFromNow();

    SpecificSegmentQueryRunner<T> specificSegmentQueryRunner = new SpecificSegmentQueryRunner<>(
        metricsEmittingQueryRunnerOuter,
        segmentSpec
    );

    PerSegmentOptimizingQueryRunner<T> perSegmentOptimizingQueryRunner = new PerSegmentOptimizingQueryRunner<>(
        specificSegmentQueryRunner,
        new PerSegmentQueryOptimizationContext(segmentDescriptor)
    );

    return new SetAndVerifyContextQueryRunner<>(
        serverConfig,
        // CPUTimeMetricQueryRunner 依然是有计时功能的runner
        CPUTimeMetricQueryRunner.safeBuild(
            perSegmentOptimizingQueryRunner,
            toolChest,
            emitter,
            cpuTimeAccumulator,
            false
        )
    );
  }
}
