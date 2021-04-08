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

package org.apache.druid.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulator;
import org.apache.druid.client.selector.QueryableDruidServer;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.guice.annotations.Client;
import org.apache.druid.guice.annotations.Merging;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.guice.http.DruidHttpClientConfig;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.LazySequence;
import org.apache.druid.java.util.common.guava.ParallelMergeCombiningSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.BySegmentResultValueClass;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.Queries;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.Result;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.aggregation.MetricManipulatorFns;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.context.ResponseContext.Key;
import org.apache.druid.query.filter.DimFilterUtils;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.server.QueryResource;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.Partitions;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.TimelineLookup;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * This is the class on the Broker that is responsible for making native Druid queries to a cluster of data servers.
 *
 * The main user of this class is {@link org.apache.druid.server.ClientQuerySegmentWalker}. In tests, its behavior
 * is partially mimicked by TestClusterQuerySegmentWalker.
 */
public class CachingClusteredClient implements QuerySegmentWalker
{
  private static final EmittingLogger log = new EmittingLogger(CachingClusteredClient.class);
  private final QueryToolChestWarehouse warehouse;
  private final TimelineServerView serverView;
  private final Cache cache;
  private final ObjectMapper objectMapper;
  private final CachePopulator cachePopulator;
  private final CacheConfig cacheConfig;
  private final DruidHttpClientConfig httpClientConfig;
  private final DruidProcessingConfig processingConfig;
  private final ForkJoinPool pool;
  private final QueryScheduler scheduler;

  @Inject
  public CachingClusteredClient(
      QueryToolChestWarehouse warehouse,
      TimelineServerView serverView,
      Cache cache,
      @Smile ObjectMapper objectMapper,
      CachePopulator cachePopulator,
      CacheConfig cacheConfig,
      @Client DruidHttpClientConfig httpClientConfig,
      DruidProcessingConfig processingConfig,
      @Merging ForkJoinPool pool,
      QueryScheduler scheduler
  )
  {
    this.warehouse = warehouse;
    this.serverView = serverView;
    this.cache = cache;
    this.objectMapper = objectMapper;
    this.cachePopulator = cachePopulator;
    this.cacheConfig = cacheConfig;
    this.httpClientConfig = httpClientConfig;
    this.processingConfig = processingConfig;
    this.pool = pool;
    this.scheduler = scheduler;

    if (cacheConfig.isQueryCacheable(Query.GROUP_BY) && (cacheConfig.isUseCache() || cacheConfig.isPopulateCache())) {
      log.warn(
          "Even though groupBy caching is enabled in your configuration, v2 groupBys will not be cached on the broker. "
          + "Consider enabling caching on your data nodes if it is not already enabled."
      );
    }

    serverView.registerSegmentCallback(
        Execs.singleThreaded("CCClient-ServerView-CB-%d"),
        new ServerView.BaseSegmentCallback()
        {
          @Override
          public ServerView.CallbackAction segmentRemoved(DruidServerMetadata server, DataSegment segment)
          {
            CachingClusteredClient.this.cache.close(segment.getId().toString());
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(final Query<T> query, final Iterable<Interval> intervals)
  {
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(final QueryPlus<T> queryPlus, final ResponseContext responseContext)
      {
        return CachingClusteredClient.this.run(queryPlus, responseContext, timeline -> timeline, false);
      }
    };
  }

  /**
   * Run a query. The timelineConverter will be given the "master" timeline and can be used to return a different
   * timeline, if desired. This is used by getQueryRunnerForSegments.
   */
  private <T> Sequence<T> run(
      final QueryPlus<T> queryPlus,
      final ResponseContext responseContext,
      final UnaryOperator<TimelineLookup<String, ServerSelector>> timelineConverter,
      final boolean specificSegments // false
  )
  {
    log.info("!!!select：进入CachingClusteredClient匿名queryrunner run()方法");

    /**
     * 1、根据查询时间区间，从整个集群中找到对应的segment以及其所在的主机信息。
     * 2、创建一个匿名函数，
     * 该函数作用是“根据segment所在主机的信息，使用netty发送http请求，并将所有获取的结果合并返回”
     * （发送http请求，异步获取请求结果：{@link org.apache.druid.client.DirectDruidClient#run(QueryPlus, ResponseContext)}）
     * 返回的类型就是Sequence<T>
     */
    final ClusterQueryResult<T> result = new SpecificQueryRunnable<>(queryPlus, responseContext)
        .run(timelineConverter, specificSegments);

    log.info("!!!select：进入CachingClusteredClient匿名queryrunner run()方法end");
    initializeNumRemainingResponsesInResponseContext(queryPlus.getQuery(), responseContext, result.numQueryServers);
    return result.sequence;
  }

  private static <T> void initializeNumRemainingResponsesInResponseContext(
      final Query<T> query,
      final ResponseContext responseContext,
      final int numQueryServers
  )
  {
    responseContext.add(
        Key.REMAINING_RESPONSES_FROM_QUERY_SERVERS,
        new NonnullPair<>(query.getMostSpecificId(), numQueryServers)
    );
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(final Query<T> query, final Iterable<SegmentDescriptor> specs)
  {
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(final QueryPlus<T> queryPlus, final ResponseContext responseContext)
      {
        return CachingClusteredClient.this.run(
            queryPlus,
            responseContext,
            timeline -> {
              final VersionedIntervalTimeline<String, ServerSelector> timeline2 =
                  new VersionedIntervalTimeline<>(Ordering.natural());
              for (SegmentDescriptor spec : specs) {
                final PartitionHolder<ServerSelector> entry = timeline.findEntry(spec.getInterval(), spec.getVersion());
                if (entry != null) {
                  final PartitionChunk<ServerSelector> chunk = entry.getChunk(spec.getPartitionNumber());
                  if (chunk != null) {
                    timeline2.add(spec.getInterval(), spec.getVersion(), chunk);
                  }
                }
              }
              return timeline2;
            },
            true
        );
      }
    };
  }

  private static class ClusterQueryResult<T>
  {
    private final Sequence<T> sequence;
    private final int numQueryServers;

    private ClusterQueryResult(Sequence<T> sequence, int numQueryServers)
    {
      this.sequence = sequence;
      this.numQueryServers = numQueryServers;
    }
  }

  /**
   * This class essentially encapsulates the major part of the logic of {@link CachingClusteredClient}. It's state and
   * methods couldn't belong to {@link CachingClusteredClient} itself, because they depend on the specific query object
   * being run, but {@link QuerySegmentWalker} API is designed so that implementations should be able to accept
   * arbitrary queries.
   */
  private class SpecificQueryRunnable<T>
  {
    private final ResponseContext responseContext;
    private QueryPlus<T> queryPlus;
    private Query<T> query;
    private final QueryToolChest<T, Query<T>> toolChest;
    @Nullable
    private final CacheStrategy<T, Object, Query<T>> strategy;
    private final boolean useCache;
    private final boolean populateCache;
    private final boolean isBySegment;
    private final int uncoveredIntervalsLimit;
    private final Map<String, Cache.NamedKey> cachePopulatorKeyMap = new HashMap<>();
    private final DataSourceAnalysis dataSourceAnalysis;
    private final List<Interval> intervals;

    SpecificQueryRunnable(final QueryPlus<T> queryPlus, final ResponseContext responseContext)
    {
      this.queryPlus = queryPlus;
      this.responseContext = responseContext;
      this.query = queryPlus.getQuery();
      this.toolChest = warehouse.getToolChest(query);
      this.strategy = toolChest.getCacheStrategy(query);

      this.useCache = CacheUtil.isUseSegmentCache(query, strategy, cacheConfig, CacheUtil.ServerType.BROKER);
      this.populateCache = CacheUtil.isPopulateSegmentCache(query, strategy, cacheConfig, CacheUtil.ServerType.BROKER);
      this.isBySegment = QueryContexts.isBySegment(query);
      // Note that enabling this leads to putting uncovered intervals information in the response headers
      // and might blow up in some cases https://github.com/apache/druid/issues/2108
      this.uncoveredIntervalsLimit = QueryContexts.getUncoveredIntervalsLimit(query);
      /**
       * 简单来说此方法是对一些特殊数据源类型参数的处理。
       * 如果是QueryDataSource类型，则获取出其中的子查询subQuery对象及对应数据源，放入DataSourceAnalysis对象封装。
       * 如果是JoinDataSource类型，则将原始数据源拆分成基础数据源和被连接数据源，分别放入DataSourceAnalysis对象封装。
       *
       * 如果不是以上两者，则不作任何处理，直接将数据源放入DataSourceAnalysis对象封装。
       *
       * 一般json查询都是TableDataSource
       */
      this.dataSourceAnalysis = DataSourceAnalysis.forDataSource(query.getDataSource());
      // For nested queries, we need to look at the intervals of the inner most query.
      // 获取此次查询的时间间隔参数intervals
      this.intervals = dataSourceAnalysis.getBaseQuerySegmentSpec()
                                         .map(QuerySegmentSpec::getIntervals)
                                         .orElseGet(() -> query.getIntervals());
    }

    private ImmutableMap<String, Object> makeDownstreamQueryContext()
    {
      final ImmutableMap.Builder<String, Object> contextBuilder = new ImmutableMap.Builder<>();

      final int priority = QueryContexts.getPriority(query);
      contextBuilder.put(QueryContexts.PRIORITY_KEY, priority);
      final String lane = QueryContexts.getLane(query);
      if (lane != null) {
        contextBuilder.put(QueryContexts.LANE_KEY, lane);
      }

      if (populateCache) {
        // prevent down-stream nodes from caching results as well if we are populating the cache
        contextBuilder.put(CacheConfig.POPULATE_CACHE, false);
        contextBuilder.put("bySegment", true);
      }
      return contextBuilder.build();
    }

    /**
     * Builds a query distribution and merge plan.
     *
     * This method returns an empty sequence if the query datasource is unknown or there is matching result-level cache.
     * Otherwise, it creates a sequence merging sequences from the regular broker cache and remote servers. If parallel
     * merge is enabled, it can merge and *combine* the underlying sequences in parallel.
     *
     * @return a pair of a sequence merging results from remote query servers and the number of remote servers
     *         participating in query processing.
     */
    /**
     * 1、根据查询的时间区间，从整个druid集群中找到对应的segment（{@link}）
     */
    ClusterQueryResult<T> run(
        final UnaryOperator<TimelineLookup<String, ServerSelector>> timelineConverter,
        final boolean specificSegments //false
    )
    {
      /**
       * serverView为启动时注入，此处实际调用的是{@link BrokerServerView#getTimeline(DataSourceAnalysis)}
       * 此处返回的是一个timeline数据（如果存在的话），
       * 暂不知道意义
       */
      final Optional<? extends TimelineLookup<String, ServerSelector>> maybeTimeline = serverView.getTimeline(
          dataSourceAnalysis
      );
      if (maybeTimeline.isPresent()){
        log.info("!!!select：存在maybeTimeline，类型为："+maybeTimeline.get().getClass());
      }else {
        log.info("!!!select：maybeTimeline为空");
      }

      // 如果没有maybeTimeline，则直接返回空查询结果
      if (!maybeTimeline.isPresent()) {
        return new ClusterQueryResult<>(Sequences.empty(), 0);
      }

      /**
       * timelineConverter这个参数是一个“函数参数”，
       * 相当于传进来一个函数，有参数有响应，
       * 此处的apply就是调用该函数，
       * 而这个函数当时定义的是“没有任何逻辑，传什么就回显什么”，
       *
       * 所以此处的timelineConverter.apply()相当于什么都没做，
       * 直接将maybeTimeline.get()赋值给了timeline
       */
      final TimelineLookup<String, ServerSelector> timeline = timelineConverter.apply(maybeTimeline.get());

      if (uncoveredIntervalsLimit > 0) {
        computeUncoveredIntervals(timeline);
      }

      /**
       * 该方法主要就是“根据此次查询的时间区间，从整个druid集群中找所有对应的segment”。
       *
       * 一个SegmentServerSelector只是一个包装类，其核心是内部存的两个对象：
       * 1、{@link ServerSelector}
       * ServerSelector与segment是一对一关系，
       * ServerSelector代表了segment所在服务器的“查找工具”，
       * 其通过ServerSelector的pick方法，可以“从历史节点和实时节点中根据segment找到对应的{@link QueryableDruidServer}”
       * QueryableDruidServer就是segment所在的服务器对象
       *
       * 2、{@link SegmentDescriptor}
       * 一个segment要由“DataSource、时间间隔、version、分区号”这四种属性来唯一标识一个segment，
       * SegmentDescriptor如其名，就是一个segment的描述，只有保存信息的作用，存了以下三种信息，
       * itvl：segment对应的时间区间
       * ver：版本（相同属性的segment存在“升级”的情况，比如按照不同的索引方式重新生成某一时间段的segment文件，就会更新一个版本）
       * part：分区号，某一个时间段的数据量太大，则会拆成多个segment文件存储，所有属性都一样，只有分区号递增。
       *
       * 而一个时间区间内的数据，很可能分散在多个segment分片上，
       * 所以返回的自然是个set集合。
       *
       * 后续可用每个SegmentServerSelector对象，找到集群中对应的server服务，然后对其发起查询请求。
       */
      final Set<SegmentServerSelector> segmentServers = computeSegmentsToQuery(timeline, specificSegments);

      /**
       * 其中populateCache和useCache必须有一个才能执行
       * useCache参数用于查询是否启用缓存，
       * populateCache参数用于查询结果是否更新缓存，
       * 这两个都是“段级”缓存的参数
       *
       * 如果开启段级缓存，则通过strategy对象查询出缓存key
       */
      @Nullable
      final byte[] queryCacheKey = computeQueryCacheKey();

      if (query.getContext().get(QueryResource.HEADER_IF_NONE_MATCH) != null) {
        @Nullable
        final String prevEtag = (String) query.getContext().get(QueryResource.HEADER_IF_NONE_MATCH);
        @Nullable
        final String currentEtag = computeCurrentEtag(segmentServers, queryCacheKey);
        if (currentEtag != null && currentEtag.equals(prevEtag)) {
          return new ClusterQueryResult<>(Sequences.empty(), 0);
        }
      }

      /**
       * 根据缓存key获取缓存结果
       */
      final List<Pair<Interval, byte[]>> alreadyCachedResults =
          pruneSegmentsWithCachedResults(queryCacheKey, segmentServers);

      /**
       * 从此次查询的上下文context中获取lane参数，
       * 并设置到此次查询对象中
       */
      query = scheduler.prioritizeAndLaneQuery(queryPlus, segmentServers);
      //重新将query放入queryPlus
      queryPlus = queryPlus.withQuery(query);

      /**
       * 迭代每个SegmentServerSelector，调用其pick方法获得所有segment所在主机对象
       * {@link ServerSelector#pick()}
       *
       * 然后将这些主机对象{@link DruidServer}去重后返回
       * key就是{@link DruidServer}主机对象，
       * value是一个SegmentDescriptor，表示该主机上包含的此次查询所需的所有Segment
       */
      final SortedMap<DruidServer, List<SegmentDescriptor>> segmentsByServer = groupSegmentsByServer(segmentServers);

      /**
       * 懒加载获取此次查询的结果，
       *
       * 此处的懒加载是指此处只是设置好了“获取查询结果的匿名函数”，
       * 还未调用，
       * 具体调用逻辑在下面的scheduler.run中
       */
      log.info("!!!：设置mergedResultSequence");
      LazySequence<T> mergedResultSequence = new LazySequence<>(
              /**
               * 创建匿名函数{@link Supplier#get()}
               *
               * 该函数根据segment所在主机的信息，使用netty发送http请求，
               * 获取所有主机返回的查询结果后，交由merge函数汇总
               */
              () -> {
        /**
         * 由sequencesByInterval的容量大小可看出，
         * 其中装的是“每一个主机的查询结果”，
         *
         * 或者说从某台主机上查询出多个segment的结果，这算是一个Sequence对象结果，
         * 这些Sequence结果会被放入sequencesByInterval集合中
         */
        List<Sequence<T>> sequencesByInterval = new ArrayList<>(alreadyCachedResults.size() + segmentsByServer.size());

        addSequencesFromCache(sequencesByInterval, alreadyCachedResults);

        /**
         * 根据segment所在主机的信息，使用netty发送http请求，
         * 然后将结果传入中sequencesByInterval
         * 其中泛型T为{@link org.apache.druid.query.groupby.ResultRow}
         */
        addSequencesFromServer(sequencesByInterval, segmentsByServer);

        //汇总
        return merge(sequencesByInterval);
      });

      /**
       * 将查询对象交由scheduler调度器来查询
       *
       * scheduler：注入进来，
       *
       * query：与原本对象相比，此方法中将上下文中的lane参数解析并设置了进去（lane默认为null）
       *
       * mergedResultSequence：
       * 其中包含了上面设置的匿名函数，可根据segment主机信息发送http请求获取查询结果，然后汇总后返回查询结果
       *
       * segmentsByServer.size()：
       * segmentsByServer是个map，
       * key是之前解析的DruidServer（druid主机对象），
       * value是SegmentDescriptor标识该主机上所有查询所需的segment
       */
      log.info("!!!：调度器准备开始查询");
      return new ClusterQueryResult<>(scheduler.run(query, mergedResultSequence), segmentsByServer.size());
    }

    private Sequence<T> merge(List<Sequence<T>> sequencesByInterval)
    {
      BinaryOperator<T> mergeFn = toolChest.createMergeFn(query);
      if (processingConfig.useParallelMergePool() && QueryContexts.getEnableParallelMerges(query) && mergeFn != null) {
        return new ParallelMergeCombiningSequence<>(
            pool,
            sequencesByInterval,
            query.getResultOrdering(),
            mergeFn,
            QueryContexts.hasTimeout(query),
            QueryContexts.getTimeout(query),
            QueryContexts.getPriority(query),
            QueryContexts.getParallelMergeParallelism(query, processingConfig.getMergePoolDefaultMaxQueryParallelism()),
            QueryContexts.getParallelMergeInitialYieldRows(query, processingConfig.getMergePoolTaskInitialYieldRows()),
            QueryContexts.getParallelMergeSmallBatchRows(query, processingConfig.getMergePoolSmallBatchRows()),
            processingConfig.getMergePoolTargetTaskRunTimeMillis(),
            reportMetrics -> {
              QueryMetrics<?> queryMetrics = queryPlus.getQueryMetrics();
              if (queryMetrics != null) {
                queryMetrics.parallelMergeParallelism(reportMetrics.getParallelism());
                queryMetrics.reportParallelMergeParallelism(reportMetrics.getParallelism());
                queryMetrics.reportParallelMergeInputSequences(reportMetrics.getInputSequences());
                queryMetrics.reportParallelMergeInputRows(reportMetrics.getInputRows());
                queryMetrics.reportParallelMergeOutputRows(reportMetrics.getOutputRows());
                queryMetrics.reportParallelMergeTaskCount(reportMetrics.getTaskCount());
                queryMetrics.reportParallelMergeTotalCpuTime(reportMetrics.getTotalCpuTime());
              }
            }
        );
      } else {
        return Sequences
            .simple(sequencesByInterval)
            .flatMerge(seq -> seq, query.getResultOrdering());
      }
    }

    private Set<SegmentServerSelector> computeSegmentsToQuery(
        TimelineLookup<String, ServerSelector> timeline,
        boolean specificSegments //false
    )
    {
      /**
       * specificSegmentswei false，
       * timeline为之前{@link BrokerServerView#getTimeline(DataSourceAnalysis)}获取
       * lookupFn = {@link VersionedIntervalTimeline#lookup(Interval, Partitions)}
       *
       * 存在一个map集合，其中包含了各种Interval时间区间，和对应的{@link VersionedIntervalTimeline.TimelineEntry}实体。
       * （暂不知道该map是怎么形成的，估计是数据摄入时形成），
       *
       * 然后从map中查找出于此次查询的时间区间重叠的键值对，
       * 将其对应TimelineEntry返回
       *
       * 这个lookupFn就是TimelineEntry的包装类
       */
      final java.util.function.Function<Interval, List<TimelineObjectHolder<String, ServerSelector>>> lookupFn
          = specificSegments ? timeline::lookupWithIncompletePartitions : timeline::lookup;


      final List<TimelineObjectHolder<String, ServerSelector>> serversLookup = toolChest.filterSegments(
          query,
          intervals.stream().flatMap(i -> lookupFn.apply(i).stream()).collect(Collectors.toList())
      );

      final Set<SegmentServerSelector> segments = new LinkedHashSet<>();
      final Map<String, Optional<RangeSet<String>>> dimensionRangeCache = new HashMap<>();

      // Filter unneeded chunks based on partition dimension
      /**
       * 此块应该是根据查询参数中的Filter条件进行一些过滤
       *
       * holder中装了当前时间区间对应的{@link VersionedIntervalTimeline.TimelineEntry}实体中的数据
       */
      for (TimelineObjectHolder<String, ServerSelector> holder : serversLookup) {
        final Set<PartitionChunk<ServerSelector>> filteredChunks;
        if (QueryContexts.isSecondaryPartitionPruningEnabled(query)) {
          filteredChunks = DimFilterUtils.filterShards(
              query.getFilter(),
              holder.getObject(),
              partitionChunk -> partitionChunk.getObject().getSegment().getShardSpec(),
              dimensionRangeCache
          );
        } else {
          filteredChunks = Sets.newHashSet(holder.getObject());
        }


        for (PartitionChunk<ServerSelector> chunk : filteredChunks) {
          /**
           * ServerSelector与segment是一对一关系，
           *
           * 象征了segment所在的服务器的“查找工具”，
           * 其通过pick方法，可以“从历史节点和实时节点中根据segment找到对应的{@link QueryableDruidServer}”
           *
           * QueryableDruidServer就是segment所在的服务器对象
           */
          ServerSelector server = chunk.getObject();

          /**
           * 一个segment要由“DataSource、时间间隔、version、分区号”这四种属性来唯一标识一个segment，
           * SegmentDescriptor如其名，就是一个segment的描述，只有保存信息的作用，存了以下三种信息，
           * itvl：segment对应的时间区间
           * ver：版本（相同属性的segment存在“升级”的情况，比如按照不同的索引方式重新生成某一时间段的segment文件，就会更新一个版本）
           * part：分区号，某一个时间段的数据量太大，则会拆成多个segment文件存储，所有属性都一样，只有分区号递增。
           */
          final SegmentDescriptor segment = new SegmentDescriptor(
              holder.getInterval(),
              holder.getVersion(),
              chunk.getChunkNumber()
          );
          /**
           * 所谓“SegmentServerSelector”只是将“ServerSelector”和“SegmentDescriptor”存起来的一个包装类，
           * 新建时只是将这两个对象放进去。
           *
           * 关于这两个类的介绍在上面
           */
          segments.add(new SegmentServerSelector(server, segment));
        }
      }
      return segments;
    }

    private void computeUncoveredIntervals(TimelineLookup<String, ServerSelector> timeline)
    {
      final List<Interval> uncoveredIntervals = new ArrayList<>(uncoveredIntervalsLimit);
      boolean uncoveredIntervalsOverflowed = false;

      for (Interval interval : intervals) {
        log.info("!!!select：computeUncoveredIntervals处理查询时间参数，查询时间："+interval.toString());
        Iterable<TimelineObjectHolder<String, ServerSelector>> lookup = timeline.lookup(interval);
        long startMillis = interval.getStartMillis();
        long endMillis = interval.getEndMillis();
        for (TimelineObjectHolder<String, ServerSelector> holder : lookup) {
          Interval holderInterval = holder.getInterval();
          long intervalStart = holderInterval.getStartMillis();
          if (!uncoveredIntervalsOverflowed && startMillis != intervalStart) {
            if (uncoveredIntervalsLimit > uncoveredIntervals.size()) {
              uncoveredIntervals.add(Intervals.utc(startMillis, intervalStart));
            } else {
              uncoveredIntervalsOverflowed = true;
            }
          }
          startMillis = holderInterval.getEndMillis();
        }

        if (!uncoveredIntervalsOverflowed && startMillis < endMillis) {
          if (uncoveredIntervalsLimit > uncoveredIntervals.size()) {
            uncoveredIntervals.add(Intervals.utc(startMillis, endMillis));
          } else {
            uncoveredIntervalsOverflowed = true;
          }
        }
      }

      if (!uncoveredIntervals.isEmpty()) {
        // Record in the response context the interval for which NO segment is present.
        // Which is not necessarily an indication that the data doesn't exist or is
        // incomplete. The data could exist and just not be loaded yet.  In either
        // case, though, this query will not include any data from the identified intervals.
        responseContext.add(ResponseContext.Key.UNCOVERED_INTERVALS, uncoveredIntervals);
        responseContext.add(ResponseContext.Key.UNCOVERED_INTERVALS_OVERFLOWED, uncoveredIntervalsOverflowed);
      }
    }

    @Nullable
    private byte[] computeQueryCacheKey()
    {
      log.info("!!!select：computeQueryCacheKey，缓存工具对象strategy："+strategy.getClass());
      if ((populateCache || useCache) // implies strategy != null
          && !isBySegment) { // explicit bySegment queries are never cached
        assert strategy != null;
        byte[] cacheKey = strategy.computeCacheKey(query);
        log.info("!!!select：computeQueryCacheKey，以开启缓存，key为："+new String(cacheKey));
        return cacheKey;
      } else {
        log.info("!!!select：computeQueryCacheKey，未开启缓存");
        return null;
      }
    }

    @Nullable
    private String computeCurrentEtag(final Set<SegmentServerSelector> segments, @Nullable byte[] queryCacheKey)
    {
      Hasher hasher = Hashing.sha1().newHasher();
      boolean hasOnlyHistoricalSegments = true;
      for (SegmentServerSelector p : segments) {
        if (!p.getServer().pick().getServer().isSegmentReplicationTarget()) {
          hasOnlyHistoricalSegments = false;
          break;
        }
        hasher.putString(p.getServer().getSegment().getId().toString(), StandardCharsets.UTF_8);
        // it is important to add the "query interval" as part ETag calculation
        // to have result level cache work correctly for queries with different
        // intervals covering the same set of segments
        hasher.putString(p.rhs.getInterval().toString(), StandardCharsets.UTF_8);
      }

      if (hasOnlyHistoricalSegments) {
        hasher.putBytes(queryCacheKey == null ? strategy.computeCacheKey(query) : queryCacheKey);

        String currEtag = StringUtils.encodeBase64String(hasher.hash().asBytes());
        responseContext.put(ResponseContext.Key.ETAG, currEtag);
        return currEtag;
      } else {
        return null;
      }
    }

    private List<Pair<Interval, byte[]>> pruneSegmentsWithCachedResults(
        final byte[] queryCacheKey,
        final Set<SegmentServerSelector> segments
    )
    {
      if (queryCacheKey == null) {
        return Collections.emptyList();
      }
      final List<Pair<Interval, byte[]>> alreadyCachedResults = new ArrayList<>();
      Map<SegmentServerSelector, Cache.NamedKey> perSegmentCacheKeys = computePerSegmentCacheKeys(
          segments,
          queryCacheKey
      );
      // Pull cached segments from cache and remove from set of segments to query
      final Map<Cache.NamedKey, byte[]> cachedValues = computeCachedValues(perSegmentCacheKeys);

      perSegmentCacheKeys.forEach((segment, segmentCacheKey) -> {
        final Interval segmentQueryInterval = segment.getSegmentDescriptor().getInterval();

        final byte[] cachedValue = cachedValues.get(segmentCacheKey);
        if (cachedValue != null) {
          // remove cached segment from set of segments to query
          segments.remove(segment);
          alreadyCachedResults.add(Pair.of(segmentQueryInterval, cachedValue));
        } else if (populateCache) {
          // otherwise, if populating cache, add segment to list of segments to cache
          final SegmentId segmentId = segment.getServer().getSegment().getId();
          addCachePopulatorKey(segmentCacheKey, segmentId, segmentQueryInterval);
        }
      });
      return alreadyCachedResults;
    }

    private Map<SegmentServerSelector, Cache.NamedKey> computePerSegmentCacheKeys(
        Set<SegmentServerSelector> segments,
        byte[] queryCacheKey
    )
    {
      // cacheKeys map must preserve segment ordering, in order for shards to always be combined in the same order
      Map<SegmentServerSelector, Cache.NamedKey> cacheKeys = Maps.newLinkedHashMap();
      for (SegmentServerSelector segmentServer : segments) {
        final Cache.NamedKey segmentCacheKey = CacheUtil.computeSegmentCacheKey(
            segmentServer.getServer().getSegment().getId().toString(),
            segmentServer.getSegmentDescriptor(),
            queryCacheKey
        );
        cacheKeys.put(segmentServer, segmentCacheKey);
      }
      return cacheKeys;
    }

    private Map<Cache.NamedKey, byte[]> computeCachedValues(Map<SegmentServerSelector, Cache.NamedKey> cacheKeys)
    {
      if (useCache) {
        return cache.getBulk(Iterables.limit(cacheKeys.values(), cacheConfig.getCacheBulkMergeLimit()));
      } else {
        return ImmutableMap.of();
      }
    }

    private void addCachePopulatorKey(
        Cache.NamedKey segmentCacheKey,
        SegmentId segmentId,
        Interval segmentQueryInterval
    )
    {
      cachePopulatorKeyMap.put(StringUtils.format("%s_%s", segmentId, segmentQueryInterval), segmentCacheKey);
    }

    @Nullable
    private Cache.NamedKey getCachePopulatorKey(String segmentId, Interval segmentInterval)
    {
      return cachePopulatorKeyMap.get(StringUtils.format("%s_%s", segmentId, segmentInterval));
    }

    private SortedMap<DruidServer, List<SegmentDescriptor>> groupSegmentsByServer(Set<SegmentServerSelector> segments)
    {
      final SortedMap<DruidServer, List<SegmentDescriptor>> serverSegments = new TreeMap<>();
      for (SegmentServerSelector segmentServer : segments) {
        /**
         * 调用pick方法获取segment所在主机对象
         */
        final QueryableDruidServer queryableDruidServer = segmentServer.getServer().pick();

        if (queryableDruidServer == null) {
          log.makeAlert(
              "No servers found for SegmentDescriptor[%s] for DataSource[%s]?! How can this be?!",
              segmentServer.getSegmentDescriptor(),
              query.getDataSource()
          ).emit();
        } else {
          final DruidServer server = queryableDruidServer.getServer();
          /**
           * 从SegmentServerSelector中获取到每一个segment所在的DruidServer，
           * 然后放入返回集合中
           * （顺便还去重了，即多个相同的DruidServer会去重）
           */
          serverSegments.computeIfAbsent(server, s -> new ArrayList<>()).add(segmentServer.getSegmentDescriptor());
        }
      }
      return serverSegments;
    }

    private void addSequencesFromCache(
        final List<Sequence<T>> listOfSequences,
        final List<Pair<Interval, byte[]>> cachedResults
    )
    {
      if (strategy == null) {
        return;
      }

      final Function<Object, T> pullFromCacheFunction = strategy.pullFromSegmentLevelCache();
      final TypeReference<Object> cacheObjectClazz = strategy.getCacheObjectClazz();
      for (Pair<Interval, byte[]> cachedResultPair : cachedResults) {
        final byte[] cachedResult = cachedResultPair.rhs;
        Sequence<Object> cachedSequence = new BaseSequence<>(
            new BaseSequence.IteratorMaker<Object, Iterator<Object>>()
            {
              @Override
              public Iterator<Object> make()
              {
                try {
                  if (cachedResult.length == 0) {
                    return Collections.emptyIterator();
                  }

                  return objectMapper.readValues(
                      objectMapper.getFactory().createParser(cachedResult),
                      cacheObjectClazz
                  );
                }
                catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }

              @Override
              public void cleanup(Iterator<Object> iterFromMake)
              {
              }
            }
        );
        listOfSequences.add(Sequences.map(cachedSequence, pullFromCacheFunction));
      }
    }

    /**
     * Create sequences that reads from remote query servers (historicals and tasks). Note that the broker will
     * hold an HTTP connection per server after this method is called.
     * listOfSequences：list，其中装了每一个主机的查询结果
     * segmentsByServer：主机对象，以及对应的该主机上查询所需的segment列表
     */
    private void addSequencesFromServer(
        final List<Sequence<T>> listOfSequences,
        final SortedMap<DruidServer, List<SegmentDescriptor>> segmentsByServer
    )
    {
      /**
       * 此处迭代每一个主机对象
       *
       */
      segmentsByServer.forEach((server, segmentsOfServer) -> {
        /**
         * （1）serverRunner
         * 可以看到serverRunner和server是一个一一对应关系，
         * 根据serverRunner中接口方法可得知，
         * 该对象与某种类型的查询无关，只与服务主机有关，
         * 其内部的方法为“接收query查询对象，返回查询结果”，可以理解为QueryRunner中包含了主机信息，所以可以用来查询数据。
         *
         * 此处serverRunner类型为{@link org.apache.druid.client.DirectDruidClient}
         */
        final QueryRunner serverRunner = serverView.getQueryRunner(server);
        log.info("!!!：为生成预查询结果，serverRunner类型:"+serverRunner.getClass());

        if (serverRunner == null) {
          log.error("Server[%s] doesn't have a query runner", server.getName());
          return;
        }

        // Divide user-provided maxQueuedBytes by the number of servers, and limit each server to that much.
        final long maxQueuedBytes = QueryContexts.getMaxQueuedBytes(query, httpClientConfig.getMaxQueuedBytes());
        final long maxQueuedBytesPerServer = maxQueuedBytes / segmentsByServer.size();
        final Sequence<T> serverResults;

        /**
         * 默认为false，为请求上下文中配置
         * 将其设置为true将返回与它们来自的数据段关联的结果
         * 多用于调试
         */
        if (isBySegment) {
          log.info("!!!：进入getBySegmentServerResults");
          serverResults = getBySegmentServerResults(serverRunner, segmentsOfServer, maxQueuedBytesPerServer);

        /**
         * 判断是否将查询结果存于缓存，也是在上下文中配置，默认为true
         */
        } else if (!server.isSegmentReplicationTarget() || !populateCache) {
          log.info("!!!：进入getSimpleServerResults...");
          /**
           * 这三个方法的核心查询逻辑是一样的，
           * 以直接查询不缓存为例
           *
           * 都是调用serverRunner.run()方法，
           * 其中传参为“Queries.withSpecificSegments(queryPlus.getQuery(), segmentsOfServer)”一个新的query对象
           *
           * serverRunner：包含了被查询的druid主机上的信息，通过此对象才能找到主机并查询。
           * segmentsOfServer：在这台主机上需要查的所有segment
           * maxQueuedBytesPerServer：
           *
           * serverResults就是http请求后的异步结果包装类
           *
           * Sequence<T>中的泛型为{@link org.apache.druid.query.groupby.ResultRow}
           */
          serverResults = getSimpleServerResults(serverRunner, segmentsOfServer, maxQueuedBytesPerServer);
        } else {
          log.info("!!!：进入getAndCacheServerResults");
          serverResults = getAndCacheServerResults(serverRunner, segmentsOfServer, maxQueuedBytesPerServer);
        }
        listOfSequences.add(serverResults);
      });
    }

    @SuppressWarnings("unchecked")
    private Sequence<T> getBySegmentServerResults(
        final QueryRunner serverRunner,
        final List<SegmentDescriptor> segmentsOfServer,
        long maxQueuedBytesPerServer
    )
    {
      Sequence<Result<BySegmentResultValueClass<T>>> resultsBySegments = serverRunner
          .run(
              queryPlus.withQuery(
                  Queries.withSpecificSegments(queryPlus.getQuery(), segmentsOfServer)
              ).withMaxQueuedBytes(maxQueuedBytesPerServer),
              responseContext
          );
      // bySegment results need to be de-serialized, see DirectDruidClient.run()
      return (Sequence<T>) resultsBySegments
          .map(result -> result.map(
              resultsOfSegment -> resultsOfSegment.mapResults(
                  toolChest.makePreComputeManipulatorFn(query, MetricManipulatorFns.deserializing())::apply
              )
          ));
    }

    @SuppressWarnings("unchecked")
    private Sequence<T> getSimpleServerResults(
        final QueryRunner serverRunner,
        final List<SegmentDescriptor> segmentsOfServer,
        long maxQueuedBytesPerServer
    )
    {
      /**
       * 相当于将此server上的所有待查segment列表放进query查询对象
       *
       * 然后将查询对象交由serverRunner（{@link org.apache.druid.client.DirectDruidClient}）
       */
      return serverRunner.run(
          queryPlus.withQuery(
              Queries.withSpecificSegments(queryPlus.getQuery(), segmentsOfServer)
          ).withMaxQueuedBytes(maxQueuedBytesPerServer),
          responseContext
      );
    }

    private Sequence<T> getAndCacheServerResults(
        final QueryRunner serverRunner,
        final List<SegmentDescriptor> segmentsOfServer,
        long maxQueuedBytesPerServer
    )
    {
      @SuppressWarnings("unchecked")
      final Query<T> downstreamQuery = query.withOverriddenContext(makeDownstreamQueryContext());
      final Sequence<Result<BySegmentResultValueClass<T>>> resultsBySegments = serverRunner.run(
          queryPlus
              .withQuery(
                  Queries.withSpecificSegments(
                      downstreamQuery,
                      segmentsOfServer
                  )
              )
              .withMaxQueuedBytes(maxQueuedBytesPerServer),
          responseContext
      );
      final Function<T, Object> cacheFn = strategy.prepareForSegmentLevelCache();

      return resultsBySegments
          .map(result -> {
            final BySegmentResultValueClass<T> resultsOfSegment = result.getValue();
            final Cache.NamedKey cachePopulatorKey =
                getCachePopulatorKey(resultsOfSegment.getSegmentId(), resultsOfSegment.getInterval());
            Sequence<T> res = Sequences.simple(resultsOfSegment.getResults());
            if (cachePopulatorKey != null) {
              res = cachePopulator.wrap(res, cacheFn::apply, cache, cachePopulatorKey);
            }
            return res.map(
                toolChest.makePreComputeManipulatorFn(downstreamQuery, MetricManipulatorFns.deserializing())::apply
            );
          })
          .flatMerge(seq -> seq, query.getResultOrdering());
    }
  }
}
