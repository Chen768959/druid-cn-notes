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

package org.apache.druid.server;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import org.apache.druid.client.CachingClusteredClient;
import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.FluentQueryRunnerBuilder;
import org.apache.druid.query.GlobalTableDataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.PostProcessingOperator;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.ResultLevelCachingQueryRunner;
import org.apache.druid.query.RetryQueryRunner;
import org.apache.druid.query.RetryQueryRunnerConfig;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.server.initialization.ServerConfig;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Query handler for Broker processes (see CliBroker).
 *
 * This class is responsible for:
 *
 * 1) Running queries on the cluster using its 'clusterClient'
 * 2) Running queries locally (when all datasources are global) using its 'localClient'
 * 3) Inlining subqueries if necessary, in service of the above two goals
 */
public class ClientQuerySegmentWalker implements QuerySegmentWalker
{
  private static final Logger log = new Logger(ClientQuerySegmentWalker.class);

  private final ServiceEmitter emitter;
  private final QuerySegmentWalker clusterClient;
  private final QuerySegmentWalker localClient;
  private final QueryToolChestWarehouse warehouse;
  private final JoinableFactory joinableFactory;
  private final RetryQueryRunnerConfig retryConfig;
  private final ObjectMapper objectMapper;
  private final ServerConfig serverConfig;
  private final Cache cache;
  private final CacheConfig cacheConfig;

  public ClientQuerySegmentWalker(
      ServiceEmitter emitter,
      QuerySegmentWalker clusterClient,
      QuerySegmentWalker localClient,
      QueryToolChestWarehouse warehouse,
      JoinableFactory joinableFactory,
      RetryQueryRunnerConfig retryConfig,
      ObjectMapper objectMapper,
      ServerConfig serverConfig,
      Cache cache,
      CacheConfig cacheConfig
  )
  {
    this.emitter = emitter;
    this.clusterClient = clusterClient;
    this.localClient = localClient;
    this.warehouse = warehouse;
    this.joinableFactory = joinableFactory;
    this.retryConfig = retryConfig;
    this.objectMapper = objectMapper;
    this.serverConfig = serverConfig;
    this.cache = cache;
    this.cacheConfig = cacheConfig;
  }

  @Inject
  ClientQuerySegmentWalker(
      ServiceEmitter emitter,
      CachingClusteredClient clusterClient,
      LocalQuerySegmentWalker localClient,
      QueryToolChestWarehouse warehouse,
      JoinableFactory joinableFactory,
      RetryQueryRunnerConfig retryConfig,
      ObjectMapper objectMapper,
      ServerConfig serverConfig,
      Cache cache,
      CacheConfig cacheConfig
  )
  {
    this(
        emitter,
        (QuerySegmentWalker) clusterClient,
        (QuerySegmentWalker) localClient,
        warehouse,
        joinableFactory,
        retryConfig,
        objectMapper,
        serverConfig,
        cache,
        cacheConfig
    );
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    intervals.forEach(interval -> {
      log.info("!!!select：此次查询interval="+interval.toString());
    });

    //该类与自定义的扩展类有关
    final QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);

    // transform TableDataSource to GlobalTableDataSource when eligible
    // before further transformation to potentially inline
    /**
     * 如果datasoource类型是TableDataSource，则转型成GlobalTableDataSource
     *
     * 每个datasource是可以包含很多“子datasource”的，
     * 所以此方法将datasource以及其子source中，所有的TableDataSource类型都转化成GlobalTableDataSource
     */
    final DataSource freeTradeDataSource = globalizeIfPossible(query.getDataSource());
    // do an inlining dry run to see if any inlining is necessary, without actually running the queries.
    /**
     * 查找配置文件中的druid.server.http.maxSubqueryRows参数
     * 该值设置了join子查询结果的最大行数
     */
    final int maxSubqueryRows = QueryContexts.getMaxSubqueryRows(query, serverConfig.getMaxSubqueryRows());
    /**
     * 获取inlineDryRun数据源，只用于下面的测试，
     *
     * inlineIfNecessary方法：
     * 首先递归查找了传参datasource，以及其内的所有自datasource，
     * 如果发现是“QueryDataSource”类型，
     * 就将其作为一个“全新query进行查询（从中获取query对象）”
     * （然后就是一系列正常查询的流程了，由walker根据此query生成newQuery和baseQueryRunner，
     * 然后调用的也是basequery的run方法获取查询结果）
     * 然后将查询结果放入InlineDataSource中，并返回。
     *
     * 可以说inlineIfNecessary()方法就是将某datasource中的所有子QueryDataSource类型的查询（子查询）全部查出来，
     * 然后讲这些子查询结果装入其中，形成一个新的datasource（InlineDataSource类型）
     *
     * 注意1：
     * 最后一个boolean参数，是用来控制“是否不进行子查询”的，
     * 为true的话，则不会进行QueryDataSource类型的查询
     *
     * 注意2：
     * 倒数第二个参数“maxSubqueryRows”
     * 对应的是配置文件中的druid.server.http.maxSubqueryRows参数，也是在此处使用的，
     * 所有子查询结果会被累加计数，如果数据量超过此限制，则会报错
     */
    final DataSource inlineDryRun = inlineIfNecessary(
        freeTradeDataSource,
        toolChest,
        new AtomicInteger(),
        maxSubqueryRows,
        true
    );

    /**
     * 判断此次查询能否在本机，或其他从机上完成，
     * 如果都不能，则报错。
     *
     * 可以看出确定能否查询需要query和数据源
     *
     * query.query.withDataSource(datasource)方法会生成一个新的query对象，
     * 新对象和原对象一模一样，唯一的区别就是数据源是传参
     */
    if (!canRunQueryUsingClusterWalker(query.withDataSource(inlineDryRun))
        && !canRunQueryUsingLocalWalker(query.withDataSource(inlineDryRun))) {
      // Dry run didn't go well.
      throw new ISE("Cannot handle subquery structure for dataSource: %s", query.getDataSource());
    }

    // Now that we know the structure is workable, actually do the inlining (if necessary).
    final Query<T> newQuery = query.withDataSource(
        inlineIfNecessary(
            freeTradeDataSource,
            toolChest,
            new AtomicInteger(),
            maxSubqueryRows,
            false
        )
    );

    if (canRunQueryUsingLocalWalker(newQuery)) {
      // No need to decorate since LocalQuerySegmentWalker does its own.
      return new QuerySwappingQueryRunner<>(
          localClient.getQueryRunnerForIntervals(newQuery, intervals),
          query,
          newQuery
      );
    } else if (canRunQueryUsingClusterWalker(newQuery)) {
      // Note: clusterClient.getQueryRunnerForIntervals() can return an empty sequence if there is no segment
      // to query, but this is not correct when there's a right or full outer join going on.
      // See https://github.com/apache/druid/issues/9229 for details.
      return new QuerySwappingQueryRunner<>(
          decorateClusterRunner(newQuery, clusterClient.getQueryRunnerForIntervals(newQuery, intervals)),
          query,
          newQuery
      );
    } else {
      // We don't expect to ever get here, because the logic earlier in this method should have rejected any query
      // that can't be run with either the local or cluster walkers. If this message ever shows up it is a bug.
      throw new ISE("Inlined query could not be run");
    }
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    // Inlining isn't done for segments-based queries, but we still globalify the table datasources if possible
    final Query<T> freeTradeQuery = query.withDataSource(globalizeIfPossible(query.getDataSource()));

    if (canRunQueryUsingClusterWalker(query)) {
      return new QuerySwappingQueryRunner<>(
          decorateClusterRunner(freeTradeQuery, clusterClient.getQueryRunnerForSegments(freeTradeQuery, specs)),
          query,
          freeTradeQuery
      );
    } else {
      // We don't expect end-users to see this message, since it only happens when specific segments are requested;
      // this is not typical end-user behavior.
      throw new ISE(
          "Cannot run query on specific segments (must be table-based; outer query, if present, must be "
          + "handleable by the query toolchest natively)");
    }
  }

  /**
   * Checks if a query can be handled wholly by {@link #localClient}. Assumes that it is a
   * {@link LocalQuerySegmentWalker} or something that behaves similarly.
   */
  private <T> boolean canRunQueryUsingLocalWalker(Query<T> query)
  {
    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(query.getDataSource());
    final QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);

    // 1) Must be based on a concrete datasource that is not a table.
    // 2) Must be based on globally available data (so we have a copy here on the Broker).
    // 3) If there is an outer query, it must be handleable by the query toolchest (the local walker does not handle
    //    subqueries on its own).
    return analysis.isConcreteBased() && !analysis.isConcreteTableBased() && analysis.isGlobal()
           && (!analysis.isQuery()
               || toolChest.canPerformSubquery(((QueryDataSource) analysis.getDataSource()).getQuery()));
  }

  /**
   * Checks if a query can be handled wholly by {@link #clusterClient}. Assumes that it is a
   * {@link CachingClusteredClient} or something that behaves similarly.
   */
  private <T> boolean canRunQueryUsingClusterWalker(Query<T> query)
  {
    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(query.getDataSource());
    final QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);

    // 1) Must be based on a concrete table (the only shape the Druid cluster can handle).
    // 2) If there is an outer query, it must be handleable by the query toolchest (the cluster walker does not handle
    //    subqueries on its own).
    return analysis.isConcreteTableBased()
           && (!analysis.isQuery()
               || toolChest.canPerformSubquery(((QueryDataSource) analysis.getDataSource()).getQuery()));
  }


  private DataSource globalizeIfPossible(
      final DataSource dataSource
  )
  {
    if (dataSource instanceof TableDataSource) {
      log.info("!!!select：datasource为TableDataSource");
      GlobalTableDataSource maybeGlobal = new GlobalTableDataSource(((TableDataSource) dataSource).getName());
      if (joinableFactory.isDirectlyJoinable(maybeGlobal)) {
        return maybeGlobal;
      }
      return dataSource;
    } else {
      log.info("!!!select：datasource为其他类型");
      List<DataSource> currentChildren = dataSource.getChildren();
      List<DataSource> newChildren = new ArrayList<>(currentChildren.size());
      for (DataSource child : currentChildren) {
        child.getTableNames().stream().forEach(name->{
          log.info("!!!select：datasource的children包含table："+name);
        });
        log.info("!!!select：datasource的children结束");
        newChildren.add(globalizeIfPossible(child));
      }
      return dataSource.withChildren(newChildren);
    }
  }

  /**
   * Replace QueryDataSources with InlineDataSources when necessary and possible. "Necessary" is defined as:
   *
   * 1) For outermost subqueries: inlining is necessary if the toolchest cannot handle it.
   * 2) For all other subqueries (e.g. those nested under a join): inlining is always necessary.
   *
   * @param dataSource           datasource to process.
   * @param toolChestIfOutermost if provided, and if the provided datasource is a {@link QueryDataSource}, this method
   *                             will consider whether the toolchest can handle a subquery on the datasource using
   *                             {@link QueryToolChest#canPerformSubquery}. If the toolchest can handle it, then it will
   *                             not be inlined. See {@link org.apache.druid.query.groupby.GroupByQueryQueryToolChest}
   *                             for an example of a toolchest that can handle subqueries.
   * @param dryRun               if true, does not actually execute any subqueries, but will inline empty result sets.
   */
  /**
   * 在某个前提下，尝试将QueryDataSources替换成InlineDataSources
   *
   * @param dataSource 原始datasource
   * @param toolChestIfOutermost 自定义的querysource
   * @param subqueryRowLimitAccumulator 一个原子int
   * @param maxSubqueryRows druid.server.http.maxSubqueryRows参数，关联子查询结果最大可接受行数
   * @param dryRun 为true，则实际上不进行任何子查询，关联空的结果集
   */
  @SuppressWarnings({"rawtypes", "unchecked"}) // Subquery, toolchest, runner handling all use raw types
  private DataSource inlineIfNecessary(
      final DataSource dataSource,
      @Nullable final QueryToolChest toolChestIfOutermost,
      final AtomicInteger subqueryRowLimitAccumulator,
      final int maxSubqueryRows,
      final boolean dryRun
  )
  {
    log.info("!!!select：inlineIfNecessary，查看datasource类型："+ dataSource.getClass()+
            "...当前int原子为："+subqueryRowLimitAccumulator.get()+"...maxRows为："+maxSubqueryRows);
    if (dataSource instanceof QueryDataSource) {
      // This datasource is a subquery.
      //从datasource中找到子查询对象
      final Query subQuery = ((QueryDataSource) dataSource).getQuery();
      //获取该子查询的自定义处理类
      final QueryToolChest toolChest = warehouse.getToolChest(subQuery);

      if (toolChestIfOutermost != null && toolChestIfOutermost.canPerformSubquery(subQuery)) {
        log.info("!!!select：inlineIfNecessary，进入存在toolChestIfOutermost的条件分支");
        // Strip outer queries that are handleable by the toolchest, and inline subqueries that may be underneath
        // them (e.g. subqueries nested under a join).
        final Stack<DataSource> stack = new Stack<>();

        DataSource current = dataSource;
        while (current instanceof QueryDataSource) {
          stack.push(current);
          current = Iterables.getOnlyElement(current.getChildren());
        }

        assert !(current instanceof QueryDataSource); // lgtm [java/contradictory-type-checks]
        current = inlineIfNecessary(current, null, subqueryRowLimitAccumulator, maxSubqueryRows, dryRun);

        while (!stack.isEmpty()) {
          current = stack.pop().withChildren(Collections.singletonList(current));
        }

        assert current instanceof QueryDataSource;

        if (toolChest.canPerformSubquery(((QueryDataSource) current).getQuery())) {
          return current;
        } else {
          // Something happened during inlining that means the toolchest is no longer able to handle this subquery.
          // We need to consider inlining it.
          return inlineIfNecessary(current, toolChestIfOutermost, subqueryRowLimitAccumulator, maxSubqueryRows, dryRun);
        }
        /**
         * 如果此条子查询在整个集群中可以被查出结果，
         *
         */
      } else if (canRunQueryUsingLocalWalker(subQuery) || canRunQueryUsingClusterWalker(subQuery)) {
        log.info("!!!select：inlineIfNecessary，进入处理子查询逻辑");
        // Subquery needs to be inlined. Assign it a subquery id and run it.
        // 为子查询分配一个id
        final Query subQueryWithId = subQuery.withDefaultSubQueryId();
        log.info("!!!select：inlineIfNecessary，分配id后的子查询query类型："+subQueryWithId.getClass());

        final Sequence<?> queryResults;

        // 为true则不进行真正的子查询，直接给子查询结果赋个空值
        if (dryRun) {
          queryResults = Sequences.empty();
        } else {
          //获取子查询的queryrunner，其逻辑和正在进行的一样，都是由walker生成newQuery和baseQueryRunner，下面实际上调用的也是basequery的run方法
          /**
           * 获取子查询的queryrunner，然后调用其run方法获取查询结果。
           *
           * 注意：
           * 整个过程和当前正在进行的查询一模一样，
           * 都是由walker根据子查询query生成newQuery和baseQueryRunner，
           * 然后调用的也是basequery的run方法获取查询结果，
           * 期间子查询如果还有子查询，则也会走到此处，然后获取“子子查询的结果”，
           *
           * 可以说此处就将子查询，以及对应的的后续所有自查询结果获取到了。
           *
           * 可以说摸清了一条查询的逻辑，其子查询逻辑也就摸清了。
           */
          final QueryRunner subqueryRunner = subQueryWithId.getRunner(this);
          queryResults = subqueryRunner.run(
              QueryPlus.wrap(subQueryWithId),
              DirectDruidClient.makeResponseContextForQuery()
          );
        }

        /**
         * 生成InlineDataSource，
         * 可以说所谓的InlineDataSource就是“将所有子查询查出来，然后把其结果放入其中后的一种datasource”，
         * 简单点说就是“包含所有子查询结果的datasource”
         *
         * 期间还判断了子查询结果不能超过limit
         *
         * @param query 子查询对象
         * @param results 子查询结果
         * @param toolChest
         * @param limitAccumulator 原子int，从调用之初就被新建并传进来了
         * @param limit druid.server.http.maxSubqueryRows参数，关联子查询结果最大可接受行数
         */
        return toInlineDataSource(
            subQueryWithId,
            queryResults,
            warehouse.getToolChest(subQueryWithId),
            subqueryRowLimitAccumulator,
            maxSubqueryRows
        );
      } else {
        log.info("!!!select：inlineIfNecessary，无法内联子查询");
        // Cannot inline subquery. Attempt to inline one level deeper, and then try again.
        return inlineIfNecessary(
            dataSource.withChildren(
                Collections.singletonList(
                    inlineIfNecessary(
                        Iterables.getOnlyElement(dataSource.getChildren()),
                        null,
                        subqueryRowLimitAccumulator,
                        maxSubqueryRows,
                        dryRun
                    )
                )
            ),
            toolChestIfOutermost,
            subqueryRowLimitAccumulator,
            maxSubqueryRows,
            dryRun
        );
      }
    } else {
      // Not a query datasource. Walk children and see if there's anything to inline.
      //递归查找dataSource的所有子datasource看其是否为QueryDataSource类型
      return dataSource.withChildren(
          dataSource.getChildren()
                    .stream()
                    .map(child -> inlineIfNecessary(child, null, subqueryRowLimitAccumulator, maxSubqueryRows, dryRun))
                    .collect(Collectors.toList())
      );
    }
  }

  /**
   * Decorate query runners created by {@link #clusterClient}, adding result caching, result merging, metric
   * emission, etc. Not to be used on runners from {@link #localClient}, since we expect it to do this kind
   * of decoration to itself.
   *
   * @param query             the query
   * @param baseClusterRunner runner from {@link #clusterClient}
   */
  private <T> QueryRunner<T> decorateClusterRunner(Query<T> query, QueryRunner<T> baseClusterRunner)
  {
    final QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);

    return new FluentQueryRunnerBuilder<>(toolChest)
        .create(
            new SetAndVerifyContextQueryRunner<>(
                serverConfig,
                new RetryQueryRunner<>(
                    baseClusterRunner,
                    clusterClient::getQueryRunnerForSegments,
                    retryConfig,
                    objectMapper
                )
            )
        )
        .applyPreMergeDecoration()
        .mergeResults()
        .applyPostMergeDecoration()
        .emitCPUTimeMetric(emitter)
        .postProcess(
            objectMapper.convertValue(
                query.<String>getContextValue("postProcessing"),
                new TypeReference<PostProcessingOperator<T>>() {}
            )
        )
        .map(
            runner ->
                new ResultLevelCachingQueryRunner<>(
                    runner,
                    toolChest,
                    query,
                    objectMapper,
                    cache,
                    cacheConfig
                )
        );
  }

  /**
   * Convert the results of a particular query into a materialized (List-based) InlineDataSource.
   *
   * @param query            the query 子查询对象
   * @param results          query results 子查询结果
   * @param toolChest        toolchest for the query 自定义的查询工具
   * @param limitAccumulator an accumulator for tracking the number of accumulated rows in all subqueries for a
   *                         particular master query 原子int，从调用之初就被新建并传进来了
   * @param limit            user-configured limit. If negative, will be treated as {@link Integer#MAX_VALUE}.
   *                         If zero, this method will throw an error
   *                         immediately.druid.server.http.maxSubqueryRows参数，关联子查询结果最大可接受行数
   *
   * @throws ResourceLimitExceededException if the limit is exceeded
   */
  private static <T, QueryType extends Query<T>> InlineDataSource toInlineDataSource(
      final QueryType query,
      final Sequence<T> results,
      final QueryToolChest<T, QueryType> toolChest,
      final AtomicInteger limitAccumulator,
      final int limit
  )
  {
    log.info("!!!select：toInlineDataSource中");
    //immediately.druid.server.http.maxSubqueryRows参数设置为负数，则表示为最大值
    final int limitToUse = limit < 0 ? Integer.MAX_VALUE : limit;

    if (limitAccumulator.get() >= limitToUse) {
      throw new ResourceLimitExceededException("Cannot issue subquery, maximum[%d] reached", limitToUse);
    }

    final RowSignature signature = toolChest.resultArraySignature(query);

    final List<Object[]> resultList = new ArrayList<>();

    toolChest.resultsAsArrays(query, results).accumulate(
        resultList,
        (acc, in) -> {
          // 将子查询结果行数累加进limitAccumulator，并且与limitToUse参数进行比较
          if (limitAccumulator.getAndIncrement() >= limitToUse) {
            throw new ResourceLimitExceededException(
                "Subquery generated results beyond maximum[%d]",
                limitToUse
            );
          }
          acc.add(in);
          return acc;
        }
    );

    return InlineDataSource.fromIterable(resultList, signature);
  }

  /**
   * A {@link QueryRunner} which validates that a *specific* query is passed in, and then swaps it with another one.
   * Useful since the inlining we do relies on passing the modified query to the underlying {@link QuerySegmentWalker},
   * and callers of {@link #getQueryRunnerForIntervals} aren't able to do this themselves.
   */
  private static class QuerySwappingQueryRunner<T> implements QueryRunner<T>
  {
    private final QueryRunner<T> baseRunner;
    private final Query<T> query;
    private final Query<T> newQuery;

    public QuerySwappingQueryRunner(QueryRunner<T> baseRunner, Query<T> query, Query<T> newQuery)
    {
      this.baseRunner = baseRunner;
      this.query = query;
      this.newQuery = newQuery;
    }

    @Override
    public Sequence<T> run(QueryPlus<T> queryPlus, ResponseContext responseContext)
    {
      //noinspection ObjectEquality
      if (queryPlus.getQuery() != query) {
        throw new ISE("Unexpected query received");
      }

      return baseRunner.run(queryPlus.withQuery(newQuery), responseContext);
    }
  }
}
