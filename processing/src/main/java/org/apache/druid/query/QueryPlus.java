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

package org.apache.druid.query;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.epinephelinae.GroupByMergingQueryRunnerV2;
import org.apache.druid.query.groupby.epinephelinae.Grouper;
import org.apache.druid.query.groupby.resource.GroupByQueryResource;
import org.apache.druid.query.groupby.strategy.GroupByStrategy;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.UnaryOperator;

/**
 * An immutable composite object of {@link Query} + extra stuff needed in {@link QueryRunner}s.
 */
@PublicApi
public final class QueryPlus<T>
{
  private static final Logger log = new Logger(QueryPlus.class);
  /**
   * Returns the minimum bare QueryPlus object with the given query. {@link #getQueryMetrics()} of the QueryPlus object,
   * returned from this factory method, returns {@code null}.
   */
  public static <T> QueryPlus<T> wrap(Query<T> query)
  {
    Preconditions.checkNotNull(query);
    return new QueryPlus<>(query, null, null);
  }

  private final Query<T> query;
  private final QueryMetrics<?> queryMetrics;
  private final String identity;

  private QueryPlus(Query<T> query, QueryMetrics<?> queryMetrics, String identity)
  {
    this.query = query;
    this.queryMetrics = queryMetrics;
    this.identity = identity;
  }

  public Query<T> getQuery()
  {
    return query;
  }

  @Nullable
  public QueryMetrics<?> getQueryMetrics()
  {
    return queryMetrics;
  }

  /**
   * Returns the same QueryPlus object with the identity replaced. This new identity will affect future calls to
   * {@link #withoutQueryMetrics()} but will not affect any currently-existing queryMetrics.
   */
  public QueryPlus<T> withIdentity(String identity)
  {
    return new QueryPlus<>(query, queryMetrics, identity);
  }

  /**
   * Returns the same QueryPlus object, if it already has {@link QueryMetrics} ({@link #getQueryMetrics()} returns not
   * null), or returns a new QueryPlus object with {@link Query} from this QueryPlus and QueryMetrics created using the
   * given {@link QueryToolChest}, via {@link QueryToolChest#makeMetrics(Query)} method.
   *
   * By convention, callers of {@code withQueryMetrics()} must also call .getQueryMetrics().emit() on the returned
   * QueryMetrics object, regardless if this object is the same as the object on which .withQueryMetrics() was initially
   * called (i. e. it already had non-null QueryMetrics), or if it is a new QueryPlus object. See {@link
   * MetricsEmittingQueryRunner} for example.
   */
  public QueryPlus<T> withQueryMetrics(QueryToolChest<T, ? extends Query<T>> queryToolChest)
  {
    if (queryMetrics != null) {
      return this;
    } else {
      final QueryMetrics metrics = ((QueryToolChest) queryToolChest).makeMetrics(query);

      if (identity != null) {
        metrics.identity(identity);
      }

      return new QueryPlus<>(query, metrics, identity);
    }
  }

  /**
   * Returns a QueryPlus object without the components which are unsafe for concurrent use from multiple threads,
   * therefore couldn't be passed down in concurrent or async {@link QueryRunner}s.
   *
   * Currently the only unsafe component is {@link QueryMetrics}, i. e. {@code withoutThreadUnsafeState()} call is
   * equivalent to {@link #withoutQueryMetrics()}.
   */
  public QueryPlus<T> withoutThreadUnsafeState()
  {
    return withoutQueryMetrics();
  }

  /**
   * Returns the same QueryPlus object, if it doesn't have {@link QueryMetrics} ({@link #getQueryMetrics()} returns
   * null), or returns a new QueryPlus object with {@link Query} from this QueryPlus and null as QueryMetrics.
   */
  private QueryPlus<T> withoutQueryMetrics()
  {
    if (queryMetrics == null) {
      return this;
    } else {
      return new QueryPlus<>(query, null, identity);
    }
  }

  /**
   * Equivalent of withQuery(getQuery().withOverriddenContext(ImmutableMap.of(MAX_QUEUED_BYTES_KEY, maxQueuedBytes))).
   */
  public QueryPlus<T> withMaxQueuedBytes(long maxQueuedBytes)
  {
    return new QueryPlus<>(
        query.withOverriddenContext(ImmutableMap.of(QueryContexts.MAX_QUEUED_BYTES_KEY, maxQueuedBytes)),
        queryMetrics,
        identity
    );
  }

  /**
   * Returns a QueryPlus object with {@link QueryMetrics} from this QueryPlus object, and the provided {@link Query}.
   * 创建一个新的QueryPlus对象，且内部包裹传参replacementQuery对象
   */
  public <U> QueryPlus<U> withQuery(Query<U> replacementQuery)
  {
    return new QueryPlus<>(replacementQuery, queryMetrics, identity);
  }

  /**
   * 参数walker为启动时注入而来，
   * broker和historical启动时注入的对象不同
   * broker启动时walker为{@link org.apache.druid.server.ClientQuerySegmentWalker}
   * historical启动时walker为{@link org.apache.druid.server.coordination.ServerManager}
   *
   * 先根据请求对象“query”由walker找到对应的queryrunner对象，
   * 不同的请求类型，其处理对象queryRunner也不同，
   * 如group by查询，最终对应的就是{@link GroupByMergingQueryRunnerV2}来处理。
   *
   * queryRunner更像是一个“处理链”，每个queryRunner的run都会返回一个Sequence迭代器结果，
   * 且内部可能还有一个queryrunner被调用run方法，并将其返回的Sequence和当前queryRunner的Sequence合并后返回。
   *
   * 此处的QueryPlus#run方法就是通过walker找到此次查询的对应顶部queryrunner，调用run方法执行整个queryRunner链的run方法。
   */
  public Sequence<T> run(QuerySegmentWalker walker, ResponseContext context)
  {
    /**{@link GroupByQuery}*/
    log.info("QueryPlus初始baseQuery："+query.getClass());
    /**
     * 1、此处实际上是调用walker的getQueryRunnerForSegments()方法获取的queryRunner
     * {@link BaseQuery#getRunner(QuerySegmentWalker)}
     * |->{@link org.apache.druid.query.spec.MultipleSpecificSegmentSpec#lookup(Query, QuerySegmentWalker)}
     * |->walker.getQueryRunnerForSegments()，其中broker和historical节点的walker是不同的
     * broker节点：{@link org.apache.druid.server.ClientQuerySegmentWalker#getQueryRunnerForSegments(Query, Iterable)}
     * historical节点：{@link org.apache.druid.server.coordination.ServerManager#getQueryRunnerForSegments(Query, Iterable)}
     *
     * 参数walker为启动时注入而来，
     * broker和historical启动时注入的对象不同
     * broker启动时walker为{@link org.apache.druid.server.ClientQuerySegmentWalker}
     * historical启动时walker为{@link org.apache.druid.server.coordination.ServerManager}
     *
     * 另：请求到达broker和到达historical后此处获取的queryRunner是不同的
     * 到达broker获取的queryRunner类型为
     * {@link org.apache.druid.server.ClientQuerySegmentWalker $QuerySwappingQueryRunner}
     * 请求到达historical获取的queryRunner类型为
     * {@link org.apache.druid.query.CPUTimeMetricQueryRunner}
     *
     * 2、his节点ServerManager创建queryRunner的具体流程
     * {@link org.apache.druid.server.coordination.ServerManager#getQueryRunnerForSegments(Query, Iterable)}
     * ServerManager：其中包含了segmentManager，该对象与启动时加载的segment列表相关。
     *
     * queryRunner是一个“链式”结构，每一个queryRunner中都会包含一个子queryRunner，且每个runner中的run方法除了执行自身逻辑外，
     * 还会执行子runner的run方法，最终真正执行查询逻辑的是整个runner链最末尾的runner。
     *
     * ServerManager创建queryRunner时，主要创建了两组Runner：
     * （1）每个时间片段segment都创建了个对应的QueryRunner链用来在该片段上查询数据。
     * 这些Runner构成了一个Runner列表。
     * （2）在外层还有一个Runner链，主要用于使用线程池并发的处理每一个QueryRunner。
     */
    log.info("!!!runner：开始执行创建runner");
    QueryRunner<T> queryRunner = query.getRunner(walker);
    log.info("!!!runner：runner创建完毕");

    log.info("!!!select：query生成的queryRunner对象："+queryRunner.getClass());
    /**
     * 请求到达broker时，为以下逻辑：
     * 此处的queryRunner{@link org.apache.druid.server.ClientQuerySegmentWalker}中的QuerySwappingQueryRunner对象，
     * 其内部还包裹着一个baseRunner，后续每一个baseRunner都调用了其内部的baseRunner的run，一层层调用，整个顺序如下
     * QuerySwappingQueryRunner
     * {@link org.apache.druid.query.FluentQueryRunnerBuilder.FluentQueryRunner#run(QueryPlus, ResponseContext)}
     * {@link ResultLevelCachingQueryRunner#run(QueryPlus, ResponseContext)}
     * {@link CPUTimeMetricQueryRunner#run(QueryPlus, ResponseContext)}
     * |->{@link FinalizeResultsQueryRunner#run(QueryPlus, ResponseContext)}
     * |->{@link org.apache.druid.query.groupby.GroupByQueryQueryToolChest#preMergeQueryDecoration(QueryRunner)}
     * |->{@link org.apache.druid.server.SetAndVerifyContextQueryRunner#run(QueryPlus, ResponseContext)}
     * |->{@link org.apache.druid.query.RetryQueryRunner#run(QueryPlus, ResponseContext)}
     * |->{@link org.apache.druid.client.CachingClusteredClient#run(QueryPlus, ResponseContext, UnaryOperator, boolean)}
     *
     * 此处返回的Sequence<T>结果，由最后的CachingClusteredClient中的run方法返回。
     *
     * ====================================================================================================
     * 请求到达historical时为以下逻辑：
     * queryRunner为{@link org.apache.druid.query.CPUTimeMetricQueryRunner}
     * |->（进入CPUTimeMetricQueryRunner）{@link CPUTimeMetricQueryRunner#run(QueryPlus, ResponseContext)}
     * |->（进入FinalizeResultsQueryRunner）{@link FinalizeResultsQueryRunner#run(QueryPlus, ResponseContext)}
     * |->（进入匿名函数queryRunner）run方法为该响应匿名函数：{@link org.apache.druid.query.groupby.GroupByQueryQueryToolChest#mergeResults(QueryRunner)}
     *      |->{@link org.apache.druid.query.groupby.GroupByQueryQueryToolChest#initAndMergeGroupByResults(GroupByQuery, QueryRunner, ResponseContext)}
     *      |->{@link org.apache.druid.query.groupby.GroupByQueryQueryToolChest#mergeGroupByResults(GroupByStrategy, GroupByQuery, GroupByQueryResource, QueryRunner, ResponseContext)}
     *      |->{@link org.apache.druid.query.groupby.GroupByQueryQueryToolChest#mergeGroupByResultsWithoutPushDown(GroupByStrategy, GroupByQuery, GroupByQueryResource, QueryRunner, ResponseContext)}
     *      |->{@link org.apache.druid.query.groupby.strategy.GroupByStrategyV2#mergeResults(QueryRunner, GroupByQuery, ResponseContext)}
     * |->（进入匿名函数mergingQueryRunner）run方法为该响应匿名函数：{@link org.apache.druid.query.groupby.GroupByQueryRunnerFactory#mergeRunners(ExecutorService, Iterable)}
     * |->（进入GroupByMergingQueryRunnerV2）{@link GroupByMergingQueryRunnerV2#run(QueryPlus, ResponseContext)}
     * 至此GroupByMergingQueryRunnerV2.run提供真正的BaseSequence结果，且其中包含make方法，为真正的查询结果方法，
     * 在后续Sequence.toYielder时会被调用，执行真正的查询逻辑
     *
     * 以上QueryRunner链最后涉及到线程池并发执行一个runner列表，
     * 这个runner列表就是上一步提到的各个segment查询对应的QueryRunner。
     * 以groupby查询为例，针对单个segment的Runner.run()为：
     * {@link org.apache.druid.query.groupby.GroupByQueryRunnerFactory.GroupByQueryRunner#run(QueryPlus, ResponseContext)}
     *
     * =======================================================================================
     * 返回的Sequence中的make方法会返回迭代器，该迭代器可迭代查询结果，迭代器创建逻辑：
     * {@link org.apache.druid.query.groupby.epinephelinae.RowBasedGrouperHelper#makeGrouperIterator(Grouper, GroupByQuery, List, Closeable)}
     */
    log.info("!!!runner：开始执行runner.run");
    Sequence<T> res = queryRunner.run(this, context);
    log.info("!!!runner：runner.run执行完毕");
    return res;
  }

  public QueryPlus<T> optimizeForSegment(PerSegmentQueryOptimizationContext optimizationContext)
  {
    return new QueryPlus<>(query.optimizeForSegment(optimizationContext), queryMetrics, identity);
  }
}
