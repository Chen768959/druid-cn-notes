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
import org.apache.druid.query.groupby.resource.GroupByQueryResource;
import org.apache.druid.query.groupby.strategy.GroupByStrategy;

import javax.annotation.Nullable;
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

  public Sequence<T> run(QuerySegmentWalker walker, ResponseContext context)
  {
    /**{@link GroupByQuery}*/
    log.info("QueryPlus初始baseQuery："+query.getClass());
    /**
     * {@link BaseQuery#getRunner(QuerySegmentWalker)}
     *
     * 参数walker为启动时注入而来，
     * broker和historical启动时注入的对象不同
     * broker启动时walker为{@link org.apache.druid.server.ClientQuerySegmentWalker}
     * historical启动时walker为{@link org.apache.druid.server.coordination.ServerManager}
     *
     * 此处实际上是调用walker的getQueryRunnerForSegments()方法获取QueryRunner
     * 所以此处queryRunner的创建只和query和walker对象有关
     * （历史节点创建queryrunner逻辑;{@link org.apache.druid.server.coordination.ServerManager#getQueryRunnerForSegments(Query, Iterable)}）
     *
     * 另：请求到达broker和到达historical后此处获取的queryRunner是不同的
     * 到达broker获取的queryRunner类型为
     * {@link org.apache.druid.server.ClientQuerySegmentWalker $QuerySwappingQueryRunner}
     *
     * 请求到达historical获取的queryRunner类型为
     * {@link org.apache.druid.query.CPUTimeMetricQueryRunner}
     */
    QueryRunner<T> queryRunner = query.getRunner(walker);

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
     * |->{@link CPUTimeMetricQueryRunner#run(QueryPlus, ResponseContext)}
     * |->{@link FinalizeResultsQueryRunner#run(QueryPlus, ResponseContext)}
     * |->run方法为该响应匿名函数：{@link org.apache.druid.query.groupby.GroupByQueryQueryToolChest#mergeResults(QueryRunner)}
     * |->{@link org.apache.druid.query.groupby.GroupByQueryQueryToolChest#initAndMergeGroupByResults(GroupByQuery, QueryRunner, ResponseContext)}
     * |->{@link org.apache.druid.query.groupby.GroupByQueryQueryToolChest#mergeGroupByResults(GroupByStrategy, GroupByQuery, GroupByQueryResource, QueryRunner, ResponseContext)}
     * |->{@link org.apache.druid.query.groupby.GroupByQueryQueryToolChest#mergeGroupByResultsWithoutPushDown(GroupByStrategy, GroupByQuery, GroupByQueryResource, QueryRunner, ResponseContext)}
     * |->{@link org.apache.druid.query.groupby.strategy.GroupByStrategyV2#mergeResults(QueryRunner, GroupByQuery, ResponseContext)}
     * |->run方法为该响应匿名函数：{@link org.apache.druid.query.groupby.GroupByQueryRunnerFactory#mergeRunners(ExecutorService, Iterable)}
     * |->{@link GroupByMergingQueryRunnerV2#run(QueryPlus, ResponseContext)}
     * 至此GroupByMergingQueryRunnerV2.run提供真正的BaseSequence结果，且其中包含make方法，为真正的查询结果方法，
     * 在后续Sequence.toYielder时会被调用，执行真正的查询逻辑
     */
    Sequence<T> res = queryRunner.run(this, context);
    return res;
  }

  public QueryPlus<T> optimizeForSegment(PerSegmentQueryOptimizationContext optimizationContext)
  {
    return new QueryPlus<>(query.optimizeForSegment(optimizationContext), queryMetrics, identity);
  }
}
