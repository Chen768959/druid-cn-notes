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

package org.apache.druid.query.groupby.epinephelinae;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.collections.Releaser;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Accumulator;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.AbstractPrioritizedCallable;
import org.apache.druid.query.ChainedExecutionQueryRunner;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.ReferenceCountingSegmentQueryRunner;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.RowBasedGrouperHelper.RowBasedKey;
import org.apache.druid.query.groupby.epinephelinae.vector.VectorGroupByEngine;
import org.apache.druid.segment.StorageAdapter;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Class that knows how to merge a collection of groupBy {@link QueryRunner} objects, called {@code queryables},
 * using a buffer provided by {@code mergeBufferPool} and a parallel executor provided by {@code exec}. Outputs a
 * fully aggregated stream of {@link ResultRow} objects. Does not apply post-aggregators.
 *
 * The input {@code queryables} are expected to come from a {@link GroupByQueryEngineV2}. This code runs on data
 * servers, like Historicals.
 *
 * This class has some resemblance to {@link GroupByRowProcessor}. See the javadoc of that class for a discussion of
 * similarities and differences.
 *
 * Used by
 * {@link org.apache.druid.query.groupby.strategy.GroupByStrategyV2#mergeRunners(ListeningExecutorService, Iterable)}.
 */
public class GroupByMergingQueryRunnerV2 implements QueryRunner<ResultRow>
{
  private static final Logger log = new Logger(GroupByMergingQueryRunnerV2.class);
  private static final String CTX_KEY_MERGE_RUNNERS_USING_CHAINED_EXECUTION = "mergeRunnersUsingChainedExecution";

  private final GroupByQueryConfig config;
  private final Iterable<QueryRunner<ResultRow>> queryables;
  private final ListeningExecutorService exec;
  private final QueryWatcher queryWatcher;
  private final int concurrencyHint;
  private final BlockingPool<ByteBuffer> mergeBufferPool;
  private final ObjectMapper spillMapper;
  private final String processingTmpDir;
  private final int mergeBufferSize;

  /**
   *
   * @param config
   * @param exec 异步线程池 {@link org.apache.druid.query.MetricsEmittingExecutorService}
   * @param queryWatcher 负责当前查询的“可见性”
   * @param queryables 多个需要被合并的queryrunner
   * @param concurrencyHint：processingConfig.getNumThreads() 可用线程数，默认为“jvm可用cpu核心数-1”
   * @param mergeBufferPool
   * @param mergeBufferSize
   * @param spillMapper
   * @param processingTmpDir 配置文件“tmpDir”参数配置
   */
  public GroupByMergingQueryRunnerV2(
      GroupByQueryConfig config,
      ExecutorService exec,
      QueryWatcher queryWatcher,
      Iterable<QueryRunner<ResultRow>> queryables,
      int concurrencyHint,
      BlockingPool<ByteBuffer> mergeBufferPool,
      int mergeBufferSize,
      ObjectMapper spillMapper,
      String processingTmpDir
  )
  {  
    this.config = config;
    this.exec = MoreExecutors.listeningDecorator(exec);
    this.queryWatcher = queryWatcher;
    this.queryables = Iterables.unmodifiableIterable(Iterables.filter(queryables, Predicates.notNull()));
    this.concurrencyHint = concurrencyHint;
    this.mergeBufferPool = mergeBufferPool;
    this.spillMapper = spillMapper;
    this.processingTmpDir = processingTmpDir;
    this.mergeBufferSize = mergeBufferSize;
  }

  @Override
  public Sequence<ResultRow> run(final QueryPlus<ResultRow> queryPlus, final ResponseContext responseContext)
  {
    // 一次groupBy查询的json请求所对应的查询对象
    final GroupByQuery query = (GroupByQuery) queryPlus.getQuery();
    final GroupByQueryConfig querySpecificConfig = config.withOverrides(query);

    // CTX_KEY_MERGE_RUNNERS_USING_CHAINED_EXECUTION is here because realtime servers use nested mergeRunners calls
    // (one for the entire query and one for each sink). We only want the outer call to actually do merging with a
    // merge buffer, otherwise the query will allocate too many merge buffers. This is potentially sub-optimal as it
    // will involve materializing the results for each sink before starting to feed them into the outer merge buffer.
    // I'm not sure of a better way to do this without tweaking how realtime servers do queries.
    final boolean forceChainedExecution = query.getContextBoolean(
        CTX_KEY_MERGE_RUNNERS_USING_CHAINED_EXECUTION,
        false
    );
    final QueryPlus<ResultRow> queryPlusForRunners = queryPlus
        .withQuery(
            query.withOverriddenContext(ImmutableMap.of(CTX_KEY_MERGE_RUNNERS_USING_CHAINED_EXECUTION, true))
        )
        .withoutThreadUnsafeState();

    if (QueryContexts.isBySegment(query) || forceChainedExecution) {
      ChainedExecutionQueryRunner<ResultRow> runner = new ChainedExecutionQueryRunner<>(exec, queryWatcher, queryables);
      return runner.run(queryPlusForRunners, responseContext);
    }

    final boolean isSingleThreaded = querySpecificConfig.isSingleThreaded();

    final File temporaryStorageDirectory = new File(
        processingTmpDir,
        StringUtils.format("druid-groupBy-%s_%s", UUID.randomUUID(), query.getId())
    );

    final int priority = QueryContexts.getPriority(query);

    // Figure out timeoutAt time now, so we can apply the timeout to both the mergeBufferPool.take and the actual
    // query processing together.
    final long queryTimeout = QueryContexts.getTimeout(query);
    final boolean hasTimeout = QueryContexts.hasTimeout(query);
    final long timeoutAt = System.currentTimeMillis() + queryTimeout;

    /**
     * 此处用到的手法和请求到达broker，然后懒加载获取Sequence是一致的。
     *
     * 都是返回一个Sequence，然后往里传一个“IteratorMaker”匿名函数，
     * 这个IteratorMaker中的make方法才是真正的查询逻辑，
     * 也只有在后面调用Sequence转Yielder的方法时，才会调用这个真正的查询逻辑进行查询，
     * 所以我们重点看以下的make方法：
     */
    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<ResultRow, CloseableGrouperIterator<RowBasedKey, ResultRow>>()
        {
          /**
           * 真正的查询逻辑，在后续Sequence转Yielder时会被调用。
           * 然后获取迭代器，用于迭代查询结果。
           */
          @Override
          public CloseableGrouperIterator<RowBasedKey, ResultRow> make()
          {
            // Closer的目的是收集所有“可关闭”的对象（如流）然后可以统一关闭，
            // 将这些Closeable对象注册进resources后，可以统一关闭
            final Closer resources = Closer.create();

            try {
              // 磁盘上的临时存储空间，目录地址为配置文件“tmpDir”参数配置
              final LimitedTemporaryStorage temporaryStorage = new LimitedTemporaryStorage(
                  temporaryStorageDirectory,
                  querySpecificConfig.getMaxOnDiskStorage()
              );

              // 将上面的临时存储空间对象放入“引用计数包装类”中，这样会记录后续有多少处引用，只有当所有引用都将其close后，才会真正把该资源close
              final ReferenceCountingResourceHolder<LimitedTemporaryStorage> temporaryStorageHolder =
                  ReferenceCountingResourceHolder.fromCloseable(temporaryStorage);

              // 临时存储空间对象注册进resources统一维护关闭
              resources.register(temporaryStorageHolder);

              // If parallelCombine is enabled, we need two merge buffers for parallel aggregating and parallel combining
              final int numMergeBuffers = querySpecificConfig.getNumParallelCombineThreads() > 1 ? 2 : 1;

              // 从mergeBufferPool池中拿出“numMergeBuffers（1）”个buffer对象
              final List<ReferenceCountingResourceHolder<ByteBuffer>> mergeBufferHolders = getMergeBuffersHolder(
                  numMergeBuffers,
                  hasTimeout,
                  timeoutAt
              );
              // 把刚刚拿到的buffer也注册进resources统一维护关闭
              resources.registerAll(mergeBufferHolders);

              // 第一位buffer
              final ReferenceCountingResourceHolder<ByteBuffer> mergeBufferHolder = mergeBufferHolders.get(0);

              // 无
              final ReferenceCountingResourceHolder<ByteBuffer> combineBufferHolder = numMergeBuffers == 2 ?
                                                                                      mergeBufferHolders.get(1) :
                                                                                      null;

              /**
               * @param query               此次groupby查询的json请求对象
               * @param subquery            NULL
               * @param config              配置对象
               * @param bufferSupplier      从mergebuffer池中取出的空byteBuffer
               * @param combineBufferHolder 无
               * @param concurrencyHint     Grouper查询的可用线程数，默认为“jvm可用cpu核心数-1”
               * @param temporaryStorage    磁盘上的临时存储空间，目录地址为配置文件“tmpDir”参数配置
               * @param spillMapper         JSON映射工具
               * @param grouperSorter       异步线程池 {@link org.apache.druid.query.MetricsEmittingExecutorService}
               *                            用于并发的执行结果合并（concurrencyHint为-1时不使用，因为concurrencyHint为-1时为但线程）
               * @param priority           请求的“pri ority”参数，查询优先级
               * @param hasQueryTimeout    是否设置查询超时限制，默认不限制（来自请求参数“timeout”）
               * @param queryTimeoutAt     时间戳long类型，当到达此时间还未查出结果时，则算此次查询超时
               * @param mergeBufferSize
               */
              Pair<Grouper<RowBasedKey>, Accumulator<AggregateResult, ResultRow>> pair = RowBasedGrouperHelper.createGrouperAccumulatorPair(
                      query,
                      null,
                      config,
                      Suppliers.ofInstance(mergeBufferHolder.get()),
                      combineBufferHolder,
                      concurrencyHint,
                      temporaryStorage,
                      spillMapper,
                      exec,
                      priority,
                      hasTimeout,
                      timeoutAt,
                      mergeBufferSize
                  );

              /**
               * grouper，
               * 聚焦于group by查询
               * 根据concurrencyHint（group by查询可用线程数）的不同，其创建对象也不同
               * 多线程时创建的是{@link ConcurrentGrouper}
               * 会被放于迭代器中
               */
              final Grouper<RowBasedKey> grouper = pair.lhs;
              final Accumulator<AggregateResult, ResultRow> accumulator = pair.rhs;

              /**
               * {@link ConcurrentGrouper#init()}
               */
              grouper.init();

              final ReferenceCountingResourceHolder<Grouper<RowBasedKey>> grouperHolder =
                  ReferenceCountingResourceHolder.fromCloseable(grouper);
              resources.register(grouperHolder);

              List<ListenableFuture<AggregateResult>> futures = Lists.newArrayList(
                      Iterables.transform(
                          queryables,
                          new Function<QueryRunner<ResultRow>, ListenableFuture<AggregateResult>>()
                          {
                            @Override
                            public ListenableFuture<AggregateResult> apply(final QueryRunner<ResultRow> input)
                            {
                              if (input == null) {
                                throw new ISE("Null queryRunner! Looks to be some segment unmapping action happening");
                              }

                              /**
                               * 使用guava提交异步任务，返回结果
                               * exec是在创建当前综合queryRunner对象时传参进来的，是一个异步线程池 {@link org.apache.druid.query.MetricsEmittingExecutorService}
                               */
                              ListenableFuture<AggregateResult> future = exec.submit(
                                  new AbstractPrioritizedCallable<AggregateResult>(priority)
                                  {
                                    @Override
                                    public AggregateResult call()
                                    {
                                      try (
                                          // These variables are used to close releasers automatically.
                                          @SuppressWarnings("unused")
                                          Releaser bufferReleaser = mergeBufferHolder.increment();
                                          @SuppressWarnings("unused")
                                          Releaser grouperReleaser = grouperHolder.increment()
                                      ) {
                                        // Return true if OK, false if resources were exhausted.
//                                        log.info("!!!：V2异步执行查询任务，查询input类型："+input.getClass());
                                        /**
                                         * 此处input：
                                         * {@link org.apache.druid.server.SetAndVerifyContextQueryRunner#run(QueryPlus, ResponseContext)}
                                         * |->{@link org.apache.druid.query.CPUTimeMetricQueryRunner#run(QueryPlus, ResponseContext)}
                                         * |->{@link org.apache.druid.query.PerSegmentOptimizingQueryRunner#run(QueryPlus, ResponseContext)}
                                         * |->{@link org.apache.druid.query.spec.SpecificSegmentQueryRunner#run(QueryPlus, ResponseContext)}
                                         *    |->{@link org.apache.druid.query.MetricsEmittingQueryRunner#run(QueryPlus, ResponseContext)}
                                         *    |->{@link org.apache.druid.query.BySegmentQueryRunner#run(QueryPlus, ResponseContext)}
                                         *    |->{@link org.apache.druid.client.CachingQueryRunner#run(QueryPlus, ResponseContext)}
                                         *    |->{@link org.apache.druid.query.MetricsEmittingQueryRunner#run(QueryPlus, ResponseContext)}
                                         *    |->{@link ReferenceCountingSegmentQueryRunner#run(QueryPlus, ResponseContext)}
                                         *    |->{@link org.apache.druid.query.groupby.GroupByQueryRunnerFactory.GroupByQueryRunner#run(QueryPlus, ResponseContext)}
                                         *    |->{@link org.apache.druid.query.groupby.strategy.GroupByStrategyV2#process(GroupByQuery, StorageAdapter)}
                                         *    |->{@link org.apache.druid.query.groupby.epinephelinae.GroupByQueryEngineV2#process(GroupByQuery, StorageAdapter, NonBlockingPool, GroupByQueryConfig)}
                                         *    |->{@link VectorGroupByEngine#process(GroupByQuery, StorageAdapter, ByteBuffer, DateTime, Filter, Interval, GroupByQueryConfig)}
                                         */
                                        return input.run(queryPlusForRunners, responseContext)
                                                    .accumulate(AggregateResult.ok(), accumulator);
                                      }
                                      catch (QueryInterruptedException e) {
                                        throw e;
                                      }
                                      catch (Exception e) {
                                        log.error(e, "Exception with one of the sequences!");
                                        throw new RuntimeException(e);
                                      }
                                    }
                                  }
                              );

                              if (isSingleThreaded) {
                                waitForFutureCompletion(
                                    query,
                                    ImmutableList.of(future),
                                    hasTimeout,
                                    timeoutAt - System.currentTimeMillis()
                                );
                              }

                              return future;
                            }
                          }
                      )
                  );

              if (!isSingleThreaded) {
                waitForFutureCompletion(query, futures, hasTimeout, timeoutAt - System.currentTimeMillis());
              }

              /**
               * 创建迭代器
               * grouper：聚焦于group by查询，多线程时创建的是{@link ConcurrentGrouper}
               * query：此次请求对象
               * resources：收集了所有“可关闭”的对象，用于统一关闭
               *
               * 此迭代器可迭代出group by 查询结果
               */
              return RowBasedGrouperHelper.makeGrouperIterator(
                  grouper,
                  query,
                  resources
              );
            }
            catch (Throwable t) {
              // Exception caught while setting up the iterator; release resources.
              try {
                resources.close();
              }
              catch (Exception ex) {
                t.addSuppressed(ex);
              }
              throw t;
            }
          }

          @Override
          public void cleanup(CloseableGrouperIterator<RowBasedKey, ResultRow> iterFromMake)
          {
            iterFromMake.close();
          }
        }
    );
  }

  private List<ReferenceCountingResourceHolder<ByteBuffer>> getMergeBuffersHolder(
      int numBuffers,
      boolean hasTimeout,
      long timeoutAt
  )
  {
    try {
      if (numBuffers > mergeBufferPool.maxSize()) {
        throw new ResourceLimitExceededException(
            "Query needs " + numBuffers + " merge buffers, but only "
            + mergeBufferPool.maxSize() + " merge buffers were configured. "
            + "Try raising druid.processing.numMergeBuffers."
        );
      }
      final List<ReferenceCountingResourceHolder<ByteBuffer>> mergeBufferHolder;
      // This will potentially block if there are no merge buffers left in the pool.
      if (hasTimeout) {
        final long timeout = timeoutAt - System.currentTimeMillis();
        if (timeout <= 0) {
          throw new TimeoutException();
        }
        if ((mergeBufferHolder = mergeBufferPool.takeBatch(numBuffers, timeout)).isEmpty()) {
          throw new TimeoutException("Cannot acquire enough merge buffers");
        }
      } else {
        mergeBufferHolder = mergeBufferPool.takeBatch(numBuffers);
      }
      return mergeBufferHolder;
    }
    catch (Exception e) {
      throw new QueryInterruptedException(e);
    }
  }

  private void waitForFutureCompletion(
      GroupByQuery query,
      List<ListenableFuture<AggregateResult>> futures,
      boolean hasTimeout,
      long timeout
  )
  {
    ListenableFuture<List<AggregateResult>> future = Futures.allAsList(futures);
    try {
      if (queryWatcher != null) {
        queryWatcher.registerQueryFuture(query, future);
      }

      if (hasTimeout && timeout <= 0) {
        throw new TimeoutException();
      }

      final List<AggregateResult> results = hasTimeout ? future.get(timeout, TimeUnit.MILLISECONDS) : future.get();

      for (AggregateResult result : results) {
        if (!result.isOk()) {
          GuavaUtils.cancelAll(true, future, futures);
          throw new ResourceLimitExceededException(result.getReason());
        }
      }
    }
    catch (InterruptedException e) {
      log.warn(e, "Query interrupted, cancelling pending results, query id [%s]", query.getId());
      GuavaUtils.cancelAll(true, future, futures);
      throw new QueryInterruptedException(e);
    }
    catch (CancellationException e) {
      GuavaUtils.cancelAll(true, future, futures);
      throw new QueryInterruptedException(e);
    }
    catch (TimeoutException e) {
      log.info("Query timeout, cancelling pending results for query id [%s]", query.getId());
      GuavaUtils.cancelAll(true, future, futures);
      throw new QueryInterruptedException(e);
    }
    catch (ExecutionException e) {
      GuavaUtils.cancelAll(true, future, futures);
      throw new RuntimeException(e);
    }
  }

}
