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

package org.apache.druid.query.groupby;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.groupby.epinephelinae.GroupByMergingQueryRunnerV2;
import org.apache.druid.query.groupby.strategy.GroupByStrategy;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;

import java.util.concurrent.ExecutorService;

/**
 *
 */
public class GroupByQueryRunnerFactory implements QueryRunnerFactory<ResultRow, GroupByQuery>
{
  private static final Logger log = new Logger(GroupByQueryRunnerFactory.class);

  private final GroupByStrategySelector strategySelector;
  private final GroupByQueryQueryToolChest toolChest;

  @Inject
  public GroupByQueryRunnerFactory(
      GroupByStrategySelector strategySelector,
      GroupByQueryQueryToolChest toolChest
  )
  {
    this.strategySelector = strategySelector;
    this.toolChest = toolChest;
  }

  @Override
  public QueryRunner<ResultRow> createRunner(final Segment segment)
  {
    return new GroupByQueryRunner(segment, strategySelector);
  }

  @Override
  public QueryRunner<ResultRow> mergeRunners(
      final ExecutorService exec,
      final Iterable<QueryRunner<ResultRow>> queryRunners
  )
  {
    // mergeRunners should take ListeningExecutorService at some point
    final ListeningExecutorService queryExecutor = MoreExecutors.listeningDecorator(exec);

    return new QueryRunner<ResultRow>()
    {
      @Override
      public Sequence<ResultRow> run(QueryPlus<ResultRow> queryPlus, ResponseContext responseContext)
      {

        GroupByStrategy groupByStrategy =strategySelector.strategize((GroupByQuery) queryPlus.getQuery());
        /**
         * groupByStrategy类型为{@link org.apache.druid.query.groupby.strategy.GroupByStrategyV2}
         */
        log.info("!!!：进入mergeRunners中匿名QueryRunner，groupByStrategy类型为："+groupByStrategy.getClass());
        /**
         * executor类型{@link org.apache.druid.query.MetricsEmittingExecutorService}
         */
        log.info("!!!：进入mergeRunners中匿名QueryRunner，queryExecutor类型为："+queryExecutor.getClass());
        for (QueryRunner<ResultRow> runner:queryRunners){
          /**
           * 包含{@link org.apache.druid.server.SetAndVerifyContextQueryRunner}
           */
          log.info("!!!：进入mergeRunners中匿名QueryRunner，queryRunners中包含："+runner.getClass());
        }
        QueryRunner<ResultRow> rowQueryRunner = groupByStrategy.mergeRunners(queryExecutor, queryRunners);

        /**
         * 此处调用的是{@link GroupByMergingQueryRunnerV2#run(QueryPlus, ResponseContext)}
         */
        return rowQueryRunner.run(queryPlus, responseContext);
      }
    };
  }

  @Override
  public QueryToolChest<ResultRow, GroupByQuery> getToolchest()
  {
    return toolChest;
  }

  private static class GroupByQueryRunner implements QueryRunner<ResultRow>
  {
    private final StorageAdapter adapter;
    private final GroupByStrategySelector strategySelector;

    public GroupByQueryRunner(Segment segment, final GroupByStrategySelector strategySelector)
    {
      this.adapter = segment.asStorageAdapter();
      this.strategySelector = strategySelector;
    }

    @Override
    public Sequence<ResultRow> run(QueryPlus<ResultRow> queryPlus, ResponseContext responseContext)
    {
      log.info("!!!：进入GroupByQueryRunner，run方法");
      Query<ResultRow> query = queryPlus.getQuery();
      if (!(query instanceof GroupByQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", query.getClass(), GroupByQuery.class);
      }

      GroupByStrategy groupByStrategy = strategySelector.strategize((GroupByQuery) query);

      log.info("!!!：进入GroupByQueryRunner，groupByStrategy为："+groupByStrategy.getClass());
      /**
       * {@link org.apache.druid.query.groupby.epinephelinae.GroupByQueryEngineV2#process(GroupByQuery, StorageAdapter, NonBlockingPool, GroupByQueryConfig)}
       */
      return groupByStrategy.process((GroupByQuery) query, adapter);
    }
  }

  @VisibleForTesting
  public GroupByStrategySelector getStrategySelector()
  {
    return strategySelector;
  }
}
