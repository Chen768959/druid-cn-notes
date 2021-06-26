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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.query.aggregation.MetricManipulatorFns;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.groupby.GroupByQuery;

/**
 * Query runner that applies {@link QueryToolChest#makePostComputeManipulatorFn(Query, MetricManipulationFn)} to the
 * result stream. It is expected to be the last runner in the pipeline, after results are fully merged.
 *
 * Note that despite the type parameter "T", this runner may not actually return sequences with type T. This most
 * commonly happens when an upstream {@link BySegmentQueryRunner} changes the result stream to type
 * {@code Result<BySegmentResultValue<T>>}, in which case this class will retain the structure, but call the finalizer
 * function on each result in the by-segment list (which may change their type from T to something else).
 */
public class FinalizeResultsQueryRunner<T> implements QueryRunner<T>
{
  private static final Logger log = new Logger(FinalizeResultsQueryRunner.class);

  private final QueryRunner<T> baseRunner;
  private final QueryToolChest<T, Query<T>> toolChest;

  public FinalizeResultsQueryRunner(
      QueryRunner<T> baseRunner,
      QueryToolChest<T, Query<T>> toolChest
  )
  {
    this.baseRunner = baseRunner;
    this.toolChest = toolChest;
  }

  @Override
  public Sequence<T> run(final QueryPlus<T> queryPlus, ResponseContext responseContext)
  {
    /**{@link GroupByQuery}*/
    final Query<T> query = queryPlus.getQuery();
    final boolean isBySegment = QueryContexts.isBySegment(query);
    final boolean shouldFinalize = QueryContexts.isFinalize(query, true);

    final Query<T> queryToRun;
    final Function<T, ?> finalizerFn;
    final MetricManipulationFn metricManipulationFn;

    if (shouldFinalize) {
      queryToRun = query.withOverriddenContext(ImmutableMap.of("finalize", false));
      metricManipulationFn = MetricManipulatorFns.finalizing();
    } else {
      queryToRun = query;
      metricManipulationFn = MetricManipulatorFns.identity();
    }

    if (isBySegment) {
      finalizerFn = new Function<T, Result<BySegmentResultValue<T>>>()
      {
        final Function<T, T> baseFinalizer = toolChest.makePostComputeManipulatorFn(
            query,
            metricManipulationFn
        );

        @Override
        public Result<BySegmentResultValue<T>> apply(T input)
        {
          //noinspection unchecked (input is not actually a T; see class-level javadoc)
          Result<BySegmentResultValueClass<T>> result = (Result<BySegmentResultValueClass<T>>) input;

          if (input == null) {
            throw new ISE("Cannot have a null result!");
          }

          BySegmentResultValue<T> resultsClass = result.getValue();

          return new Result<>(
              result.getTimestamp(),
              new BySegmentResultValueClass<>(
                  Lists.transform(resultsClass.getResults(), baseFinalizer),
                  resultsClass.getSegmentId(),
                  resultsClass.getInterval()
              )
          );
        }
      };
    } else {
      finalizerFn = toolChest.makePostComputeManipulatorFn(query, metricManipulationFn);
    }

    log.info("!!!：FinalizeResultsQueryRunner中baseRunner为："+baseRunner.getClass());
    //noinspection unchecked (Technically unsound, but see class-level javadoc for rationale)
    /**
     * 此处的baseRunner指{@link org.apache.druid.query.groupby.GroupByQueryQueryToolChest#preMergeQueryDecoration(QueryRunner)}
     * 此方法返回的runner
     *
     * his中baseRunner.run为
     * {@link org.apache.druid.query.groupby.GroupByQueryQueryToolChest#mergeResults(QueryRunner)}
     * 响应的匿名函数方法
     *
     * 此处的第一个参数“baseRunner.run(queryPlus.withQuery(queryToRun), responseContext)”
     * 才是真正的Sequences响应结果
     *
     * Sequences.map：
     * 将baseRunner.run的Sequence结果，和finalizerFn函数进行“合并”，然后返回一个新的Sequence。
     * finalizerFn函数会如map函数一样，处理每一个结果行，然后返回结果
     */
    return (Sequence<T>) Sequences.map(
        baseRunner.run(queryPlus.withQuery(queryToRun), responseContext),
        finalizerFn
    );
  }
}
