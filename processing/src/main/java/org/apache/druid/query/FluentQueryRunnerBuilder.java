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

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.context.ResponseContext;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class FluentQueryRunnerBuilder<T>
{
  private static final Logger log = new Logger(FluentQueryRunnerBuilder.class);

  final QueryToolChest<T, Query<T>> toolChest;

  public FluentQueryRunner create(QueryRunner<T> baseRunner)
  {
    return new FluentQueryRunner(baseRunner);
  }

  public FluentQueryRunnerBuilder(QueryToolChest<T, Query<T>> toolChest)
  {
    this.toolChest = toolChest;
  }

  public class FluentQueryRunner implements QueryRunner<T>
  {
    private QueryRunner<T> baseRunner;

    public FluentQueryRunner(QueryRunner<T> runner)
    {
      this.baseRunner = runner;
    }

    @Override
    public Sequence<T> run(QueryPlus<T> queryPlus, ResponseContext responseContext)
    {
//      log.info("!!!：FluentQueryRunner中baseRunner为："+baseRunner.getClass());
      return baseRunner.run(queryPlus, responseContext);
    }

    public FluentQueryRunner from(QueryRunner<T> runner)
    {
      return new FluentQueryRunner(runner);
    }

    public FluentQueryRunner applyPostMergeDecoration()
    {
      return from(new FinalizeResultsQueryRunner<>(toolChest.postMergeQueryDecoration(baseRunner), toolChest));
    }

    public FluentQueryRunner applyPreMergeDecoration()
    {
      return from(new UnionQueryRunner<>(toolChest.preMergeQueryDecoration(baseRunner)));
    }

    public FluentQueryRunner emitCPUTimeMetric(ServiceEmitter emitter)
    {
      return emitCPUTimeMetric(emitter, new AtomicLong(0L));
    }

    public FluentQueryRunner emitCPUTimeMetric(ServiceEmitter emitter, AtomicLong accumulator)
    {
      return from(
          CPUTimeMetricQueryRunner.safeBuild(
              baseRunner,
              toolChest,
              emitter,
              accumulator,
              true
          )
      );
    }

    public FluentQueryRunner postProcess(PostProcessingOperator<T> postProcessing)
    {
      return from(postProcessing != null ? postProcessing.postProcess(baseRunner) : baseRunner);
    }

    public FluentQueryRunner mergeResults()
    {
      return from(toolChest.mergeResults(baseRunner));
    }

    public FluentQueryRunner map(final Function<QueryRunner<T>, QueryRunner<T>> mapFn)
    {
      return from(mapFn.apply(baseRunner));
    }
  }
}
