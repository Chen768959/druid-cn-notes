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
import org.apache.druid.common.guava.CombiningSequence;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.context.ResponseContext;

import java.util.Comparator;
import java.util.function.BinaryOperator;
import java.util.function.Function;

/**
 */
@PublicApi
public class ResultMergeQueryRunner<T> extends BySegmentSkippingQueryRunner<T>
{
  private final Function<Query<T>, Comparator<T>> comparatorGenerator;
  private final Function<Query<T>, BinaryOperator<T>> mergeFnGenerator;

  /**
   *
   * @param baseRunner 所有“单分片runner”的查询结果runner
   */
  public ResultMergeQueryRunner(
      QueryRunner<T> baseRunner,
      Function<Query<T>, Comparator<T>> comparatorGenerator,
      Function<Query<T>, BinaryOperator<T>> mergeFnGenerator
  )
  {
    super(baseRunner);
    Preconditions.checkNotNull(comparatorGenerator);
    Preconditions.checkNotNull(mergeFnGenerator);
    this.comparatorGenerator = comparatorGenerator;
    this.mergeFnGenerator = mergeFnGenerator;
  }

  @Override
  public Sequence<T> doRun(QueryRunner<T> baseRunner, QueryPlus<T> queryPlus, ResponseContext context)
  {
    Query<T> query = queryPlus.getQuery();
    // 在各个分片的查询结果外面包上一层“CombiningSequence”，该sequence的accumulate方法，会combine每个单分片的结果。
    return CombiningSequence.create(
        // baseRunner中包含了每个分片的agg结果
        baseRunner.run(queryPlus, context), // 执行所有“单分片runner”.run()，获取所有单分片的查询结果
        comparatorGenerator.apply(query),
        mergeFnGenerator.apply(query));
  }
}
