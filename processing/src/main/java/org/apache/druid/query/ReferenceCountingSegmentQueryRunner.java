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
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentReference;

import java.util.concurrent.ExecutorService;

/**
 * 查询请求在构建queryrunner链时，该runner处于最尾端（即最后一个被调用run）
 *
 */
public class ReferenceCountingSegmentQueryRunner<T> implements QueryRunner<T>
{
  private static final Logger log = new Logger(ReferenceCountingSegmentQueryRunner.class);

  private final QueryRunnerFactory<T, Query<T>> factory;
  private final SegmentReference segment;
  private final SegmentDescriptor descriptor;

  /**
   *
   * @param factory 根据query的class类型(如groupby查询就是GroupByQuery)来获取QueryRunnerFactory对象
   * @param segment 查询的segment的实际数据对象
   * @param descriptor 该segment的描述信息
   */
  public ReferenceCountingSegmentQueryRunner(
      QueryRunnerFactory<T, Query<T>> factory,
      SegmentReference segment,
      SegmentDescriptor descriptor
  )
  {
    this.factory = factory;
    this.segment = segment;
    this.descriptor = descriptor;
  }

  /**
   * 1、使用factory创建queryRunner（并将segment传入其中）
   * 2、调用刚刚创建的QueryRunner的run方法，获取Sequence结果
   */
  @Override
  public Sequence<T> run(final QueryPlus<T> queryPlus, ResponseContext responseContext)
  {
    return segment.acquireReferences().map(closeable -> {
      try {
        /**
         * 创建queryrunner，并将segment数据对象装进去
         * 如果是groupby查询，则使用GroupByQueryRunnerFactory创建runner，方法如下：
         * {@link org.apache.druid.query.groupby.GroupByQueryRunnerFactory#createRunner(Segment)}
         * 创建GroupByQueryRunner
         */
        QueryRunner<T> queryRunner = factory.createRunner(segment);

        log.info("!!!：ReferenceCountingSegmentQueryRunner中factory.createRunner(segment)为："+queryRunner.getClass());
        /**
         * {@link org.apache.druid.query.groupby.GroupByQueryRunnerFactory.GroupByQueryRunner#run(QueryPlus, ResponseContext)}
         */
        final Sequence<T> baseSequence = queryRunner.run(queryPlus, responseContext);
        return Sequences.withBaggage(baseSequence, closeable);
      }
      catch (Throwable t) {
        try {
          closeable.close();
        }
        catch (Exception e) {
          t.addSuppressed(e);
        }
        throw t;
      }
    }).orElseGet(() -> {
      log.info("!!!：ReferenceCountingSegmentQueryRunner未获取到acquireReferences值");
      return new ReportTimelineMissingSegmentQueryRunner<T>(descriptor).run(queryPlus, responseContext);
    });
  }
}
