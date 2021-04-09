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
import org.apache.druid.segment.SegmentReference;

import java.util.concurrent.ExecutorService;

public class ReferenceCountingSegmentQueryRunner<T> implements QueryRunner<T>
{
  private static final Logger log = new Logger(ReferenceCountingSegmentQueryRunner.class);

  private final QueryRunnerFactory<T, Query<T>> factory;
  private final SegmentReference segment;
  private final SegmentDescriptor descriptor;

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

  @Override
  public Sequence<T> run(final QueryPlus<T> queryPlus, ResponseContext responseContext)
  {
    return segment.acquireReferences().map(closeable -> {
      try {
        QueryRunner<T> queryRunner = factory.createRunner(segment);
        /**
         * 进入{@link org.apache.druid.query.groupby.GroupByQueryRunnerFactory#mergeRunners(ExecutorService, Iterable)}
         * 然后返回{@link org.apache.druid.query.groupby.GroupByQueryRunnerFactory.GroupByQueryRunner#run(QueryPlus, ResponseContext)}
         */
        log.info("!!!：ReferenceCountingSegmentQueryRunner中factory.createRunner(segment)为："+queryRunner.getClass());
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
