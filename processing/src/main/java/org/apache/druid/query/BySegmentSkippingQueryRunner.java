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
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.groupby.GroupByQueryQueryToolChest;

/**
 */
public abstract class BySegmentSkippingQueryRunner<T> implements QueryRunner<T>
{
  private static final Logger log = new Logger(BySegmentSkippingQueryRunner.class);

  private final QueryRunner<T> baseRunner;

  public BySegmentSkippingQueryRunner(
      QueryRunner<T> baseRunner
  )
  {
    this.baseRunner = baseRunner;
  }

  @Override
  public Sequence<T> run(QueryPlus<T> queryPlus, ResponseContext responseContext)
  {
    if (QueryContexts.isBySegment(queryPlus.getQuery())) {
      log.info("!!!：BySegmentSkippingQueryRunner run方法进入isBySegment if");
      return baseRunner.run(queryPlus, responseContext);
    }

    return doRun(baseRunner, queryPlus, responseContext);
  }

  protected abstract Sequence<T> doRun(QueryRunner<T> baseRunner, QueryPlus<T> queryPlus, ResponseContext context);
}
