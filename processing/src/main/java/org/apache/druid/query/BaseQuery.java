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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 *
 */
@ExtensionPoint
public abstract class BaseQuery<T> implements Query<T>
{
  private static final Logger log = new Logger(BaseQuery.class);
  public static void checkInterrupted()
  {
    if (Thread.interrupted()) {
      throw new QueryInterruptedException(new InterruptedException());
    }
  }

  public static final String QUERY_ID = "queryId";
  public static final String SUB_QUERY_ID = "subQueryId";
  public static final String SQL_QUERY_ID = "sqlQueryId";
  private final DataSource dataSource;
  private final boolean descending;
  private final Map<String, Object> context;
  private final QuerySegmentSpec querySegmentSpec;
  private volatile Duration duration;
  private final Granularity granularity;

  public BaseQuery(
      DataSource dataSource,
      QuerySegmentSpec querySegmentSpec,
      boolean descending,
      Map<String, Object> context
  )
  {
    this(dataSource, querySegmentSpec, descending, context, Granularities.ALL);
  }

  public BaseQuery(
      DataSource dataSource,
      QuerySegmentSpec querySegmentSpec,
      boolean descending,
      Map<String, Object> context,
      Granularity granularity
  )
  {
    Preconditions.checkNotNull(dataSource, "dataSource can't be null");
    Preconditions.checkNotNull(querySegmentSpec, "querySegmentSpec can't be null");
    Preconditions.checkNotNull(granularity, "Must specify a granularity");

    this.dataSource = dataSource;
    this.context = context;
    this.querySegmentSpec = querySegmentSpec;
    this.descending = descending;
    this.granularity = granularity;
  }

  @JsonProperty
  @Override
  public DataSource getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  @Override
  public boolean isDescending()
  {
    return descending;
  }

  @JsonProperty("intervals")
  public QuerySegmentSpec getQuerySegmentSpec()
  {
    return querySegmentSpec;
  }

  /**
   * groupby请求到达之初，
   * 请求对象被封装成{@link GroupByQuery}，
   * 然后调用该query对象的getRunner方法，但是所有子对象中都没有此方法，所以实际调用的是以下方法。
   *
   *
   */
  @Override
  public QueryRunner<T> getRunner(QuerySegmentWalker walker)
  {
    /**
     * walker为启动时注入而来，
     * broker和historical启动时注入的对象不同
     * broker启动时walker为{@link org.apache.druid.server.ClientQuerySegmentWalker}
     * historical启动时walker为{@link org.apache.druid.server.coordination.ServerManager}
     *
     * 这两个walker不同，也决定了broker和historical在面对查询请求时，其二者的处理逻辑不同
     */
    log.info("!!!select：BaseQuery内getRunner时，walker类型："+walker.getClass());

    /**
     * 此处的this就是当前query对象，也是此次的请求对象，
     * 在groupby请求中，为{@link GroupByQuery}，是请求参数的封装。
     *
     * 无论是broker还是historical都是调用以下逻辑
     * 1、获取QuerySegmentSpec：
     * {@link this#getQuerySegmentSpecForLookUp(BaseQuery)}
     * 首先是从“请求对象”中获取，其次获取的QuerySegmentSpec是“此次请求涉及segment的信息列表（的封装）”
     *
     * 2、QuerySegmentSpec调用lookup方法
     * {@link org.apache.druid.query.spec.MultipleSpecificSegmentSpec#lookup(Query, QuerySegmentWalker)}
     * 这是“intervals封装对象”提供的方法，该方法只有一行代码：
     * “walker.getQueryRunnerForSegments(query, descriptors)”
     *
     *
     * 3、通过传入的walker，获取queryrunner
     * broker节点：{@link org.apache.druid.server.ClientQuerySegmentWalker#getQueryRunnerForSegments(Query, Iterable)}
     * historical节点：{@link org.apache.druid.server.coordination.ServerManager#getQueryRunnerForSegments(Query, Iterable)}
     */
    return getQuerySegmentSpecForLookUp(this).lookup(this, walker);
  }

  /**
   * 从请求对象中获取此次查询“涉及哪些segment”（至于请求对象中为何会有该内容，怀疑是broker节点通过intervals属性查出后，在请求his节点时传入）
   * 然后返回{@link org.apache.druid.query.spec.MultipleSpecificSegmentSpec}
   * 该对象是对“List<SegmentDescriptor>”的封装，单个SegmentDescriptor代表某个segment的信息
   *
   * @param query 此次查询的请求对象
   * @return 此次查询涉及到的segments的描述信息的封装：{@link org.apache.druid.query.spec.MultipleSpecificSegmentSpec}。
   */
  @VisibleForTesting
  public static QuerySegmentSpec getQuerySegmentSpecForLookUp(BaseQuery<?> query)
  {
    /**
     * 1、DataSourceAnalysis.forDataSource：
     * 获取数据源，（暂按照table数据源来分析）。
     * 入参的datasource来自于此处查询请求的参数中，暂定table datasource
     *
     * 如果是table datasource，
     * 则此处只是创建一个新的DataSourceAnalysis对象，
     * 然后将datasource、“Query对象”，这些参数存进去。
     *
     * 2、getBaseQuerySegmentSpec()：
     * {@link DataSourceAnalysis#getBaseQuerySegmentSpec()}
     * |->{@link BaseQuery#getQuerySegmentSpec()}
     * 此处的BaseQuery就是此次请求对象（本质是{@link GroupByQuery}），
     * getQuerySegmentSpec就是从请求对象中获取QuerySegmentSpec，即此次请求涉及哪些segment的信息列表
     *
     * （暂不考虑）.orElseGet(query::getQuerySegmentSpec)：
     * 如果上一步获取不到，则使用getQuerySegmentSpec()获取
     *
     */
    return DataSourceAnalysis.forDataSource(query.getDataSource())
                             .getBaseQuerySegmentSpec()
                             .orElseGet(query::getQuerySegmentSpec);
  }

  @Override
  public List<Interval> getIntervals()
  {
    return querySegmentSpec.getIntervals();
  }

  @Override
  public Duration getDuration()
  {
    if (duration == null) {
      Duration totalDuration = new Duration(0);
      for (Interval interval : querySegmentSpec.getIntervals()) {
        if (interval != null) {
          totalDuration = totalDuration.plus(interval.toDuration());
        }
      }
      duration = totalDuration;
    }

    return duration;
  }

  @Override
  @JsonProperty
  public Granularity getGranularity()
  {
    return granularity;
  }

  @Override
  public DateTimeZone getTimezone()
  {
    return granularity instanceof PeriodGranularity
           ? ((PeriodGranularity) granularity).getTimeZone()
           : DateTimeZone.UTC;
  }

  @Override
  @JsonProperty
  public Map<String, Object> getContext()
  {
    return context;
  }

  @Override
  public <ContextType> ContextType getContextValue(String key)
  {
    return context == null ? null : (ContextType) context.get(key);
  }

  @Override
  public <ContextType> ContextType getContextValue(String key, ContextType defaultValue)
  {
    ContextType retVal = getContextValue(key);
    return retVal == null ? defaultValue : retVal;
  }

  @Override
  public boolean getContextBoolean(String key, boolean defaultValue)
  {
    return QueryContexts.parseBoolean(this, key, defaultValue);
  }

  /**
   * @deprecated use {@link #computeOverriddenContext(Map, Map) computeOverriddenContext(getContext(), overrides))}
   * instead. This method may be removed in the next minor or major version of Druid.
   */
  @Deprecated
  protected Map<String, Object> computeOverridenContext(final Map<String, Object> overrides)
  {
    return computeOverriddenContext(getContext(), overrides);
  }

  public static Map<String, Object> computeOverriddenContext(
      final Map<String, Object> context,
      final Map<String, Object> overrides
  )
  {
    Map<String, Object> overridden = new TreeMap<>();
    if (context != null) {
      overridden.putAll(context);
    }
    overridden.putAll(overrides);

    return overridden;
  }

  /**
   * Default implementation of {@link Query#getResultOrdering()} that uses {@link Ordering#natural()}.
   *
   * If your query result type T is not Comparable, you must override this method.
   */
  @Override
  @SuppressWarnings("unchecked") // assumes T is Comparable; see method javadoc
  public Ordering<T> getResultOrdering()
  {
    Ordering retVal = Ordering.natural();
    return descending ? retVal.reverse() : retVal;
  }

  @Nullable
  @Override
  public String getId()
  {
    return (String) getContextValue(QUERY_ID);
  }

  @Override
  public Query<T> withSubQueryId(String subQueryId)
  {
    return withOverriddenContext(ImmutableMap.of(SUB_QUERY_ID, subQueryId));
  }

  @Nullable
  @Override
  public String getSubQueryId()
  {
    return (String) getContextValue(SUB_QUERY_ID);
  }

  @Override
  public Query<T> withId(String id)
  {
    return withOverriddenContext(ImmutableMap.of(QUERY_ID, id));
  }

  @Nullable
  @Override
  public String getSqlQueryId()
  {
    return (String) getContextValue(SQL_QUERY_ID);
  }

  @Override
  public Query<T> withSqlQueryId(String sqlQueryId)
  {
    return withOverriddenContext(ImmutableMap.of(SQL_QUERY_ID, sqlQueryId));
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BaseQuery<?> baseQuery = (BaseQuery<?>) o;

    // Must use getDuration() instead of "duration" because duration is lazily computed.
    return descending == baseQuery.descending &&
           Objects.equals(dataSource, baseQuery.dataSource) &&
           Objects.equals(context, baseQuery.context) &&
           Objects.equals(querySegmentSpec, baseQuery.querySegmentSpec) &&
           Objects.equals(getDuration(), baseQuery.getDuration()) &&
           Objects.equals(granularity, baseQuery.granularity);
  }

  @Override
  public int hashCode()
  {
    // Must use getDuration() instead of "duration" because duration is lazily computed.
    return Objects.hash(dataSource, descending, context, querySegmentSpec, getDuration(), granularity);
  }
}
