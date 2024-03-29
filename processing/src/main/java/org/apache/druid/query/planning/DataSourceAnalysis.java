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

package org.apache.druid.query.planning;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.spec.QuerySegmentSpec;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Analysis of a datasource for purposes of deciding how to execute a particular query.
 * 分析数据源
 * The analysis breaks a datasource down in the following way:
 * 通过以下方式分析数据源
 *
 * <pre>
 *
 *                             Q  <-- Possible query datasource(s) [may be none, or multiple stacked]
 *                             |
 *                             Q  <-- Base query datasource, returned by {@link #getBaseQuery()} if it exists
 *                             |
 *                             J  <-- Possible join tree, expected to be left-leaning
 *                            / \
 *                           J  Dj <--  Other leaf datasources
 *   Base datasource        / \         which will be joined
 *  (bottom-leftmost) -->  Db Dj  <---- into the base datasource
 *
 * </pre>
 *
 * The base datasource (Db) is returned by {@link #getBaseDataSource()}. The other leaf datasources are returned by
 * {@link #getPreJoinableClauses()}. The outer query datasources are available as part of {@link #getDataSource()},
 * which just returns the original datasource that was provided for analysis.
 *
 * The base datasource (Db) will never be a join, but it can be any other type of datasource (table, query, etc).
 * Note that join trees are only flattened if they occur at the top of the overall tree (or underneath an outer query),
 * and that join trees are only flattened to the degree that they are left-leaning. Due to these facts, it is possible
 * for the base or leaf datasources to include additional joins.
 *
 * The base datasource is the one that will be considered by the core Druid query stack for scanning via
 * {@link org.apache.druid.segment.Segment} and {@link org.apache.druid.segment.StorageAdapter}. The other leaf
 * datasources must be joinable onto the base data.
 *
 * The idea here is to keep things simple and dumb. So we focus only on identifying left-leaning join trees, which map
 * neatly onto a series of hash table lookups at query time. The user/system generating the queries, e.g. the druid-sql
 * layer (or the end user in the case of native queries), is responsible for containing the smarts to structure the
 * tree in a way that will lead to optimal execution.
 */
public class DataSourceAnalysis
{
  private final DataSource dataSource;
  private final DataSource baseDataSource;
  @Nullable
  private final Query<?> baseQuery;
  private final List<PreJoinableClause> preJoinableClauses;

  private static final Logger log = new Logger(DataSourceAnalysis.class);

  private DataSourceAnalysis(
      DataSource dataSource,
      DataSource baseDataSource,
      @Nullable Query<?> baseQuery,
      List<PreJoinableClause> preJoinableClauses
  )
  {
    if (baseDataSource instanceof JoinDataSource) {
      // The base cannot be a join (this is a class invariant).
      // If it happens, it's a bug in the datasource analyzer.
      throw new IAE("Base dataSource cannot be a join! Original dataSource was: %s", dataSource);
    }

    this.dataSource = dataSource;
    this.baseDataSource = baseDataSource;
    this.baseQuery = baseQuery;
    this.preJoinableClauses = preJoinableClauses;
  }

  /**
   * 简单来说此方法是对一些特殊数据源类型参数的处理。
   * 如果是QueryDataSource类型，则获取出其中的子查询subQuery对象及对应数据源，放入DataSourceAnalysis对象封装。
   * 如果是JoinDataSource类型，则将原始数据源拆分成基础数据源和被连接数据源，分别放入DataSourceAnalysis对象封装。
   *
   * 如果不是以上两者，则不作任何处理，直接将数据源放入DataSourceAnalysis对象封装。
   *
   * @param dataSource
   */
  public static DataSourceAnalysis forDataSource(final DataSource dataSource)
  {
//    log.info("!!!select：forDataSource，进入时datasource："+dataSource.getClass());
    // Strip outer queries, retaining querySegmentSpecs as we go down (lowest will become the 'baseQuerySegmentSpec').
    Query<?> baseQuery = null;
    // table datasource
    DataSource current = dataSource;

    //从QueryDataSource中提炼出“子查询对象”，然后将baseQuery赋值成自查询对象，current也赋值成子查询的datasource
    while (current instanceof QueryDataSource) {
      final Query<?> subQuery = ((QueryDataSource) current).getQuery();

      if (!(subQuery instanceof BaseQuery)) {
        // We must verify that the subQuery is a BaseQuery, because it is required to make "getBaseQuerySegmentSpec"
        // work properly. All builtin query types are BaseQuery, so we only expect this with funky extension queries.
        throw new IAE("Cannot analyze subquery of class[%s]", subQuery.getClass().getName());
      }

      baseQuery = subQuery;
      current = subQuery.getDataSource();
    }

    /**
     * 如果是JoinDataSource类型，
     * 则{@link this#flattenJoin(JoinDataSource)}，
     * 将数据源解析成两部分并返回，left为基础数据源。right为联接数据源
     *
     * baseQuery还是null
     */
    if (current instanceof JoinDataSource) {
      final Pair<DataSource, List<PreJoinableClause>> flattened = flattenJoin((JoinDataSource) current);
      return new DataSourceAnalysis(dataSource, flattened.lhs, baseQuery, flattened.rhs);
    } else {

      /**
       * 如果既不是query datasource，也不是join datasource，（比如一般的json查询就是TableDataSource类型）
       * 则创建一个新的DataSourceAnalysis对象，并将这些参数存进去
       * 其中baseQuery为空，
       * dataSource=current
       */
      return new DataSourceAnalysis(dataSource, current, baseQuery, Collections.emptyList());
    }
  }

  /**
   * Flatten a datasource into two parts: the left-hand side datasource (the 'base' datasource), and a list of join
   * clauses, if any.
   *
   * @throws IllegalArgumentException if dataSource cannot be fully flattened.
   */
  private static Pair<DataSource, List<PreJoinableClause>> flattenJoin(final JoinDataSource dataSource)
  {
    DataSource current = dataSource;
    final List<PreJoinableClause> preJoinableClauses = new ArrayList<>();

    while (current instanceof JoinDataSource) {
      final JoinDataSource joinDataSource = (JoinDataSource) current;
      current = joinDataSource.getLeft();
      preJoinableClauses.add(
          new PreJoinableClause(
              joinDataSource.getRightPrefix(),
              joinDataSource.getRight(),
              joinDataSource.getJoinType(),
              joinDataSource.getConditionAnalysis()
          )
      );
    }

    // Join clauses were added in the order we saw them while traversing down, but we need to apply them in the
    // going-up order. So reverse them.
    Collections.reverse(preJoinableClauses);

    return Pair.of(current, preJoinableClauses);
  }

  /**
   * Returns the topmost datasource: the original one passed to {@link #forDataSource(DataSource)}.
   */
  public DataSource getDataSource()
  {
    return dataSource;
  }

  /**
   * Returns the base (bottom-leftmost) datasource.
   */
  public DataSource getBaseDataSource()
  {
    return baseDataSource;
  }

  /**
   * If {@link #getBaseDataSource()} is a {@link TableDataSource}, returns it. Otherwise, returns an empty Optional.
   *
   * Note that this can return empty even if {@link #isConcreteTableBased()} is true. This happens if the base
   * datasource is a {@link UnionDataSource} of {@link TableDataSource}.
   */
  public Optional<TableDataSource> getBaseTableDataSource()
  {
    if (baseDataSource instanceof TableDataSource) {
      return Optional.of((TableDataSource) baseDataSource);
    } else {
      return Optional.empty();
    }
  }

  /**
   * If {@link #getBaseDataSource()} is a {@link UnionDataSource}, returns it. Otherwise, returns an empty Optional.
   */
  public Optional<UnionDataSource> getBaseUnionDataSource()
  {
    if (baseDataSource instanceof UnionDataSource) {
      return Optional.of((UnionDataSource) baseDataSource);
    } else {
      return Optional.empty();
    }
  }

  /**
   * Returns the bottommost (i.e. innermost) {@link Query} from a possible stack of outer queries at the root of
   * the datasource tree. This is the query that will be applied to the base datasource plus any joinables that might
   * be present.
   *
   * @return the query associated with the base datasource if {@link #isQuery()} is true, else empty
   */
  public Optional<Query<?>> getBaseQuery()
  {
    return Optional.ofNullable(baseQuery);
  }

  /**
   * Returns the {@link QuerySegmentSpec} that is associated with the base datasource, if any. This only happens
   * when there is an outer query datasource. In this case, the base querySegmentSpec is the one associated with the
   * innermost subquery.
   *
   * This {@link QuerySegmentSpec} is taken from the query returned by {@link #getBaseQuery()}.
   *
   * @return the query segment spec associated with the base datasource if {@link #isQuery()} is true, else empty
   */
  public Optional<QuerySegmentSpec> getBaseQuerySegmentSpec()
  {
    /**
     * getBaseQuery().map(query -> ((BaseQuery<?>) query) 防空指针用的
     * getQuerySegmentSpec返回的对象，其实是json中intervals键值对，
     * “也就是查询时指定的时间范围”
     */
    return getBaseQuery().map(query -> ((BaseQuery<?>) query).getQuerySegmentSpec());
  }

  /**
   * Returns join clauses corresponding to joinable leaf datasources (every leaf except the bottom-leftmost).
   */
  public List<PreJoinableClause> getPreJoinableClauses()
  {
    return preJoinableClauses;
  }

  /**
   * Returns true if all servers have the ability to compute this datasource. These datasources depend only on
   * globally broadcast data, like lookups or inline data or broadcast segments.
   */
  public boolean isGlobal()
  {
    return dataSource.isGlobal();
  }

  /**
   * Returns true if this datasource can be computed by the core Druid query stack via a scan of a concrete base
   * datasource. All other datasources involved, if any, must be global.
   */
  public boolean isConcreteBased()
  {
    return baseDataSource.isConcrete() && preJoinableClauses.stream()
                                                            .allMatch(clause -> clause.getDataSource().isGlobal());
  }

  /**
   * Returns true if this datasource is concrete-based (see {@link #isConcreteBased()}, and the base datasource is a
   * {@link TableDataSource} or a {@link UnionDataSource} composed entirely of {@link TableDataSource}. This is an
   * important property, because it corresponds to datasources that can be handled by Druid's distributed query stack.
   */
  public boolean isConcreteTableBased()
  {
    // At the time of writing this comment, UnionDataSource children are required to be tables, so the instanceof
    // check is redundant. But in the future, we will likely want to support unions of things other than tables,
    // so check anyway for future-proofing.
    return isConcreteBased() && (baseDataSource instanceof TableDataSource
                                 || (baseDataSource instanceof UnionDataSource &&
                                     baseDataSource.getChildren()
                                                   .stream()
                                                   .allMatch(ds -> ds instanceof TableDataSource)));
  }

  /**
   * Returns true if this datasource represents a subquery (that is, whether it is a {@link QueryDataSource}).
   */
  public boolean isQuery()
  {
    return dataSource instanceof QueryDataSource;
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
    DataSourceAnalysis that = (DataSourceAnalysis) o;
    return Objects.equals(dataSource, that.dataSource);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSource);
  }

  @Override
  public String toString()
  {
    return "DataSourceAnalysis{" +
           "dataSource=" + dataSource +
           ", baseDataSource=" + baseDataSource +
           ", baseQuery=" + baseQuery +
           ", preJoinableClauses=" + preJoinableClauses +
           '}';
  }
}
