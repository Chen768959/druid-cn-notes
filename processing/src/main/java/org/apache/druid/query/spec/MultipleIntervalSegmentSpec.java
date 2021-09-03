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

package org.apache.druid.query.spec;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.List;

/**
 * 该对象类似于Interval时间段属性的封装。
 *
 * 在一次查询请求中，一般要定义json中的intervals属性，
 * 而这个MultipleIntervalSegmentSpec就是该属性的封装
 */
public class MultipleIntervalSegmentSpec implements QuerySegmentSpec
{
  private static final Logger log = new Logger(MultipleSpecificSegmentSpec.class);
  private final List<Interval> intervals;

  @JsonCreator
  public MultipleIntervalSegmentSpec(
      @JsonProperty("intervals") List<Interval> intervals
  )
  {
    this.intervals = Collections.unmodifiableList(JodaUtils.condenseIntervals(intervals));
  }

  @Override
  @JsonProperty("intervals")
  public List<Interval> getIntervals()
  {
    return intervals;
  }

  /**
   * lookup本意为“查询”
   * getQueryRunnerForIntervals：字面含义“通过walker，找到intervals时间段对应的QueryRunner”
   */
  @Override
  public <T> QueryRunner<T> lookup(Query<T> query, QuerySegmentWalker walker)
  {
    log.info("!!!：创建MultipleSpecificSegmentSpec，MultipleIntervalSegmentSpec调用lookup");
    /**
     * query就是外面调用的query
     * {@link org.apache.druid.server.ClientQuerySegmentWalker#getQueryRunnerForIntervals(Query, Iterable)}
     */
    return walker.getQueryRunnerForIntervals(query, intervals);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
           "intervals=" + intervals +
           '}';
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

    MultipleIntervalSegmentSpec that = (MultipleIntervalSegmentSpec) o;

    if (intervals != null ? !intervals.equals(that.intervals) : that.intervals != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return intervals != null ? intervals.hashCode() : 0;
  }
}
