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
import com.google.common.collect.Iterables;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.SegmentDescriptor;
import org.joda.time.Interval;

import java.util.List;
import java.util.Objects;

/**
 * 1、在broker节点查询时也会调用，会将某台主机上所有待查询的分片信息都封装进来。
 *
 * 2、在his节点查询的初始阶段也会被调用，
 *    会将来自broker节点的查询请求，反序列化成这个对象，
 *    这样就知道此次请求需要查询那些分片信息。
 */
public class MultipleSpecificSegmentSpec implements QuerySegmentSpec
{
  private static final Logger log = new Logger(MultipleSpecificSegmentSpec.class);

  // 所有待查询的分片信息
  private final List<SegmentDescriptor> descriptors;

  private volatile List<Interval> intervals = null;

  /**
   * 在broker节点查询时
   * 由{@link org.apache.druid.query.Queries#withSpecificSegments(Query, List)}调用
   * @param descriptors 某主机上，所有待查询的分片信息
   */
  @JsonCreator
  public MultipleSpecificSegmentSpec(
      @JsonProperty("segments") List<SegmentDescriptor> descriptors
  )
  {
    this.descriptors = descriptors;
//    log.info("!!!：创建MultipleSpecificSegmentSpec，descriptors.size="+descriptors.size());
  }

  @JsonProperty("segments")
  public List<SegmentDescriptor> getDescriptors()
  {
    return descriptors;
  }

  @Override
  public List<Interval> getIntervals()
  {
//    log.info("!!!：创建MultipleSpecificSegmentSpec，调用getIntervals()");
    if (intervals != null) {
      return intervals;
    }

    intervals = JodaUtils.condenseIntervals(
        Iterables.transform(
            descriptors,
            input -> input.getInterval()
        )
    );

    return intervals;
  }

  /**
   * his节点调用
   * lookup本意为“查询”
   * getQueryRunnerForIntervals：字面含义“通过walker，找到descriptors(segment信息列表)对应的QueryRunner”
   */
  @Override
  public <T> QueryRunner<T> lookup(Query<T> query, QuerySegmentWalker walker)
  {
//    log.info("!!!：创建MultipleSpecificSegmentSpec，spec调用lookup");
    /**
     * walker是his节点启动时注入进来的
     * query是此次查询对象
     * descriptors为此次查询涉及到那些segment信息列表
     */
    return walker.getQueryRunnerForSegments(query, descriptors);
  }

  @Override
  public String toString()
  {
    return "MultipleSpecificSegmentSpec{" +
           "descriptors=" + descriptors +
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
    MultipleSpecificSegmentSpec that = (MultipleSpecificSegmentSpec) o;
    return Objects.equals(descriptors, that.descriptors);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(descriptors);
  }
}
