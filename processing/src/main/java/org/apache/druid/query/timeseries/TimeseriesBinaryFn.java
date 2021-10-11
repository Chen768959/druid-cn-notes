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

package org.apache.druid.query.timeseries;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.granularity.AllGranularity;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregatorFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;

/**
 */
public class TimeseriesBinaryFn implements BinaryOperator<Result<TimeseriesResultValue>>
{
  private static final EmittingLogger log = new EmittingLogger(DruidProcessingConfig.class);

  private final Granularity gran;
  private final List<AggregatorFactory> aggregations;

  public TimeseriesBinaryFn(
      Granularity granularity,
      List<AggregatorFactory> aggregations
  )
  {
    this.gran = granularity;
    this.aggregations = aggregations;
  }

  @Override
  public Result<TimeseriesResultValue> apply(Result<TimeseriesResultValue> arg1, Result<TimeseriesResultValue> arg2)
  {
    for (AggregatorFactory factory : aggregations) {
      log.info("!!!：his节点合并runner，执行runner，pre,遍历factory："+factory.getClass()+"...arg1:"+arg1+"...arg2:"+arg2);
    }
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      if (arg1!=null){
        log.info("!!!：his节点合并runner，执行runner，pre,json_arg1："+objectMapper.writeValueAsString(arg1));
      }
      if (arg2!=null){
        log.info("!!!：his节点合并runner，执行runner，pre,json_arg2："+objectMapper.writeValueAsString(arg2));
      }
    }catch (Exception e){
      log.info("!!!：his节点合并runner，执行runner，pre,解析arg失败");
    }

    if (arg1 == null) {
      return arg2;
    }

    if (arg2 == null) {
      return arg1;
    }

    TimeseriesResultValue arg1Val = arg1.getValue();
    TimeseriesResultValue arg2Val = arg2.getValue();

    Map<String, Object> retVal = new LinkedHashMap<String, Object>();

    for (AggregatorFactory factory : aggregations) {
      log.info("!!!：his节点合并runner，执行runner，遍历factory："+factory.getClass());
      final String metricName = factory.getName();
      retVal.put(metricName, factory.combine(arg1Val.getMetric(metricName), arg2Val.getMetric(metricName)));
    }

    return (gran instanceof AllGranularity) ?
           new Result<TimeseriesResultValue>(
               arg1.getTimestamp(),
               new TimeseriesResultValue(retVal)
           ) :
           new Result<TimeseriesResultValue>(
               gran.bucketStart(arg1.getTimestamp()),
               new TimeseriesResultValue(retVal)
           );
  }

}
