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

package org.apache.druid.segment.realtime;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.IngestionSpec;
import org.apache.druid.segment.indexing.RealtimeIOConfig;
import org.apache.druid.segment.indexing.RealtimeTuningConfig;

import java.io.IOException;

/**
 * A Fire Department has a Firehose and a Plumber.
 * <p>
 * This is a metaphor for a realtime stream (Firehose) and a coordinator of sinks (Plumber). The Firehose provides the
 * realtime stream of data.  The Plumber directs each drop of water from the firehose into the correct sink and makes
 * sure that the sinks don't overflow.
 *
 * 实时数据流被比喻成Firehose，
 * 分发器sink被比喻成Plumber，
 * Firehose提供实时数据流，Plumber将数据导入正确的目的地
 */
public class FireDepartment extends IngestionSpec<RealtimeIOConfig, RealtimeTuningConfig>
{
  private final DataSchema dataSchema;
  private final RealtimeIOConfig ioConfig;
  private final RealtimeTuningConfig tuningConfig;
  private final FireDepartmentMetrics metrics = new FireDepartmentMetrics();

  @JsonCreator
  public FireDepartment(
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("ioConfig") RealtimeIOConfig ioConfig,
      @JsonProperty("tuningConfig") RealtimeTuningConfig tuningConfig
  )
  {
    super(dataSchema, ioConfig, tuningConfig);
    Preconditions.checkNotNull(dataSchema, "dataSchema");
    Preconditions.checkNotNull(ioConfig, "ioConfig");

    this.dataSchema = dataSchema;
    this.ioConfig = ioConfig;
    this.tuningConfig = tuningConfig == null ? RealtimeTuningConfig.makeDefaultTuningConfig(null) : tuningConfig;

  }

  /**
   * Provides the data schema for the feed that this FireDepartment is in charge of.
   *
   * @return the Schema for this feed.
   */
  @JsonProperty("dataSchema")
  @Override
  public DataSchema getDataSchema()
  {
    return dataSchema;
  }

  @JsonProperty("ioConfig")
  @Override
  public RealtimeIOConfig getIOConfig()
  {
    return ioConfig;
  }

  @JsonProperty("tuningConfig")
  @Override
  public RealtimeTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  public Firehose connect() throws IOException
  {
    return ioConfig.getFirehoseFactory()
                   .connect(Preconditions.checkNotNull(dataSchema.getParser(), "inputRowParser"), null);
  }

  public FireDepartmentMetrics getMetrics()
  {
    return metrics;
  }
}
