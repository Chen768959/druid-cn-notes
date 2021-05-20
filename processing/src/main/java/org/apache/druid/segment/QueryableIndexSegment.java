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

package org.apache.druid.segment;

import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

/**
 */
public class QueryableIndexSegment implements Segment
{
  private final QueryableIndex index;
  private final QueryableIndexStorageAdapter storageAdapter;
  private final SegmentId segmentId;

  /**
   * index对象{@link SimpleQueryableIndex}本身没有逻辑，只是个包装类，将所有的inDir目录下的segment信息存储其中。
   * 此index对象中较为重要的解析结果为：
   * 1、metadata：xxxx.smoosh文件中的metadata.drd信息所转化
   * 2、columns：map类型 key为所有列名，value为该列的包装类
   * 3、dimensionHandlers：map类型，key为各维度列名，value为该列数据类型对应的handler对象
   * 4、fileMapper：包含了同文件夹下所有smoosh文件的File对象，以及meta.smoosh内的所有数据信息
   *
   */
  public QueryableIndexSegment(QueryableIndex index, final SegmentId segmentId)
  {
    this.index = index;
    this.storageAdapter = new QueryableIndexStorageAdapter(index);
    this.segmentId = segmentId;
  }

  @Override
  public SegmentId getId()
  {
    return segmentId;
  }

  @Override
  public Interval getDataInterval()
  {
    return index.getDataInterval();
  }

  @Override
  public QueryableIndex asQueryableIndex()
  {
    return index;
  }

  @Override
  public StorageAdapter asStorageAdapter()
  {
    return storageAdapter;
  }

  @Override
  public void close()
  {
    // this is kinda nasty
    index.close();
  }
}
