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

package org.apache.druid.segment.loading;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SimpleQueryableIndex;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.timeline.DataSegment;

import java.io.File;
import java.io.IOException;

/**
 */
public class MMappedQueryableSegmentizerFactory implements SegmentizerFactory
{

  private final IndexIO indexIO;

  public MMappedQueryableSegmentizerFactory(@JacksonInject IndexIO indexIO)
  {
    this.indexIO = Preconditions.checkNotNull(indexIO, "Null IndexIO");
  }

  @Override
  public Segment factorize(DataSegment dataSegment, File parentDir, boolean lazy) throws SegmentLoadingException
  {
    try {
      /**
       * {@link org.apache.druid.segment.IndexIO#loadIndex(File, boolean)}
       * {@link org.apache.druid.segment.IndexIO.V9IndexLoader#load(File, ObjectMapper, boolean)}
       *
       * indexIO.loadIndex最终返回的{@link SimpleQueryableIndex}本身没有逻辑，只是个包装类，将所有的inDir目录下的segment信息存储其中。
       * 此index对象中较为重要的解析结果为：
       * 1、metadata：xxxx.smoosh文件中的metadata.drd信息所转化
       * 2、columns：map类型 key为所有列名，value为该列的包装类
       * 3、dimensionHandlers：map类型，key为各维度列名，value为该列数据类型对应的handler对象
       * 4、fileMapper：包含了同文件夹下所有smoosh文件的File对象，以及meta.smoosh内的所有数据信息
       *
       * {@link QueryableIndexSegment}也是个包装类，内部最重要的就是这个index对象，象征着parentDir目录所对应的segment
       */
      return new QueryableIndexSegment(indexIO.loadIndex(parentDir, lazy), dataSegment.getId());
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, "%s", e.getMessage());
    }
  }
}
