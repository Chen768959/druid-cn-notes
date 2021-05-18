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

package org.apache.druid.segment.column;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.segment.data.BlockLayoutColumnarLongsSupplier;
import org.apache.druid.segment.data.ColumnarLongs;
import org.apache.druid.segment.serde.ColumnPartSerde;
import org.apache.druid.segment.serde.LongNumericColumnPartSerde;
import org.apache.druid.segment.serde.Serializer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * 该类描述一个“列”，
 * 其中包含了该列信息，以及该列上的所有值
 */
public class ColumnDescriptor implements Serializer
{
  public static Builder builder()
  {
    return new Builder();
  }

  private final ValueType valueType;
  private final boolean hasMultipleValues;
  private final List<ColumnPartSerde> parts;

  /** 该json内容由{@link org.apache.druid.segment.IndexIO.V9IndexLoader#deserializeColumn(ObjectMapper, ByteBuffer, SmooshedFileMapper)} */
  @JsonCreator
  public ColumnDescriptor(
      @JsonProperty("valueType") ValueType valueType, // 该列的数据类型
      @JsonProperty("hasMultipleValues") boolean hasMultipleValues, // 该列上每行是否有多个值
      @JsonProperty("parts") List<ColumnPartSerde> parts //依然是该列的描述信息，包含了该列类型，排序策略等
  )
  {
    this.valueType = valueType;
    this.hasMultipleValues = hasMultipleValues;
    this.parts = parts;
  }

  @JsonProperty
  public ValueType getValueType()
  {
    return valueType;
  }

  @JsonProperty
  public boolean isHasMultipleValues()
  {
    return hasMultipleValues;
  }

  @JsonProperty
  public List<ColumnPartSerde> getParts()
  {
    return parts;
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    long size = 0;
    for (ColumnPartSerde part : parts) {
      size += part.getSerializer().getSerializedSize();
    }
    return size;
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    for (ColumnPartSerde part : parts) {
      part.getSerializer().writeTo(channel, smoosher);
    }
  }

  /**
   *
   * @param buffer bytebuffer，其中包含了指定列的所有值
   * @param columnConfig
   * @param smooshedFiles 包含了meta.smoosh同文件夹下的所有smoosh文件的File对象，以及meta.smoosh内的所有数据信息
   *
   * @return org.apache.druid.segment.column.ColumnHolder
   */
  public ColumnHolder read(ByteBuffer buffer, ColumnConfig columnConfig, SmooshedFileMapper smooshedFiles)
  {
    /**
     * 此方法最终要返回ColumnHolder对象，
     * builder相当于是一个生成ColumnHolder对象的工具，期间会把各个阶段的参数都传入builder对象，
     * 最后再由builder生成ColumnHolder对象
     */
    final ColumnBuilder builder = new ColumnBuilder()
        .setType(valueType)
        .setHasMultipleValues(hasMultipleValues)
        .setFileMapper(smooshedFiles);

    /**
     * parts中包含了当前列的描述信息，如果当前列中只有一种类型数据，则只有一个part
     * part中包含了列名、列排序策略等信息
     * {@link ColumnPartSerde}接口中描述了每一种类型对应哪一种ColumnPartSerde实现类，
     * 以long类型数据为例，part对应{@link org.apache.druid.segment.serde.LongNumericColumnPartSerde}
     */
    for (ColumnPartSerde part : parts) {
      /**
       * {@link LongNumericColumnPartSerde#getDeserializer()}：
       * 该方法返回了一个匿名函数，也就是这个read方法，
       *
       * 其调用.read(buffer, builder, columnConfig)，
       * 就是调用{@link LongNumericColumnPartSerde#getDeserializer()}返回的匿名函数：
       * read()方法解析出了当前列的类型、压缩策略、排序、编码等信息，
       * 然后又创建了个{@link BlockLayoutColumnarLongsSupplier}简单工厂，
       * 该工厂get方法可根据以上信息构建{@link ColumnarLongs}对象.
       * 最后把这些信息全部传给builder对象。
       */
      part.getDeserializer().read(buffer, builder, columnConfig);
    }

    /**
     * 经过上面的逻辑，read中已经拥有了该列的各种信息，
     * 此处调用{@link ColumnBuilder#build()}
     * 仅仅只是将builder中的列关键信息传构造方法创建{@link SimpleColumnHolder}对象，
     * 具体传了那些参数可看其构造方法
     *
     * 返回的也是这个{@link SimpleColumnHolder}对象，
     * 该对象中包含了两个比较重要的参数，
     * 1、capabilitiesBuilder:capabilitiesBuilder包含了列的数据类型等列描述信息。
     * 2、其中包含了一个{@link BlockLayoutColumnarLongsSupplier}简单工厂，该工厂在创建时拥有了“包含该列所有值信息的bytebuffer”
     * 该工厂get方法可根据以上信息构建{@link org.apache.druid.segment.data.ColumnarLongs}对象.
     */
    return builder.build();
  }

  public static class Builder
  {
    @Nullable
    private ValueType valueType = null;
    @Nullable
    private Boolean hasMultipleValues = null;

    private final List<ColumnPartSerde> parts = new ArrayList<>();

    public Builder setValueType(ValueType valueType)
    {
      if (this.valueType != null && this.valueType != valueType) {
        throw new IAE("valueType[%s] is already set, cannot change to[%s]", this.valueType, valueType);
      }
      this.valueType = valueType;
      return this;
    }

    public Builder setHasMultipleValues(boolean hasMultipleValues)
    {
      if (this.hasMultipleValues != null && this.hasMultipleValues != hasMultipleValues) {
        throw new IAE(
            "hasMultipleValues[%s] is already set, cannot change to[%s]", this.hasMultipleValues, hasMultipleValues
        );
      }
      this.hasMultipleValues = hasMultipleValues;
      return this;
    }

    public Builder addSerde(ColumnPartSerde serde)
    {
      parts.add(serde);
      return this;
    }

    public ColumnDescriptor build()
    {
      Preconditions.checkNotNull(valueType, "must specify a valueType");
      return new ColumnDescriptor(valueType, hasMultipleValues == null ? false : hasMultipleValues, parts);
    }
  }
}
