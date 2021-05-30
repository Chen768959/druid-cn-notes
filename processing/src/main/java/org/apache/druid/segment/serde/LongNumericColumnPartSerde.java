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

package org.apache.druid.segment.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.BlockLayoutColumnarLongsSupplier;
import org.apache.druid.segment.data.ColumnarLongs;
import org.apache.druid.segment.data.CompressedColumnarLongsSupplier;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.GenericIndexed;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 */
public class LongNumericColumnPartSerde implements ColumnPartSerde
{
  @JsonCreator
  public static LongNumericColumnPartSerde createDeserializer(
      @JsonProperty("byteOrder") ByteOrder byteOrder
  )
  {
    return new LongNumericColumnPartSerde(byteOrder, null);
  }

  private final ByteOrder byteOrder;
  @Nullable
  private final Serializer serializer;

  private LongNumericColumnPartSerde(ByteOrder byteOrder, @Nullable Serializer serializer)
  {
    this.byteOrder = byteOrder;
    this.serializer = serializer;
  }

  @JsonProperty
  public ByteOrder getByteOrder()
  {
    return byteOrder;
  }

  public static SerializerBuilder serializerBuilder()
  {
    return new SerializerBuilder();
  }

  public static class SerializerBuilder
  {
    @Nullable
    private ByteOrder byteOrder = null;
    @Nullable
    private Serializer delegate = null;

    public SerializerBuilder withByteOrder(final ByteOrder byteOrder)
    {
      this.byteOrder = byteOrder;
      return this;
    }

    public SerializerBuilder withDelegate(final Serializer delegate)
    {
      this.delegate = delegate;
      return this;
    }

    public LongNumericColumnPartSerde build()
    {
      return new LongNumericColumnPartSerde(byteOrder, delegate);
    }
  }

  @Nullable
  @Override
  public Serializer getSerializer()
  {
    return serializer;
  }

  @Override
  public Deserializer getDeserializer()
  {
    /**
     * 该方法主要是解析“columnSupplier”，然后传给builder
     * buffer: bytebuffer，其中包含了指定列的所有值
     * builder: 一个工具类，用于构建最终所需的对象，在该方法中产生的各种数据需要传给builder对象
     * columnConfig:
     */
    return (buffer, builder, columnConfig) -> {
      /**
       * 该fromByteBuffer()方法中创建一个匿名函数包装类{@link BlockLayoutColumnarLongsSupplier#BlockLayoutColumnarLongsSupplier(int, int, ByteBuffer, ByteOrder, CompressionFactory.LongEncodingReader, CompressionStrategy)}
       * 该对象中包含了{@link GenericIndexed}，其是该列信息的包装类，后续聚合查询也是通过此对象查询指定列的各行信息。
       * 该匿名函数包装类对象的get()方法提供ColumnarLongs类型数据，也就是其对应的“列”的信息对象
       *
       * 最后将{@link BlockLayoutColumnarLongsSupplier}装入{@link CompressedColumnarLongsSupplier}并返回
       *
       * CompressedColumnarLongsSupplier的get方法实际上就是调用其内部{@link BlockLayoutColumnarLongsSupplier#get()}方法，
       * 然后获取指定列的{@link ColumnarLongs}对象，后续查询时也是通过此对象对列信息进行访问
       */
      final CompressedColumnarLongsSupplier column = CompressedColumnarLongsSupplier.fromByteBuffer(
          buffer,
          byteOrder
      );

      // 包装类，其只负责包装column和IndexIO.LEGACY_FACTORY.getBitmapFactory().makeEmptyImmutableBitmap()
      LongNumericColumnSupplier columnSupplier = new LongNumericColumnSupplier(
          column,
          IndexIO.LEGACY_FACTORY.getBitmapFactory().makeEmptyImmutableBitmap()
      );

      builder.setType(ValueType.LONG)
             .setHasMultipleValues(false)
             .setNumericColumnSupplier(columnSupplier);
    };
  }
}
