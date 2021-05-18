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

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.segment.data.BlockLayoutColumnarLongsSupplier;
import org.apache.druid.segment.serde.LongNumericColumnPartSerde;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 */
public class ColumnBuilder
{
  private final ColumnCapabilitiesImpl capabilitiesBuilder = ColumnCapabilitiesImpl.createDefault();

  @Nullable
  private Supplier<? extends BaseColumn> columnSupplier = null;
  @Nullable
  private Supplier<BitmapIndex> bitmapIndex = null;
  @Nullable
  private Supplier<SpatialIndex> spatialIndex = null;
  @Nullable
  private SmooshedFileMapper fileMapper = null;


  /**
   * 由{@link ColumnDescriptor#read(ByteBuffer, ColumnConfig, SmooshedFileMapper)}逻辑传入
   * fileMapper:包含了meta.smoosh同文件夹下的所有smoosh文件的File对象，以及meta.smoosh内的所有数据信息
   */
  public ColumnBuilder setFileMapper(SmooshedFileMapper fileMapper)
  {
    this.fileMapper = fileMapper;
    return this;
  }

  public SmooshedFileMapper getFileMapper()
  {
    return this.fileMapper;
  }

  /**
   * 由{@link ColumnDescriptor#read(ByteBuffer, ColumnConfig, SmooshedFileMapper)}逻辑传入
   * type为该列的数据类型
   */
  public ColumnBuilder setType(ValueType type)
  {
    this.capabilitiesBuilder.setType(type);
    return this;
  }

  /**
   * 由{@link ColumnDescriptor#read(ByteBuffer, ColumnConfig, SmooshedFileMapper)}逻辑传入
   * hasMultipleValues为该列上每行是否有多个值
   */
  public ColumnBuilder setHasMultipleValues(boolean hasMultipleValues)
  {
    this.capabilitiesBuilder.setHasMultipleValues(hasMultipleValues);
    return this;
  }

  public ColumnBuilder setDictionaryEncodedColumnSupplier(Supplier<? extends DictionaryEncodedColumn<?>> columnSupplier)
  {
    this.columnSupplier = columnSupplier;
    this.capabilitiesBuilder.setDictionaryEncoded(true);
    this.capabilitiesBuilder.setDictionaryValuesSorted(true);
    this.capabilitiesBuilder.setDictionaryValuesUnique(true);
    return this;
  }

  @SuppressWarnings("unused")
  public ColumnBuilder setFilterable(boolean filterable)
  {
    this.capabilitiesBuilder.setFilterable(filterable);
    return this;
  }

  public ColumnBuilder setComplexColumnSupplier(Supplier<? extends ComplexColumn> columnSupplier)
  {
    this.columnSupplier = columnSupplier;
    return this;
  }

  /**
   * 由{@link LongNumericColumnPartSerde#getDeserializer()}返回的read()匿名函数中定义
   * (不过调用该匿名函数的地方还是{@link ColumnDescriptor#read(ByteBuffer, ColumnConfig, SmooshedFileMapper)})
   * columnSupplier：包装类，其只负责包装“column”和IndexIO.LEGACY_FACTORY.getBitmapFactory().makeEmptyImmutableBitmap()
   * 其中column是{@link BlockLayoutColumnarLongsSupplier}简单工厂，
   * 该工厂get方法可根据以上信息构建{@link org.apache.druid.segment.data.ColumnarLongs}对象.
   */
  public ColumnBuilder setNumericColumnSupplier(Supplier<? extends NumericColumn> columnSupplier)
  {
    this.columnSupplier = columnSupplier;
    return this;
  }

  public ColumnBuilder setBitmapIndex(Supplier<BitmapIndex> bitmapIndex)
  {
    this.bitmapIndex = bitmapIndex;
    this.capabilitiesBuilder.setHasBitmapIndexes(true);
    return this;
  }

  public ColumnBuilder setSpatialIndex(Supplier<SpatialIndex> spatialIndex)
  {
    this.spatialIndex = spatialIndex;
    this.capabilitiesBuilder.setHasSpatialIndexes(true);
    return this;
  }

  public ColumnBuilder setHasNulls(boolean nullable)
  {
    this.capabilitiesBuilder.setHasNulls(nullable);
    return this;
  }
  public ColumnBuilder setHasNulls(ColumnCapabilities.Capable nullable)
  {
    this.capabilitiesBuilder.setHasNulls(nullable);
    return this;
  }

  public ColumnHolder build()
  {
    Preconditions.checkState(capabilitiesBuilder.getType() != null, "Type must be set.");

    /**
     * capabilitiesBuilder:capabilitiesBuilder包含了列的数据类型等列描述信息
     * columnSupplier:其中包含了一个{@link BlockLayoutColumnarLongsSupplier}简单工厂，
     *                该工厂get方法可根据以上信息构建{@link org.apache.druid.segment.data.ColumnarLongs}对象.
     * bitmapIndex:
     * spatialIndex:
     */
    return new SimpleColumnHolder(capabilitiesBuilder, columnSupplier, bitmapIndex, spatialIndex);
  }
}
