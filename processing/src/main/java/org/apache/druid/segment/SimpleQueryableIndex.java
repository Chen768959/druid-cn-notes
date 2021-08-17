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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.SimpleColumnHolder;
import org.apache.druid.segment.data.Indexed;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 */
public class SimpleQueryableIndex extends AbstractIndex implements QueryableIndex
{
  private final Interval dataInterval;
  private final List<String> columnNames;
  private final Indexed<String> availableDimensions;
  private final BitmapFactory bitmapFactory;
  private final Map<String, Supplier<ColumnHolder>> columns;
  private final SmooshedFileMapper fileMapper;
  @Nullable
  private final Metadata metadata;
  private final Supplier<Map<String, DimensionHandler>> dimensionHandlers;

  /**
   * 将所有参数都传入此对象，其中比较重要的参数为
   * 1、metadata：xxxx.smoosh文件中的metadata.drd信息所转化
   * 2、columns：map类型 key为所有列名，value为该列的包装类
   * 3、dimensionHandlers：map类型，key为各维度列名，value为该列数据类型对应的handler对象
   *
   * @param dataInterval
   * @param dimNames 所有的“维度”列名，即其中不包含count列和value列，dims中的所有列在上面cols中也都能找到
   * @param bitmapFactory
   * @param columns key为列名，value为该列的包装类
   * @param fileMapper 包含了meta.smoosh同文件夹下的所有smoosh文件的File对象，以及meta.smoosh内的所有数据信息
   * @param metadata Metadata.class对象由xxxx.smoosh文件中的metadata.drd信息所转化
   * @param lazy
   */
  public SimpleQueryableIndex(
      Interval dataInterval,
      Indexed<String> dimNames,
      BitmapFactory bitmapFactory,
      Map<String, Supplier<ColumnHolder>> columns,
      SmooshedFileMapper fileMapper,
      @Nullable Metadata metadata,
      boolean lazy
  )
  {
    Preconditions.checkNotNull(columns.get(ColumnHolder.TIME_COLUMN_NAME));
    this.dataInterval = Preconditions.checkNotNull(dataInterval, "dataInterval");
    // ImmutableList是一个不可变、线程安全的列表集合，它只会获取传入对象的一个副本，而不会影响到原来的变量或者对象
    ImmutableList.Builder<String> columnNamesBuilder = ImmutableList.builder();

    for (String column : columns.keySet()) {
      // 迭代columns中的所有列名，将除了"__time"列以外的列名全部存入columnNamesBuilder中
      if (!ColumnHolder.TIME_COLUMN_NAME.equals(column)) {
        columnNamesBuilder.add(column);
      }
    }
    this.columnNames = columnNamesBuilder.build(); // 除了"__time"列以外的所有列名
    this.availableDimensions = dimNames; // 所有的“维度”列名，即其中不包含count列和value列，dims中的所有列在上面cols中也都能找到
    this.bitmapFactory = bitmapFactory;
    this.columns = columns; // key为列名，value为该列的包装类
    this.fileMapper = fileMapper; // 包含了meta.smoosh同文件夹下的所有smoosh文件的File对象，以及meta.smoosh内的所有数据信息
    this.metadata = metadata; // Metadata.class对象由xxxx.smoosh文件中的metadata.drd信息所转化

    if (lazy) {
      this.dimensionHandlers = Suppliers.memoize(() -> {
            Map<String, DimensionHandler> dimensionHandlerMap = Maps.newLinkedHashMap();
            for (String dim : availableDimensions) {
              ColumnCapabilities capabilities = getColumnHolder(dim).getCapabilities();
              DimensionHandler handler = DimensionHandlerUtils.getHandlerFromCapabilities(dim, capabilities, null);
              dimensionHandlerMap.put(dim, handler);
            }
            return dimensionHandlerMap;
          }
      );
    } else {
      // key为“维度列名”，value为该列数据类型对应的handler对象long对应的就是LongDimensionHandler
      Map<String, DimensionHandler> dimensionHandlerMap = Maps.newLinkedHashMap();
      // 迭代availableDimensions，即所有的“维度”列名
      for (String dim : availableDimensions) {
        /**
         * 从“columns”中找到dim列对应包装类{@link org.apache.druid.segment.column.SimpleColumnHolder}并返回,
         * 之后调用
         * {@link SimpleColumnHolder#getCapabilities()}
         * 得到该列的描述信息对象capabilities
         */
        ColumnCapabilities capabilities = getColumnHolder(dim).getCapabilities();
        // 将dim列名传给handler并创建类型对应的handler对象，long对应的就是LongDimensionHandler
        DimensionHandler handler = DimensionHandlerUtils.getHandlerFromCapabilities(dim, capabilities, null);
        // 所以此map key为“维度列名”，value为每种数据类型对应的handler对象，此时其中只有该列列名
        dimensionHandlerMap.put(dim, handler);
      }

      // dimensionHandlers是一你匿名函数工厂，可由其get方法提供懒加载对象的功能，
      // 此处相当于返回dimensionHandlers返回dimensionHandlerMap对象
      // key为“维度列名”，value为该列数据类型对应的handler对象long对应的就是LongDimensionHandler
      this.dimensionHandlers = () -> dimensionHandlerMap;
    }
  }

  @VisibleForTesting
  public SimpleQueryableIndex(
      Interval interval,
      List<String> columnNames,
      Indexed<String> availableDimensions,
      BitmapFactory bitmapFactory,
      Map<String, Supplier<ColumnHolder>> columns,
      SmooshedFileMapper fileMapper,
      @Nullable Metadata metadata,
      Supplier<Map<String, DimensionHandler>> dimensionHandlers
  )
  {
    this.dataInterval = interval;
    this.columnNames = columnNames;
    this.availableDimensions = availableDimensions;
    this.bitmapFactory = bitmapFactory;
    this.columns = columns;
    this.fileMapper = fileMapper;
    this.metadata = metadata;
    this.dimensionHandlers = dimensionHandlers;
  }

  @Override
  public Interval getDataInterval()
  {
    return dataInterval;
  }

  @Override
  public int getNumRows()
  {
    return columns.get(ColumnHolder.TIME_COLUMN_NAME).get().getLength();
  }

  @Override
  public List<String> getColumnNames()
  {
    return columnNames;
  }

  @Override
  public StorageAdapter toStorageAdapter()
  {
    return new QueryableIndexStorageAdapter(this);
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    return availableDimensions;
  }

  @Override
  public BitmapFactory getBitmapFactoryForDimensions()
  {
    return bitmapFactory;
  }

  @Nullable
  @Override
  public ColumnHolder getColumnHolder(String columnName)
  {
    // 找到"__time"对应包装类ColumnHolder并返回SimpleColumnHolder（该列的包装类）
    Supplier<ColumnHolder> columnHolderSupplier = columns.get(columnName);
    return columnHolderSupplier == null ? null : columnHolderSupplier.get();
  }

  @VisibleForTesting
  public Map<String, Supplier<ColumnHolder>> getColumns()
  {
    return columns;
  }

  @VisibleForTesting
  public SmooshedFileMapper getFileMapper()
  {
    return fileMapper;
  }

  @Override
  public void close()
  {
    fileMapper.close();
  }

  @Override
  public Metadata getMetadata()
  {
    return metadata;
  }

  @Override
  public Map<String, DimensionHandler> getDimensionHandlers()
  {
    return dimensionHandlers.get();
  }

}
