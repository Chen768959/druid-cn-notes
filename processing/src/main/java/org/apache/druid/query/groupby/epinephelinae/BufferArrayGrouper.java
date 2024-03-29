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

package org.apache.druid.query.groupby.epinephelinae;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.primitives.Ints;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.collections.StupidPool;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.epinephelinae.column.GroupByColumnSelectorStrategy;
import org.apache.druid.query.groupby.epinephelinae.vector.VectorGroupByEngine;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A buffer grouper for array-based aggregation.  This grouper stores aggregated values in the buffer using the grouping
 * key as the index.
 * <p>
 * The buffer is divided into 2 separate regions, i.e., used flag buffer and value buffer.  The used flag buffer is a
 * bit set to represent which keys are valid.  If a bit of an index is set, that key is valid.  Finally, the value
 * buffer is used to store aggregated values.  The first index is reserved for
 * {@link GroupByColumnSelectorStrategy#GROUP_BY_MISSING_VALUE}.
 * <p>
 * This grouper is available only when the grouping key is a single indexed dimension of a known cardinality because it
 * directly uses the dimension value as the index for array access.  Since the cardinality for the grouping key across
 * different segments cannot be currently retrieved, this grouper can be used only when performing per-segment query
 * execution.
 */
public class BufferArrayGrouper implements VectorGrouper, IntGrouper
{
  private static final Logger log = new Logger(BufferArrayGrouper.class);

  private final Supplier<ByteBuffer> bufferSupplier;
  /**
   * {@link VectorGroupByEngine.VectorGroupByEngineIterator#makeGrouper()}中创建当前grouper对象时入参该aggregators，
   * aggregators通过{@link AggregatorAdapters#factorizeVector(VectorColumnSelectorFactory, List)}创建
   *
   * 其是基于查询请求对象的“aggregations”参数（{@link GroupByQuery#getAggregatorSpecs()}），
   * 根据查询请求参数，获取各聚合器工厂
   * 并且其中还装了“游标中的列选择器”，从中可获取游标中的segment数据
   */
  private final AggregatorAdapters aggregators;
  private final int cardinalityWithMissingValue;
  /**
   * 所有聚合值的大小
   */
  private final int recordSize; // size of all aggregated values

  private boolean initialized = false;
  private WritableMemory usedFlagMemory;
  // 带查询内容就在其中
  private ByteBuffer valBuffer;

  // Scratch objects used by aggregateVector(). Only set if initVectorized() is called.
  @Nullable
  private int[] vAggregationPositions = null;
  @Nullable
  private int[] vAggregationRows = null;

  /**
   * Computes required buffer capacity for a grouping key of the given cardinaltiy and aggregatorFactories.
   * This method assumes that the given cardinality doesn't count nulls.
   *
   * Returns -1 if cardinality + 1 (for null) > Integer.MAX_VALUE. Returns computed required buffer capacity
   * otherwise.
   */
  static long requiredBufferCapacity(int cardinality, AggregatorFactory[] aggregatorFactories)
  {
    final long cardinalityWithMissingValue = computeCardinalityWithMissingValue(cardinality);
    // Cardinality should be in the integer range. See DimensionDictionarySelector.
    if (cardinalityWithMissingValue > Integer.MAX_VALUE) {
      return -1;
    }
    final long recordSize = Arrays.stream(aggregatorFactories)
                                  .mapToLong(AggregatorFactory::getMaxIntermediateSizeWithNulls)
                                  .sum();

    return getUsedFlagBufferCapacity(cardinalityWithMissingValue) +  // total used flags size
           cardinalityWithMissingValue * recordSize;                 // total values size
  }

  private static long computeCardinalityWithMissingValue(int cardinality)
  {
    return (long) cardinality + 1;
  }

  /**
   * Compute the number of bytes to store all used flag bits.
   */
  private static long getUsedFlagBufferCapacity(long cardinalityWithMissingValue)
  {
    return (cardinalityWithMissingValue + Byte.SIZE - 1) / Byte.SIZE;
  }

  /**
   *
   * @param bufferSupplier
   * @param aggregators {@link VectorGroupByEngine.VectorGroupByEngineIterator#makeGrouper()}中创建当前grouper对象时入参，
   * 调用{@link AggregatorAdapters#factorizeVector(VectorColumnSelectorFactory, List)}创建
   * 基于查询请求对象的“aggregations”参数（{@link GroupByQuery#getAggregatorSpecs()}），
   * 根据查询请求参数，获取各聚合器工厂
   * 并且其中还装了“游标中的列选择器”，从中可获取游标中的segment数据
   * @param cardinality
   */
  public BufferArrayGrouper(
      // the buffer returned from the below supplier can have dirty bits and should be cleared during initialization
      final Supplier<ByteBuffer> bufferSupplier,
      final AggregatorAdapters aggregators,
      final int cardinality
  )
  {
    Preconditions.checkNotNull(aggregators, "aggregators");
    Preconditions.checkArgument(cardinality > 0, "Cardinality must a non-zero positive number");

    this.bufferSupplier = Preconditions.checkNotNull(bufferSupplier, "bufferSupplier");
    this.aggregators = aggregators;
    this.cardinalityWithMissingValue = Ints.checkedCast(computeCardinalityWithMissingValue(cardinality));
    this.recordSize = aggregators.spaceNeeded();
  }

  @Override
  public void init()
  {
    if (!initialized) {
      final ByteBuffer buffer = bufferSupplier.get();

      final int usedFlagBufferEnd = Ints.checkedCast(getUsedFlagBufferCapacity(cardinalityWithMissingValue));

      // Sanity check on buffer capacity.
      if (usedFlagBufferEnd + (long) cardinalityWithMissingValue * recordSize > buffer.capacity()) {
        // Should not happen in production, since we should only select array-based aggregation if we have
        // enough scratch space.
        throw new ISE(
            "Records of size[%,d] and possible cardinality[%,d] exceeds the buffer capacity[%,d].",
            recordSize,
            cardinalityWithMissingValue,
            valBuffer.capacity()
        );
      }

      // Slice up the buffer.
      buffer.position(0);
      buffer.limit(usedFlagBufferEnd);
      usedFlagMemory = WritableMemory.wrap(buffer.slice(), ByteOrder.nativeOrder());

      buffer.position(usedFlagBufferEnd);
      buffer.limit(buffer.capacity());
      valBuffer = buffer.slice();
      reset();

      initialized = true;
    }
  }

  @Override
  public void initVectorized(final int maxVectorSize)
  {
    init();

    this.vAggregationPositions = new int[maxVectorSize];
    this.vAggregationRows = new int[maxVectorSize];
  }

  @Override
  public boolean isInitialized()
  {
    return initialized;
  }

  @Override
  public AggregateResult aggregateKeyHash(final int dimIndex)
  {
    Preconditions.checkArgument(
        dimIndex >= 0 && dimIndex < cardinalityWithMissingValue,
        "Invalid dimIndex[%s]",
        dimIndex
    );

    initializeSlotIfNeeded(dimIndex);
    aggregators.aggregateBuffered(valBuffer, dimIndex * recordSize);
    return AggregateResult.ok();
  }

  /**
   * 聚合结果，并存入内部valBuffer
   * @param keySpace 一块内存空间（单位字节），其大小与维度数量、游标中单行向量最大大小（默认512）相关
   * @param startRow 每次聚合一个粒度，startRow为该粒度的起始时间index（__time列数组下标）
   * @param endRow 该粒度的结束时间index（__time列数组下标）
   */
  @Override
  public AggregateResult aggregateVector(Memory keySpace, int startRow, int endRow)
  {
    // 要处理的行数：endRow和startRow都是__time列数组下标，__time列数组每一项都代表一行数据，所以二者相减就等于要处理的行数
    final int numRows = endRow - startRow;

    // Hoisted bounds check on keySpace.
    if (keySpace.getCapacity() < (long) numRows * Integer.BYTES) {
      throw new IAE("Not enough keySpace capacity for the provided start/end rows");
    }
    // We use integer indexes into the keySpace.
    if (keySpace.getCapacity() > Integer.MAX_VALUE) {
      throw new ISE("keySpace too large to handle");
    }

    if (vAggregationPositions == null || vAggregationRows == null) {
      throw new ISE("Grouper was not initialized for vectorization");
    }

    if (keySpace.getCapacity() == 0) {
      // Empty key space, assume keys are all zeroes.
      final int dimIndex = 1;

      initializeSlotIfNeeded(dimIndex);

      aggregators.aggregateVector(
          valBuffer,
          dimIndex * recordSize,
          startRow,
          endRow
      );
    } else {
      for (int i = 0; i < numRows; i++) {
        // +1 matches what hashFunction() would do.
        // dimIndex值与查询行号是一一对应关系
        final int dimIndex = keySpace.getInt(((long) i) * Integer.BYTES) + 1;

        if (dimIndex < 0 || dimIndex >= cardinalityWithMissingValue) {
          throw new IAE("Invalid dimIndex[%s]", dimIndex);
        }

        // recordSize：所有聚合值的大小
        // 由此可见vAggregationPositions数组中存储了“每行所有聚合值的位置”，
        // i对应的是“第几行”，第i位的值就是“第i个所有聚合值处于内存空间的偏移量位置”
        vAggregationPositions[i] = dimIndex * recordSize;

        initializeSlotIfNeeded(dimIndex);
      }

      /**
       * {@link AggregatorAdapters#aggregateVector(ByteBuffer, int, int[], int[])}
       *
       * aggregators是当前grouper被创建时入参进来的，
       * 里面包含了请求对象的“aggregations”参数（{@link GroupByQuery#getAggregatorSpecs()}）对应的各个聚合器工厂，
       * 还装了“游标中的列选择器”，从中可获取游标中的segment数据。
       *
       * 此处就是调用各个聚合器结合游标的列选择器，查询所有的聚合结果，并装入当前grouper的valBuffer中
       */
      aggregators.aggregateVector(
          valBuffer,
          numRows,
          vAggregationPositions,
          Groupers.writeAggregationRows(vAggregationRows, startRow, endRow)
      );
    }

    return AggregateResult.ok();
  }

  private void initializeSlotIfNeeded(int dimIndex)
  {
    final int index = dimIndex / Byte.SIZE;
    final int extraIndex = dimIndex % Byte.SIZE;
    final int usedFlagMask = 1 << extraIndex;

    final byte currentByte = usedFlagMemory.getByte(index);

    if ((currentByte & usedFlagMask) == 0) {
      usedFlagMemory.putByte(index, (byte) (currentByte | usedFlagMask));
      aggregators.init(valBuffer, dimIndex * recordSize);
    }
  }

  private boolean isUsedSlot(int dimIndex)
  {
    final int index = dimIndex / Byte.SIZE;
    final int extraIndex = dimIndex % Byte.SIZE;
    final int usedFlagMask = 1 << extraIndex;

    return (usedFlagMemory.getByte(index) & usedFlagMask) != 0;
  }

  @Override
  public void reset()
  {
    // Clear the entire usedFlagBuffer
    usedFlagMemory.clear();
  }

  @Override
  public IntGrouperHashFunction hashFunction()
  {
    return key -> key + 1;
  }

  @Override
  public void close()
  {
    aggregators.close();
  }

  @Override
  public CloseableIterator<Entry<Memory>> iterator()
  {
    /**
     * 该迭代器中的对象都是从aggregators中获取的{@link this#aggregators}
     * 获取next对象调用{@link AggregatorAdapters#get(ByteBuffer, int, int)}
     */
    final CloseableIterator<Entry<Integer>> iterator = iterator(false);
    final WritableMemory keyMemory = WritableMemory.allocate(Integer.BYTES);
    return new CloseableIterator<Entry<Memory>>()
    {
      @Override
      public boolean hasNext()
      {
        return iterator.hasNext();
      }

      @Override
      public Entry<Memory> next()
      {
        final Entry<Integer> integerEntry = iterator.next();
        keyMemory.putInt(0, integerEntry.getKey());
        return new Entry<>(keyMemory, integerEntry.getValues());
      }

      @Override
      public void close() throws IOException
      {
        iterator.close();
      }
    };
  }

  // 生成查询结果的迭代器
  @Override
  public CloseableIterator<Entry<Integer>> iterator(boolean sorted)
  {
    if (sorted) {
      throw new UnsupportedOperationException("sorted iterator is not supported yet");
    }

    return new CloseableIterator<Entry<Integer>>()
    {
      // initialize to the first used slot
      private int next = findNext(-1);

      @Override
      public boolean hasNext()
      {
        return next >= 0;
      }

      @Override
      public Entry<Integer> next()
      {
        if (next < 0) {
          throw new NoSuchElementException();
        }

        //当前下标（初始时为0）
        final int current = next;
        //获取接下来的查询下标（下一次调用此next方法时，该next又会被赋值给上面的current属性）
        next = findNext(current);
        /**
         * values数组，用来装“聚合结果”
         * 假设我们有一个聚合需求，如求sum，
         * 则下面每一个value都是一个sum的结果
         *
         * 而每一个value都是由aggregators.get(valBuffer, recordOffset, i);创建而来
         * 由此可见aggregators对象负责创建聚合结果
         */
        final Object[] values = new Object[aggregators.size()];
        final int recordOffset = current * recordSize;
        //aggregators中包含了所有聚合器，此处迭代每一个
        for (int i = 0; i < aggregators.size(); i++) {
          /**
           * 从聚合器中查询聚合查询聚合结果
           * 我们以{@link org.apache.druid.query.aggregation.LongSumVectorAggregator}聚合器为例，
           * 其只是从valBuffer中getLong(recordOffset)
           *
           * 所以结果早就在valBuffer中了。
           * 得找到valBuffer是何时被填充了聚合结果
           */
          values[i] = aggregators.get(valBuffer, recordOffset, i);
//          log.info("!!!：查询聚合结果position："+recordOffset+"...value："+values[i]);
        }
        // shift by -1 since values are initially shifted by +1 so they are all positive and
        // GroupByColumnSelectorStrategy.GROUP_BY_MISSING_VALUE is -1
        return new Entry<>(current - 1, values);
      }

      @Override
      public void close()
      {
        // do nothing
      }

      private int findNext(int current)
      {
        // shift by +1 since we're looking for the next used slot after the current position
        for (int i = current + 1; i < cardinalityWithMissingValue; i++) {
          if (isUsedSlot(i)) {
            return i;
          }
        }
        // no more slots
        return -1;
      }
    };
  }
}
