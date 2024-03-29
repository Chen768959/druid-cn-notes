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

package org.apache.druid.segment.data;

import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.LongSumVectorAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.LongColumnSelector;
import org.apache.druid.segment.historical.HistoricalColumnSelector;
import org.apache.druid.segment.vector.BaseLongVectorValueSelector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.VectorSelectorUtils;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.roaringbitmap.PeekableIntIterator;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Arrays;

/**
 * Resource that provides random access to a packed array of primitive longs. Backs up {@link
 * org.apache.druid.segment.column.LongsColumn}.
 */
public interface ColumnarLongs extends Closeable
{
  static final Logger log = new Logger(ColumnarLongs.class);

  int size();

  long get(int index);

  default void get(long[] out, int start, int length)
  {
    for (int i = 0; i < length; i++) {
//      log.info("!!!：调用get方法获取ColumnarLongs（1），i="+i);
      out[i] = get(i + start);
    }
  }

  default void get(long[] out, int[] indexes, int length)
  {
    for (int i = 0; i < length; i++) {
//      log.info("!!!：调用get方法获取ColumnarLongs（2），i="+i);
      out[i] = get(indexes[i]);
    }
  }

  @Override
  void close();

  default ColumnValueSelector<Long> makeColumnValueSelector(ReadableOffset offset, ImmutableBitmap nullValueBitmap)
  {
    if (nullValueBitmap.isEmpty()) {
      class HistoricalLongColumnSelector implements LongColumnSelector, HistoricalColumnSelector<Long>
      {
        @Override
        public boolean isNull()
        {
          return false;
        }

        @Override
        public long getLong()
        {
//          log.info("!!!：调用get方法获取ColumnarLongs（3）");
          return ColumnarLongs.this.get(offset.getOffset());
        }

        @Override
        public double getDouble(int offset)
        {
//          log.info("!!!：调用get方法获取ColumnarLongs（4）");
          return ColumnarLongs.this.get(offset);
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("columnar", ColumnarLongs.this);
          inspector.visit("offset", offset);
        }
      }
      return new HistoricalLongColumnSelector();
    } else {
      class HistoricalLongColumnSelectorWithNulls implements LongColumnSelector, HistoricalColumnSelector<Long>
      {
        private PeekableIntIterator nullIterator = nullValueBitmap.peekableIterator();
        private int nullMark = -1;
        private int offsetMark = -1;

        @Override
        public boolean isNull()
        {
          final int i = offset.getOffset();
          if (i < offsetMark) {
            // offset was reset, reset iterator state
            nullMark = -1;
            nullIterator = nullValueBitmap.peekableIterator();
          }
          offsetMark = i;
          if (nullMark < i) {
            nullIterator.advanceIfNeeded(offsetMark);
            if (nullIterator.hasNext()) {
              nullMark = nullIterator.next();
            }
          }
          return nullMark == offsetMark;
        }

        @Override
        public long getLong()
        {
          //noinspection AssertWithSideEffects (ignore null handling test initialization check side effect)
          assert NullHandling.replaceWithDefault() || !isNull();
//          log.info("!!!：调用get方法获取ColumnarLongs（5）");
          return ColumnarLongs.this.get(offset.getOffset());
        }

        @Override
        public double getDouble(int offset)
        {
          assert NullHandling.replaceWithDefault() || !nullValueBitmap.get(offset);
          return ColumnarLongs.this.get(offset);
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("columnar", ColumnarLongs.this);
          inspector.visit("offset", offset);
          inspector.visit("nullValueBitmap", nullValueBitmap);
        }
      }
      return new HistoricalLongColumnSelectorWithNulls();
    }
  }

  default VectorValueSelector makeVectorValueSelector(
      final ReadableVectorOffset theOffset,
      final ImmutableBitmap nullValueBitmap
  )
  {
    class ColumnarLongsVectorValueSelector extends BaseLongVectorValueSelector
    {
      private final long[] longVector;

      // 初始无id，为-1
      private int id = ReadableVectorOffset.NULL_ID;

      private PeekableIntIterator nullIterator = nullValueBitmap.peekableIterator();
      private int offsetMark = -1;

      @Nullable
      private boolean[] nullVector = null;

      private ColumnarLongsVectorValueSelector()
      {
        super(theOffset);
        this.longVector = new long[offset.getMaxVectorSize()];
      }

      @Nullable
      @Override
      public boolean[] getNullVector()
      {
        computeVectorsIfNeeded();
        return nullVector;
      }

      /**
       * 该方法返回需要被写入buffer中的vector数组
       */
      @Override
      public long[] getLongVector()
      {
//        log.info("!!!：进入getLongVector，当前类"+this.getClass());
        // 该方法进行longVector数组的计算，执行该方法之前longVector中没有值
        computeVectorsIfNeeded();
        for (long a:longVector){
          if (a!=0){
//            log.info("!!!：selector.getLongVector，计算之后:"+a);
          }
        }
        return longVector;
      }

      private void computeVectorsIfNeeded()
      {
        /**
         * 如果当前id和offset中的id相等，则不进行以下逻辑
         * offset为final属性，类型为：{@link ReadableVectorOffset}
         *
         * 由此处逻辑可推测：
         * 会不会存在某种缓存机制，此逻辑只执行一遍，
         * 且当前对象为VectorValueSelector，
         * 该对象可能存在多个，每个selector都有个offset属性作为该对象缓存机制
         *
         * 第一次请求不会走此if条件
         */
        if (id == offset.getId()) {
//          log.info("!!!：computeVectorsIfNeeded，id == offset.getId()");
          return;
        }

        /**
         * offset.isContiguous()：
         * 注释为“判断当前批次是否为连续范围”
         * 批次、连续范围
         */
        if (offset.isContiguous()) {
//          log.info("!!!：computeVectorsIfNeeded，offset.isContiguous()成立");
          if (offset.getStartOffset() < offsetMark) {
            nullIterator = nullValueBitmap.peekableIterator();
          }
          offsetMark = offset.getStartOffset() + offset.getCurrentVectorSize();

          /**
           * 此方法的最终目的是将指定列的数据填充进longVector
           *
           * ColumnarLongs.this.get()
           * 这种写法是调用“匿名函数对象的外部对象的get方法”，
           * 当前匿名对象是通过某个ColumnarLongs的子类的makeVectorValueSelector()创建返回的，
           * 所以此处的ColumnarLongs.this指的就是这个ColumnarLongs的子类对象。
           *
           * 而这个外部类：ColumnarLongs.this对象中“含有指定列的所有值信息”
           * todo 为了找到这个外部类ColumnarLongs.this对象，就得知道是“谁调用了makeVectorValueSelector()”方法获取内部类
           * todo 当前内部类存在于聚合器{@link LongSumVectorAggregator#selector}属性上，
           * todo 找到其外部类就是要找到{@link LongSumVectorAggregator#selector}selector属性是在哪里被赋值的，
           * todo 在哪里由那个外部类通过调用此makeVectorValueSelector()所创建的selector
           *
           * 该外部类为
           * {@link org.apache.druid.segment.data.BlockLayoutColumnarLongsSupplier.BlockLayoutColumnarLongs}
           * 其从属于{@link BlockLayoutColumnarLongsSupplier}
           * 一个BlockLayoutColumnarLongsSupplier对象，对应一个GenericIndexed对象，
           * 对应一个“fromBuffer”，也对应“一个列的所有值内容，
           * 所以该外部类也对应且包含一个列的所有值信息。
           */
          ColumnarLongs.this.get(longVector, offset.getStartOffset(), offset.getCurrentVectorSize());

        /**
         * 当前批次不为连续范围
         */
        } else {
//          log.info("!!!：computeVectorsIfNeeded，isContiguous()不成立");
          final int[] offsets = offset.getOffsets();
          if (offsets[offsets.length - 1] < offsetMark) {
            nullIterator = nullValueBitmap.peekableIterator();
          }
          offsetMark = offsets[offsets.length - 1];

          // 将数据填充进longVector
          ColumnarLongs.this.get(longVector, offsets, offset.getCurrentVectorSize());
        }

        nullVector = VectorSelectorUtils.populateNullVector(nullVector, offset, nullIterator);

        id = offset.getId();
      }
    }

    return new ColumnarLongsVectorValueSelector();
  }
}
