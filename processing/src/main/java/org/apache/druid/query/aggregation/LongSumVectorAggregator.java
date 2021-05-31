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

package org.apache.druid.query.aggregation;

import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class LongSumVectorAggregator implements VectorAggregator
{
  private static final Logger log = new Logger(LongSumVectorAggregator.class);

  private final VectorValueSelector selector;

  public LongSumVectorAggregator(final VectorValueSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    buf.putLong(position, 0);
  }

  @Override
  public void aggregate(final ByteBuffer buf, final int position, final int startRow, final int endRow)
  {
    final long[] vector = selector.getLongVector();

    long sum = 0;
    for (int i = startRow; i < endRow; i++) {
      sum += vector[i];
    }

    buf.putLong(position, buf.getLong(position) + sum);
  }

  /**
   * 逻辑;
   * 调用该聚合器内部{@link this#selector}对象的方法，获取启动时加载的某列的所有数据，然后再累加到buf中。
   *
   * {@link this#selector}的实现类由{@link org.apache.druid.segment.data.ColumnarLongs#makeVectorValueSelector}创建。
   * 其getLongVector()方法，
   *
   * @param buf 存放聚合结果
   * @param numRows
   * @param positions
   * @param rows
   * @param positionOffset
   */
  @Override
  public void aggregate(
      final ByteBuffer buf,
      final int numRows,
      final int[] positions,
      @Nullable final int[] rows,
      final int positionOffset
  )
  {
    log.info("!!!：LongSumVectorAggregator，selector类型："+selector.getClass());
    /**
     * selector：ColumnarLongsVectorValueSelector
     * 由{@link org.apache.druid.segment.data.ColumnarLongs#makeVectorValueSelector}创建，
     * getLongVector()也在其中定义
     *
     * vector中包含了“待聚合的数据”
     * ====================================================================
     *
     *
     */
    final long[] vector = selector.getLongVector();

    // 按一定算法将vector中数据填充进buffer中
    for (int i = 0; i < numRows; i++) {
      final int position = positions[i] + positionOffset;
      // value为累加结果，每次将position位置的值加上vector中的值形成新的累加结果
      long value = buf.getLong(position) + vector[rows != null ? rows[i] : i];
      buf.putLong(position, value);

      log.info("!!!：LongSumVectorAggregator.aggregate聚合结果"+i+"："+value);
    }
  }

  @Override
  public Object get(final ByteBuffer buf, final int position)
  {
    return buf.getLong(position);
  }

  @Override
  public void close()
  {
    // Nothing to close.
  }
}
