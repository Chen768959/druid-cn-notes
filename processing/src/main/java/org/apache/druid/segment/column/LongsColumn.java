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

import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.data.ColumnarLongs;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.VectorValueSelector;

/**
 */
public class LongsColumn implements NumericColumn
{
  private static final Logger log = new Logger(LongsColumn.class);
  /**
   * Factory method to create LongsColumn.
   */
  public static LongsColumn create(ColumnarLongs column, ImmutableBitmap nullValueBitmap)
  {
    if (nullValueBitmap.isEmpty()) {
      return new LongsColumn(column);
    } else {
      return new LongsColumnWithNulls(column, nullValueBitmap);
    }
  }

  final ColumnarLongs column;

  LongsColumn(final ColumnarLongs column)
  {
    this.column = column;
  }

  @Override
  public int length()
  {
    return column.size();
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(ReadableOffset offset)
  {
    return column.makeColumnValueSelector(offset, IndexIO.LEGACY_FACTORY.getBitmapFactory().makeEmptyImmutableBitmap());
  }

  @Override
  public VectorValueSelector makeVectorValueSelector(ReadableVectorOffset offset)
  {
    return column.makeVectorValueSelector(offset, IndexIO.LEGACY_FACTORY.getBitmapFactory().makeEmptyImmutableBitmap());
  }

  @Override
  public long getLongSingleValueRow(int rowNum)
  {
//    log.info("!!!：调用get方法获取ColumnarLongs（0）");
    long res = column.get(rowNum);
//    log.info("!!!：调用get方法获取ColumnarLongs（0），完毕，long="+res);
    return res;
  }

  @Override
  public void close()
  {
    column.close();
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("column", column);
  }
}
