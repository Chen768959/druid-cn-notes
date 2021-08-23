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

import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.Grouper.Entry;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.function.Function;

public class CloseableGrouperIterator<KeyType, T> implements CloseableIterator<T>
{
  private static final Logger log = new Logger(CloseableGrouperIterator.class);

  private final Function<Entry<KeyType>, T> transformer;
  private final CloseableIterator<Entry<KeyType>> iterator;
  private final Closer closer;

  /**
   *
   * @param iterator
   * @param transformer
   * @param closeable
   */
  public CloseableGrouperIterator(
      final CloseableIterator<Entry<KeyType>> iterator,
      final Function<Grouper.Entry<KeyType>, T> transformer,
      final Closeable closeable
  )
  {
    this.iterator = iterator;
    this.transformer = transformer;
    this.closer = Closer.create();

    closer.register(iterator);
    closer.register(closeable);
  }

  @Override
  public T next()
  {
    /**
     * 历史节点：
     * 获取到聚合结果。
     * 此处的iterator指的是{@link BufferArrayGrouper#iterator(boolean)}产生的迭代器，
     * 其next也是调用的其中，可查询聚合结果
     */
    Entry<KeyType> memoryEntry = iterator.next();

    log.info("!!!：delegate.next()，memoryEntry获取完毕");
    /**
     * 获取列值，并将上面的聚合结果和其进行组合，然后返回ResultRow
     */
    T resultRow = transformer.apply(memoryEntry);

    return resultRow;
  }

  @Override
  public boolean hasNext()
  {
    return iterator.hasNext();
  }

  @Override
  public void remove()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close()
  {
    try {
      closer.close();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
