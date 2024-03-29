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

package org.apache.druid.java.util.common.guava;

import java.io.Closeable;
import java.util.Iterator;

/**
 */
public class BaseSequence<T, IterType extends Iterator<T>> implements Sequence<T>
{
  private final IteratorMaker<T, IterType> maker;

  public BaseSequence(IteratorMaker<T, IterType> maker)
  {
    this.maker = maker;
  }

  @Override
  public <OutType> OutType accumulate(final OutType initValue, final Accumulator<OutType, T> fn)
  {
    IterType iterator = maker.make();
    OutType accumulated = initValue;

    try {
      while (iterator.hasNext()) {
        accumulated = fn.accumulate(accumulated, iterator.next());
      }
    }
    catch (Throwable t) {
      try {
        maker.cleanup(iterator);
      }
      catch (Exception e) {
        t.addSuppressed(e);
      }
      throw t;
    }
    maker.cleanup(iterator);
    return accumulated;
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(
      final OutType initValue,
      final YieldingAccumulator<OutType, T> accumulator
  )
  {
    final IterType iterator = maker.make();// 返回List<Sequence<T>>结果集的迭代器

    try {
      return makeYielder(initValue, accumulator, iterator);
    }
    catch (Throwable t) {
      try {
        maker.cleanup(iterator);
      }
      catch (Exception e) {
        t.addSuppressed(e);
      }
      throw t;
    }
  }

  private <OutType> Yielder<OutType> makeYielder(
      final OutType initValue,
      final YieldingAccumulator<OutType, T> accumulator,
      final IterType iter // List<Sequence<T>>结果集的迭代器
  )
  {
    OutType retVal = initValue;
    while (!accumulator.yielded() && iter.hasNext()) {
      /**
       * 遍历List<Sequence<T>>结果集的每一个Sequence，
       * 然后通过accumulate入参进行“聚合”计算后，将计算结果作为新的“retVal”，
       * 再与下一个Sequence进行聚合计算，
       */
      retVal = accumulator.accumulate(retVal, iter.next());
    }

    if (!accumulator.yielded()) {
      return Yielders.done(
          retVal,
          (Closeable) () -> maker.cleanup(iter)
      );
    }

    final OutType finalRetVal = retVal;
    /**
     * 返回最终的Yielder结果集“retVal”
     */
    return new Yielder<OutType>()
    {
      @Override
      public OutType get()
      {
        return finalRetVal;
      }

      @Override
      public Yielder<OutType> next(OutType initValue)
      {
        accumulator.reset();
        try {
          return makeYielder(initValue, accumulator, iter);
        }
        catch (Throwable t) {
          try {
            maker.cleanup(iter);
          }
          catch (Exception e) {
            t.addSuppressed(e);
          }
          throw t;
        }
      }

      @Override
      public boolean isDone()
      {
        return false;
      }

      @Override
      public void close()
      {
        maker.cleanup(iter);
      }
    };
  }

  public interface IteratorMaker<T, IterType extends Iterator<T>>
  {
    IterType make();

    void cleanup(IterType iterFromMake);
  }
}
