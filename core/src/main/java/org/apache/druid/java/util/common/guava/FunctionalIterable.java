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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import org.apache.druid.java.util.common.guava.nary.TrinaryFn;
import org.apache.druid.java.util.common.guava.nary.TrinaryTransformIterable;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * 用于操作“Iterable<T> delegate”列表的，函数式工具类
 */
public class FunctionalIterable<T> implements Iterable<T>
{
  private final Iterable<T> delegate;

  public static <T> FunctionalIterable<T> create(Iterable<T> delegate)
  {
    return new FunctionalIterable<>(delegate);
  }

  // 待处理列表，每个该函数式工具类必须传入这个待处理列表
  public FunctionalIterable(
      Iterable<T> delegate
  )
  {
    this.delegate = delegate;
  }

  @Override
  public Iterator<T> iterator()
  {
    return delegate.iterator();
  }

  public <RetType> FunctionalIterable<RetType> transform(Function<T, RetType> fn)
  {
    return new FunctionalIterable<>(Iterables.transform(delegate, fn));
  }

  /**
   * 使用fn方法处理当前待处理列表中的每一个对象，
   * 并将各个返回结果合并为一个列表并返回
   */
  public <RetType> FunctionalIterable<RetType> transformCat(Function<T, Iterable<RetType>> fn)
  {
    /**
     * delegate就是Iterable<SegmentDescriptor>
     * 即segment信息列表
     *
     * Iterables.transform(delegate, fn)：
     * 即map操作，fn处理每一个segment信息，然后每一个segment信息处理后都返回一个“Iterable<RetType>列表”
     *
     * Iterables.concat：
     * 将这些Iterable<RetType>列表合并为一个Iterable<RetType>列表
     */
    return new FunctionalIterable<>(Iterables.concat(Iterables.transform(delegate, fn)));
  }

  public <RetType> FunctionalIterable<RetType> keep(Function<T, RetType> fn)
  {
    return new FunctionalIterable<>(Iterables.filter(Iterables.transform(delegate, fn), Predicates.notNull()));
  }

  public FunctionalIterable<T> filter(Predicate<T> pred)
  {
    return new FunctionalIterable<>(Iterables.filter(delegate, pred));
  }

  public FunctionalIterable<T> drop(int numToDrop)
  {
    return new FunctionalIterable<>(new DroppingIterable<>(delegate, numToDrop));
  }

  public FunctionalIterable<T> limit(int limit)
  {
    return new FunctionalIterable<>(Iterables.limit(delegate, limit));
  }

  public <InType1, InType2, RetType> FunctionalIterable<RetType> trinaryTransform(
      final Iterable<InType1> iterable1,
      final Iterable<InType2> iterable2,
      final TrinaryFn<T, InType1, InType2, RetType> trinaryFn
  )
  {
    return new FunctionalIterable<>(TrinaryTransformIterable.create(delegate, iterable1, iterable2, trinaryFn));
  }
}
