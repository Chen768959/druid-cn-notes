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

import com.google.common.base.Supplier;
import org.apache.druid.java.util.emitter.EmittingLogger;

/**
 */
public class LazySequence<T> implements Sequence<T>
{
  private static final EmittingLogger log = new EmittingLogger(LazySequence.class);
  private final Supplier<Sequence<T>> provider;

  public LazySequence(
      Supplier<Sequence<T>> provider
  )
  {
    this.provider = provider;
  }

  @Override
  public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, T> accumulator)
  {
    return provider.get().accumulate(initValue, accumulator);
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
  {
    /**
     * 此处provider.get()实际上就是调用之前准备的匿名函数，用于发送http请求获取查询结果，
     * 也是在此处才真正进行查询的
     * 此匿名函数存在于{@link org.apache.druid.client.DirectDruidClient#run(QueryPlus, ResponseContext)}创建LazySequence时
     */
    return provider.get().toYielder(initValue, accumulator);
  }
}
