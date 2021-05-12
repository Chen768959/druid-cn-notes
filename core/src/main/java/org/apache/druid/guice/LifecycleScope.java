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

package org.apache.druid.guice;

import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Scope;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * A scope that adds objects to the Lifecycle.  This is by definition also a lazy singleton scope.
 */
public class LifecycleScope implements Scope
{
  private static final Logger log = new Logger(LifecycleScope.class);
  private final Lifecycle.Stage stage;

  private Lifecycle lifecycle;
  private final List<Object> instances = new ArrayList<>();

  public LifecycleScope(Lifecycle.Stage stage)
  {
    this.stage = stage;
  }

  public void setLifecycle(Lifecycle lifecycle)
  {
    synchronized (instances) {
      this.lifecycle = lifecycle;
      for (Object instance : instances) {
        lifecycle.addManagedInstance(instance, stage);
      }
    }
  }

  /**
   * Provider类型为被注入对象的包装类，
   * 其get方法返回需要注入的对象。
   *
   * 此处接受unscoped调用get获取需要被注入的对象，
   * 然后放入lifecycle.addMaybeStartManagedInstance中
   */
  @Override
  public <T> Provider<T> scope(final Key<T> key, final Provider<T> unscoped)
  {
    return new Provider<T>()
    {
      private volatile T value = null;

      @Override
      public synchronized T get()
      {
        if (value == null) {
          final T retVal = unscoped.get();
          new Logger(Scope.class).info("scope，unscoped："+unscoped.toString()+"...retVal："+retVal.toString()+"...class："+retVal.getClass());

          synchronized (instances) {
            if (lifecycle == null) {
              instances.add(retVal);
            } else {
              try {
                lifecycle.addMaybeStartManagedInstance(retVal, stage);
              }
              catch (Exception e) {
                log.warn(e, "Caught exception when trying to create a[%s]", key);
                return null;
              }
            }
          }

          value = retVal;
        }

        return value;
      }
    };
  }
}
