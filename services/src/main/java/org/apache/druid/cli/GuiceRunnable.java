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

package org.apache.druid.cli;

import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.log.StartupLoggingConfig;
import org.apache.druid.utils.JvmUtils;

import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 */
public abstract class GuiceRunnable implements Runnable
{
  private final Logger log;

  private Injector baseInjector;

  public GuiceRunnable(Logger log)
  {
    this.log = log;
  }

  /**
   * This method overrides {@link Runnable} just in order to be able to define it as "entry point" for
   * "Unused declarations" inspection in IntelliJ.
   */
  @Override
  public abstract void run();

  @Inject
  public void configure(Injector injector)
  {
    this.baseInjector = injector;
  }

  protected abstract List<? extends Module> getModules();

  public Injector makeInjector()
  {
    try {
      return Initialization.makeInjectorWithModules(baseInjector, getModules());
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Lifecycle initLifecycle(Injector injector)
  {
    try {
      log.info("!!!：进入initLifecycle");
      /**
       * Lifecycle定义在{@link org.apache.druid.guice.LifecycleModule#getLifecycle(Injector)}方法上，
       * 其返回对象，就是Lifecycle的实现类
       */
      final Lifecycle lifecycle = injector.getInstance(Lifecycle.class);
      final StartupLoggingConfig startupLoggingConfig = injector.getInstance(StartupLoggingConfig.class);

      //堆外内存大小
      Long directSizeBytes = null;
      try {
        directSizeBytes = JvmUtils.getRuntimeInfo().getDirectMemorySizeBytes();
      }
      catch (UnsupportedOperationException ignore) {
        // querying direct memory is not supported
      }

      // 输出日志
      log.info(
          "Starting up with processors[%,d], memory[%,d], maxMemory[%,d]%s. Properties follow.",
          JvmUtils.getRuntimeInfo().getAvailableProcessors(),
          JvmUtils.getRuntimeInfo().getTotalHeapSizeBytes(),
          JvmUtils.getRuntimeInfo().getMaxHeapSizeBytes(),
          directSizeBytes != null ? StringUtils.format(", directMemory[%,d]", directSizeBytes) : ""
      );

      if (startupLoggingConfig.isLogProperties()) {
        final Set<String> maskProperties = Sets.newHashSet(startupLoggingConfig.getMaskProperties());
        final Properties props = injector.getInstance(Properties.class);

        for (String propertyName : Ordering.natural().sortedCopy(props.stringPropertyNames())) {
          String property = props.getProperty(propertyName);
          for (String masked : maskProperties) {
            if (propertyName.contains(masked)) {
              property = "<masked>";
              break;
            }
          }
          log.info("* %s: %s", propertyName, property);
        }
      }

      try {
        /**
         * 迭代了lifecycle中的handlers集合，
         * handler是一个map集合，的key是{@link Lifecycle.Stage#values()}中的4种状态，
         * value是List<Handler>类型
         * 此集合就是每个Stage对应的handler列表。
         *
         *
         */
        lifecycle.start();
      }
      catch (Throwable t) {
        log.error(t, "Error when starting up.  Failing.");
        System.exit(1);
      }

      return lifecycle;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
