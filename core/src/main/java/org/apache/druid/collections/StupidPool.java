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

package org.apache.druid.collections;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import org.apache.druid.java.util.common.Cleaners;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;

import java.lang.ref.WeakReference;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class StupidPool<T> implements NonBlockingPool<T>
{
  private static final Logger log = new Logger(StupidPool.class);

  /**
   * StupidPool Implementation Note
   * It is assumed that StupidPools are never reclaimed by the GC, either stored in static fields or global singleton
   * injector like Guice. Otherwise false positive "Not closed! Object leaked from..." could be reported. To avoid
   * this, StupidPool should be made closeable (or implement {@link org.apache.druid.java.util.common.lifecycle.LifecycleStop}
   * and registered in the global lifecycle), in this close() method all {@link ObjectResourceHolder}s should be drained
   * from the {@code objects} queue, and notifier.disable() called for them.
   */
  @VisibleForTesting
  final Queue<ObjectResourceHolder> objects = new ConcurrentLinkedQueue<>();

  /**
   * {@link ConcurrentLinkedQueue}'s size() is O(n) queue traversal apparently for the sake of being 100%
   * wait-free, that is not required by {@code StupidPool}. In {@code poolSize} we account the queue size
   * ourselves, to avoid traversal of {@link #objects} in {@link #tryReturnToPool}.
   */
  @VisibleForTesting
  final AtomicLong poolSize = new AtomicLong(0);

  private final String name;
  private final Supplier<T> generator;

  private final AtomicLong createdObjectsCounter = new AtomicLong(0);
  private final AtomicLong leakedObjectsCounter = new AtomicLong(0);

  //note that this is just the max entries in the cache, pool can still create as many buffers as needed.
  private final int objectsCacheMaxCount;

  public StupidPool(String name, Supplier<T> generator)
  {
    this(name, generator, 0, Integer.MAX_VALUE);
  }

  public StupidPool(String name, Supplier<T> generator, int initCount, int objectsCacheMaxCount)
  {
//    log.info("!!!：创建StupidPool对象，name为："+name);
    Preconditions.checkArgument(
        initCount <= objectsCacheMaxCount,
        "initCount[%s] must be less/equal to objectsCacheMaxCount[%s]",
        initCount,
        objectsCacheMaxCount
    );
    this.name = name;
    this.generator = generator;
    this.objectsCacheMaxCount = objectsCacheMaxCount;

    //创建队列的初始ObjectResourceHolder对象
    for (int i = 0; i < initCount; i++) {
      objects.add(makeObjectWithHandler());
      poolSize.incrementAndGet();
    }
//    log.info("!!!：StupidPool对象创建完毕，内部队列长度："+objects.size()+"...initCount："+initCount+"...name为："+name);
  }

  @Override
  public String toString()
  {
    return "StupidPool{" +
           "name=" + name +
           ", objectsCacheMaxCount=" + objectsCacheMaxCount +
           ", poolSize=" + poolSize() +
           "}";
  }

  /**
   * 当前StupidPool相当于装了ObjectResourceHolder对象的队列
   * 此处的take就是从队列头部“取出”（取出后队列中就不存在了）一个ObjectResourceHolder对象
   *
   * 这个ObjectResourceHolder对象定义于StupidPool内部，
   * 其作用很简单，相当于对于某个泛型T对象的封装，可以其通过get()方法获取内部这个泛型对象。
   * 如获取long向量bytebuffer，也是通过此方法
   *
   * 如果内部队列中没有元素（是个新的StupidPool），
   * 则此take方法会调用makeObjectWithHandler()创建个新的holder包装类，
   * 包装类中的初始对象就是StupidPool中generator工具产生的，
   * 以long聚合器来说，此时往StupidPool中产生的是个空buffer。
   *
   * 然后返回该holder
   */
  @Override
  public ResourceHolder<T> take()
  {
    ObjectResourceHolder resourceHolder = objects.poll();
    if (resourceHolder == null) {
//      log.info("!!!：StupidPool.take()，不存在resourceHolder");
      return makeObjectWithHandler();
    } else {
//      log.info("!!!：StupidPool.take()，存在resourceHolder");
      // 当前池大小减一
      poolSize.decrementAndGet();
      return resourceHolder;
    }
  }

  private ObjectResourceHolder makeObjectWithHandler()
  {
    /** {@link org.apache.druid.segment.CompressedPool}*/
//    log.info("!!!：StupidPool中generator类型："+generator.getClass());
    T object = generator.get();
    createdObjectsCounter.incrementAndGet();
    ObjectId objectId = new ObjectId();
    ObjectLeakNotifier notifier = new ObjectLeakNotifier(this);
    // Using objectId as referent for Cleaner, because if the object itself (e. g. ByteBuffer) is leaked after taken
    // from the pool, and the ResourceHolder is not closed, Cleaner won't notify about the leak.
    return new ObjectResourceHolder(object, objectId, Cleaners.register(objectId, notifier), notifier);
  }

  @VisibleForTesting
  public long poolSize()
  {
    return poolSize.get();
  }

  @VisibleForTesting
  long leakedObjectsCount()
  {
    return leakedObjectsCounter.get();
  }

  @VisibleForTesting
  public long objectsCreatedCount()
  {
    return createdObjectsCounter.get();
  }

  private void tryReturnToPool(T object, ObjectId objectId, Cleaners.Cleanable cleanable, ObjectLeakNotifier notifier)
  {
    long currentPoolSize;
    do {
      currentPoolSize = poolSize.get();
      if (currentPoolSize >= objectsCacheMaxCount) {
        notifier.disable();
        // Effectively does nothing, because notifier is disabled above. The purpose of this call is to deregister the
        // cleaner from the internal global linked list of all cleaners in the JVM, and let it be reclaimed itself.
        cleanable.clean();

        // Important to use the objectId after notifier.disable() (in the logging statement below), otherwise VM may
        // already decide that the objectId is unreachable and run Cleaner before notifier.disable(), that would be
        // reported as a false-positive "leak". Ideally reachabilityFence(objectId) should be inserted here.
        log.debug("cache num entries is exceeding in [%s], objectId [%s]", this, objectId);
        return;
      }
    } while (!poolSize.compareAndSet(currentPoolSize, currentPoolSize + 1));
    /**
     * 将此次关闭的holder中的被包装对象放入一个新holder中，然后添加进内部队列
     */
    if (!objects.offer(new ObjectResourceHolder(object, objectId, cleanable, notifier))) {
      // 如果添加失败
      impossibleOffsetFailed(object, objectId, cleanable, notifier);
    }
  }

  /**
   * This should be impossible, because {@link ConcurrentLinkedQueue#offer(Object)} event don't have `return false;` in
   * it's body in OpenJDK 8.
   */
  private void impossibleOffsetFailed(
      T object,
      ObjectId objectId,
      Cleaners.Cleanable cleanable,
      ObjectLeakNotifier notifier
  )
  {
    poolSize.decrementAndGet();
    notifier.disable();
    // Effectively does nothing, because notifier is disabled above. The purpose of this call is to deregister the
    // cleaner from the internal global linked list of all cleaners in the JVM, and let it be reclaimed itself.
    cleanable.clean();
    log.error(
        new ISE("Queue offer failed"),
        "Could not offer object [%s] back into the queue, objectId [%s]",
        object,
        objectId
    );
  }

  private class ObjectResourceHolder implements ResourceHolder<T>
  {
    private final AtomicReference<T> objectRef;
    private ObjectId objectId;
    private Cleaners.Cleanable cleanable;
    private ObjectLeakNotifier notifier;

    /**
     * 可以看到此包装类真正的对象object是在创建ObjectResourceHolder时被传入的
     */
    ObjectResourceHolder(
        final T object,
        final ObjectId objectId,
        final Cleaners.Cleanable cleanable,
        final ObjectLeakNotifier notifier
    )
    {
      this.objectRef = new AtomicReference<>(object);
      this.objectId = objectId;
      this.cleanable = cleanable;
      this.notifier = notifier;
    }

    // WARNING: it is entirely possible for a caller to hold onto the object and call ObjectResourceHolder.close,
    // Then still use that object even though it will be offered to someone else in StupidPool.take
    @Override
    public T get()
    {
      final T object = objectRef.get();
      if (object == null) {
        throw new ISE("Already Closed!");
      }

      return object;
    }

    /**
     * 关闭某个holder时调用
     * 这里会尝试再将此holder装回内部队列
     */
    @Override
    public void close()
    {
      final T object = objectRef.get();
      if (object != null && objectRef.compareAndSet(object, null)) {
        try {
//          log.info("!!!：close，尝、试将ObjectResourceHolder返回Pool，PoolName："+name+"...objId；"+objectId);
          tryReturnToPool(object, objectId, cleanable, notifier);
        }
        finally {
          // Need to null reference to objectId because if ObjectResourceHolder is closed, but leaked, this reference
          // will prevent reporting leaks of ResourceHandlers when this object and objectId are taken from the pool
          // again.
          objectId = null;
          // Nulling cleaner and notifier is not strictly needed, but harmless for sure.
          cleanable = null;
          notifier = null;
        }
      }
    }
  }

  private static class ObjectLeakNotifier implements Runnable
  {
    /**
     * Don't reference {@link StupidPool} directly to prevent it's leak through the internal global chain of Cleaners.
     */
    final WeakReference<StupidPool<?>> poolReference;
    final AtomicLong leakedObjectsCounter;
    final AtomicBoolean disabled = new AtomicBoolean(false);

    ObjectLeakNotifier(StupidPool<?> pool)
    {
      poolReference = new WeakReference<>(pool);
      leakedObjectsCounter = pool.leakedObjectsCounter;
    }

    @Override
    public void run()
    {
      try {
        if (!disabled.getAndSet(true)) {
          leakedObjectsCounter.incrementAndGet();
          log.warn("Not closed! Object leaked from %s. Allowing gc to prevent leak.", poolReference.get());
        }
      }
      // Exceptions must not be thrown in Cleaner.clean(), which calls this ObjectReclaimer.run() method
      catch (Exception e) {
        try {
          log.error(e, "Exception in ObjectLeakNotifier.run()");
        }
        catch (Exception ignore) {
          // ignore
        }
      }
    }

    public void disable()
    {
      disabled.set(true);
    }
  }

  /**
   * Plays the role of the reference for Cleaner, see comment in {@link #makeObjectWithHandler}
   */
  private static class ObjectId
  {
  }
}
