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

package org.apache.druid.server.coordination;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.ServerTypeConfig;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
@ManageLifecycle
public class SegmentLoadDropHandler implements DataSegmentChangeHandler
{
  private static final EmittingLogger log = new EmittingLogger(SegmentLoadDropHandler.class);

  // Synchronizes removals from segmentsToDelete
  private final Object segmentDeleteLock = new Object();

  // Synchronizes start/stop of this object.
  private final Object startStopLock = new Object();

  private final ObjectMapper jsonMapper;
  private final SegmentLoaderConfig config;
  private final DataSegmentAnnouncer announcer;
  private final DataSegmentServerAnnouncer serverAnnouncer;
  private final SegmentManager segmentManager;
  private final ScheduledExecutorService exec;
  private final ConcurrentSkipListSet<DataSegment> segmentsToDelete;

  private volatile boolean started = false;

  // Keep history of load/drop request status in a LRU cache to maintain idempotency if same request shows up
  // again and to return status of a completed request. Maximum size of this cache must be significantly greater
  // than number of pending load/drop requests. so that history is not lost too quickly.
  private final Cache<DataSegmentChangeRequest, AtomicReference<Status>> requestStatuses;
  private final Object requestStatusesLock = new Object();

  // This is the list of unresolved futures returned to callers of processBatch(List<DataSegmentChangeRequest>)
  // Threads loading/dropping segments resolve these futures as and when some segment load/drop finishes.
  private final LinkedHashSet<CustomSettableFuture> waitingFutures = new LinkedHashSet<>();

  @Inject
  public SegmentLoadDropHandler(
      ObjectMapper jsonMapper,
      SegmentLoaderConfig config,
      DataSegmentAnnouncer announcer,
      DataSegmentServerAnnouncer serverAnnouncer,
      SegmentManager segmentManager,
      ServerTypeConfig serverTypeConfig
  )
  {
    this(
        jsonMapper,
        config,
        announcer,
        serverAnnouncer,
        segmentManager,
        Executors.newScheduledThreadPool(
            config.getNumLoadingThreads(),
            Execs.makeThreadFactory("SimpleDataSegmentChangeHandler-%s")
        ),
        serverTypeConfig
    );
  }

  @VisibleForTesting
  SegmentLoadDropHandler(
      ObjectMapper jsonMapper,
      SegmentLoaderConfig config,
      DataSegmentAnnouncer announcer,
      DataSegmentServerAnnouncer serverAnnouncer,
      SegmentManager segmentManager,
      ScheduledExecutorService exec,
      ServerTypeConfig serverTypeConfig
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.announcer = announcer;
    this.serverAnnouncer = serverAnnouncer;
    this.segmentManager = segmentManager;

    this.exec = exec;
    this.segmentsToDelete = new ConcurrentSkipListSet<>();
    requestStatuses = CacheBuilder.newBuilder().maximumSize(config.getStatusQueueMaxSize()).initialCapacity(8).build();
  }

  /**
   * 此start方法非直接调用，
   * {@link Lifecycle.AnnotationBasedHandle}是一个handler包装类，
   * 其内部会装一个handler，也就是当前SegmentLoadDropHandler，
   *
   * 在历史节点启动时，会调用SegmentLoadDropHandler的包装类，也就是
   * {@link Lifecycle.AnnotationBasedHandler#start()}方法。
   *
   * 该方法会检查内部handler对象中是否有@LifecycleStart注解
   * 如果存在，则调用被此注解注解的方法，也就是调用此start()。
   *
   * {@link this#loadLocalCache()}
   * 获取本地所有segment缓存信息文件对象，
   * 遍历cachedSegments中的每一个缓存信息文件对象，
   * 将缓存信息文件对象加载成Segment对象，然后放入{@link SegmentManager#dataSources}中与对应的数据源进行绑定，
   * 后续可由该属性找到各数据源的已加载segment对象
   *
   * =====================================================================================================================
   * 加载总逻辑：
   * 1、首先获取获取info_dir路径，如：apache-druid-0.20.1/var/druid/segment-cache/info_dir。
   * 该路径包含了“缓存信息文件”（并不是缓存数据），每个文件中包含了缓存数据的真正文件路径。其内部是json格式数据
   *
   * 2、遍历并加载所有“缓存信息文件”为List<DataSegment>对象
   *
   * 3、遍历每一个缓存文件对象，找到对应真正的segment缓存文件夹路径，
   * （如：apache-druid-0.20.1/var/druid/segment-cache/test-file/2020-03-01T00:00:00.000Z_2020-04-01T00:00:00.000Z/2021-03-11T12:41:03.686Z/0）
   * 该文件夹下包含：
   * （1）factory.json：文件中只有一个信息，指定了SegmentizerFactory，即用哪一种工厂类，可以加载这些segment文件
   * （2）meta.smoosh：其中记录了各种数据在当前文件夹下xxxx.smoosh中的始末位置。
   * （3）0000.smoosh：包含3部分，各列数据、index.drd数据、metadata.drd数据。
   * index.drd数据：包含了所属的segment中有哪些维度、时间区间、以及使用哪种bitmap。
   * metadata.drd数据：存储了指标聚合函数、查询粒度、时间戳配置等
   *
   * 4、使用从factory.json中加载出来的工厂类来加载文件夹中xxxx.smoosh文件的segment缓存信息。
   * 所谓“加载”，就是将整个xxxx.smoosh文件以“内存映射”的方式映射到内存，并以bytebuffer的格式存在历史节点中。
   * 每次加载数据，都是从这个bytebuffer中截取对应数据的bytebuffer片段。
   * 首先加载index.drd信息，得到所有列名。
   * 再加载metadata.drd信息。
   * 再根据刚刚得到的列名，加载每列的具体值。
   * 再加载__time列的值。
   * 最后加载完毕，相当于得到了缓存文件对象所指的segment缓存文件夹内的segment的所有数据，数据以bytebuffer的形式储存，再将这些数据装进包装类并返回。
   *
   * 5、第4步中得到一个“缓存文件对象”的segment详情包装对象“adapter”，
   * 也可以说该adapter是：“某数据源的某时间片段的segment的所有信息”。
   * adapter中的数据来自文件夹如“apache-druid-0.20.1/var/druid/segment-cache/test-file/2020-03-01T00:00:00.000Z_2020-04-01T00:00:00.000Z/2021-03-11T12:41:03.686Z/0”
   * （以上adapter对象的获取发生在SegmentManager中）
   * SegmentManager中还有一个“dataSources”Map属性，
   * key：各数据源名称，
   * value：即该数据源上的“总加载信息”，其中包含了数据源的各时间轴上的segment信息（也就是上面所说的adapter对象）。
   *
   * 接下来就是将adapter放入SegmentManager的dataSources属性中，
   * 找到对应数据源，然后放入其value的对应时间轴中。
   *
   * 至此所有缓存信息都以“时间轴为单位”，加载成segment对象，并放入SegmentManager的dataSources属性中。
   */
  @LifecycleStart
  public void start() throws IOException
  {
    log.info("SegmentLoadDropHandler.satrt()被调用，this地址："+this.toString());
    synchronized (startStopLock) {
      if (started) {
        return;
      }

      log.info("Starting...");
      try {
        if (!config.getLocations().isEmpty()) {
          /**
           * 加载本地segment文件缓存
           * {@link SegmentLoadDropHandler#start()}
           * {@link SegmentLoadDropHandler#loadLocalCache()}
           * {@link org.apache.druid.server.coordination.SegmentLoadDropHandler#addSegments(Collection, DataSegmentChangeCallback)}
           * {@link org.apache.druid.server.coordination.SegmentLoadDropHandler#loadSegment(DataSegment, DataSegmentChangeCallback, boolean)}
           * {@link org.apache.druid.server.SegmentManager#loadSegment(DataSegment, boolean)}
           * {@link org.apache.druid.server.SegmentManager#getAdapter(DataSegment, boolean)}
           * {@link org.apache.druid.segment.loading.SegmentLoaderLocalCacheManager#getSegment(DataSegment, boolean)}
           *
           * 获取本地所有segment缓存信息文件对象，
           * 遍历cachedSegments中的每一个缓存信息文件对象，
           * 将缓存信息文件对象加载成Segment对象，然后放入{@link SegmentManager#dataSources}中与对应的数据源进行绑定，
           * 后续可由该属性找到各数据源的已加载segment对象
           */
          loadLocalCache();
          serverAnnouncer.announce();
        }
      }
      catch (Exception e) {
        Throwables.propagateIfPossible(e, IOException.class);
        throw new RuntimeException(e);
      }
      started = true;
      log.info("Started.");
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (startStopLock) {
      if (!started) {
        return;
      }

      log.info("Stopping...");
      try {
        if (!config.getLocations().isEmpty()) {
          serverAnnouncer.unannounce();
        }
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
      finally {
        started = false;
      }
      log.info("Stopped.");
    }
  }

  public boolean isStarted()
  {
    return started;
  }

  /**
   * 获取本地所有segment缓存信息文件对象，
   * 遍历cachedSegments中的每一个缓存信息文件对象，
   * 将缓存信息文件对象加载成Segment对象，然后放入{@link SegmentManager#dataSources}中与对应的数据源进行绑定，
   * 后续可由该属性找到各数据源的已加载segment对象
   */
  private void loadLocalCache()
  {
    // 获取系统当前时间
    final long start = System.currentTimeMillis();
    // 获取info_dir路径，
    // 如：apache-druid-0.20.1/var/druid/segment-cache/info_dir
    // 该路径包含了“缓存信息文件”（并不是缓存数据），其内部是json格式数据
    // 每个文件中都包含了关于缓存数据的真正文件路径，已经对应时间区间、包含列、所属数据源等
    File baseDir = config.getInfoDir();
    if (!baseDir.isDirectory()) {
      if (baseDir.exists()) {
        throw new ISE("[%s] exists but not a directory.", baseDir);
      } else if (!baseDir.mkdirs()) {
        throw new ISE("Failed to create directory[%s].", baseDir);
      }
    }

    List<DataSegment> cachedSegments = new ArrayList<>();
    File[] segmentsToLoad = baseDir.listFiles();
    int ignored = 0;
    // 迭代info_dir路径中的每一个“缓存信息文件”
    for (int i = 0; i < segmentsToLoad.length; i++) {
      File file = segmentsToLoad[i];
      log.info("Loading segment cache file [%d/%d][%s].", i + 1, segmentsToLoad.length, file);
      try {
        // 将“缓存信息文件”反序列化成DataSegment对象
        final DataSegment segment = jsonMapper.readValue(file, DataSegment.class);

        // 缓存信息文件中的identifier字段就是文件本身的文件名
        if (!segment.getId().toString().equals(file.getName())) {
          log.warn("Ignoring cache file[%s] for segment[%s].", file.getPath(), segment.getId());
          // 文件名与文件内容identifier字段不符则忽略该文件
          ignored++;
        } else if (segmentManager.isSegmentCached(segment)) {
          cachedSegments.add(segment);
        } else {
          log.warn("Unable to find cache file for %s. Deleting lookup entry", segment.getId());

          File segmentInfoCacheFile = new File(baseDir, segment.getId().toString());
          if (!segmentInfoCacheFile.delete()) {
            log.warn("Unable to delete segmentInfoCacheFile[%s]", segmentInfoCacheFile);
          }
        }
      }
      catch (Exception e) {
        log.makeAlert(e, "Failed to load segment from segmentInfo file")
           .addData("file", file)
           .emit();
      }
    }

    if (ignored > 0) {
      log.makeAlert("Ignored misnamed segment cache files on startup.")
         .addData("numIgnored", ignored)
         .emit();
    }

    /**
     * cachedSegments中包含了所有segment缓存信息文件对象，
     * 遍历cachedSegments中的每一个缓存信息文件对象，
     * 将缓存信息文件对象加载成Segment对象，然后放入{@link SegmentManager#dataSources}中与对应的数据源进行绑定，
     * 后续可由该属性找到各数据源的已加载segment对象
     */
    addSegments(
        cachedSegments,
        () -> log.info("Cache load took %,d ms", System.currentTimeMillis() - start)
    );
  }

  /**
   * Load a single segment. If the segment is loaded successfully, this function simply returns. Otherwise it will
   * throw a SegmentLoadingException
   *
   * @throws SegmentLoadingException if it fails to load the given segment
   *
   * 将DataSegment加载成Segment对象，然后放入{@link SegmentManager#dataSources}中与对应的数据源进行绑定，
   * 后续可由该属性找到各数据源的已加载segment对象
   */
  private void loadSegment(DataSegment segment, DataSegmentChangeCallback callback, boolean lazy) throws SegmentLoadingException
  {
    // segment为缓存信息文件的对象实体
    final boolean loaded;
    try {
      /**
       * 该方法功能就一句话“将DataSegment加载成Segment对象，然后放入dataSources与对应的数据源进行绑定”
       * 对应该功能，整个方法分为两部分
       * 1、调用{@link this#getAdapter(DataSegment, boolean)}方法将DataSegment加载成Segment对象并返回。
       * 2、{@link dataSources}是个map，表示“各数据源已加载的segment信息”。
       * 所以该方法第二部分就是把刚刚加载出的segment与数据源的“总加载信息对象”绑定，具体是和时间轴绑定的。
       * 这样后面查询时，通过某数据源的时间轴信息就直接能从{@link dataSources}中找到其上面的segment对象，用于查询
       *
       * 返回segment是否加载成功
       */
      loaded = segmentManager.loadSegment(segment, lazy);
    }
    catch (Exception e) {
      removeSegment(segment, callback, false);
      throw new SegmentLoadingException(e, "Exception loading segment[%s]", segment.getId());
    }

    // 如果此次segment为新加载，则执行此逻辑
    // 第一次还未被加载的segment信息文件中的json数据应该是不全的，
    // 经过此次加载后，再由此处写回文件，才是完全的信息文件，下次再启动时就无需写入了
    if (loaded) {
      // config.getInfoDir()：segment信息文件所在目录
      // segment.getId().toString()：具体当前segment信息文件文件名
      File segmentInfoCacheFile = new File(config.getInfoDir(), segment.getId().toString());
      if (!segmentInfoCacheFile.exists()) {
        try {
          // 将segment信息对象写回对应文件中
          // 由此可看出，第一次还未被加载的segment信息文件中的json数据应该是不全的，
          // 经过此次加载后，再由此处写回文件，才是完全的信息文件，下次再启动时就无需写入了
          jsonMapper.writeValue(segmentInfoCacheFile, segment);
        }
        catch (IOException e) {
          removeSegment(segment, callback, false);
          throw new SegmentLoadingException(
              e,
              "Failed to write to disk segment info cache file[%s]",
              segmentInfoCacheFile
          );
        }
      }
    }
  }

  @Override
  public void addSegment(DataSegment segment, DataSegmentChangeCallback callback)
  {
    Status result = null;
    try {
      log.info("Loading segment %s", segment.getId());
      /*
         The lock below is used to prevent a race condition when the scheduled runnable in removeSegment() starts,
         and if (segmentsToDelete.remove(segment)) returns true, in which case historical will start deleting segment
         files. At that point, it's possible that right after the "if" check, addSegment() is called and actually loads
         the segment, which makes dropping segment and downloading segment happen at the same time.
       */
      if (segmentsToDelete.contains(segment)) {
        /*
           Both contains(segment) and remove(segment) can be moved inside the synchronized block. However, in that case,
           each time when addSegment() is called, it has to wait for the lock in order to make progress, which will make
           things slow. Given that in most cases segmentsToDelete.contains(segment) returns false, it will save a lot of
           cost of acquiring lock by doing the "contains" check outside the synchronized block.
         */
        synchronized (segmentDeleteLock) {
          segmentsToDelete.remove(segment);
        }
      }
      loadSegment(segment, DataSegmentChangeCallback.NOOP, false);
      // announce segment even if the segment file already exists.
      try {
        announcer.announceSegment(segment);
      }
      catch (IOException e) {
        throw new SegmentLoadingException(e, "Failed to announce segment[%s]", segment.getId());
      }

      result = Status.SUCCESS;
    }
    catch (Exception e) {
      log.makeAlert(e, "Failed to load segment for dataSource")
         .addData("segment", segment)
         .emit();
      result = Status.failed(e.getMessage());
    }
    finally {
      updateRequestStatus(new SegmentChangeRequestLoad(segment), result);
      callback.execute();
    }
  }

  // segments：缓存文件对象集合
  private void addSegments(Collection<DataSegment> segments, final DataSegmentChangeCallback callback)
  {
    ExecutorService loadingExecutor = null;
    try (final BackgroundSegmentAnnouncer backgroundSegmentAnnouncer =
             new BackgroundSegmentAnnouncer(announcer, exec, config.getAnnounceIntervalMillis())) {

      // 开启广播，
      backgroundSegmentAnnouncer.startAnnouncing();

      loadingExecutor = Execs.multiThreaded(config.getNumBootstrapThreads(), "Segment-Load-Startup-%s");

      final int numSegments = segments.size();
      final CountDownLatch latch = new CountDownLatch(numSegments);
      final AtomicInteger counter = new AtomicInteger(0);
      final CopyOnWriteArrayList<DataSegment> failedSegments = new CopyOnWriteArrayList<>();
      //加载每一个segment
      for (final DataSegment segment : segments) {
        loadingExecutor.submit(
            () -> {
              try {
                log.info(
                    "Loading segment[%d/%d][%s]",
                    counter.incrementAndGet(),
                    numSegments,
                    segment.getId()
                );
                /**
                 * 加载segment
                 * 将DataSegment加载成Segment对象，然后放入{@link SegmentManager#dataSources}中与对应的数据源进行绑定，
                 * 后续可由该属性找到各数据源的已加载segment对象
                 */
                loadSegment(segment, callback, config.isLazyLoadOnStart());
                try {
                  // 将segment传入backgroundSegmentAnnouncer对象内部队列中，等待被广播
                  backgroundSegmentAnnouncer.announceSegment(segment);
                }
                catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new SegmentLoadingException(e, "Loading Interrupted");
                }
              }
              catch (SegmentLoadingException e) {
                log.error(e, "[%s] failed to load", segment.getId());
                failedSegments.add(segment);
              }
              finally {
                latch.countDown();
              }
            }
        );
      }

      try {
        latch.await();

        if (failedSegments.size() > 0) {
          log.makeAlert("%,d errors seen while loading segments", failedSegments.size())
             .addData("failedSegments", failedSegments)
             .emit();
        }
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.makeAlert(e, "LoadingInterrupted").emit();
      }

      backgroundSegmentAnnouncer.finishAnnouncing();
    }
    catch (SegmentLoadingException e) {
      log.makeAlert(e, "Failed to load segments -- likely problem with announcing.")
         .addData("numSegments", segments.size())
         .emit();
    }
    finally {
      callback.execute();
      if (loadingExecutor != null) {
        loadingExecutor.shutdownNow();
      }
    }
  }

  @Override
  public void removeSegment(DataSegment segment, DataSegmentChangeCallback callback)
  {
    removeSegment(segment, callback, true);
  }

  private void removeSegment(
      final DataSegment segment,
      final DataSegmentChangeCallback callback,
      final boolean scheduleDrop
  )
  {
    Status result = null;
    try {
      announcer.unannounceSegment(segment);
      segmentsToDelete.add(segment);

      Runnable runnable = () -> {
        try {
          synchronized (segmentDeleteLock) {
            if (segmentsToDelete.remove(segment)) {
              segmentManager.dropSegment(segment);

              File segmentInfoCacheFile = new File(config.getInfoDir(), segment.getId().toString());
              if (!segmentInfoCacheFile.delete()) {
                log.warn("Unable to delete segmentInfoCacheFile[%s]", segmentInfoCacheFile);
              }
            }
          }
        }
        catch (Exception e) {
          log.makeAlert(e, "Failed to remove segment! Possible resource leak!")
             .addData("segment", segment)
             .emit();
        }
      };

      if (scheduleDrop) {
        log.info(
            "Completely removing [%s] in [%,d] millis",
            segment.getId(),
            config.getDropSegmentDelayMillis()
        );
        exec.schedule(
            runnable,
            config.getDropSegmentDelayMillis(),
            TimeUnit.MILLISECONDS
        );
      } else {
        runnable.run();
      }

      result = Status.SUCCESS;
    }
    catch (Exception e) {
      log.makeAlert(e, "Failed to remove segment")
         .addData("segment", segment)
         .emit();
      result = Status.failed(e.getMessage());
    }
    finally {
      updateRequestStatus(new SegmentChangeRequestDrop(segment), result);
      callback.execute();
    }
  }

  public Collection<DataSegment> getPendingDeleteSnapshot()
  {
    return ImmutableList.copyOf(segmentsToDelete);
  }

  public ListenableFuture<List<DataSegmentChangeRequestAndStatus>> processBatch(List<DataSegmentChangeRequest> changeRequests)
  {
    boolean isAnyRequestDone = false;

    Map<DataSegmentChangeRequest, AtomicReference<Status>> statuses = Maps.newHashMapWithExpectedSize(changeRequests.size());

    for (DataSegmentChangeRequest cr : changeRequests) {
      AtomicReference<Status> status = processRequest(cr);
      if (status.get().getState() != Status.STATE.PENDING) {
        isAnyRequestDone = true;
      }
      statuses.put(cr, status);
    }

    CustomSettableFuture future = new CustomSettableFuture(waitingFutures, statuses);

    if (isAnyRequestDone) {
      future.resolve();
    } else {
      synchronized (waitingFutures) {
        waitingFutures.add(future);
      }
    }

    return future;
  }

  private AtomicReference<Status> processRequest(DataSegmentChangeRequest changeRequest)
  {
    synchronized (requestStatusesLock) {
      if (requestStatuses.getIfPresent(changeRequest) == null) {
        changeRequest.go(
            new DataSegmentChangeHandler()
            {
              @Override
              public void addSegment(DataSegment segment, DataSegmentChangeCallback callback)
              {
                requestStatuses.put(changeRequest, new AtomicReference<>(Status.PENDING));
                exec.submit(
                    () -> SegmentLoadDropHandler.this.addSegment(
                        ((SegmentChangeRequestLoad) changeRequest).getSegment(),
                        () -> resolveWaitingFutures()
                    )
                );
              }

              @Override
              public void removeSegment(DataSegment segment, DataSegmentChangeCallback callback)
              {
                requestStatuses.put(changeRequest, new AtomicReference<>(Status.PENDING));
                SegmentLoadDropHandler.this.removeSegment(
                    ((SegmentChangeRequestDrop) changeRequest).getSegment(),
                    () -> resolveWaitingFutures(),
                    true
                );
              }
            },
            this::resolveWaitingFutures
        );
      }
      return requestStatuses.getIfPresent(changeRequest);
    }
  }

  private void updateRequestStatus(DataSegmentChangeRequest changeRequest, Status result)
  {
    if (result == null) {
      result = Status.failed("Unknown reason. Check server logs.");
    }
    synchronized (requestStatusesLock) {
      AtomicReference<Status> statusRef = requestStatuses.getIfPresent(changeRequest);
      if (statusRef != null) {
        statusRef.set(result);
      }
    }
  }

  private void resolveWaitingFutures()
  {
    LinkedHashSet<CustomSettableFuture> waitingFuturesCopy;
    synchronized (waitingFutures) {
      waitingFuturesCopy = new LinkedHashSet<>(waitingFutures);
      waitingFutures.clear();
    }
    for (CustomSettableFuture future : waitingFuturesCopy) {
      future.resolve();
    }
  }

  private static class BackgroundSegmentAnnouncer implements AutoCloseable
  {
    private static final EmittingLogger log = new EmittingLogger(BackgroundSegmentAnnouncer.class);

    private final int intervalMillis;
    private final DataSegmentAnnouncer announcer;
    private final ScheduledExecutorService exec;
    private final LinkedBlockingQueue<DataSegment> queue;
    private final SettableFuture<Boolean> doneAnnouncing;

    private final Object lock = new Object();

    private volatile boolean finished = false;
    @Nullable
    private volatile ScheduledFuture startedAnnouncing = null;
    @Nullable
    private volatile ScheduledFuture nextAnnoucement = null;

    public BackgroundSegmentAnnouncer(
        DataSegmentAnnouncer announcer,
        ScheduledExecutorService exec,
        int intervalMillis
    )
    {
      this.announcer = announcer;
      this.exec = exec;
      this.intervalMillis = intervalMillis;
      this.queue = new LinkedBlockingQueue<>();
      this.doneAnnouncing = SettableFuture.create();
    }

    public void announceSegment(final DataSegment segment) throws InterruptedException
    {
      if (finished) {
        throw new ISE("Announce segment called after finishAnnouncing");
      }
      queue.put(segment);
    }

    public void startAnnouncing() // 开始广播
    {
      if (intervalMillis <= 0) {
        return;
      }

      log.info("Starting background segment announcing task");

      // schedule background announcing task
      nextAnnoucement = startedAnnouncing = exec.schedule(
          new Runnable()
          {
            @Override
            public void run() // 延迟，只执行一次
            {
              synchronized (lock) {
                try {
                  if (!(finished && queue.isEmpty())) {
                    final List<DataSegment> segments = new ArrayList<>();
                    // 将queue队列中对象取出到segments中
                    queue.drainTo(segments);
                    try {
                      // announcer为注入对象
                      log.info("广播器announcer类型："+announcer.getClass());
                      announcer.announceSegments(segments);
                      nextAnnoucement = exec.schedule(this, intervalMillis, TimeUnit.MILLISECONDS);
                    }
                    catch (IOException e) {
                      doneAnnouncing.setException(
                          new SegmentLoadingException(e, "Failed to announce segments[%s]", segments)
                      );
                    }
                  } else {
                    doneAnnouncing.set(true);
                  }
                }
                catch (Exception e) {
                  doneAnnouncing.setException(e);
                }
              }
            }
          },
          intervalMillis,
          TimeUnit.MILLISECONDS
      );
    }

    public void finishAnnouncing() throws SegmentLoadingException
    {
      synchronized (lock) {
        finished = true;
        // announce any remaining segments
        try {
          final List<DataSegment> segments = new ArrayList<>();
          queue.drainTo(segments);
          announcer.announceSegments(segments);
        }
        catch (Exception e) {
          throw new SegmentLoadingException(e, "Failed to announce segments[%s]", queue);
        }

        // get any exception that may have been thrown in background announcing
        try {
          // check in case intervalMillis is <= 0
          if (startedAnnouncing != null) {
            startedAnnouncing.cancel(false);
          }
          // - if the task is waiting on the lock, then the queue will be empty by the time it runs
          // - if the task just released it, then the lock ensures any exception is set in doneAnnouncing
          if (doneAnnouncing.isDone()) {
            doneAnnouncing.get();
          }
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new SegmentLoadingException(e, "Loading Interrupted");
        }
        catch (ExecutionException e) {
          throw new SegmentLoadingException(e.getCause(), "Background Announcing Task Failed");
        }
      }
      log.info("Completed background segment announcing");
    }

    @Override
    public void close()
    {
      // stop background scheduling
      synchronized (lock) {
        finished = true;
        if (nextAnnoucement != null) {
          nextAnnoucement.cancel(false);
        }
      }
    }
  }

  // Future with cancel() implementation to remove it from "waitingFutures" list
  private static class CustomSettableFuture extends AbstractFuture<List<DataSegmentChangeRequestAndStatus>>
  {
    private final LinkedHashSet<CustomSettableFuture> waitingFutures;
    private final Map<DataSegmentChangeRequest, AtomicReference<Status>> statusRefs;

    private CustomSettableFuture(
        LinkedHashSet<CustomSettableFuture> waitingFutures,
        Map<DataSegmentChangeRequest, AtomicReference<Status>> statusRefs
    )
    {
      this.waitingFutures = waitingFutures;
      this.statusRefs = statusRefs;
    }

    public void resolve()
    {
      synchronized (statusRefs) {
        if (isDone()) {
          return;
        }

        List<DataSegmentChangeRequestAndStatus> result = new ArrayList<>(statusRefs.size());
        statusRefs.forEach(
            (request, statusRef) -> result.add(new DataSegmentChangeRequestAndStatus(request, statusRef.get()))
        );

        set(result);
      }
    }

    @Override
    public boolean cancel(boolean interruptIfRunning)
    {
      synchronized (waitingFutures) {
        waitingFutures.remove(this);
      }
      return true;
    }
  }

  public static class Status
  {
    public enum STATE
    {
      SUCCESS, FAILED, PENDING
    }

    private final STATE state;
    @Nullable
    private final String failureCause;

    public static final Status SUCCESS = new Status(STATE.SUCCESS, null);
    public static final Status PENDING = new Status(STATE.PENDING, null);

    @JsonCreator
    Status(
        @JsonProperty("state") STATE state,
        @JsonProperty("failureCause") @Nullable String failureCause
    )
    {
      Preconditions.checkNotNull(state, "state must be non-null");
      this.state = state;
      this.failureCause = failureCause;
    }

    public static Status failed(String cause)
    {
      return new Status(STATE.FAILED, cause);
    }

    @JsonProperty
    public STATE getState()
    {
      return state;
    }

    @Nullable
    @JsonProperty
    public String getFailureCause()
    {
      return failureCause;
    }

    @Override
    public String toString()
    {
      return "Status{" +
             "state=" + state +
             ", failureCause='" + failureCause + '\'' +
             '}';
    }
  }

  public static class DataSegmentChangeRequestAndStatus
  {
    private final DataSegmentChangeRequest request;
    private final Status status;

    @JsonCreator
    public DataSegmentChangeRequestAndStatus(
        @JsonProperty("request") DataSegmentChangeRequest request,
        @JsonProperty("status") Status status
    )
    {
      this.request = request;
      this.status = status;
    }

    @JsonProperty
    public DataSegmentChangeRequest getRequest()
    {
      return request;
    }

    @JsonProperty
    public Status getStatus()
    {
      return status;
    }

    @Override
    public String toString()
    {
      return "DataSegmentChangeRequestAndStatus{" +
             "request=" + request +
             ", status=" + status +
             '}';
    }
  }
}

