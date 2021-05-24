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

package org.apache.druid.segment.loading;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SimpleQueryableIndex;
import org.apache.druid.server.coordination.DataSegmentChangeCallback;
import org.apache.druid.server.coordination.SegmentLoadDropHandler;
import org.apache.druid.timeline.DataSegment;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 */
public class SegmentLoaderLocalCacheManager implements SegmentLoader
{
  private static final EmittingLogger log = new EmittingLogger(SegmentLoaderLocalCacheManager.class);

  private final IndexIO indexIO;
  private final SegmentLoaderConfig config;
  private final ObjectMapper jsonMapper;

  private final List<StorageLocation> locations;

  // This directoryWriteRemoveLock is used when creating or removing a directory
  private final Object directoryWriteRemoveLock = new Object();

  /**
   * A map between segment and referenceCountingLocks.
   *
   * These locks should be acquired whenever getting or deleting files for a segment.
   * If different threads try to get or delete files simultaneously, one of them creates a lock first using
   * {@link #createOrGetLock}. And then, all threads compete with each other to get the lock.
   * Finally, the lock should be released using {@link #unlock}.
   *
   * An example usage is:
   *
   * final ReferenceCountingLock lock = createOrGetLock(segment);
   * synchronized (lock) {
   *   try {
   *     doSomething();
   *   }
   *   finally {
   *     unlock(lock);
   *   }
   * }
   */
  private final ConcurrentHashMap<DataSegment, ReferenceCountingLock> segmentLocks = new ConcurrentHashMap<>();

  private final StorageLocationSelectorStrategy strategy;

  // Note that we only create this via injection in historical and realtime nodes. Peons create these
  // objects via SegmentLoaderFactory objects, so that they can store segments in task-specific
  // directories rather than statically configured directories.
  @Inject
  public SegmentLoaderLocalCacheManager(
      IndexIO indexIO,
      SegmentLoaderConfig config,
      @Json ObjectMapper mapper
  )
  {
    this.indexIO = indexIO;
    this.config = config;
    this.jsonMapper = mapper;
    this.locations = new ArrayList<>();
    for (StorageLocationConfig locationConfig : config.getLocations()) {
      locations.add(
          new StorageLocation(
              locationConfig.getPath(),
              locationConfig.getMaxSize(),
              locationConfig.getFreeSpacePercent()
          )
      );
    }
    this.strategy = config.getStorageLocationSelectorStrategy(locations);
  }

  @Override
  public boolean isSegmentLoaded(final DataSegment segment)
  {
    return findStorageLocationIfLoaded(segment) != null;
  }

  private StorageLocation findStorageLocationIfLoaded(final DataSegment segment)
  {
    for (StorageLocation location : locations) {
      File localStorageDir = new File(location.getPath(), DataSegmentPusher.getDefaultStorageDir(segment, false));
      if (localStorageDir.exists()) {
        return location;
      }
    }
    return null;
  }

  /**
   * 调用factorize路径
   * {@link SegmentLoadDropHandler#start()}
   * {@link SegmentLoadDropHandler#loadLocalCache()}
   * {@link org.apache.druid.server.coordination.SegmentLoadDropHandler#addSegments(Collection, DataSegmentChangeCallback)}
   * {@link org.apache.druid.server.coordination.SegmentLoadDropHandler#loadSegment(DataSegment, DataSegmentChangeCallback, boolean)}
   * {@link org.apache.druid.server.SegmentManager#loadSegment(DataSegment, boolean)}
   * {@link org.apache.druid.server.SegmentManager#getAdapter(DataSegment, boolean)}
   * {@link org.apache.druid.segment.loading.SegmentLoaderLocalCacheManager#getSegment(DataSegment, boolean)}
   */
  @Override
  public Segment getSegment(DataSegment segment, boolean lazy) throws SegmentLoadingException
  {
    final ReferenceCountingLock lock = createOrGetLock(segment);
    final File segmentFiles;
    synchronized (lock) {
      try {
        /**
         * segment是缓存信息对象（不是真正的segment对象），
         * 该segment缓存信息对象原文件所属于/home/operation/software/apache-druid-0.20.1/var/druid/segment-cache/info_dir目录
         * 根据“缓存信息文件对象”，获取该时间区间上的真正segment缓存数据所在的文件夹对象
         *
         * 此处获得的segmentFiles是缓存segment文件所在文件夹
         * 如（apache-druid-0.20.1/var/druid/segment-cache/test-file/2020-03-01T00:00:00.000Z_2020-04-01T00:00:00.000Z/2021-03-11T12:41:03.686Z/0）
         */
        segmentFiles = getSegmentFiles(segment);
      }
      finally {
        unlock(segment, lock);
      }
    }
    // 该时间区间文件夹下除了对应的segment数据文件，还有factory.json文件，
    // 该文件中只有一个信息，也就是指定了SegmentizerFactory，即用哪一种工厂类，可以加载这些segment文件
    File factoryJson = new File(segmentFiles, "factory.json");
    final SegmentizerFactory factory;

    if (factoryJson.exists()) {
      try {
        factory = jsonMapper.readValue(factoryJson, SegmentizerFactory.class);
      }
      catch (IOException e) {
        throw new SegmentLoadingException(e, "%s", e.getMessage());
      }
    } else {
      factory = new MMappedQueryableSegmentizerFactory(indexIO);
    }

    /**
     * {@link org.apache.druid.segment.loading.MMappedQueryableSegmentizerFactory#factorize(DataSegment, File, boolean)}
     * {@link org.apache.druid.segment.IndexIO#loadIndex(File, boolean)}
     * {@link org.apache.druid.segment.IndexIO.V9IndexLoader#load(File, ObjectMapper, boolean)}
     *
     * segment：本次需要加载的segment的“缓存文件信息”
     * segmentFiles：真正的segment缓存数据文件所在文件夹
     *
     * 返回的是一个Segment对象，其中有两部分
     * （1）segmentId，由传参segment得来，根据此id可知返回的Segment对象是哪一个segment
     * （2）根据segmentFiles目录，得到该目录下所有segment相关文件的解析结果对象，一个SimpleQueryableIndex：
     * SimpleQueryableIndex{@link SimpleQueryableIndex}本身没有逻辑，只是个包装类，将所有的inDir目录下的segment信息存储其中。
     * 此index对象中较为重要的解析结果为：
     * 1、metadata：xxxx.smoosh文件中的metadata.drd信息所转化
     * 2、columns：map类型 key为所有列名，value为该列的包装类
     * 3、dimensionHandlers：map类型，key为各维度列名，value为该列数据类型对应的handler对象
     * 4、fileMapper：包含了同文件夹下所有smoosh文件的File对象，以及meta.smoosh内的所有数据信
     */
    return factory.factorize(segment, segmentFiles, lazy);
  }

  @Override
  public File getSegmentFiles(DataSegment segment) throws SegmentLoadingException
  {
    final ReferenceCountingLock lock = createOrGetLock(segment);
    synchronized (lock) {
      try {
        StorageLocation loc = findStorageLocationIfLoaded(segment);
        String storageDir = DataSegmentPusher.getDefaultStorageDir(segment, false);

        if (loc == null) {
          loc = loadSegmentWithRetry(segment, storageDir);
        }
        return new File(loc.getPath(), storageDir);
      }
      finally {
        unlock(segment, lock);
      }
    }
  }

  /**
   * location may fail because of IO failure, most likely in two cases:<p>
   * 1. druid don't have the write access to this location, most likely the administrator doesn't config it correctly<p>
   * 2. disk failure, druid can't read/write to this disk anymore
   *
   * Locations are fetched using {@link StorageLocationSelectorStrategy}.
   */
  private StorageLocation loadSegmentWithRetry(DataSegment segment, String storageDirStr) throws SegmentLoadingException
  {
    Iterator<StorageLocation> locationsIterator = strategy.getLocations();

    while (locationsIterator.hasNext()) {

      StorageLocation loc = locationsIterator.next();

      File storageDir = loc.reserve(storageDirStr, segment);
      if (storageDir != null) {
        try {
          loadInLocationWithStartMarker(segment, storageDir);
          return loc;
        }
        catch (SegmentLoadingException e) {
          try {
            log.makeAlert(
                e,
                "Failed to load segment in current location [%s], try next location if any",
                loc.getPath().getAbsolutePath()
            ).addData("location", loc.getPath().getAbsolutePath()).emit();
          }
          finally {
            loc.removeSegmentDir(storageDir, segment);
            cleanupCacheFiles(loc.getPath(), storageDir);
          }
        }
      }
    }
    throw new SegmentLoadingException("Failed to load segment %s in all locations.", segment.getId());
  }

  private void loadInLocationWithStartMarker(DataSegment segment, File storageDir) throws SegmentLoadingException
  {
    // We use a marker to prevent the case where a segment is downloaded, but before the download completes,
    // the parent directories of the segment are removed
    final File downloadStartMarker = new File(storageDir, "downloadStartMarker");
    synchronized (directoryWriteRemoveLock) {
      if (!storageDir.mkdirs()) {
        log.debug("Unable to make parent file[%s]", storageDir);
      }
      try {
        if (!downloadStartMarker.createNewFile()) {
          throw new SegmentLoadingException("Was not able to create new download marker for [%s]", storageDir);
        }
      }
      catch (IOException e) {
        throw new SegmentLoadingException(e, "Unable to create marker file for [%s]", storageDir);
      }
    }
    loadInLocation(segment, storageDir);

    if (!downloadStartMarker.delete()) {
      throw new SegmentLoadingException("Unable to remove marker file for [%s]", storageDir);
    }
  }

  private void loadInLocation(DataSegment segment, File storageDir) throws SegmentLoadingException
  {
    // LoadSpec isn't materialized until here so that any system can interpret Segment without having to have all the
    // LoadSpec dependencies.
    final LoadSpec loadSpec = jsonMapper.convertValue(segment.getLoadSpec(), LoadSpec.class);
    final LoadSpec.LoadSpecResult result = loadSpec.loadSegment(storageDir);
    if (result.getSize() != segment.getSize()) {
      log.warn(
          "Segment [%s] is different than expected size. Expected [%d] found [%d]",
          segment.getId(),
          segment.getSize(),
          result.getSize()
      );
    }
  }

  @Override
  public void cleanup(DataSegment segment)
  {
    if (!config.isDeleteOnRemove()) {
      return;
    }

    final ReferenceCountingLock lock = createOrGetLock(segment);
    synchronized (lock) {
      try {
        StorageLocation loc = findStorageLocationIfLoaded(segment);

        if (loc == null) {
          log.warn("Asked to cleanup something[%s] that didn't exist.  Skipping.", segment.getId());
          return;
        }

        // If storageDir.mkdirs() success, but downloadStartMarker.createNewFile() failed,
        // in this case, findStorageLocationIfLoaded() will think segment is located in the failed storageDir which is actually not.
        // So we should always clean all possible locations here
        for (StorageLocation location : locations) {
          File localStorageDir = new File(location.getPath(), DataSegmentPusher.getDefaultStorageDir(segment, false));
          if (localStorageDir.exists()) {
            // Druid creates folders of the form dataSource/interval/version/partitionNum.
            // We need to clean up all these directories if they are all empty.
            cleanupCacheFiles(location.getPath(), localStorageDir);
            location.removeSegmentDir(localStorageDir, segment);
          }
        }
      }
      finally {
        unlock(segment, lock);
      }
    }
  }

  private void cleanupCacheFiles(File baseFile, File cacheFile)
  {
    if (cacheFile.equals(baseFile)) {
      return;
    }

    synchronized (directoryWriteRemoveLock) {
      log.info("Deleting directory[%s]", cacheFile);
      try {
        FileUtils.deleteDirectory(cacheFile);
      }
      catch (Exception e) {
        log.error(e, "Unable to remove directory[%s]", cacheFile);
      }
    }

    File parent = cacheFile.getParentFile();
    if (parent != null) {
      File[] children = parent.listFiles();
      if (children == null || children.length == 0) {
        cleanupCacheFiles(baseFile, parent);
      }
    }
  }

  private ReferenceCountingLock createOrGetLock(DataSegment dataSegment)
  {
    return segmentLocks.compute(
        dataSegment,
        (segment, lock) -> {
          final ReferenceCountingLock nonNullLock;
          if (lock == null) {
            nonNullLock = new ReferenceCountingLock();
          } else {
            nonNullLock = lock;
          }
          nonNullLock.increment();
          return nonNullLock;
        }
    );
  }

  @SuppressWarnings("ObjectEquality")
  private void unlock(DataSegment dataSegment, ReferenceCountingLock lock)
  {
    segmentLocks.compute(
        dataSegment,
        (segment, existingLock) -> {
          if (existingLock == null) {
            throw new ISE("Lock has already been removed");
          } else if (existingLock != lock) {
            throw new ISE("Different lock instance");
          } else {
            if (existingLock.numReferences == 1) {
              return null;
            } else {
              existingLock.decrement();
              return existingLock;
            }
          }
        }
    );
  }

  @VisibleForTesting
  private static class ReferenceCountingLock
  {
    private int numReferences;

    private void increment()
    {
      ++numReferences;
    }

    private void decrement()
    {
      --numReferences;
    }
  }

  @VisibleForTesting
  public ConcurrentHashMap<DataSegment, ReferenceCountingLock> getSegmentLocks()
  {
    return segmentLocks;
  }

  @VisibleForTesting
  public List<StorageLocation> getLocations()
  {
    return locations;
  }
}
