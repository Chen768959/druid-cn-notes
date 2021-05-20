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

package org.apache.druid.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import org.apache.druid.common.guava.SettableSupplier;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SimpleQueryableIndex;
import org.apache.druid.segment.join.table.IndexedTable;
import org.apache.druid.segment.join.table.ReferenceCountingIndexedTable;
import org.apache.druid.segment.loading.SegmentLoader;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.PartitionHolder;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.utils.CollectionUtils;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * This class is responsible for managing data sources and their states like timeline, total segment size, and number of
 * segments.  All public methods of this class must be thread-safe.
 */
public class SegmentManager
{
  private static final EmittingLogger log = new EmittingLogger(SegmentManager.class);

  private final SegmentLoader segmentLoader;
  /**
   * dataSources是个map，表示“各数据源已加载的segment信息”
   * key：各数据源名称，
   * value：即该数据源上的“总加载信息”，其中包含2个重要属性
   * （1）该数据源已加载的所有segment数量和大小
   * （2）Timeline:“用来储存该数据源的各时间轴以及其上的各个ReferenceCountingSegment对象(segement真正的本体)”
   */
  private final ConcurrentHashMap<String, DataSourceState> dataSources = new ConcurrentHashMap<>();

  /**
   * Represent the state of a data source including the timeline, total segment size, and number of segments.
   * 即该数据源上的“总加载信息”，其中包含2个重要属性
   * （1）该数据源已加载的所有segment数量和大小
   * （2）timeline:“用来储存该数据源的各时间轴以及其上的各个ReferenceCountingSegment对象(segement真正的本体)”
   */
  public static class DataSourceState
  {
    private final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline =
        new VersionedIntervalTimeline<>(Ordering.natural());

    private final ConcurrentHashMap<SegmentId, ReferenceCountingIndexedTable> tablesLookup = new ConcurrentHashMap<>();
    private long totalSegmentSize;
    private long numSegments;

    private void addSegment(DataSegment segment)
    {
      totalSegmentSize += segment.getSize();
      numSegments++;
    }

    private void removeSegment(DataSegment segment)
    {
      totalSegmentSize -= segment.getSize();
      numSegments--;
    }

    public VersionedIntervalTimeline<String, ReferenceCountingSegment> getTimeline()
    {
      return timeline;
    }

    public ConcurrentHashMap<SegmentId, ReferenceCountingIndexedTable> getTablesLookup()
    {
      return tablesLookup;
    }

    public long getTotalSegmentSize()
    {
      return totalSegmentSize;
    }

    public long getNumSegments()
    {
      return numSegments;
    }

    public boolean isEmpty()
    {
      return numSegments == 0;
    }
  }

  @Inject
  public SegmentManager(
      SegmentLoader segmentLoader
  )
  {
    this.segmentLoader = segmentLoader;
  }

  @VisibleForTesting
  Map<String, DataSourceState> getDataSources()
  {
    return dataSources;
  }

  /**
   * Returns a map of dataSource to the total byte size of segments managed by this segmentManager.  This method should
   * be used carefully because the returned map might be different from the actual data source states.
   *
   * @return a map of dataSources and their total byte sizes
   */
  public Map<String, Long> getDataSourceSizes()
  {
    return CollectionUtils.mapValues(dataSources, SegmentManager.DataSourceState::getTotalSegmentSize);
  }

  public Set<String> getDataSourceNames()
  {
    return dataSources.keySet();
  }

  /**
   * Returns a map of dataSource to the number of segments managed by this segmentManager.  This method should be
   * carefully because the returned map might be different from the actual data source states.
   *
   * @return a map of dataSources and number of segments
   */
  public Map<String, Long> getDataSourceCounts()
  {
    return CollectionUtils.mapValues(dataSources, SegmentManager.DataSourceState::getNumSegments);
  }

  public boolean isSegmentCached(final DataSegment segment)
  {
    return segmentLoader.isSegmentLoaded(segment);
  }

  /**
   * Returns the timeline for a datasource, if it exists. The analysis object passed in must represent a scan-based
   * datasource of a single table.
   *
   * @param analysis data source analysis information
   *
   * @return timeline, if it exists
   *
   * @throws IllegalStateException if 'analysis' does not represent a scan-based datasource of a single table
   */
  public Optional<VersionedIntervalTimeline<String, ReferenceCountingSegment>> getTimeline(DataSourceAnalysis analysis)
  {
    final TableDataSource tableDataSource = getTableDataSource(analysis);
    return Optional.ofNullable(dataSources.get(tableDataSource.getName())).map(DataSourceState::getTimeline);
  }

  /**
   * Returns the collection of {@link IndexedTable} for the entire timeline (since join conditions do not currently
   * consider the queries intervals), if the timeline exists for each of its segments that are joinable.
   */
  public Optional<Stream<ReferenceCountingIndexedTable>> getIndexedTables(DataSourceAnalysis analysis)
  {
    return getTimeline(analysis).map(timeline -> {
      // join doesn't currently consider intervals, so just consider all segments
      final Stream<ReferenceCountingSegment> segments =
          timeline.lookup(Intervals.ETERNITY)
                  .stream()
                  .flatMap(x -> StreamSupport.stream(x.getObject().payloads().spliterator(), false));
      final TableDataSource tableDataSource = getTableDataSource(analysis);
      ConcurrentHashMap<SegmentId, ReferenceCountingIndexedTable> tables =
          Optional.ofNullable(dataSources.get(tableDataSource.getName())).map(DataSourceState::getTablesLookup)
                  .orElseThrow(() -> new ISE("Datasource %s does not have IndexedTables", tableDataSource.getName()));
      return segments.map(segment -> tables.get(segment.getId())).filter(Objects::nonNull);
    });
  }

  public boolean hasIndexedTables(String dataSourceName)
  {
    if (dataSources.containsKey(dataSourceName)) {
      return dataSources.get(dataSourceName).tablesLookup.size() > 0;
    }
    return false;
  }

  private TableDataSource getTableDataSource(DataSourceAnalysis analysis)
  {
    return analysis.getBaseTableDataSource()
                   .orElseThrow(() -> new ISE("Cannot handle datasource: %s", analysis.getDataSource()));
  }

  /**
   * Load a single segment.
   *
   * @param segment segment to load
   * @param lazy    whether to lazy load columns metadata
   *
   * @return true if the segment was newly loaded, false if it was already loaded
   *
   * @throws SegmentLoadingException if the segment cannot be loaded
   *
   * 该方法功能就一句话“将DataSegment加载成Segment对象，然后放入dataSources与对应的数据源进行绑定”
   * 对应该功能，整个方法分为两部分
   * 1、调用{@link this#getAdapter(DataSegment, boolean)}方法将DataSegment加载成Segment对象并返回。
   * 2、{@link dataSources}是个map，表示“各数据源已加载的segment信息”。
   * 所以该方法第二部分就是把刚刚加载出的segment与数据源的“总加载信息对象”绑定，具体是和时间轴绑定的。
   * 这样后面查询时，通过某数据源的时间轴信息就直接能从{@link dataSources}中找到其上面的segment对象，用于查询
   */
  public boolean loadSegment(final DataSegment segment, boolean lazy) throws SegmentLoadingException
  {
    // 根据信息文件，找到其真正segment相关smoosh文件所属文件夹，然后将去内容加载成真正segment对象并返回
    /**{@link org.apache.druid.segment.loading.SegmentLoaderLocalCacheManager#getSegment(DataSegment, boolean)}*/
    /**
     * segment是缓存信息对象（不是真正的segment对象），
     * 该segment缓存信息对象原文件所属于/home/operation/software/apache-druid-0.20.1/var/druid/segment-cache/info_dir目录
     * 根据“缓存信息文件对象”，获取该时间区间上的真正segment缓存数据所在的文件夹对象
     *
     * 返回的是一个Segment对象，其中有两部分
     * （1）segmentId：由传参segment（缓存信息对象）得来，根据此id可知返回的Segment对象是哪一个segment
     * （2）segment：根据segment缓存信息对象找到真正segment缓存数据所在的文件夹，
     * 得到该目录下所有segment相关文件的解析结果对象，一个SimpleQueryableIndex：
     * SimpleQueryableIndex{@link SimpleQueryableIndex}本身没有逻辑，只是个包装类，将所有的inDir目录下的segment信息存储其中。
     * 此index对象中较为重要的解析结果为：
     * 1、metadata：xxxx.smoosh文件中的metadata.drd信息所转化
     * 2、columns：map类型 key为所有列名，value为该列的包装类
     * 3、dimensionHandlers：map类型，key为各维度列名，value为该列数据类型对应的handler对象
     * 4、fileMapper：包含了同文件夹下所有smoosh文件的File对象，以及meta.smoosh内的所有数据信
     */
    final Segment adapter = getAdapter(segment, lazy);

    final SettableSupplier<Boolean> resultSupplier = new SettableSupplier<>();

    // compute() is used to ensure that the operation for a data source is executed atomically
    /**
     * dataSources是个map，
     * key：各数据源名称，
     * value：DataSourceState类型
     * 即该数据源上的“总加载信息”，其中包含2个重要属性
     * （1）该数据源已加载的所有segment数量和大小
     * （2）timeline:“用来储存该数据源的各时间轴以及其上的各个ReferenceCountingSegment对象(segement真正的本体)”
     *
     * 此处将当前segment所属数据源作为key，查找是否有对应DataSourceState，没有则新建，
     * 因为此次加载了新的segment，所以数据源的已加载时间区间上对应的segment数量和大小都要新增，
     * 将新增后的value重新放入map中
     */
    dataSources.compute(
        segment.getDataSource(),
        (k, v) -> {
          // 判断该数据源是否被加载过，之前未被加载过的话，此处其对应v就是null
          // DataSourceState指“已加载的某个时间区间”，以及该时间区间上的“segment数量和大小”
          final DataSourceState dataSourceState = v == null ? new DataSourceState() : v;
          //
          /**
           * loadedIntervals是DataSourceState中的Timeline属性，
           * 用来储存数据源的各时间轴以及其上的各个ReferenceCountingSegment对象
           * ReferenceCountingSegment相当于adapter对象（segement真正的本体）
           */
          final VersionedIntervalTimeline<String, ReferenceCountingSegment> loadedIntervals =
              dataSourceState.getTimeline();
          final PartitionHolder<ReferenceCountingSegment> entry = loadedIntervals.findEntry(
              segment.getInterval(),
              segment.getVersion()
          );
          // 所以以上三个final对象，最终的目的就是找到此次加载的segment对应那些ReferenceCountingSegment

          // 启动时新加载segment，不应该已经存在对应ReferenceCountingSegment，所以如果存在则og.warn
          if ((entry != null) && (entry.getChunk(segment.getShardSpec().getPartitionNum()) != null)) {
            log.warn("Told to load an adapter for segment[%s] that already exists", segment.getId());
            resultSupplier.set(false); // 该时间区间上已经存在对应ReferenceCountingSegment对象，则返回false

          // 此次segment在数据源时间轴上不存在对应的ReferenceCountingSegment对象
          } else {
            /** 此处{@link QueryableIndexSegment#as(Class)}返回的是null */
            IndexedTable table = adapter.as(IndexedTable.class);
            if (table != null) {
              if (dataSourceState.isEmpty() || dataSourceState.numSegments == dataSourceState.tablesLookup.size()) {
                dataSourceState.tablesLookup.put(segment.getId(), new ReferenceCountingIndexedTable(table));

              } else { log.error("Cannot load segment[%s] with IndexedTable, no existing segments are joinable", segment.getId()); }
            } else if (dataSourceState.tablesLookup.size() > 0) { log.error("Cannot load segment[%s] without IndexedTable, all existing segments are joinable", segment.getId()); }

            /**
             * loadedIntervals是DataSourceState中的Timeline属性，
             * 用来储存数据源的各时间轴以及其上的各个ReferenceCountingSegment对象
             * ReferenceCountingSegment相当于adapter对象（segement真正的本体）
             *
             * ReferenceCountingSegment：相当于adapter的包装类
             * adapter：是一个Segment对象，其中有两部分很重要
             * （1）segmentId：由传参segment（缓存信息对象）得来，根据此id可知返回的Segment对象是哪一个segment
             * （2）segment：根据segment缓存信息对象找到真正segment缓存数据所在的文件夹，
             * 得到该目录下所有segment相关文件的解析结果对象，一个SimpleQueryableIndex：
             * SimpleQueryableIndex{@link SimpleQueryableIndex}本身没有逻辑，只是个包装类，将所有的inDir目录下的segment信息存储其中。
             * 此index对象中较为重要的解析结果为：
             * 1、metadata：xxxx.smoosh文件中的metadata.drd信息所转化
             * 2、columns：map类型 key为所有列名，value为该列的包装类
             * 3、dimensionHandlers：map类型，key为各维度列名，value为该列数据类型对应的handler对象
             * 4、fileMapper：包含了同文件夹下所有smoosh文件的File对象，以及meta.smoosh内的所有数据信
             *
             * 此处相当于把adapter对象与时间轴进行绑定，并存入loadedIntervals，
             * 也相当于对应的数据源的时间轴上加载了该segment
             */
            loadedIntervals.add(
                segment.getInterval(),
                segment.getVersion(),
                segment.getShardSpec().createChunk(
                    ReferenceCountingSegment.wrapSegment(adapter, segment.getShardSpec())
                )
            );
            //加载了新的segment，所以dataSourceState内的segment数量和大小都要增加
            dataSourceState.addSegment(segment);
            resultSupplier.set(true);// 加载成功
          }
          // 返回新的dataSourceState与作为数据源的value
          return dataSourceState;
        }
    );

    return resultSupplier.get();// segment加载成功或失败
  }

  private Segment getAdapter(final DataSegment segment, boolean lazy) throws SegmentLoadingException
  {
    final Segment adapter;
    try {
      // 根据信息文件，找到其真正segment相关smoosh文件所属文件夹，然后将去内容加载成真正segment对象并返回
      /**{@link org.apache.druid.segment.loading.SegmentLoaderLocalCacheManager#getSegment(DataSegment, boolean)}*/
      /**
       * segment是缓存信息对象（不是真正的segment对象），
       * 该segment缓存信息对象原文件所属于/home/operation/software/apache-druid-0.20.1/var/druid/segment-cache/info_dir目录
       * 根据“缓存信息文件对象”，获取该时间区间上的真正segment缓存数据所在的文件夹对象
       *
       * 返回的是一个Segment对象，其中有两部分
       * （1）segmentId：由传参segment（缓存信息对象）得来，根据此id可知返回的Segment对象是哪一个segment
       * （2）segment：根据segment缓存信息对象找到真正segment缓存数据所在的文件夹，
       * 得到该目录下所有segment相关文件的解析结果对象，一个SimpleQueryableIndex：
       * SimpleQueryableIndex{@link SimpleQueryableIndex}本身没有逻辑，只是个包装类，将所有的inDir目录下的segment信息存储其中。
       * 此index对象中较为重要的解析结果为：
       * 1、metadata：xxxx.smoosh文件中的metadata.drd信息所转化
       * 2、columns：map类型 key为所有列名，value为该列的包装类
       * 3、dimensionHandlers：map类型，key为各维度列名，value为该列数据类型对应的handler对象
       * 4、fileMapper：包含了同文件夹下所有smoosh文件的File对象，以及meta.smoosh内的所有数据信
       */
      adapter = segmentLoader.getSegment(segment, lazy);
    }
    catch (SegmentLoadingException e) {
      segmentLoader.cleanup(segment);
      throw e;
    }

    if (adapter == null) {
      throw new SegmentLoadingException("Null adapter from loadSpec[%s]", segment.getLoadSpec());
    }
    return adapter;
  }

  public void dropSegment(final DataSegment segment)
  {
    final String dataSource = segment.getDataSource();

    // compute() is used to ensure that the operation for a data source is executed atomically
    dataSources.compute(
        dataSource,
        (dataSourceName, dataSourceState) -> {
          if (dataSourceState == null) {
            log.info("Told to delete a queryable for a dataSource[%s] that doesn't exist.", dataSourceName);
            return null;
          } else {
            final VersionedIntervalTimeline<String, ReferenceCountingSegment> loadedIntervals =
                dataSourceState.getTimeline();

            final ShardSpec shardSpec = segment.getShardSpec();
            final PartitionChunk<ReferenceCountingSegment> removed = loadedIntervals.remove(
                segment.getInterval(),
                segment.getVersion(),
                // remove() internally searches for a partitionChunk to remove which is *equal* to the given
                // partitionChunk. Note that partitionChunk.equals() checks only the partitionNum, but not the object.
                segment.getShardSpec().createChunk(ReferenceCountingSegment.wrapSegment(null, shardSpec))
            );
            final ReferenceCountingSegment oldQueryable = (removed == null) ? null : removed.getObject();

            if (oldQueryable != null) {
              try (final Closer closer = Closer.create()) {
                dataSourceState.removeSegment(segment);
                closer.register(oldQueryable);
                log.info("Attempting to close segment %s", segment.getId());
                final ReferenceCountingIndexedTable oldTable = dataSourceState.tablesLookup.remove(segment.getId());
                if (oldTable != null) {
                  closer.register(oldTable);
                }
              }
              catch (IOException e) {
                throw new RuntimeException(e);
              }
            } else {
              log.info(
                  "Told to delete a queryable on dataSource[%s] for interval[%s] and version[%s] that I don't have.",
                  dataSourceName,
                  segment.getInterval(),
                  segment.getVersion()
              );
            }

            // Returning null removes the entry of dataSource from the map
            return dataSourceState.isEmpty() ? null : dataSourceState;
          }
        }
    );

    segmentLoader.cleanup(segment);
  }
}
