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

package org.apache.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import org.apache.druid.client.selector.QueryableDruidServer;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.client.selector.TierSelectorStrategy;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 管理所有节点（历史节点+实时节点）的queryRunner
 * 通过此对象，可以根据主机描述，如主机名，找到该主机对应的queryRunner对象，以便用于查询
 */
@ManageLifecycle
public class BrokerServerView implements TimelineServerView
{
  private static final Logger log = new Logger(BrokerServerView.class);

  private final Object lock = new Object();

  /**
   * key：主机名
   * value：主机对应的queryRunner对象
   */
  private final ConcurrentMap<String, QueryableDruidServer> clients;
  private final Map<SegmentId, ServerSelector> selectors;
  private final Map<String, VersionedIntervalTimeline<String, ServerSelector>> timelines;
  private final ConcurrentMap<TimelineCallback, Executor> timelineCallbacks = new ConcurrentHashMap<>();

  private final QueryToolChestWarehouse warehouse;
  private final QueryWatcher queryWatcher;
  private final ObjectMapper smileMapper;
  private final HttpClient httpClient;
  private final FilteredServerInventoryView baseView;
  private final TierSelectorStrategy tierSelectorStrategy;
  private final ServiceEmitter emitter;
  private final BrokerSegmentWatcherConfig segmentWatcherConfig;
  private final Predicate<Pair<DruidServerMetadata, DataSegment>> segmentFilter;

  private final CountDownLatch initialized = new CountDownLatch(1);

  @Inject
  public BrokerServerView(
      final QueryToolChestWarehouse warehouse,
      final QueryWatcher queryWatcher,
      final @Smile ObjectMapper smileMapper,
      final @EscalatedClient HttpClient httpClient,
      final FilteredServerInventoryView baseView,
      final TierSelectorStrategy tierSelectorStrategy,
      final ServiceEmitter emitter,
      final BrokerSegmentWatcherConfig segmentWatcherConfig
  )
  {
    this.warehouse = warehouse;
    this.queryWatcher = queryWatcher;
    this.smileMapper = smileMapper;
    this.httpClient = httpClient;
    this.baseView = baseView;
    this.tierSelectorStrategy = tierSelectorStrategy;
    this.emitter = emitter;
    this.segmentWatcherConfig = segmentWatcherConfig;
    this.clients = new ConcurrentHashMap<>();
    this.selectors = new HashMap<>();
    this.timelines = new HashMap<>();

    this.segmentFilter = (Pair<DruidServerMetadata, DataSegment> metadataAndSegment) -> {
      if (segmentWatcherConfig.getWatchedTiers() != null
          && !segmentWatcherConfig.getWatchedTiers().contains(metadataAndSegment.lhs.getTier())) {
        return false;
      }

      if (segmentWatcherConfig.getWatchedDataSources() != null
          && !segmentWatcherConfig.getWatchedDataSources().contains(metadataAndSegment.rhs.getDataSource())) {
        return false;
      }

      return true;
    };
    ExecutorService exec = Execs.singleThreaded("BrokerServerView-%s");
    baseView.registerSegmentCallback(
        exec,
        new ServerView.SegmentCallback()
        {
          @Override
          public ServerView.CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
          {
            serverAddedSegment(server, segment);
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentRemoved(final DruidServerMetadata server, DataSegment segment)
          {
            serverRemovedSegment(server, segment);
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public CallbackAction segmentViewInitialized()
          {
            initialized.countDown();
            runTimelineCallbacks(TimelineCallback::timelineInitialized);
            return ServerView.CallbackAction.CONTINUE;
          }
        },
        segmentFilter
    );

    baseView.registerServerRemovedCallback(
        exec,
        server -> {
          removeServer(server);
          return CallbackAction.CONTINUE;
        }
    );
  }

  @LifecycleStart
  public void start() throws InterruptedException
  {
    if (segmentWatcherConfig.isAwaitInitializationOnStart()) {
      final long startNanos = System.nanoTime();
      log.debug("%s waiting for initialization.", getClass().getSimpleName());
      awaitInitialization();
      log.info("%s initialized in [%,d] ms.", getClass().getSimpleName(), (System.nanoTime() - startNanos) / 1000000);
    }
  }

  public boolean isInitialized()
  {
    return initialized.getCount() == 0;
  }

  public void awaitInitialization() throws InterruptedException
  {
    initialized.await();
  }

  /**
   * 将server主机描述信息加载成queryRunner对象，并存入当前对象clients map属性中
   * @param server 主机描述信息
   */
  private QueryableDruidServer addServer(DruidServer server)
  {
    // 创建该主机queryRunner（DirectDruidClient类型）
    QueryableDruidServer retVal = new QueryableDruidServer<>(server, makeDirectClient(server));
    QueryableDruidServer exists = clients.put(server.getName(), retVal);
    if (exists != null) {
      log.warn("QueryRunner for server[%s] already exists!? Well it's getting replaced", server);
    }

    return retVal;
  }

  private DirectDruidClient makeDirectClient(DruidServer server)
  {
    return new DirectDruidClient(
        warehouse,
        queryWatcher,
        smileMapper,
        httpClient,
        server.getScheme(),
        server.getHost(),
        emitter
    );
  }

  private QueryableDruidServer removeServer(DruidServer server)
  {
    for (DataSegment segment : server.iterateAllSegments()) {
      serverRemovedSegment(server.getMetadata(), segment);
    }
    return clients.remove(server.getName());
  }

  /**
   * broker启动时会调用，在his节点发生变动（segment发生变动、his集群发生变动）都可能重新调用
   *
   * 1、填充timelines
   * 该方法一次只加载一个segment的信息
   * （在broker启动时，会循环调用，加载所有segment的信息）
   * （不是加载内容）
   * 只是加载segment版本号、时间区间、数据源等信息，以及这些segment所在historic节点的ip+port
   *
   * 2、填充clients
   * 该方法每次还会加载该segment的所属主机，
   * 将主机信息加载成水印queryRunner，以用于以后的查询
   *
   * @param server segment所在historic节点的ip+port
   * @param segment segment信息
   */
  private void serverAddedSegment(final DruidServerMetadata server, final DataSegment segment)
  {
//    log.info("!!!：加载segment，serverName："+server.getName()+"...segmentDataSource："+segment.getDataSource()+"...interval："+segment.getInterval());
    SegmentId segmentId = segment.getId();
    synchronized (lock) {
      // in theory we could probably just filter this to ensure we don't put ourselves in here, to make broker tree
      // query topologies, but for now just skip all brokers, so we don't create some sort of wild infinite query
      // loop...
      if (!server.getType().equals(ServerType.BROKER)) {
        log.debug("Adding segment[%s] for server[%s]", segment, server);
        ServerSelector selector = selectors.get(segmentId);
        if (selector == null) {
          selector = new ServerSelector(segment, tierSelectorStrategy);

          VersionedIntervalTimeline<String, ServerSelector> timeline = timelines.get(segment.getDataSource());
          if (timeline == null) {
            timeline = new VersionedIntervalTimeline<>(Ordering.natural());
            timelines.put(segment.getDataSource(), timeline);
          }

          timeline.add(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(selector));
          selectors.put(segmentId, selector);
        }

        /**
         * 判断当前加载的segment所属主机，有没有被加载成queryRunner过。
         * 如果没有被加载过，则为server创建queryRunner，并装入当前对象clients map中
         */
        QueryableDruidServer queryableDruidServer = clients.get(server.getName());
        if (queryableDruidServer == null) {
          // 为server创建queryRunner，并装入当前对象clients map中
          queryableDruidServer = addServer(baseView.getInventoryValue(server.getName()));
        }
        selector.addServerAndUpdateSegment(queryableDruidServer, segment);
      }
      // run the callbacks, even if the segment came from a broker, lets downstream watchers decide what to do with it
      runTimelineCallbacks(callback -> callback.segmentAdded(server, segment));
    }
  }

  private void serverRemovedSegment(DruidServerMetadata server, DataSegment segment)
  {

    SegmentId segmentId = segment.getId();
    final ServerSelector selector;

    synchronized (lock) {
      log.debug("Removing segment[%s] from server[%s].", segmentId, server);

      // we don't store broker segments here, but still run the callbacks for the segment being removed from the server
      // since the broker segments are not stored on the timeline, do not fire segmentRemoved event
      if (server.getType().equals(ServerType.BROKER)) {
        runTimelineCallbacks(callback -> callback.serverSegmentRemoved(server, segment));
        return;
      }

      selector = selectors.get(segmentId);
      if (selector == null) {
        log.warn("Told to remove non-existant segment[%s]", segmentId);
        return;
      }

      QueryableDruidServer queryableDruidServer = clients.get(server.getName());
      if (!selector.removeServer(queryableDruidServer)) {
        log.warn(
            "Asked to disassociate non-existant association between server[%s] and segment[%s]",
            server,
            segmentId
        );
      } else {
        runTimelineCallbacks(callback -> callback.serverSegmentRemoved(server, segment));
      }

      if (selector.isEmpty()) {
        VersionedIntervalTimeline<String, ServerSelector> timeline = timelines.get(segment.getDataSource());
        selectors.remove(segmentId);

        final PartitionChunk<ServerSelector> removedPartition = timeline.remove(
            segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(selector)
        );

        if (removedPartition == null) {
          log.warn(
              "Asked to remove timeline entry[interval: %s, version: %s] that doesn't exist",
              segment.getInterval(),
              segment.getVersion()
          );
        } else {
          runTimelineCallbacks(callback -> callback.segmentRemoved(segment));
        }
      }
    }
  }

  @Override
  public Optional<VersionedIntervalTimeline<String, ServerSelector>> getTimeline(final DataSourceAnalysis analysis)
  {
    /**
     * analysis.getBaseTableDataSource()判断analysis对象中的数据源是不是TableDataSource类型，如果不是则抛出异常。
     *
     * 可以看出此处只支持TableDataSource类型的数据源
     */
    final TableDataSource table =
        analysis.getBaseTableDataSource()
                .orElseThrow(() -> new ISE("Cannot handle datasource: %s", analysis.getDataSource()));

    synchronized (lock) {
      //
      return Optional.ofNullable(timelines.get(table.getName()));
    }
  }

  @Override
  public void registerTimelineCallback(final Executor exec, final TimelineCallback callback)
  {
    timelineCallbacks.put(callback, exec);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(DruidServer server)
  {
    synchronized (lock) {
      /**
       * clients是一个，map，在broker节点启动时，就将所有segment所在主机全部u加载成了queryRunner，并存入了clients map中，
       * 主机queryRunner生成逻辑：{@link this#makeDirectClient(DruidServer)}
       *
       * 其实就是把主机信息作为入参，new了一个{@link DirectDruidClient}类型的queryRunner对象
       */
      QueryableDruidServer queryableDruidServer = clients.get(server.getName());
      if (queryableDruidServer == null) {
        log.error("No QueryableDruidServer found for %s", server.getName());
        return null;
      }
      return queryableDruidServer.getQueryRunner();
    }
  }

  @Override
  public void registerServerRemovedCallback(Executor exec, ServerRemovedCallback callback)
  {
    baseView.registerServerRemovedCallback(exec, callback);
  }

  @Override
  public void registerSegmentCallback(Executor exec, SegmentCallback callback)
  {
    baseView.registerSegmentCallback(exec, callback, segmentFilter);
  }

  private void runTimelineCallbacks(final Function<TimelineCallback, CallbackAction> function)
  {
    for (Map.Entry<TimelineCallback, Executor> entry : timelineCallbacks.entrySet()) {
      entry.getValue().execute(
          () -> {
            if (CallbackAction.UNREGISTER == function.apply(entry.getKey())) {
              timelineCallbacks.remove(entry.getKey());
            }
          }
      );
    }
  }

  @Override
  public List<ImmutableDruidServer> getDruidServers()
  {
    return clients.values().stream()
                  .map(queryableDruidServer -> queryableDruidServer.getServer().toImmutableDruidServer())
                  .collect(Collectors.toList());
  }
}
