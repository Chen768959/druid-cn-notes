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

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.aggregation.MetricManipulatorFns;
import org.apache.druid.query.context.ConcurrentResponseContext;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.context.ResponseContext.Key;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.server.QueryResource;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.timeline.DataSegment;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.joda.time.Duration;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class DirectDruidClient<T> implements QueryRunner<T>
{
  public static final String QUERY_FAIL_TIME = "queryFailTime";

  private static final Logger log = new Logger(DirectDruidClient.class);
  private static final int VAL_TO_REDUCE_REMAINING_RESPONSES = -1;

  private final QueryToolChestWarehouse warehouse;
  private final QueryWatcher queryWatcher;
  private final ObjectMapper objectMapper;
  private final HttpClient httpClient;
  // 待查询节点每次http请求使用的“http”还是“https”
  private final String scheme;
  // 待查询节点 ip + 端口
  private final String host;
  private final ServiceEmitter emitter;

  private final AtomicInteger openConnections;
  private final boolean isSmile;
  private final ScheduledExecutorService queryCancellationExecutor;

  /**
   * Removes the magical fields added by {@link #makeResponseContextForQuery()}.
   */
  public static void removeMagicResponseContextFields(ResponseContext responseContext)
  {
    responseContext.remove(ResponseContext.Key.QUERY_TOTAL_BYTES_GATHERED);
    responseContext.remove(ResponseContext.Key.REMAINING_RESPONSES_FROM_QUERY_SERVERS);
  }

  public static ResponseContext makeResponseContextForQuery()
  {
    final ResponseContext responseContext = ConcurrentResponseContext.createEmpty();
    responseContext.put(ResponseContext.Key.QUERY_TOTAL_BYTES_GATHERED, new AtomicLong());
    responseContext.put(Key.REMAINING_RESPONSES_FROM_QUERY_SERVERS, new ConcurrentHashMap<>());
    return responseContext;
  }

  /**
   * 由{@link BrokerServerView#serverAddedSegment(DruidServerMetadata, DataSegment)}调用
   *
   * 在broker节点启动时，加载所有segment信息时，就也会创建所有DirectDruidClient对象，
   * 每一台主机的各种信息，就是在创建这个DirectDruidClient对象时传入的
   */
  public DirectDruidClient(
      QueryToolChestWarehouse warehouse,
      QueryWatcher queryWatcher,
      ObjectMapper objectMapper,
      HttpClient httpClient,
      String scheme,
      String host,
      ServiceEmitter emitter
  )
  {
    this.warehouse = warehouse;
    this.queryWatcher = queryWatcher;
    this.objectMapper = objectMapper;
    this.httpClient = httpClient;
    this.scheme = scheme;
    this.host = host;
    this.emitter = emitter;

    this.isSmile = this.objectMapper.getFactory() instanceof SmileFactory;
    this.openConnections = new AtomicInteger();
    this.queryCancellationExecutor = Execs.scheduledSingleThreaded("query-cancellation-executor");
  }

  public int getNumOpenConnections()
  {
    return openConnections.get();
  }

  /**
   * （该方法调用时机：broker节点的最终懒加载Sequence结果，调用toYielder时，需要真正进行查询时的逻辑
   * 由{@link org.apache.druid.client.CachingClusteredClient.SpecificQueryRunnable#getSimpleServerResults(QueryRunner, List, long)}调用）
   *
   * 当前DirectDruidClient对象是一个his节点主机的queryRunner，broker可调用该run方法，从指定主机上查询数据。
   * 当前Runner对象会对应一个实际的his（或实时）节点主机，
   * 在broker节点启动时，加载所有segment信息时，就也会创建所有DirectDruidClient对象，
   * 每一台主机的各种信息，就是在创建这个DirectDruidClient对象时传入的
   *
   * 该方法中使用netty，异步发送查询此主机所有分片信息的请求：
   * 然后异步线程每收到一个数据包，就交由一个responseHandler对象处理，
   * handler会将“请求头+请求行+后续所有chunk包”，全部依次handle的内部queue队列中
   * 当异步线程接收并处理完所有chunk时，就将handler的处理结果“ClientResponse”对象装入一个future中。
   * （ClientResponse对象可以操作遍历handler内部的queue队列）
   * 也就是说通过future可以获取到ClientResponse是否准备好，如果准备完毕就通过其遍历结果数据集queue队列。
   * 然后这个future又会被封装进Sequence，等着Sequence被迭代时，来迭代异步的响应查询结果。
   *
   * 总结：
   * 异步请求后，异步线程收到的结果集会封装进future对象中，future可判断结果集是否准备好，
   * 如果所有结果都获取到了，则future中可查询出此次响应结果，
   * 而future存在于此次Sequence响应中
   *
   * @param queryPlus 其中包含了一个{@link MultipleSpecificSegmentSpec}对象，
   *                   该对象中封装了此次所有待查询的分片信息list，
   *                   后续在his节点收到来自broker的查询请求后，
   *                   还会把这个{@link MultipleSpecificSegmentSpec}对象反序列化出来，用来接收要查询的分片信息
   * @param context
   */
  @Override
  public Sequence<T> run(final QueryPlus<T> queryPlus, final ResponseContext context)
  {
    final Query<T> query = queryPlus.getQuery();
    QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);
    boolean isBySegment = QueryContexts.isBySegment(query);
    final JavaType queryResultType = isBySegment ? toolChest.getBySegmentResultType() : toolChest.getBaseResultType();

    /**
     * 异步发送查询请求，然后异步线程每收到一个数据包，就交由responseHandler处理，
     * handler会将“请求头+请求行+后续所有chunk包”，全部依次handle的内部queue队列中
     * 当异步线程接收并处理完所有chunk时，就将handler的处理结果“ClientResponse”对象装入以下future中。
     * （ClientResponse对象可以操作遍历handler内部的queue队列）
     * 也就是说通过future可以获取到ClientResponse是否准备好，如果准备完毕就通过其遍历结果数据集queue队列。
     */
    final ListenableFuture<InputStream> future;
    // url如：http://localhost:8083/druid/v2/
    final String url = StringUtils.format("%s://%s/druid/v2/", scheme, host);
    // cancelUrl如：http://localhost:8083/druid/v2/12f4f780-18f8-4e6e-9f92-de0868c62ce2
    final String cancelUrl = StringUtils.format("%s://%s/druid/v2/%s", scheme, host, query.getId());

    /**
     * 开始异步执行查询
     */
    try {
      log.debug("Querying queryId[%s] url[%s]", query.getId(), url);

      /**
       * 1
       */
      final long requestStartTimeNs = System.nanoTime();// 当前纳秒时间戳
      final long timeoutAt = query.getContextValue(QUERY_FAIL_TIME);
      // 从单个his(realTime)节点查询出的最大字节数
      final long maxScatterGatherBytes = QueryContexts.getMaxScatterGatherBytes(query);
      final AtomicLong totalBytesGathered = (AtomicLong) context.get(ResponseContext.Key.QUERY_TOTAL_BYTES_GATHERED);
      // 在对数据服务器的通道施加"backpressure"之前，每个查询排队的最大字节数。
      // 与maxScatterGatherBytes类似，但与该配置不同，此配置将触发"backpressure"而不是查询失败。0表示禁用
      final long maxQueuedBytes = QueryContexts.getMaxQueuedBytes(query, 0);
      // 使用"backpressure"
      final boolean usingBackpressure = maxQueuedBytes > 0;

      /**
       * 2、准备responseHandler对象，
       *    该对象提供了方法，用于将历史节点（实时节点）的响应内容装入内部queue队列中。
       *
       *    druid的http查询是用来chunk模式，会存在响应行+响应头，而所有的数据内容都被划分为多个连续chunk包形式，
       *    而这些响应内容都会被装在handler对象的内部queue队列中
       *
       *    该handler在收到响应行和响应头后，就会产生一个{@link ClientResponse<InputStream>}对象，
       *    该对象可以操作遍历queue队列的每一项内容。
       */
      final HttpResponseHandler<InputStream, InputStream> responseHandler = new HttpResponseHandler<InputStream, InputStream>()
      {
        private final AtomicLong totalByteCount = new AtomicLong(0);
        private final AtomicLong queuedByteCount = new AtomicLong(0);
        private final AtomicLong channelSuspendedTime = new AtomicLong(0);
        /**
         * 存储所有响应内容，
         * 其第0项为此次响应的http“响应行+响应头”
         * 后面的项均为递增的各个“chunk包的内容”
         */
        private final BlockingQueue<InputStreamHolder> queue = new LinkedBlockingQueue<>();
        private final AtomicBoolean done = new AtomicBoolean(false);
        private final AtomicReference<String> fail = new AtomicReference<>();
        private final AtomicReference<TrafficCop> trafficCopRef = new AtomicReference<>();

        private QueryMetrics<? super Query<T>> queryMetrics;
        private long responseStartTimeNs;

        private QueryMetrics<? super Query<T>> acquireResponseMetrics()
        {
          if (queryMetrics == null) {
            queryMetrics = toolChest.makeMetrics(query);
            queryMetrics.server(host);
          }
          return queryMetrics;
        }

        /**
         * 往queue队列中加入一个数据包，
         * queue队列第0项为此次响应的http“响应行+响应头”
         * 后面的项均为递增的各个“chunk包的内容”。
         *
         * @param buffer 响应之初为“响应行+响应头”内容，后续为每一个chunk包的buffer内容
         * @param chunkNum 响应之初会传入响应行+响应头，此时对应的chunkNum是“0”，后续chunkNum为每一个chunk包的序号
         *
         * 如果当前queue中的字节数量，没有达到请求参数中设置的单节点查询的最大量，那么此方法都会返回true
         */
        private boolean enqueue(ChannelBuffer buffer, long chunkNum) throws InterruptedException
        {
          // Increment queuedByteCount before queueing the object, so queuedByteCount is at least as high as
          // the actual number of queued bytes at any particular time.
          final InputStreamHolder holder = InputStreamHolder.fromChannelBuffer(buffer, chunkNum);
          final long currentQueuedByteCount = queuedByteCount.addAndGet(holder.getLength());
          queue.put(holder);

          // True if we should keep reading.
          return !usingBackpressure || currentQueuedByteCount < maxQueuedBytes;
        }

        /**
         * 从queue取出一个buffer，
         * 同时进行一些总量计数
         */
        private InputStream dequeue() throws InterruptedException
        {
          final InputStreamHolder holder = queue.poll(checkQueryTimeout(), TimeUnit.MILLISECONDS);
          if (holder == null) {
            throw new RE("Query[%s] url[%s] timed out.", query.getId(), url);
          }

          final long currentQueuedByteCount = queuedByteCount.addAndGet(-holder.getLength());
          if (usingBackpressure && currentQueuedByteCount < maxQueuedBytes) {
            long backPressureTime = Preconditions.checkNotNull(trafficCopRef.get(), "No TrafficCop, how can this be?")
                                                 .resume(holder.getChunkNum());
            channelSuspendedTime.addAndGet(backPressureTime);
          }

          return holder.getStream();
        }

        /**
         * 处理来自his节点的“响应行+响应头”响应结果，并装入queue队列中的第0项
         * @param response
         * @param trafficCop
         */
        @Override
        public ClientResponse<InputStream> handleResponse(HttpResponse response, TrafficCop trafficCop)
        {
          trafficCopRef.set(trafficCop);
          checkQueryTimeout();
          checkTotalBytesLimit(response.getContent().readableBytes());

          log.debug("Initial response from url[%s] for queryId[%s]", url, query.getId());
          responseStartTimeNs = System.nanoTime();
          acquireResponseMetrics().reportNodeTimeToFirstByte(responseStartTimeNs - requestStartTimeNs).emit(emitter);

          final boolean continueReading;
          try {
            log.trace("Got a response from [%s] for query ID[%s], subquery ID[%s]", url, query.getId(), query.getSubQueryId());

            // 获取响应的上下文
            final String responseContext = response.headers().get(QueryResource.HEADER_RESPONSE_CONTEXT);
            context.add(
                ResponseContext.Key.REMAINING_RESPONSES_FROM_QUERY_SERVERS,
                new NonnullPair<>(query.getMostSpecificId(), VAL_TO_REDUCE_REMAINING_RESPONSES)
            );
            // context may be null in case of error or query timeout
            if (responseContext != null) {
              context.merge(ResponseContext.deserialize(responseContext, objectMapper));
            }

            /**
             * 将查询内容装入queue队列中
             *
             * 如果当前queue中的字节数量，没有达到请求参数中设置的单节点查询的最大量，
             * 那么此enqueue()方法都会返回true
             */
            continueReading = enqueue(response.getContent(), 0L);
          }
          catch (final IOException e) {
            log.error(e, "Error parsing response context from url [%s]", url);
            return ClientResponse.finished(
                new InputStream()
                {
                  @Override
                  public int read() throws IOException
                  {
                    throw e;
                  }
                }
            );
          }
          catch (InterruptedException e) {
            log.error(e, "Queue appending interrupted");
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }

          totalByteCount.addAndGet(response.getContent().readableBytes());
          return ClientResponse.finished(
              new SequenceInputStream(
                  new Enumeration<InputStream>()
                  {
                    @Override
                    public boolean hasMoreElements()
                    {
                      if (fail.get() != null) {
                        throw new RE(fail.get());
                      }
                      checkQueryTimeout();

                      // Done is always true until the last stream has be put in the queue.
                      // Then the stream should be spouting good InputStreams.
                      synchronized (done) {
                        return !done.get() || !queue.isEmpty();
                      }
                    }

                    /**
                     * 每次从queue中取出一个buffer
                     */
                    @Override
                    public InputStream nextElement()
                    {
                      if (fail.get() != null) {
                        throw new RE(fail.get());
                      }

                      try {
                        return dequeue();
                      }
                      catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                      }
                    }
                  }
              ),
              continueReading
          );
        }

        /**
         * 处理来自his节点的“一个”chunk包，并装入queue队列
         *
         * @param clientResponse 由handleResponse()方法创建，里面包含了遍历queue的方法，可通过该对象直接获取queue的内容。
         *
         * @param chunk 新收到的chunk响应数据包。
         *              chunk模式属于http协议的一种传输规则，其响应行和响应头都没有变，
         *              但是响应体是连续发送回来多个数据包，直到发送到一个“空”包为止，意味着数据传输完毕。
         *              与普通http响应不同，普通http响应是服务端准备好所有数据后，再作为响应体发送给客户端，
         *              普通http响应体关键是存在一个“响应体字节长度”字段，放在响应体之初，客户端会根据该字段决定读取多少字节，
         *              但这个字段的前提是需要准备完毕所有响应体后才能计算出来，这样就无法做到“chunk”模式那样，有多少数据就先传输多少数据。
         *
         * @param chunkNum 此处收到的chunk包的序列号（单调递增的）
         */
        @Override
        public ClientResponse<InputStream> handleChunk(
            ClientResponse<InputStream> clientResponse,
            HttpChunk chunk,
            long chunkNum
        )
        {
          checkQueryTimeout();

          final ChannelBuffer channelBuffer = chunk.getContent();
          // 当前channel中可读取的字节数目
          final int bytes = channelBuffer.readableBytes();

          // 检查是否超过上下文配置中的“单节点合计最大可读取byte长度”
          checkTotalBytesLimit(bytes);

          // 判断内部queue还能否装入数据，只要没装满，此处就是true
          boolean continueReading = true;
          if (bytes > 0) {
            try {
              // 将此chunk内容，加入内部queue队列，此时队列若未到最大限制，则此处返回true，表示还能接着容纳数据
              continueReading = enqueue(channelBuffer, chunkNum);
            }
            catch (InterruptedException e) {
              log.error(e, "Unable to put finalizing input stream into Sequence queue for url [%s]", url);
              Thread.currentThread().interrupt();
              throw new RuntimeException(e);
            }
            totalByteCount.addAndGet(bytes);
          }

          return ClientResponse.finished(clientResponse.getObj(), continueReading);
        }

        @Override
        public ClientResponse<InputStream> done(ClientResponse<InputStream> clientResponse)
        {
          long stopTimeNs = System.nanoTime();
          long nodeTimeNs = stopTimeNs - requestStartTimeNs;
          final long nodeTimeMs = TimeUnit.NANOSECONDS.toMillis(nodeTimeNs);
          log.debug(
              "Completed queryId[%s] request to url[%s] with %,d bytes returned in %,d millis [%,f b/s].",
              query.getId(),
              url,
              totalByteCount.get(),
              nodeTimeMs,
              // Floating math; division by zero will yield Inf, not exception
              totalByteCount.get() / (0.001 * nodeTimeMs)
          );
          QueryMetrics<? super Query<T>> responseMetrics = acquireResponseMetrics();
          responseMetrics.reportNodeTime(nodeTimeNs);
          responseMetrics.reportNodeBytes(totalByteCount.get());

          if (usingBackpressure) {
            responseMetrics.reportBackPressureTime(channelSuspendedTime.get());
          }

          responseMetrics.emit(emitter);
          synchronized (done) {
            try {
              // An empty byte array is put at the end to give the SequenceInputStream.close() as something to close out
              // after done is set to true, regardless of the rest of the stream's state.
              queue.put(InputStreamHolder.fromChannelBuffer(ChannelBuffers.EMPTY_BUFFER, Long.MAX_VALUE));
            }
            catch (InterruptedException e) {
              log.error(e, "Unable to put finalizing input stream into Sequence queue for url [%s]", url);
              Thread.currentThread().interrupt();
              throw new RuntimeException(e);
            }
            finally {
              done.set(true);
            }
          }
          return ClientResponse.finished(clientResponse.getObj());
        }

        @Override
        public void exceptionCaught(final ClientResponse<InputStream> clientResponse, final Throwable e)
        {
          String msg = StringUtils.format(
              "Query[%s] url[%s] failed with exception msg [%s]",
              query.getId(),
              url,
              e.getMessage()
          );
          setupResponseReadFailure(msg, e);
        }

        private void setupResponseReadFailure(String msg, Throwable th)
        {
          fail.set(msg);
          queue.clear();
          queue.offer(
              InputStreamHolder.fromStream(
                  new InputStream()
                  {
                    @Override
                    public int read() throws IOException
                    {
                      if (th != null) {
                        throw new IOException(msg, th);
                      } else {
                        throw new IOException(msg);
                      }
                    }
                  },
                  -1,
                  0
              )
          );
        }

        // Returns remaining timeout or throws exception if timeout already elapsed.
        private long checkQueryTimeout()
        {
          long timeLeft = timeoutAt - System.currentTimeMillis();
          if (timeLeft <= 0) {
            String msg = StringUtils.format("Query[%s] url[%s] timed out.", query.getId(), url);
            setupResponseReadFailure(msg, null);
            throw new RE(msg);
          } else {
            return timeLeft;
          }
        }

        private void checkTotalBytesLimit(long bytes)
        {
          if (maxScatterGatherBytes < Long.MAX_VALUE && totalBytesGathered.addAndGet(bytes) > maxScatterGatherBytes) {
            String msg = StringUtils.format(
                "Query[%s] url[%s] max scatter-gather bytes limit reached.",
                query.getId(),
                url
            );
            setupResponseReadFailure(msg, null);
            throw new RE(msg);
          }
        }
      };

      long timeLeft = timeoutAt - System.currentTimeMillis();
      if (timeLeft <= 0) {
        throw new RE("Query[%s] url[%s] timed out.", query.getId(), url);
      }

      /**
       * 3、异步向历史节点发送http查询请求
       * {@link org.apache.druid.java.util.http.client.NettyHttpClient#go(Request, HttpResponseHandler, Duration)}
       *
       * 异步发送查询请求，然后异步线程每收到一个数据包，就交由responseHandler处理，
       * 当接收完所有chunk时，就将{@link ClientResponse<InputStream>}对象装入以下的future中。
       *
       * 也就是说通过future可以获取到ClientResponse，也就可以遍历结果数据的queue队列。
       */
      future = httpClient.go(
          //创建请求对象
          new Request(
              HttpMethod.POST,
              new URL(url)
          ).setContent(objectMapper.writeValueAsBytes(QueryContexts.withTimeout(query, timeLeft)))
           .setHeader(
               HttpHeaders.Names.CONTENT_TYPE,
               isSmile ? SmileMediaTypes.APPLICATION_JACKSON_SMILE : MediaType.APPLICATION_JSON
           ),

          //用于异步的处理此次http调用的结果
          responseHandler,
          Duration.millis(timeLeft)
      );

      /**
       * 4、将此次查询注册进queryWatcher，
       * queryWatcher目的是保证所有正在进行的、或待进行的查询的可见性
       * 即根据queryWatcher可以找到所有现有查询
       */
      queryWatcher.registerQueryFuture(query, future);

      openConnections.getAndIncrement();
      // 绑定异步回调，future执行完毕后，执行FutureCallback回调方法（只是计数+1）
      Futures.addCallback(
          future,
          new FutureCallback<InputStream>()
          {
            @Override
            public void onSuccess(InputStream result)
            {
              openConnections.getAndDecrement();
            }

            @Override
            public void onFailure(Throwable t)
            {
              openConnections.getAndDecrement();
              if (future.isCancelled()) {
                cancelQuery(query, cancelUrl);
              }
            }
          },
          // The callback is non-blocking and quick, so it's OK to schedule it using directExecutor()
          Execs.directExecutor()
      );
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    /**
     * 将异步请求结果集封装进Sequence，
     * 在调用make时，就遍历这个结果集
     */
    Sequence<T> retVal = new BaseSequence<>(
        new BaseSequence.IteratorMaker<T, JsonParserIterator<T>>()
        {
          @Override
          public JsonParserIterator<T> make()
          {
            /**
             *
             * @param typeRef
             * @param future 异步查询响应结果：
             *               异步发送查询请求，然后异步线程每收到一个数据包，就交由responseHandler处理，
             *               handler会将“请求头+请求行+后续所有chunk包”，全部依次handle的内部queue队列中
             *               当异步线程接收并处理完所有chunk时，就将handler的处理结果“ClientResponse”对象装入以下future中。
             *               （ClientResponse对象可以操作遍历handler内部的queue队列）
             *               也就是说通过future可以获取到ClientResponse是否准备好，如果准备完毕就通过其遍历结果数据集queue队列。
             *
             * @param url 此次查询的url，如：http://localhost:8083/druid/v2/
             * @param query 此次查询对象
             * @param host 待查询主机地址
             * @param objectMapper
             */
            return new JsonParserIterator<T>(
                queryResultType,
                future,
                url,
                query,
                host,
                toolChest.decorateObjectMapper(objectMapper, query)
            );
          }

          @Override
          public void cleanup(JsonParserIterator<T> iterFromMake)
          {
            CloseQuietly.close(iterFromMake);
          }
        }
    );

    // bySegment queries are de-serialized after caching results in order to
    // avoid the cost of de-serializing and then re-serializing again when adding to cache
    if (!isBySegment) {
      retVal = Sequences.map(
          retVal,
          toolChest.makePreComputeManipulatorFn(
              query,
              MetricManipulatorFns.deserializing()
          )
      );
    }

    return retVal;
  }

  private void cancelQuery(Query<T> query, String cancelUrl)
  {
    Runnable cancelRunnable = () -> {
      try {
        Future<StatusResponseHolder> responseFuture = httpClient.go(
            new Request(HttpMethod.DELETE, new URL(cancelUrl))
            .setContent(objectMapper.writeValueAsBytes(query))
            .setHeader(HttpHeaders.Names.CONTENT_TYPE, isSmile ? SmileMediaTypes.APPLICATION_JACKSON_SMILE : MediaType.APPLICATION_JSON),
            StatusResponseHandler.getInstance(),
            Duration.standardSeconds(1));

        Runnable checkRunnable = () -> {
          try {
            if (!responseFuture.isDone()) {
              log.error("Error cancelling query[%s]", query);
            }
            StatusResponseHolder response = responseFuture.get();
            if (response.getStatus().getCode() >= 500) {
              log.error("Error cancelling query[%s]: queriable node returned status[%d] [%s].",
                  query,
                  response.getStatus().getCode(),
                  response.getStatus().getReasonPhrase());
            }
          }
          catch (ExecutionException | InterruptedException e) {
            log.error(e, "Error cancelling query[%s]", query);
          }
        };
        queryCancellationExecutor.schedule(checkRunnable, 5, TimeUnit.SECONDS);
      }
      catch (IOException e) {
        log.error(e, "Error cancelling query[%s]", query);
      }
    };
    queryCancellationExecutor.submit(cancelRunnable);
  }

  @Override
  public String toString()
  {
    return "DirectDruidClient{" +
           "host='" + host + '\'' +
           ", isSmile=" + isSmile +
           '}';
  }
}
