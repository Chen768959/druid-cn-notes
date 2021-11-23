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
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.joda.ser.DateTimeSerializer;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.CountingOutputStream;
import com.google.inject.Inject;
import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.TruncatedResponseContextException;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.BufferArrayGrouper;
import org.apache.druid.query.groupby.epinephelinae.GroupByMergingQueryRunnerV2;
import org.apache.druid.query.groupby.epinephelinae.vector.VectorGroupByEngine;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.server.metrics.QueryCountStatsProvider;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.UnaryOperator;

@LazySingleton
@Path("/druid/v2/")
public class QueryResource implements QueryCountStatsProvider
{
  protected static final EmittingLogger log = new EmittingLogger(QueryResource.class);

  @Deprecated // use SmileMediaTypes.APPLICATION_JACKSON_SMILE
  protected static final String APPLICATION_SMILE = "application/smile";

  /**
   * HTTP response header name containing {@link ResponseContext} serialized string
   */
  public static final String HEADER_RESPONSE_CONTEXT = "X-Druid-Response-Context";
  public static final String HEADER_IF_NONE_MATCH = "If-None-Match";
  public static final String HEADER_ETAG = "ETag";

  protected final QueryLifecycleFactory queryLifecycleFactory;
  protected final ObjectMapper jsonMapper;
  protected final ObjectMapper smileMapper;
  protected final ObjectMapper serializeDateTimeAsLongJsonMapper;
  protected final ObjectMapper serializeDateTimeAsLongSmileMapper;
  protected final QueryScheduler queryScheduler;
  protected final AuthConfig authConfig;
  protected final AuthorizerMapper authorizerMapper;

  private final ResponseContextConfig responseContextConfig;
  private final DruidNode selfNode;

  private final AtomicLong successfulQueryCount = new AtomicLong();
  private final AtomicLong failedQueryCount = new AtomicLong();
  private final AtomicLong interruptedQueryCount = new AtomicLong();

  @Inject
  public QueryResource(
      QueryLifecycleFactory queryLifecycleFactory,
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      QueryScheduler queryScheduler,
      AuthConfig authConfig,
      AuthorizerMapper authorizerMapper,
      ResponseContextConfig responseContextConfig,
      @Self DruidNode selfNode
  )
  {
    this.queryLifecycleFactory = queryLifecycleFactory;
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.serializeDateTimeAsLongJsonMapper = serializeDataTimeAsLong(jsonMapper);
    this.serializeDateTimeAsLongSmileMapper = serializeDataTimeAsLong(smileMapper);
    this.queryScheduler = queryScheduler;
    this.authConfig = authConfig;
    this.authorizerMapper = authorizerMapper;
    this.responseContextConfig = responseContextConfig;
    this.selfNode = selfNode;
  }

  @DELETE
  @Path("{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response cancelQuery(@PathParam("id") String queryId, @Context final HttpServletRequest req)
  {
    if (log.isDebugEnabled()) {
      log.debug("Received cancel request for query [%s]", queryId);
    }
    Set<String> datasources = queryScheduler.getQueryDatasources(queryId);
    if (datasources == null) {
      log.warn("QueryId [%s] not registered with QueryScheduler, cannot cancel", queryId);
      datasources = new TreeSet<>();
    }

    Access authResult = AuthorizationUtils.authorizeAllResourceActions(
        req,
        Iterables.transform(datasources, AuthorizationUtils.DATASOURCE_WRITE_RA_GENERATOR),
        authorizerMapper
    );

    if (!authResult.isAllowed()) {
      throw new ForbiddenException(authResult.toString());
    }

    queryScheduler.cancelQuery(queryId);
    return Response.status(Response.Status.ACCEPTED).build();
  }

  @POST
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE, APPLICATION_SMILE})
  public Response doPost(
      final InputStream in,
      @QueryParam("pretty") final String pretty,

      // used to get request content-type,Accept header, remote address and auth-related headers
      @Context final HttpServletRequest req
  ) throws IOException
  {
    long beginTime = System.currentTimeMillis();
    log.info("!!!："+Thread.currentThread().getId()+"request post");
    /**
     * new一个queryLifecycle对象，其中很多属性都是从上下文中注入queryLifecycleFactory中的,
     * queryLifecycle对象控制着此次查询的各个阶段的生命周期，有四个生命周期
     * 1、初始化
     * 2、鉴权
     * 3、执行查询
     * 4、输出日志
     *
     * 接下来整个doPost方法实际上就是在执行queryLifecycle的四个生命周期方法。
     */
    final QueryLifecycle queryLifecycle = queryLifecycleFactory.factorize();
    Query<?> query = null;

    //客户端希望接收的响应数据类型
    String acceptHeader = req.getHeader("Accept");
    if (Strings.isNullOrEmpty(acceptHeader)) {
      //default to content-type
      // 没有accept，就用content-type类型当accept类型
      acceptHeader = req.getContentType();
    }

    /**
     * 创建一个ResourceIOReaderWriter对象，
     * 其中存放了
     * 1、accept请求类型（json还是smile）
     * 2、帮助json序列化和反序列化的格式对象“ObjectMapper”
     */
    final ResourceIOReaderWriter ioReaderWriter = createResourceIOReaderWriter(acceptHeader, pretty != null);

    //当前线程来自jetty的work线程池中
    final String currThreadName = Thread.currentThread().getName();
    try {
      /**
       * queryLifecycle生命周期1：初始化
       * 主要是将此次请求对象Query和queryLifecycle进行绑定。
       *
       * readQuery(req, in, ioReaderWriter)：
       * 主要就是将此次请求的json请求报文转化成{@link Query}对象，
       * 可以理解query对象就是请求的json对象。
       *
       * queryLifecycle.initialize(Query);
       * 然后将特定Query对象，以及context上下文都放入queryLifecycle中
       */
      log.info("!!!："+Thread.currentThread().getId()+"request init");
      Query<?> query1 = readQuery(req, in, ioReaderWriter);
      log.info("!!!："+Thread.currentThread().getId()+"request init2");
      queryLifecycle.initialize(query1);
      log.info("!!!："+Thread.currentThread().getId()+"request init3");

      query = queryLifecycle.getQuery();
//      log.info("!!!select：此次请求query对象："+query.getClass());
      final String queryId = query.getId();// 此次查询id

      // 重命名当前线程名，将相关信息都放进去
      final String queryThreadName = StringUtils.format(
          "%s[%s_%s_%s]",
          currThreadName,
          query.getType(),
          query.getDataSource().getTableNames(),
          queryId
      );
      Thread.currentThread().setName(queryThreadName);

      if (log.isDebugEnabled()) {
        log.debug("Got query [%s]", query);
      }

      /**
       * queryLifecycle生命周期2：鉴权
       */
      log.info("!!!："+Thread.currentThread().getId()+"request Access");
      final Access authResult = queryLifecycle.authorize(req);
      if (!authResult.isAllowed()) {
        throw new ForbiddenException(authResult.toString());
      }

      /**
       * queryLifecycle生命周期3：执行查询
       *
       * Query对象：
       * 客户端的请求会被包装成query对象，每种请求不一样，
       * 以timeseries数据源为例，查询请求会被包装成{@link org.apache.druid.query.timeseries.TimeseriesQuery}
       * 其中内容就是此次查询的json内容。
       * （在queryLifecycle生命周期1初始化中与{@link QueryLifecycle#baseQuery}属性进行绑定）
       *
       * walker对象：
       * walker对象作用是根据query请求，合理的创建queryrunner对象，
       * walker对象在程序启动时就会创建好，并注入到queryLifecycle中，
       * 此处是{@link ClientQuerySegmentWalker},
       * walker接口有两个方法，
       * 一个是根据时间限制条件，创建queryrunner，
       * 一个是根据segment条件，创建queryrunner。
       * 以timeseries数据源为例，
       * 调用的是{@link ClientQuerySegmentWalker#getQueryRunnerForIntervals(Query, Iterable)}
       * 使用这种方式创建的queryrunner一般是个包装类，
       * 里面包含了一个“baseQueryRunner”，一个“newQuery”，以及“最初的query对象TimeseriesQuery”
       *
       * queryrunner
       * 就像上面说的，
       * 虽然walker返回的是{@link org.apache.druid.server.ClientQuerySegmentWalker.QuerySwappingQueryRunner}
       * 但是此类实际上是一个包装类，
       * 其内部包含了一个“baseQueryRunner”，一个“newQuery”，以及“最初的query对象TimeseriesQuery”
       * 它的run方法实际上是，将基础query和newQuery“关联起来”，然后传参给baseQueryRunner，并调用baseQueryRunner的run方法。
       *
       * 所以一次查询的关键实际上是：
       * 1、walker如何生成newQuery和baseQueryRunner，以及生成他们的意义。
       * 2、baseQueryRunner的run方法做了什么
       *
       * 此处返回的结果，实际上由{@link org.apache.druid.client.CachingClusteredClient#run(QueryPlus, ResponseContext, UnaryOperator, boolean)}返回，
       * 该方法主要做了以下两件事
       * 1、根据查询时间区间，从整个集群中找到对应的segment以及其所在的主机信息。
       * 2、创建一个匿名函数（创建LazySequence对象时设置的匿名函数），
       * 该函数作用是“根据segment所在主机的信息，使用netty发送http请求，并将所有获取的结果合并返回”
       * （发送http请求，异步获取请求结果：{@link org.apache.druid.client.DirectDruidClient#run(QueryPlus, ResponseContext)}）
       * 返回的类型就是Sequence<T>
       * 而该匿名函数只有在接下来Sequence进行toYielder时才会被执行
       *
       * ===============================================================================================================
       * 请求到达historical节点：
       * broker节点收到查询请求后，会将请求拆分成子请求，然后发送给historical节点，
       * 历史节点也是该方法接收请求，然后也会走到此处.
       *
       * “！！！”
       * 无论是broker节点还是历史节点，其懒加载原理都如下：
       * 此处获得的结果对象中都包含了一个{@link org.apache.druid.java.util.common.guava.BaseSequence.IteratorMaker}类型的匿名函数，
       * 且会在其中设置一个make()方法，该方法返回了一个{@link org.apache.druid.java.util.common.parsers.CloseableIterator}类型的迭代器，
       * 通过该迭代器，可以读取查询结果，每次迭代一个ResultRow类型的对象，也就是一行结果。
       *
       * 在后续Sequence进行toYielder或者要从Sequence中取结果时，就会调用make获取迭代器，然后再用迭代器获取结果，
       * 也就是在这时候，才会真正查询结果。
       *
       * 所以历史节点查询的关键就是：
       * （1）历史节点的make()方法如何获取迭代器
       * 通过此方法{@link org.apache.druid.query.groupby.epinephelinae.vector.VectorGroupByEngine#process(GroupByQuery, StorageAdapter, ByteBuffer, DateTime, Filter, Interval, GroupByQueryConfig)}
       * 返回BaseSequence结果对象的同时，设置了一个IteratorMaker匿名函数，
       * 其make()方法返回了一个{@link VectorGroupByEngine.VectorGroupByEngineIterator}迭代器，
       * 后续BaseSequence通过此迭代器的next方法，可获取查询结果ResultRow对象
       *
       * （2）此迭代器的next()方法是如何返回{@link ResultRow}查询结果行对象的。
       * 这块有点绕。。
       * 先来看迭代器的next()方法
       * {@link VectorGroupByEngine.VectorGroupByEngineIterator#next()}
       * 真正查询出结果的是其内部的
       * {@link org.apache.druid.query.groupby.epinephelinae.CloseableGrouperIterator}迭代器的next方法
       * 而此迭代器查询ResultRow分为2步，
       * 1、先是“再通过其内部一个迭代器”获取该行的聚合函数
       * 2、再通过一个匿名方法，获取该行的dimension值
       * 其内部迭代器和匿名方法都是在创建此CloseableGrouperIterator迭代器是传参进入的。
       * 创建方法为{@link org.apache.druid.query.groupby.epinephelinae.vector.VectorGroupByEngine.VectorGroupByEngineIterator#initNewDelegate()}
       *
       * 直接关注“获取聚合函数的迭代器的next方法”和“获取dimension值的匿名方法”
       * 1、{@link BufferArrayGrouper#iterator(boolean)}创建了查询聚合函数的迭代器，其next()方法获取了该行聚合结果
       *
       *
       * 2、{@link org.apache.druid.query.groupby.epinephelinae.vector.VectorGroupByEngine.VectorGroupByEngineIterator#initNewDelegate()}
       * 其中就包含了获取dimension值的匿名方法
       */
      log.info("!!!："+Thread.currentThread().getId()+"request execute");
      final QueryLifecycle.QueryResponse queryResponse = queryLifecycle.execute();
//      log.info("!!!：doPost获取到queryResponse");
      //该results对象中包含了真正执行查询的匿名函数，只有调用了，才会进行查询
      final Sequence<?> results = queryResponse.getResults();
      final ResponseContext responseContext = queryResponse.getResponseContext();
      //获取请求头中的If-None-Match参数
      final String prevEtag = getPreviousEtag(req);

      /**
       * 正常情况下不会进入此条件。
       *
       * 如果要进入此条件，客户端请求时，请求头中必须带有“If-None-Match”header key
       * （一般情况下客户端不会专门指定该key）
       *
       * 关于If-None-Match与ETAG的介绍：
       * （1）客户端第一次请求服务端时，服务端在response响应header头中会生成一个ETAG key，其value为一串code码
       * （2）客户端第二次请求服务端时，request的header中可以带一个If-None-Match key，其value就是之前ETAG对应的code码，
       *      然后服务端会比较此次查询结果的ETAG值，如果未变化，就将If-None-Match的值设为false，返回状态为304，然后不返回查询结果
       *      “客户端继续使用本地缓存”
       * （即满足如下的if条件）
       */
      if (prevEtag != null && prevEtag.equals(responseContext.get(ResponseContext.Key.ETAG))) {
        //发送日志和监控给“getRemoteAddr”地址，也就是请求客户端地址
        /**
         * queryLifecycle生命周期4：输出异常日志
         */
        queryLifecycle.emitLogsAndMetrics(null, req.getRemoteAddr(), -1);
        //successfulQueryCount值+1，看样子是记录成功查询的数量的
        successfulQueryCount.incrementAndGet();
        return Response.notModified().build();
      }

      /**
       * druid中的查询结果对象是一种“Sequence”类型的对象，
       * 是对迭代器的高级封装。
       *
       * 而Yielder对象作用是在遍历Sequence对象时，进行“暂停”。
       *
       * Yielder对象可以看成一个链表，
       * 头部Yielder对象，后面跟了一连串的Yielder对象。
       * Yielder的get方法可获取当前对象值，next可获得下一个Yielder。
       *
       * 此处实际上是调用{@link org.apache.druid.java.util.common.guava.LazySequence#toYielder(Object, YieldingAccumulator)}
       * 将此次查询的Sequence转换成Yielder，
       * 而也正是此处，才会真正执行远程http的集群查询，具体匿名执行逻辑在
       * {@link org.apache.druid.client.DirectDruidClient#run(QueryPlus, ResponseContext)}创建LazySequence的时候
       */
      final Yielder<?> yielder = Yielders.each(results);
//      log.info("!!!：yielder中数据类型："+yielder.get().getClass());

      try {
        boolean shouldFinalize = QueryContexts.isFinalize(query, true);
        boolean serializeDateTimeAsLong =
            QueryContexts.isSerializeDateTimeAsLong(query, false)
            || (!shouldFinalize && QueryContexts.isSerializeDateTimeAsLongInner(query, false));

        //新建一个负责输出的对象
        final ObjectWriter jsonWriter = ioReaderWriter.newOutputWriter(
            queryLifecycle.getToolChest(),
            queryLifecycle.getQuery(),
            serializeDateTimeAsLong
        );

        Response.ResponseBuilder responseBuilder = Response
            .ok(
                new StreamingOutput()
                {
                  @Override
                  public void write(OutputStream outputStream) throws WebApplicationException
                  {
                    Exception e = null;

                    // CountingOutputStream：该流可以记录已经往此流中写入了多少字节
                    CountingOutputStream os = new CountingOutputStream(outputStream);
                    try {
                      // json serializer will always close the yielder
                      /**
                       * 将yielder中的结果数据直接序列化后写入输出流
                       */
                      jsonWriter.writeValue(os, yielder);

                      os.flush(); // Some types of OutputStream suppress flush errors in the .close() method.
                      os.close();

                      long endT = System.currentTimeMillis();
                      log.info("!!!："+Thread.currentThread().getId()+"request end1："+(endT-beginTime));
                    }
                    catch (Exception ex) {
                      e = ex;
                      log.noStackTrace().error(ex, "Unable to send query response.");
                      throw new RuntimeException(ex);
                    }
                    finally {
                      Thread.currentThread().setName(currThreadName);

                      queryLifecycle.emitLogsAndMetrics(e, req.getRemoteAddr(), os.getCount());

                      if (e == null) {
                        successfulQueryCount.incrementAndGet();
                      } else {
                        failedQueryCount.incrementAndGet();
                      }
                    }
                  }
                },
                ioReaderWriter.getContentType()
            )
            .header("X-Druid-Query-Id", queryId);

        Object entityTag = responseContext.remove(ResponseContext.Key.ETAG);
        if (entityTag != null) {
          responseBuilder.header(HEADER_ETAG, entityTag);
        }

        DirectDruidClient.removeMagicResponseContextFields(responseContext);

        //Limit the response-context header, see https://github.com/apache/druid/issues/2331
        //Note that Response.ResponseBuilder.header(String key,Object value).build() calls value.toString()
        //and encodes the string using ASCII, so 1 char is = 1 byte
        final ResponseContext.SerializationResult serializationResult = responseContext.serializeWith(
            jsonMapper,
            responseContextConfig.getMaxResponseContextHeaderSize()
        );

        if (serializationResult.isTruncated()) {
          final String logToPrint = StringUtils.format(
              "Response Context truncated for id [%s]. Full context is [%s].",
              queryId,
              serializationResult.getFullResult()
          );
          if (responseContextConfig.shouldFailOnTruncatedResponseContext()) {
            log.error(logToPrint);
            throw new QueryInterruptedException(
                new TruncatedResponseContextException(
                    "Serialized response context exceeds the max size[%s]",
                    responseContextConfig.getMaxResponseContextHeaderSize()
                ),
                selfNode.getHostAndPortToUse()
            );
          } else {
            log.warn(logToPrint);
          }
        }
        Response buildRes = responseBuilder
                .header(HEADER_RESPONSE_CONTEXT, serializationResult.getResult())
                .build();
        long endTime = System.currentTimeMillis();
        log.info("!!!："+Thread.currentThread().getId()+"request end2："+(endTime-beginTime));
        return buildRes;
      }
      catch (QueryException e) {
        // make sure to close yielder if anything happened before starting to serialize the response.
        yielder.close();
        throw e;
      }
      catch (Exception e) {
        // make sure to close yielder if anything happened before starting to serialize the response.
        yielder.close();
        throw new RuntimeException(e);
      }
      finally {
        // do not close yielder here, since we do not want to close the yielder prior to
        // StreamingOutput having iterated over all the results
      }
    }
    catch (QueryInterruptedException e) {
      interruptedQueryCount.incrementAndGet();
      queryLifecycle.emitLogsAndMetrics(e, req.getRemoteAddr(), -1);
      return ioReaderWriter.gotError(e);
    }
    catch (QueryCapacityExceededException cap) {
      failedQueryCount.incrementAndGet();
      queryLifecycle.emitLogsAndMetrics(cap, req.getRemoteAddr(), -1);
      return ioReaderWriter.gotLimited(cap);
    }
    catch (QueryUnsupportedException unsupported) {
      failedQueryCount.incrementAndGet();
      queryLifecycle.emitLogsAndMetrics(unsupported, req.getRemoteAddr(), -1);
      return ioReaderWriter.gotUnsupported(unsupported);
    }
    catch (ForbiddenException e) {
      // don't do anything for an authorization failure, ForbiddenExceptionMapper will catch this later and
      // send an error response if this is thrown.
      throw e;
    }
    catch (Exception e) {
      failedQueryCount.incrementAndGet();
      queryLifecycle.emitLogsAndMetrics(e, req.getRemoteAddr(), -1);

      log.noStackTrace()
         .makeAlert(e, "Exception handling request")
         .addData("query", query != null ? jsonMapper.writeValueAsString(query) : "unparseable query")
         .addData("peer", req.getRemoteAddr())
         .emit();

      return ioReaderWriter.gotError(e);
    }
    finally {
      Thread.currentThread().setName(currThreadName);
    }
  }

  /**
   *
   * @param req 客户端请求对象
   * @param in 对应客户端请求的输入流
   * @param ioReaderWriter 里面包含了序列化和反序列化工具类，以及accept格式
   * @return org.apache.druid.query.Query<?>
   */
  private Query<?> readQuery(
      final HttpServletRequest req,
      final InputStream in,
      final ResourceIOReaderWriter ioReaderWriter
  ) throws IOException
  {
    // 获取ObjectMapper工具类，将此次请求的输入流中的内容反序列化成Query对象
    // 可以理解为客户端的json请求会被序列化成Query对象
    log.info("!!! objectmapper:"+ioReaderWriter.getInputMapper().getClass());
    Query baseQuery = ioReaderWriter.getInputMapper().readValue(in, Query.class);
    // 获取请求头中的If-None-Match值，该值一开始是由服务端传的，即如果客户端又会传给服务端就是用缓存（不过此值看来在druid中不是这么用的）
    String prevEtag = getPreviousEtag(req);

    if (prevEtag != null) {
      baseQuery = baseQuery.withOverriddenContext(
          ImmutableMap.of(HEADER_IF_NONE_MATCH, prevEtag)
      );
    }

    return baseQuery;
  }

  private static String getPreviousEtag(final HttpServletRequest req)
  {
    return req.getHeader(HEADER_IF_NONE_MATCH);
  }

  protected ObjectMapper serializeDataTimeAsLong(ObjectMapper mapper)
  {
    return mapper.copy().registerModule(new SimpleModule().addSerializer(DateTime.class, new DateTimeSerializer()));
  }

  protected ResourceIOReaderWriter createResourceIOReaderWriter(String requestType, boolean pretty)
  {
    //判断客户端希望获取的数据类型是否是“application/x-jackson-smile”或“application/smile”类型
    boolean isSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(requestType) ||
                      APPLICATION_SMILE.equals(requestType);

    //确定返回客户端的数据类型，是x-jackson-smile还是json
    //smile是json的二进制版本，smile的速度更快
    String contentType = isSmile ? SmileMediaTypes.APPLICATION_JACKSON_SMILE : MediaType.APPLICATION_JSON;
    return new ResourceIOReaderWriter(
        contentType,
        isSmile ? smileMapper : jsonMapper,
        isSmile ? serializeDateTimeAsLongSmileMapper : serializeDateTimeAsLongJsonMapper,
        pretty
    );
  }

  protected static class ResourceIOReaderWriter
  {
    private final String contentType;
    private final ObjectMapper inputMapper;
    private final ObjectMapper serializeDateTimeAsLongInputMapper;
    private final boolean isPretty;

    ResourceIOReaderWriter(
        String contentType,
        ObjectMapper inputMapper,
        ObjectMapper serializeDateTimeAsLongInputMapper,
        boolean isPretty
    )
    {
      this.contentType = contentType;
      this.inputMapper = inputMapper;
      this.serializeDateTimeAsLongInputMapper = serializeDateTimeAsLongInputMapper;
      this.isPretty = isPretty;
    }

    String getContentType()
    {
      return contentType;
    }

    ObjectMapper getInputMapper()
    {
      return inputMapper;
    }

    ObjectWriter newOutputWriter(
        @Nullable QueryToolChest toolChest,
        @Nullable Query query,
        boolean serializeDateTimeAsLong
    )
    {
      final ObjectMapper mapper = serializeDateTimeAsLong ? serializeDateTimeAsLongInputMapper : inputMapper;
      final ObjectMapper decoratedMapper;
      if (toolChest != null) {
        decoratedMapper = toolChest.decorateObjectMapper(mapper, Preconditions.checkNotNull(query, "query"));
      } else {
        decoratedMapper = mapper;
      }
      return isPretty ? decoratedMapper.writerWithDefaultPrettyPrinter() : decoratedMapper.writer();
    }

    Response ok(Object object) throws IOException
    {
      return Response.ok(newOutputWriter(null, null, false).writeValueAsString(object), contentType).build();
    }

    Response gotError(Exception e) throws IOException
    {
      return Response.serverError()
                     .type(contentType)
                     .entity(
                         newOutputWriter(null, null, false)
                             .writeValueAsBytes(QueryInterruptedException.wrapIfNeeded(e))
                     )
                     .build();
    }

    Response gotLimited(QueryCapacityExceededException e) throws IOException
    {
      return Response.status(QueryCapacityExceededException.STATUS_CODE)
                     .entity(newOutputWriter(null, null, false).writeValueAsBytes(e))
                     .build();
    }

    Response gotUnsupported(QueryUnsupportedException e) throws IOException
    {
      return Response.status(QueryUnsupportedException.STATUS_CODE)
                     .entity(newOutputWriter(null, null, false).writeValueAsBytes(e))
                     .build();
    }
  }

  @Override
  public long getSuccessfulQueryCount()
  {
    return successfulQueryCount.get();
  }

  @Override
  public long getFailedQueryCount()
  {
    return failedQueryCount.get();
  }

  @Override
  public long getInterruptedQueryCount()
  {
    return interruptedQueryCount.get();
  }
}
