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
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.TruncatedResponseContextException;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.server.metrics.QueryCountStatsProvider;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.joda.time.DateTime;

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
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

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
    // new一个queryLifecycle对象，其中很多属性都是从上下文中注入进来的
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
       * readQuery(req, in, ioReaderWriter)：
       * 利用自定义的ObjectMapper对象（注入进来的），
       * 根据请求，解析成对应的Query对象，并返回
       *
       * queryLifecycle.initialize(Query);
       * 主要是根据请求，获得对应的Query对象，
       * 然后将特定Query对象，以及context上下文都放入queryLifecycle中
       */
      queryLifecycle.initialize(readQuery(req, in, ioReaderWriter));
      //拿出的是刚刚解析出来的特定query对象
      query = queryLifecycle.getQuery();
      log.info("!!!select：此次请求query对象："+query.getClass());
      final String queryId = query.getId();

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

      // 根据请求对象，判断此次请求是否被授权
      final Access authResult = queryLifecycle.authorize(req);
      if (!authResult.isAllowed()) {
        throw new ForbiddenException(authResult.toString());
      }

      /**
       * ！！！执行查询请求
       *
       * Query对象：
       * 客户端的请求会被包装成query对象，每种请求不一样，
       * 以timeseries数据源为例，查询请求会被包装成{@link org.apache.druid.query.timeseries.TimeseriesQuery}
       * 然后在上面被传入queryLifecycle。
       *
       * walker对象：
       * walker对象作用是根据query请求，合理的创建queryrunner对象，
       * 在程序启动时就会创建好，并注入到queryLifecycle中，
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
       * 就想上面说的，
       * 虽然walker返回的是{@link org.apache.druid.server.ClientQuerySegmentWalker.QuerySwappingQueryRunner}
       * 但是此类实际上是一个包装类，
       * 其内部包含了一个“baseQueryRunner”，一个“newQuery”，以及“最初的query对象TimeseriesQuery”
       * 它的run方法实际上是，将基础query和newQuery“关联起来”，然后传参给baseQueryRunner，并调用baseQueryRunner的run方法。
       *
       * 所以一次查询的关键实际上是：
       * 1、walker如何生成newQuery和baseQueryRunner，以及生成他们的意义。
       * 2、baseQueryRunner的run方法做了什么
       */
      final QueryLifecycle.QueryResponse queryResponse = queryLifecycle.execute();
      final Sequence<?> results = queryResponse.getResults();
      final ResponseContext responseContext = queryResponse.getResponseContext();
      //获取请求头中的If-None-Match参数
      final String prevEtag = getPreviousEtag(req);

      /**
       * 客户端请求查询时请求头会传一个If-None-Match参数，
       * 服务端查询完毕后，结果数据里有一个ETag参数，
       *
       * 此处二者相等时满足以下条件
       */
      if (prevEtag != null && prevEtag.equals(responseContext.get(ResponseContext.Key.ETAG))) {
        //发送日志和监控给“getRemoteAddr”地址，也就是请求客户端地址
        queryLifecycle.emitLogsAndMetrics(null, req.getRemoteAddr(), -1);
        //successfulQueryCount值+1，看样子是记录成功查询的数量的
        successfulQueryCount.incrementAndGet();
        /**
         * 不是很明白，为什么此处返回了个空的响应
         * 是因为上面的queryLifecycle.emitLogsAndMetrics(null, req.getRemoteAddr(), -1);已经发送了想要的结果吗？
         */
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
       *
       */
      final Yielder<?> yielder = Yielders.each(results);

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

        return responseBuilder
            .header(HEADER_RESPONSE_CONTEXT, serializationResult.getResult())
            .build();
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
