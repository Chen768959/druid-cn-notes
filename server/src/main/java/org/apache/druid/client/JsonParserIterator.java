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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.ResourceLimitExceededException;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class JsonParserIterator<T> implements Iterator<T>, Closeable
{
  private static final Logger LOG = new Logger(JsonParserIterator.class);

  private JsonParser jp;
  private ObjectCodec objectCodec;
  private final JavaType typeRef;
  private final Future<InputStream> future;
  private final String url;
  private final String host;
  private final ObjectMapper objectMapper;
  private final boolean hasTimeout;
  private final long timeoutAt;
  private final String queryId;

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
  public JsonParserIterator(
      JavaType typeRef,
      Future<InputStream> future,
      String url,
      @Nullable Query<T> query,
      String host,
      ObjectMapper objectMapper
  )
  {
    this.typeRef = typeRef;
    this.future = future;
    this.url = url;
    if (query != null) {
      this.timeoutAt = query.<Long>getContextValue(DirectDruidClient.QUERY_FAIL_TIME, -1L);
      this.queryId = query.getId();
    } else {
      this.timeoutAt = -1;
      this.queryId = null;
    }
    this.jp = null;
    this.host = host;
    this.objectMapper = objectMapper;
    this.hasTimeout = timeoutAt > -1;
  }

  @Override
  public boolean hasNext()
  {
    init();

    if (jp.isClosed()) {
      return false;
    }
    if (jp.getCurrentToken() == JsonToken.END_ARRAY) {
      CloseQuietly.close(jp);
      return false;
    }

    return true;
  }

  @Override
  public T next()
  {
    //初始化，即为jp赋值
    init();

    try {
      // 将json数据反序列化成pojo
      final T retVal = objectCodec.readValue(jp, typeRef);
      jp.nextToken();
      return retVal;
    }
    catch (IOException e) {
      // check for timeout, a failure here might be related to a timeout, so lets just attribute it
      if (checkTimeout()) {
        TimeoutException timeoutException = timeoutQuery();
        timeoutException.addSuppressed(e);
        throw interruptQuery(timeoutException);
      } else {
        throw interruptQuery(e);
      }
    }
  }

  @Override
  public void remove()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException
  {
    if (jp != null) {
      jp.close();
    }
  }

  private boolean checkTimeout()
  {
    long timeLeftMillis = timeoutAt - System.currentTimeMillis();
    return checkTimeout(timeLeftMillis);
  }

  private boolean checkTimeout(long timeLeftMillis)
  {
    if (hasTimeout && timeLeftMillis < 1) {
      return true;
    }
    return false;
  }

  private void init()
  {
    if (jp == null) {
      try {
        long timeLeftMillis = timeoutAt - System.currentTimeMillis();
        if (checkTimeout(timeLeftMillis)) {
          throw interruptQuery(timeoutQuery());
        }
        // 获取请求结果输入流
        InputStream is = hasTimeout ? future.get(timeLeftMillis, TimeUnit.MILLISECONDS) : future.get();

        if (is != null) {
          //从输入流中获取JsonParser对象
          jp = objectMapper.getFactory().createParser(is);

        } else if (checkTimeout()) {
          throw interruptQuery(timeoutQuery());
        } else {
          // if we haven't timed out completing the future, then this is the likely cause
          throw interruptQuery(new ResourceLimitExceededException("url[%s] max bytes limit reached.", url));
        }

        final JsonToken nextToken = jp.nextToken();
        if (nextToken == JsonToken.START_ARRAY) {
          jp.nextToken();
          objectCodec = jp.getCodec();
        } else if (nextToken == JsonToken.START_OBJECT) {
          throw interruptQuery(jp.getCodec().readValue(jp, QueryInterruptedException.class));
        } else {
          throw interruptQuery(
              new IAE("Next token wasn't a START_ARRAY, was[%s] from url[%s]", jp.getCurrentToken(), url)
          );
        }
      }
      catch (IOException | InterruptedException | ExecutionException | CancellationException | TimeoutException e) {
        throw interruptQuery(e);
      }
    }
  }

  private TimeoutException timeoutQuery()
  {
    return new TimeoutException(StringUtils.format("url[%s] timed out", url));
  }

  private QueryInterruptedException interruptQuery(Exception cause)
  {
    LOG.warn(cause, "Query [%s] to host [%s] interrupted", queryId, host);
    return new QueryInterruptedException(cause, host);
  }
}

