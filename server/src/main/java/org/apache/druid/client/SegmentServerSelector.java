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

import com.google.common.base.Preconditions;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.SegmentDescriptor;

import javax.annotation.Nullable;

/**
 * Given a {@link SegmentDescriptor}, get a {@link ServerSelector} to use to pick a {@link DruidServer} to query.
 * 传入一个{@link SegmentDescriptor}参数，返回一个{@link ServerSelector}对象，这个“选择器”被用于选择哪一个{@link DruidServer}来进行查询
 *
 * Used by {@link CachingClusteredClient} on the broker to fan out queries to historical and realtime data.
 * 一个请求到达broker后，逻辑会走向CachingClusteredClient，然后到达这里，最终由这个对象“向历史节点和实时节点去索取此次查询的数据结果”
 *
 * Used by {@link org.apache.druid.server.LocalQuerySegmentWalker} on the broker for on broker queries
 */
public class SegmentServerSelector extends Pair<ServerSelector, SegmentDescriptor>
{
  /**
   * This is for a segment hosted on a remote server, where {@link ServerSelector} may be used to pick
   *
   * 针对在远程服务器上的segment
   * a {@link DruidServer} to query.
   */
  public SegmentServerSelector(ServerSelector server, SegmentDescriptor segment)
  {
    super(server, segment);
    Preconditions.checkNotNull(server, "ServerSelector must not be null");
    Preconditions.checkNotNull(segment, "SegmentDescriptor must not be null");
  }

  /**
   * This is for a segment hosted locally
   */
  public SegmentServerSelector(SegmentDescriptor segment)
  {
    super(null, segment);
    Preconditions.checkNotNull(segment, "SegmentDescriptor must not be null");
  }

  /**
   * This may be null if {@link SegmentDescriptor} is locally available, but will definitely not be null for segments
   * which must be queried remotely (e.g. {@link CachingClusteredClient})
   */
  @Nullable
  public ServerSelector getServer()
  {
    return lhs;
  }

  public SegmentDescriptor getSegmentDescriptor()
  {
    return rhs;
  }
}
