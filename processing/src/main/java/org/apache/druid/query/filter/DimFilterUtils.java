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

package org.apache.druid.query.filter;

import com.google.common.base.Function;
import com.google.common.collect.RangeSet;
import org.apache.druid.timeline.partition.ShardSpec;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 *
 */
public class DimFilterUtils
{
  static final byte SELECTOR_CACHE_ID = 0x0;
  static final byte AND_CACHE_ID = 0x1;
  static final byte OR_CACHE_ID = 0x2;
  static final byte NOT_CACHE_ID = 0x3;
  static final byte EXTRACTION_CACHE_ID = 0x4;
  static final byte REGEX_CACHE_ID = 0x5;
  static final byte SEARCH_QUERY_TYPE_ID = 0x6;
  static final byte JAVASCRIPT_CACHE_ID = 0x7;
  static final byte SPATIAL_CACHE_ID = 0x8;
  static final byte IN_CACHE_ID = 0x9;
  static final byte BOUND_CACHE_ID = 0xA;
  static final byte INTERVAL_CACHE_ID = 0xB;
  static final byte LIKE_CACHE_ID = 0xC;
  static final byte COLUMN_COMPARISON_CACHE_ID = 0xD;
  static final byte EXPRESSION_CACHE_ID = 0xE;
  static final byte TRUE_CACHE_ID = 0xF;
  static final byte FALSE_CACHE_ID = 0x11;
  public static final byte BLOOM_DIM_FILTER_CACHE_ID = 0x10;

  public static final byte STRING_SEPARATOR = (byte) 0xFF;

  static byte[] computeCacheKey(byte cacheIdKey, List<DimFilter> filters)
  {
    if (filters.size() == 1) {
      return filters.get(0).getCacheKey();
    }

    byte[][] cacheKeys = new byte[filters.size()][];
    int totalSize = 0;
    int index = 0;
    for (DimFilter field : filters) {
      cacheKeys[index] = field.getCacheKey();
      totalSize += cacheKeys[index].length;
      ++index;
    }

    ByteBuffer retVal = ByteBuffer.allocate(1 + totalSize);
    retVal.put(cacheIdKey);
    for (byte[] cacheKey : cacheKeys) {
      retVal.put(cacheKey);
    }
    return retVal.array();
  }

  /**
   * Filter the given iterable of objects by removing any object whose ShardSpec, obtained from the converter function,
   * does not fit in the RangeSet of the dimFilter {@link DimFilter#getDimensionRangeSet(String)}. The returned set
   * contains the filtered objects in the same order as they appear in input.
   *
   * If you plan to call this multiple times with the same dimFilter, consider using
   * {@link #filterShards(DimFilter, Iterable, Function, Map)} instead with a cached map
   *
   * @param dimFilter The filter to use
   * @param input     The iterable of objects to be filtered
   * @param converter The function to convert T to ShardSpec that can be filtered by
   * @param <T>       This can be any type, as long as transform function is provided to convert this to ShardSpec
   *
   * @return The set of filtered object, in the same order as input
   */
  public static <T> Set<T> filterShards(DimFilter dimFilter, Iterable<T> input, Function<T, ShardSpec> converter)
  {
    return filterShards(dimFilter, input, converter, new HashMap<>());
  }

  /**
   * Filter the given iterable of objects by removing any object whose ShardSpec, obtained from the converter function,
   * does not fit in the RangeSet of the dimFilter {@link DimFilter#getDimensionRangeSet(String)}. The returned set
   * contains the filtered objects in the same order as they appear in input.
   *
   * DimensionRangedCache stores the RangeSets of different dimensions for the dimFilter. It should be re-used
   * between calls with the same dimFilter to save redundant calls of {@link DimFilter#getDimensionRangeSet(String)}
   * on same dimensions.
   *
   * @param dimFilter           The filter to use
   * @param input               The iterable of objects to be filtered
   * @param converter           The function to convert T to ShardSpec that can be filtered by
   * @param dimensionRangeCache The cache of RangeSets of different dimensions for the dimFilter
   * @param <T>                 This can be any type, as long as transform function is provided to convert this to ShardSpec
   *
   * @return The set of filtered object, in the same order as input
   */
  /**
   * 在创建分片时，可能会设置该分片的各维度的存值范围，
   * 入参传入了一个“维度过滤器”，相当于指定了某维度的查询范围。
   * 此方法根据每一个分片的各维度存值范围，与此处指定的各维度查询范围做比较。
   * 排除掉了“不可能包含任一查询维度值”的分片。
   *
   * 返回的是个分片结果集，
   * 里面每个分片，都有可能包含需要被查询的数据
   *
   * @param dimFilter 维度过滤器，相当于“只查询过滤器里选择的范围”
   * @param input 查询时间区间上的某一个segment的信息对象的“所有分片信息”
   * @param converter partitionChunk -> partitionChunk.getObject().getSegment().getShardSpec()
   * @param dimensionRangeCache 空的hashmap
   */
  public static <T> Set<T> filterShards(
      final DimFilter dimFilter,
      final Iterable<T> input,
      final Function<T, ShardSpec> converter,
      final Map<String, Optional<RangeSet<String>>> dimensionRangeCache
  )
  {
    Set<T> retSet = new LinkedHashSet<>();

    // 迭代单个segment的每一个分片信息
    for (T obj : input) {
      // 通过入参的匿名函数获取这个分片信息对象
      ShardSpec shard = converter.apply(obj);
      boolean include = true;

      if (dimFilter != null && shard != null) {
        Map<String, RangeSet<String>> filterDomain = new HashMap<>();
        // 获取该分片上的所有维度名
        List<String> dimensions = shard.getDomainDimensions();
        // 迭代分片的每一个维度名
        for (String dimension : dimensions) {
          // 使用维度过滤器，获得dimension维度的“可能的取值范围RangeSet<String>”
          Optional<RangeSet<String>> optFilterRangeSet = dimensionRangeCache
              .computeIfAbsent(dimension, d -> Optional.ofNullable(dimFilter.getDimensionRangeSet(d)));

          if (optFilterRangeSet.isPresent()) {
            filterDomain.put(dimension, optFilterRangeSet.get());
          }
        }

        // 根据分片信息，判断其是否可能包含任一维度的值
        if (!filterDomain.isEmpty() && !shard.possibleInDomain(filterDomain)) {
          include = false;// 该分片一定不包含此维度
        }
      }

      // 该分片如果可能包含任意维度信息，则此处都是true
      // 只要确定该分片不可能包含任意查询维度，则不将该分片放入结果集
      if (include) {
        retSet.add(obj);
      }
    }
    return retSet;
  }
}
