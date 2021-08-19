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

package org.apache.druid.query.groupby.epinephelinae.vector;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.collections.StupidPool;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.AggregateResult;
import org.apache.druid.query.groupby.epinephelinae.BufferArrayGrouper;
import org.apache.druid.query.groupby.epinephelinae.CloseableGrouperIterator;
import org.apache.druid.query.groupby.epinephelinae.GroupByMergingQueryRunnerV2;
import org.apache.druid.query.groupby.epinephelinae.GroupByQueryEngineV2;
import org.apache.druid.query.groupby.epinephelinae.HashVectorGrouper;
import org.apache.druid.query.groupby.epinephelinae.VectorGrouper;
import org.apache.druid.query.vector.VectorCursorGranularizer;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.SimpleQueryableIndex;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VectorColumnProcessorFactory;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorCursor;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Collectors;

public class VectorGroupByEngine
{
  private static final Logger log = new Logger(VectorGroupByEngine.class);

  private VectorGroupByEngine()
  {
    // No instantiation.
  }

  public static boolean canVectorize(
      final GroupByQuery query,
      final StorageAdapter adapter,
      @Nullable final Filter filter
  )
  {
    Function<String, ColumnCapabilities> capabilitiesFunction = name ->
        query.getVirtualColumns().getColumnCapabilitiesWithFallback(adapter, name);

    return canVectorizeDimensions(capabilitiesFunction, query.getDimensions())
           && query.getDimensions().stream().allMatch(DimensionSpec::canVectorize)
           && query.getAggregatorSpecs().stream().allMatch(aggregatorFactory -> aggregatorFactory.canVectorize(adapter))
           && VirtualColumns.shouldVectorize(query, query.getVirtualColumns(), adapter)
           && adapter.canVectorize(filter, query.getVirtualColumns(), false);
  }

  public static boolean canVectorizeDimensions(
      final Function<String, ColumnCapabilities> capabilitiesFunction,
      final List<DimensionSpec> dimensions
  )
  {
    return dimensions
        .stream()
        .allMatch(
            dimension -> {
              if (dimension.mustDecorate()) {
                // group by on multi value dimensions are not currently supported
                // DimensionSpecs that decorate may turn singly-valued columns into multi-valued selectors.
                // To be safe, we must return false here.
                return false;
              }

              // Now check column capabilities.
              final ColumnCapabilities columnCapabilities = capabilitiesFunction.apply(dimension.getDimension());
              // null here currently means the column does not exist, nil columns can be vectorized
              if (columnCapabilities == null) {
                return true;
              }
              // strings must be single valued, dictionary encoded, and have unique dictionary entries
              if (ValueType.STRING.equals(columnCapabilities.getType())) {
                return columnCapabilities.hasMultipleValues().isFalse() &&
                       columnCapabilities.isDictionaryEncoded().isTrue() &&
                       columnCapabilities.areDictionaryValuesUnique().isTrue();
              }
              return columnCapabilities.hasMultipleValues().isFalse();
            });
  }

  /**
   *
   * @param query 此次查询请求的对象
   * @param storageAdapter
   * 从{@link com.sun.corba.se.spi.activation.ServerManager}中获取出{@link org.apache.druid.segment.ReferenceCountingSegment}
   * 然后再通过：
   * {@link ReferenceCountingSegment#asStorageAdapter()}
   * 将ReferenceCountingSegment转换成适配器{@link QueryableIndexStorageAdapter}
   * 其中包含了启动时加载的segment对象数据，具体可查看{@link SimpleQueryableIndex}
   * @param processingBuffer 从{@link StupidPool#take()}获取的bufferHolder中，调用get获取的bytebuffer
   * @param fudgeTimestamp
   * @param filter 获取查询对象中的“filter”属性
   * @param interval 要查询的时间片段
   * @param config groupBy查询的配置参数
   */
  public static Sequence<ResultRow> process(
      final GroupByQuery query,
      final StorageAdapter storageAdapter,
      final ByteBuffer processingBuffer,
      @Nullable final DateTime fudgeTimestamp,
      @Nullable final Filter filter,
      final Interval interval,
      final GroupByQueryConfig config
  )
  {
    if (!canVectorize(query, storageAdapter, filter)) {
      throw new ISE("Cannot vectorize");
    }

    /**
     * 此次查询的结果对象
     */
    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<ResultRow, CloseableIterator<ResultRow>>()
        {

          /**
           * 以下make方法为后续Sequence转Yielder时，或者toList等被调用时，才会被调用，
           * 也是真正的查询结果的执行方法
           */
          @Override
          public CloseableIterator<ResultRow> make()
          {
            /**
             * {@link QueryableIndexStorageAdapter#makeVectorCursor(Filter, Interval, VirtualColumns, boolean, int, QueryMetrics)}
             *
             * 获取“cursor游标”，游标可以用来查询每一行数据
             */
            final VectorCursor cursor = storageAdapter.makeVectorCursor(
                // 查询请求中的“filter”过滤条件
                Filters.toFilter(query.getDimFilter()),
                // 查询时间条件
                interval,
                query.getVirtualColumns(),
                false,
                QueryContexts.getVectorSize(query),
                null
            );

            // 无数据则直接返回空结果
            if (cursor == null) {
              // Return empty iterator.
              return new CloseableIterator<ResultRow>()
              {
                @Override
                public boolean hasNext()
                {
                  return false;
                }

                @Override
                public ResultRow next()
                {
                  throw new NoSuchElementException();
                }

                @Override
                public void close()
                {
                  // Nothing to do.
                }
              };
            }

            try {
              // 该factory中包含index对象，即包含segment的全量数据信息
              final VectorColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();
              // 迭代每一个请求维度信息后，返回的对象
              final List<GroupByVectorColumnSelector> dimensions = query.getDimensions().stream().map(
                  // 迭代请求对象中的每个查询维度信息
                  dimensionSpec ->
                      DimensionHandlerUtils.makeVectorProcessor(
                          dimensionSpec,
                          GroupByVectorColumnProcessorFactory.instance(),
                          columnSelectorFactory
                      )
              ).collect(Collectors.toList());

              /**
               * 创建“查询结果迭代器”，通过此迭代器可迭代每一行查询结果。
               *
               * 其next()：{@link VectorGroupByEngineIterator#next()}
               *
               * @param query 此次查询请求的对象
               * @param config groupBy查询的配置参数
               * @param storageAdapter
               * 从{@link com.sun.corba.se.spi.activation.ServerManager}中获取出{@link org.apache.druid.segment.ReferenceCountingSegment}
               * 然后再通过：
               * {@link ReferenceCountingSegment#asStorageAdapter()}
               * 将ReferenceCountingSegment转换成适配器{@link QueryableIndexStorageAdapter}
               * 其中包含了启动时加载的segment对象数据，具体可查看{@link SimpleQueryableIndex}
               *
               * @param cursor 游标，可以用来查询每一行数据
               * @param queryInterval 要查询的时间区间
               * @param dimensions
               * 迭代请求对象中的每一个维度信息，然后获取对应的GroupByVectorColumnSelector组成列表，
               * 由{@link DimensionHandlerUtils#makeVectorProcessor(DimensionSpec, VectorColumnProcessorFactory, VectorColumnSelectorFactory)}创建
               *
               * @param processingBuffer 从{@link StupidPool#take()}获取的bufferHolder中，调用get获取的bytebuffer
               * @param fudgeTimestamp
               */
              return new VectorGroupByEngineIterator(
                  query,
                  config,
                  storageAdapter,
                  cursor,
                  interval,
                  dimensions,
                  processingBuffer,
                  fudgeTimestamp
              );
            }
            catch (Throwable e) {
              try {
                cursor.close();
              }
              catch (Throwable e2) {
                e.addSuppressed(e2);
              }
              throw e;
            }
          }

          @Override
          public void cleanup(CloseableIterator<ResultRow> iterFromMake)
          {
            try {
              iterFromMake.close();
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }
    );
  }

  @VisibleForTesting
  static class VectorGroupByEngineIterator implements CloseableIterator<ResultRow>
  {
    private final GroupByQuery query;
    private final GroupByQueryConfig querySpecificConfig;
    private final StorageAdapter storageAdapter;
    private final VectorCursor cursor;
    private final List<GroupByVectorColumnSelector> selectors;
    private final ByteBuffer processingBuffer;
    // 来自于查询请求context上下文中的“fudgeTimestamp”属性
    private final DateTime fudgeTimestamp;
    private final int keySize;
    private final WritableMemory keySpace;
    /**
     * vectorGrouper.iterator()中会产生真正的迭代器，后续的next等方法实际上就是调用该方法产生的迭代器。
     *
     * vectorGrouper由makeGrouper()产生，{@link this#makeGrouper()}
     * 最后生成{@link BufferArrayGrouper}
     * 而他的{@link BufferArrayGrouper#iterator()}方法产生了真正的迭代器
     */
    private final VectorGrouper vectorGrouper;

    @Nullable
    private final VectorCursorGranularizer granulizer;

    // Granularity-bucket iterator and current bucket.
    private final Iterator<Interval> bucketIterator;

    // bucketIterator迭代器与查询时间相关，且和查询粒度相关，应该是将总时间区间按粒度来切割，然后再由该迭代器迭代出来。
    @Nullable
    private Interval bucketInterval;

    private int partiallyAggregatedRows = -1;

    // 该迭代器真正负责数据的遍历，next实际上就是调用他的next()方法
    @Nullable
    private CloseableGrouperIterator<Memory, ResultRow> delegate = null;

    /**
     *
     * @param query 此次查询请求的对象
     * @param config groupBy查询的配置参数
     * @param storageAdapter
     * 从{@link com.sun.corba.se.spi.activation.ServerManager}中获取出{@link org.apache.druid.segment.ReferenceCountingSegment}
     * 然后再通过：
     * {@link ReferenceCountingSegment#asStorageAdapter()}
     * 将ReferenceCountingSegment转换成适配器{@link QueryableIndexStorageAdapter}
     * 其中包含了启动时加载的segment对象数据，具体可查看{@link SimpleQueryableIndex}
     *
     * @param cursor 游标，可以用来查询每一行数据
     * @param queryInterval 要查询的时间区间
     * @param selectors
     * 迭代请求对象中的每一个维度信息，然后获取对应的GroupByVectorColumnSelector组成列表，
     * 由{@link DimensionHandlerUtils#makeVectorProcessor(DimensionSpec, VectorColumnProcessorFactory, VectorColumnSelectorFactory)}创建
     *
     * @param processingBuffer 从{@link StupidPool#take()}获取的bufferHolder中，调用get获取的bytebuffer
     * @param fudgeTimestamp 来自于查询请求context上下文中的“fudgeTimestamp”属性
     */
    VectorGroupByEngineIterator(
        final GroupByQuery query,
        final GroupByQueryConfig config,
        final StorageAdapter storageAdapter,
        final VectorCursor cursor,
        final Interval queryInterval,
        final List<GroupByVectorColumnSelector> selectors,
        final ByteBuffer processingBuffer,
        @Nullable final DateTime fudgeTimestamp
    )
    {
      this.query = query;
      this.querySpecificConfig = config;
      this.storageAdapter = storageAdapter;
      this.cursor = cursor;
      this.selectors = selectors;
      this.processingBuffer = processingBuffer;
      this.fudgeTimestamp = fudgeTimestamp;
      this.keySize = selectors.stream().mapToInt(GroupByVectorColumnSelector::getGroupingKeySize).sum();
      this.keySpace = WritableMemory.allocate(keySize * cursor.getMaxVectorSize());
      this.vectorGrouper = makeGrouper();

      // bucketIterator迭代器与查询时间相关，且和查询粒度相关，应该是将总时间区间按粒度来切割，然后再由该迭代器迭代出来。
      this.granulizer = VectorCursorGranularizer.create(storageAdapter, cursor, query.getGranularity(), queryInterval);
      if (granulizer != null) {
        this.bucketIterator = granulizer.getBucketIterable().iterator();
      } else {
        this.bucketIterator = Collections.emptyIterator();
      }

      this.bucketInterval = this.bucketIterator.hasNext() ? this.bucketIterator.next() : null;
    }

    /**
     * ResultRow结果实际由delegate.next()查询得来，
     * delegate本身是个迭代器，其next()分为两步
     * 1、第一步是再由其内部的迭代器的next查询当前行的聚合结果
     * 2、第二步由匿名方法得到当前行的dimension列的结果
     *
     * 这两步中无论是delegate的内部迭代器，还是匿名方法，都是创建delegate时传入进去的，
     * 也就是{@link this#initNewDelegate()}时作为参数传入delegate。
     *
     * ====================================================================================
     * 该对象为查询结果的迭代器对象。
     * 观察其next()方法{@link this#next()}
     *
     * （1）next()方法概览
     * {@link this#next()}
     * |->delegate.next()   {@link CloseableGrouperIterator#next()}
     * |->vectorGrouper.iterator().next()
     *
     * 所以当前对象的next方法，实际上是调用“vectorGrouper对象生成的iterator迭代器的next()方法”。
     *
     * （2）vectorGrouper对象的创建
     * vectorGrouper = {@link this#makeGrouper()}
     * vectorGrouper的实现类实际上是{@link BufferArrayGrouper}
     *
     * （3）vectorGrouper.iterator()
     * 也就是{@link BufferArrayGrouper#iterator()}
     * 其内部又调用了自身的{@link BufferArrayGrouper#iterator(boolean)}方法，
     * 该方法创建并返回了一个全新的匿名迭代器
     *
     * （4）vectorGrouper.iterator().next()
     * 指的也就是上面创建的匿名迭代器的next方法
     * {@link BufferArrayGrouper#iterator(boolean)}
     * 此方法是从valBuffer（ByteBuffer类型）中获取聚合结果，而聚合结果在之前就已经写入到了valBuffer中
     *
     * --------------------------------------------------
     * 每次遍历的ResultRow代表“一行结果”，内部存在一个数组，该数组依次为一行的每一个记录。
     *
     * 当前迭代器中存在一个{@link delegate}迭代器，真正负责每行数据的查询，
     * 其初始化方法为{@link this#initNewDelegate()}
     * （valBuffer也是在该初始化时被填充的）
     *
     * |->{@link this#initNewDelegate()}    todo 从该逻辑开始详细推导
     * |->{@link BufferArrayGrouper#aggregateVector(Memory, int, int)}
     * |->{@link AggregatorAdapters#aggregateVector(ByteBuffer, int, int[], int[])}（buffer在此处被填充）
     */
    @Override
    public ResultRow next()
    {
      /**
       * 执行hasNext()，顺便就初始化了真正的迭代器delegate
       * （其中将聚合结果传入了bytebuffer）
       */
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      return delegate.next();
    }

    @Override
    public boolean hasNext()
    {
      // 判断迭代器是否存在，且迭代器内部hasNext条件成立
      if (delegate != null && delegate.hasNext()) {
        return true;
      } else {
        final boolean moreToRead = !cursor.isDone() || partiallyAggregatedRows >= 0;

        if (bucketInterval != null && moreToRead) {
          while (delegate == null || !delegate.hasNext()) {
            if (delegate != null) {
              delegate.close();
              vectorGrouper.reset();
            }

            // 初始化迭代器
            delegate = initNewDelegate();
          }
          return true;
        } else {
          return false;
        }
      }
    }

    @Override
    public void close() throws IOException
    {
      Closer closer = Closer.create();
      closer.register(vectorGrouper);
      if (delegate != null) {
        closer.register(delegate);
      }
      closer.register(cursor);
      closer.close();
    }

    @VisibleForTesting
    VectorGrouper makeGrouper()
    {
      final VectorGrouper grouper;

      final int cardinalityForArrayAggregation = GroupByQueryEngineV2.getCardinalityForArrayAggregation(
          querySpecificConfig,
          query,
          storageAdapter,
          processingBuffer
      );

      if (cardinalityForArrayAggregation >= 0) {
        log.info("!!!：生成BufferArrayGrouper");
        grouper = new BufferArrayGrouper(
            Suppliers.ofInstance(processingBuffer),
            AggregatorAdapters.factorizeVector(
                cursor.getColumnSelectorFactory(),
                query.getAggregatorSpecs()
            ),
            cardinalityForArrayAggregation
        );
      } else {
        log.info("!!!：生成HashVectorGrouper");
        grouper = new HashVectorGrouper(
            Suppliers.ofInstance(processingBuffer),
            keySize,
            AggregatorAdapters.factorizeVector(
                cursor.getColumnSelectorFactory(),
                query.getAggregatorSpecs()
            ),
            querySpecificConfig.getBufferGrouperMaxSize(),
            querySpecificConfig.getBufferGrouperMaxLoadFactor(),
            querySpecificConfig.getBufferGrouperInitialBuckets()
        );
      }

      grouper.initVectorized(cursor.getMaxVectorSize());

      return grouper;
    }

    // 初始化，且填充buffer
    private CloseableGrouperIterator<Memory, ResultRow> initNewDelegate()
    {
      // bucketIterator迭代器与查询时间相关，且和查询粒度相关，应该是将总时间区间按粒度来切割，然后再由该迭代器迭代出来。
      // Method must not be called unless there's a current bucketInterval.
      assert bucketInterval != null;

      // 此次查询的起始时间
      final DateTime timestamp = fudgeTimestamp != null
                                 ? fudgeTimestamp
                                 : query.getGranularity().toDateTime(bucketInterval.getStartMillis());

      // 循环从游标中读取
      while (!cursor.isDone()) {
        final int startOffset;

        if (partiallyAggregatedRows < 0) {
          granulizer.setCurrentOffsets(bucketInterval);
          startOffset = granulizer.getStartOffset();
        } else {
          startOffset = granulizer.getStartOffset() + partiallyAggregatedRows;
        }

        if (granulizer.getEndOffset() > startOffset) {
          // Write keys to the keySpace.
          int keyOffset = 0;
          for (final GroupByVectorColumnSelector selector : selectors) {
            selector.writeKeys(keySpace, keySize, keyOffset, startOffset, granulizer.getEndOffset());
            keyOffset += selector.getGroupingKeySize();
          }

          // Aggregate this vector.
          // 查询聚合结果
          /**{@link BufferArrayGrouper#aggregateVector(Memory, int, int)}*/
          final AggregateResult result = vectorGrouper.aggregateVector(
              keySpace,
              startOffset,
              granulizer.getEndOffset()
          );

          if (result.isOk()) {
            partiallyAggregatedRows = -1;
          } else {
            if (partiallyAggregatedRows < 0) {
              partiallyAggregatedRows = result.getCount();
            } else {
              partiallyAggregatedRows += result.getCount();
            }
          }
        } else {
          partiallyAggregatedRows = -1;
        }

        if (partiallyAggregatedRows >= 0) {
          break;
        } else if (!granulizer.advanceCursorWithinBucket()) {
          // Advance bucketInterval.
          bucketInterval = bucketIterator.hasNext() ? bucketIterator.next() : null;
          break;
        }
      }

      final boolean resultRowHasTimestamp = query.getResultRowHasTimestamp();
      final int resultRowDimensionStart = query.getResultRowDimensionStart();
      final int resultRowAggregatorStart = query.getResultRowAggregatorStart();

      return new CloseableGrouperIterator<>(
          /**
           * 此处的迭代器，
           * 是后续next时真正调用的迭代器
           */
          vectorGrouper.iterator(),

          /**
           * 传参entry是聚合的结果，
           * 也就是上面vectorGrouper.iterator()的查询结果，
           *
           * 本函数的目的是获得查询的dimensions结果，
           * 然后和传参的聚合结果一起封装成新的ResultRow对象并返回。
           */
          entry -> {
            final ResultRow resultRow = ResultRow.create(query.getResultRowSizeWithoutPostAggregators());

            // 此时resultRow中内容为null

            // Add timestamp, if necessary.
            if (resultRowHasTimestamp) {
              resultRow.set(0, timestamp.getMillis());
            }

            // 此时resultRow中内容为null

            // Add dimensions.
            /**
             * 添加dimensions
             */
            int keyOffset = 0;
            for (int i = 0; i < selectors.size(); i++) {
              final GroupByVectorColumnSelector selector = selectors.get(i);
              selector.writeKeyToResultRow(
                  entry.getKey(),
                  keyOffset,
                  resultRow,
                  resultRowDimensionStart + i
              );

              keyOffset += selector.getGroupingKeySize();
            }
            // 此时resultRow中已经包含了该行的dimensions和所聚合的时间

            // Convert dimension values to desired output types, possibly.
            GroupByQueryEngineV2.convertRowTypesToOutputTypes(
                query.getDimensions(),
                resultRow,
                resultRowDimensionStart
            );

            // 此时resultRow中还是只有该行的dimensions和所聚合的时间

            // Add aggregations.
            /**
             * entry是查询出来的聚合结果，
             * 其getValues是个数组，其中包含了每行的聚合结果。
             * 此处将每行的聚合结果，和该行的dimension进行了合并
             */
            for (int i = 0; i < entry.getValues().length; i++) {
              resultRow.set(resultRowAggregatorStart + i, entry.getValues()[i]);
            }

            // 此时resultRow中额外还加入了该行的聚合结果

            return resultRow;
          },
          () -> {} // Grouper will be closed when VectorGroupByEngineIterator is closed.
      );
    }
  }
}
