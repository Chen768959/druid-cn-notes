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

package org.apache.druid.query.aggregation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Ints;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.epinephelinae.vector.VectorGroupByEngine;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.CompressedPools;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.data.BlockLayoutColumnarLongsSupplier;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ColumnPartSerde;
import org.apache.druid.segment.serde.DictionaryEncodedColumnPartSerde;
import org.apache.druid.segment.serde.LongNumericColumnPartSerde;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A class that helps query engines use Buffer- or VectorAggregators in a consistent way.
 *
 * The two main benefits this class provides are:
 *
 * (1) Query engines can treat BufferAggregators and VectorAggregators the same for operations that are equivalent
 * across them, like "init", "get", "relocate", and "close".
 * (2) Query engines are freed from the need to manage how much space each individual aggregator needs. They only
 * need to allocate a block of size "spaceNeeded".
 */
public class AggregatorAdapters implements Closeable
{
  private static final Logger log = new Logger(AggregatorAdapters.class);

  private final List<Adapter> adapters;
  private final List<AggregatorFactory> factories;
  private final int[] aggregatorPositions;
  private final int spaceNeeded;

  private AggregatorAdapters(final List<Adapter> adapters)
  {
    this.adapters = adapters;
    this.factories = adapters.stream().map(Adapter::getFactory).collect(Collectors.toList());
    this.aggregatorPositions = new int[adapters.size()];

    long nextPosition = 0;
    for (int i = 0; i < adapters.size(); i++) {
      final AggregatorFactory aggregatorFactory = adapters.get(i).getFactory();
      aggregatorPositions[i] = Ints.checkedCast(nextPosition);
      nextPosition += aggregatorFactory.getMaxIntermediateSizeWithNulls();
    }

    // 安全的将long类型的nextPosition转化为int类型
    /**
     * checkedCast(long value) ：
     * 如果可能，返回等于value的int值。
     * 参数：value - int类型范围内的任何值
     * 返回：int值等于value
     * throw：IllegalArgumentException - 如果value大于Integer.MAX_VALUE或小于Integer.MIN_VALUE
     */
    this.spaceNeeded = Ints.checkedCast(nextPosition);
  }

  /**
   * Create an adapters object based on {@link VectorAggregator}.
   */
  public static AggregatorAdapters factorizeVector(
      final VectorColumnSelectorFactory columnSelectorFactory,
      final List<AggregatorFactory> aggregatorFactories
  )
  {
    final Adapter[] adapters = new Adapter[aggregatorFactories.size()];
    for (int i = 0; i < aggregatorFactories.size(); i++) {
      final AggregatorFactory aggregatorFactory = aggregatorFactories.get(i);
      adapters[i] = new VectorAggregatorAdapter(
          aggregatorFactory,
          aggregatorFactory.factorizeVector(columnSelectorFactory)
      );
    }

    return new AggregatorAdapters(Arrays.asList(adapters));
  }

  /**
   * Create an adapters object based on {@link BufferAggregator}.
   */
  public static AggregatorAdapters factorizeBuffered(
      final ColumnSelectorFactory columnSelectorFactory,
      final List<AggregatorFactory> aggregatorFactories
  )
  {
    final Adapter[] adapters = new Adapter[aggregatorFactories.size()];
    for (int i = 0; i < aggregatorFactories.size(); i++) {
      final AggregatorFactory aggregatorFactory = aggregatorFactories.get(i);
      adapters[i] = new BufferAggregatorAdapter(
          aggregatorFactory,
          aggregatorFactory.factorizeBuffered(columnSelectorFactory)
      );
    }

    return new AggregatorAdapters(Arrays.asList(adapters));
  }

  /**
   * Return the amount of buffer bytes needed by all aggregators wrapped up in this object.
   */
  public int spaceNeeded()
  {
    return spaceNeeded;
  }

  /**
   * Return the {@link AggregatorFactory} objects that were used to create this object.
   */
  public List<AggregatorFactory> factories()
  {
    return factories;
  }

  /**
   * Return the individual positions of each aggregator within a hypothetical buffer of size {@link #spaceNeeded()}.
   */
  public int[] aggregatorPositions()
  {
    return aggregatorPositions;
  }

  /**
   * Return the number of aggregators in this object.
   */
  public int size()
  {
    return adapters.size();
  }

  /**
   * Initialize all aggregators.
   *
   * @param buf      aggregation buffer
   * @param position position in buffer where our block of size {@link #spaceNeeded()} starts
   */
  public void init(final ByteBuffer buf, final int position)
  {
    for (int i = 0; i < adapters.size(); i++) {
      adapters.get(i).init(buf, position + aggregatorPositions[i]);
    }
  }

  /**
   * Call {@link BufferAggregator#aggregate(ByteBuffer, int)} on all of our aggregators.
   *
   * This method is only valid if the underlying aggregators are {@link BufferAggregator}.
   */
  public void aggregateBuffered(final ByteBuffer buf, final int position)
  {
    for (int i = 0; i < adapters.size(); i++) {
      final Adapter adapter = adapters.get(i);
      adapter.asBufferAggregator().aggregate(buf, position + aggregatorPositions[i]);
    }
  }

  /**
   * Call {@link VectorAggregator#aggregate(ByteBuffer, int, int, int)} on all of our aggregators.
   *
   * This method is only valid if the underlying aggregators are {@link VectorAggregator}.
   */
  public void aggregateVector(
      final ByteBuffer buf,
      final int position,
      final int start,
      final int end
  )
  {
    for (int i = 0; i < adapters.size(); i++) {
      final Adapter adapter = adapters.get(i);
      adapter.asVectorAggregator().aggregate(buf, position + aggregatorPositions[i], start, end);
    }
  }

  /**
   * Call {@link VectorAggregator#aggregate(ByteBuffer, int, int[], int[], int)} on all of our aggregators.
   * This method is only valid if the underlying aggregators are {@link VectorAggregator}.
   *
   * @param buf
   * @param numRows 此次聚合要处理的行数
   * @param positions 数组中存储了“每行所有聚合值的位置”，其i对应的是“第几行”，第i位的值就是“第i个所有聚合值处于内存空间的偏移量位置”
   * @param rows 从0开始的数组，其中装了一个长度为“numRows”的从0开始的递增数列，比如numRows为3，那数组就是[0,1,2]
   */
  public void aggregateVector(
      final ByteBuffer buf,
      final int numRows,
      final int[] positions,
      @Nullable final int[] rows
  )
  {
    /**
     *
     * 循环调用多个adapter
     *
     * 一次查询首先通过了{@link org.apache.druid.query.groupby.epinephelinae.vector.VectorGroupByEngine#process(GroupByQuery, StorageAdapter, ByteBuffer, DateTime, Filter, Interval, GroupByQueryConfig)}
     * 得到了{@link org.apache.druid.java.util.common.guava.Sequence}，
     * 其中内部匿名函数make()方法可创建{@link VectorGroupByEngine.VectorGroupByEngineIterator}，
     * 也是在此时，根据各种传参，VectorGroupByEngineIterator创建了此次查询所对应的AggregatorAdapters，
     * 也就是此处的{@link this#adapters}
     *
     * 一次聚合查询所用的adapter都来自adapters，
     * 每个聚合器又属于各adapter，
     * 每个聚合器创建时会放入对应selector
     * selector属于{@link org.apache.druid.segment.data.ColumnarLongs}，
     * 相当于selector构建了ColumnarLongs和聚合器的一一对应关系。
     * ColumnarLongs就是节点启动时加载segment文件时的产物
     *
     * todo 再捋一遍查询逻辑，理清
     * todo {@link org.apache.druid.query.groupby.epinephelinae.vector.VectorGroupByEngine#process(GroupByQuery, StorageAdapter, ByteBuffer, DateTime, Filter, Interval, GroupByQueryConfig)}
     * todo 之前的调用逻辑，因为其创建VectorGroupByEngineIterator以及查询所对应的AggregatorAdapters所用的信息都是此次调用的传参，
     * todo 可以说是此次调用决定了此次查询使用那些启动时已成功加载的{@link org.apache.druid.segment.data.ColumnarLongs}
     */
    for (int i = 0; i < adapters.size(); i++) {
      final Adapter adapter = adapters.get(i);
      /**
       * buf在此处被填充，
       * 每一次循环都是往buffer中putLong
       *
       * adapter：{@link VectorAggregatorAdapter}
       * adapter.asVectorAggregator().aggregate()：
       * {@link org.apache.druid.query.aggregation.LongSumVectorAggregator#aggregate(ByteBuffer, int, int[], int[], int)}
       * 其中主要是将vector（long[]）中的数据按一定算法填充进buffer中。
       * 所以主要关注vector数组如何生成：final long[] vector = selector.getLongVector();
       *
       * vector由{@link org.apache.druid.segment.data.ColumnarLongs#makeVectorValueSelector}中的getLongVector()方法创建
       * 而getLongVector()中又通过以下方法为vector数组赋值
       * {@link org.apache.druid.segment.data.BlockLayoutColumnarLongsSupplier.BlockLayoutColumnarLongs#get(long[], int, int)}
       * （该方法第一个long[]参数就是vector数组）
       * 此BlockLayoutColumnarLongs#get方法中：
       * 又从一个“buffer”中读取数据到vector数组中，所以这个“buffer”才是一切数据的来源，
       *
       * 该buffer的产生逻辑为：
       * {@link BlockLayoutColumnarLongsSupplier.BlockLayoutColumnarLongs#loadBuffer(int)}的子类，以下为获取语句
       * holder = singleThreadedLongBuffers.get(bufferNum);
       * buffer = holder.get();
       * 其中singleThreadedLongBuffers.get(bufferNum)为：
       * {@link GenericIndexed.BufferIndexed#singleThreadedVersionOne()}.get(final int index)
       * （holder的由来：
       * {@link CompressedPools.LITTLE_ENDIAN_BYTE_BUF_POOL}StupidPool队列的take()方法，从中取了一个holder，buffer就在其中，
       * 不过此holder和buffer都是空的）
       *
       * 真正的数据来自
       * {@link GenericIndexed.BufferIndexed#singleThreadedVersionOne()}.get(final int index)
       * 方法中的copyBuffer
       * 每一个copyBuffer中都包含了各种查询的结果数据，或者数据源中的各种数据
       * 每一个copyBuffer都存在于一个对应的GenericIndexed对象，
       * 在创建GenericIndexed对象时
       * {@link GenericIndexed#GenericIndexed(ByteBuffer, ObjectStrategy, boolean)}
       * 就会将copyBuffer作为参数传入其中。
       *
       * 可以理解为在启动历史节点时，就会初始化创建很多GenericIndexed对象，然后每个GenericIndexed对象中的copyBuffer包含了各种查询信息。
       * GenericIndexed构建流程1：
       * {@link org.apache.druid.segment.IndexIO.V9IndexLoader#load(File, ObjectMapper, boolean)}
       * {@link org.apache.druid.segment.IndexIO.V9IndexLoader#deserializeColumn(ObjectMapper, ByteBuffer, SmooshedFileMapper)}
       * {@link org.apache.druid.segment.column.ColumnDescriptor#read(ByteBuffer, ColumnConfig, SmooshedFileMapper)}
       * {@link LongNumericColumnPartSerde#getDeserializer()}
       * {@link org.apache.druid.segment.data.CompressedColumnarLongsSupplier#fromByteBuffer(ByteBuffer, ByteOrder)}
       * {@link CompressionFactory#getLongSupplier(int, int, ByteBuffer, ByteOrder, CompressionFactory.LongEncodingFormat, CompressionStrategy)}
       * {@link BlockLayoutColumnarLongsSupplier#BlockLayoutColumnarLongsSupplier(int, int, ByteBuffer, ByteOrder, CompressionFactory.LongEncodingReader, CompressionStrategy)}
       * {@link GenericIndexed#read(ByteBuffer, ObjectStrategy)}
       * {@link GenericIndexed#createGenericIndexedVersionOne(ByteBuffer, ObjectStrategy)}
       * {@link GenericIndexed#GenericIndexed(ByteBuffer, ObjectStrategy, boolean)} //创建GenericIndexed对象
       *
       * GenericIndexed构建流程2：
       * {@link org.apache.druid.segment.IndexIO.V9IndexLoader#load(File, ObjectMapper, boolean)}
       * {@link org.apache.druid.segment.IndexIO.V9IndexLoader#deserializeColumn(ObjectMapper, ByteBuffer, SmooshedFileMapper)}
       * {@link org.apache.druid.segment.column.ColumnDescriptor#read(ByteBuffer, ColumnConfig, SmooshedFileMapper)}
       * {@link DictionaryEncodedColumnPartSerde#getDeserializer()}.readSingleValuedColumn(VERSION version, ByteBuffer buffer)
       * {@link org.apache.druid.segment.data.CompressedVSizeColumnarIntsSupplier#fromByteBuffer(ByteBuffer, ByteOrder)}
       * {@link GenericIndexed#read(ByteBuffer, ObjectStrategy)}
       * {@link GenericIndexed#createGenericIndexedVersionOne(ByteBuffer, ObjectStrategy)}
       * {@link GenericIndexed#GenericIndexed(ByteBuffer, ObjectStrategy, boolean)} //创建GenericIndexed对象
       *
       * GenericIndexed构建流程3：
       * {@link org.apache.druid.segment.IndexIO.V9IndexLoader#load(File, ObjectMapper, boolean)}
       * {@link GenericIndexed#read(ByteBuffer, ObjectStrategy, SmooshedFileMapper)}// GenericIndexed.read2 13、14进入
       * {@link GenericIndexed#createGenericIndexedVersionOne(ByteBuffer, ObjectStrategy)}
       * {@link GenericIndexed#GenericIndexed(ByteBuffer, ObjectStrategy, boolean)} //创建GenericIndexed对象
       *
       * GenericIndexed构建流程4：
       * {@link org.apache.druid.segment.IndexIO.V9IndexLoader#load(File, ObjectMapper, boolean)}
       * {@link org.apache.druid.segment.IndexIO.V9IndexLoader#deserializeColumn(ObjectMapper, ByteBuffer, SmooshedFileMapper)}
       * {@link org.apache.druid.segment.column.ColumnDescriptor#read(ByteBuffer, ColumnConfig, SmooshedFileMapper)}
       * {@link DictionaryEncodedColumnPartSerde#getDeserializer()}.read(ByteBuffer buffer, ObjectStrategy<T> strategy, SmooshedFileMapper fileMapper)
       * {@link GenericIndexed#read(ByteBuffer, ObjectStrategy, SmooshedFileMapper)}// GenericIndexed.read2 17、18进入
       * {@link GenericIndexed#createGenericIndexedVersionOne(ByteBuffer, ObjectStrategy)}
       * {@link GenericIndexed#GenericIndexed(ByteBuffer, ObjectStrategy, boolean)} //创建GenericIndexed对象
       *
       * 由此可见，所有历史节点启动时创建的GenericIndexed，都由
       * {@link org.apache.druid.segment.IndexIO.V9IndexLoader#load(File, ObjectMapper, boolean)}
       * 创建得来
       * 其请求路径
       * druid-server:
       * SegmentLoadDropHandler.start()// 历史节点启动时作为handler被调用start方法
       * ·····
       * SegmentLoaderLocalCacheManager.getSegment(DataSegment segment, boolean lazy)
       * druid-processing:
       * {@link org.apache.druid.segment.loading.MMappedQueryableSegmentizerFactory#factorize(DataSegment, File, boolean)}
       * {@link org.apache.druid.segment.IndexIO#loadIndex(File, boolean)}
       * {@link org.apache.druid.segment.IndexIO.V9IndexLoader#load(File, ObjectMapper, boolean)}
       *
       * 在历史节点启动时，SegmentLoadDropHandler的start方法会被Lifecycle调用，由此开始加载segment缓存文件
       * SegmentLoadDropHandler.start()
       *
       * 所以整个逻辑可看成：
       * “buf”中包含了整个查询的各种结果值，
       * 此时每一次循环都是为了将一部分查询结果值放入“buf”中，
       * 而这查询结果其底层是StupidPool队列返回的holder中的数据
       */
      log.info("!!!：aggregateVector，adapter类型："+adapter.getClass()+"...VectorAggregator类型："+adapter.asVectorAggregator().getClass());

      /**
       * adapter.asVectorAggregator()获取“聚合器”
       * 聚合器本身不储存数据，只是提供各种聚合方法，我们以long类型的sum聚合器为例
       * {@link LongSumVectorAggregator}
       *
       * 其aggregate()方法就是具体聚合计算方法
       * {@link LongSumVectorAggregator#aggregate(ByteBuffer, int, int[], int[], int)}
       *
       * 聚合器中有一个selector对象：是ColumnarLongs的内部类，其可提供外部对象ColumnarLongs所对应一个列的所有数据
       *
       * selector由{@link org.apache.druid.segment.data.ColumnarLongs#makeVectorValueSelector}创建，
       * 该匿名对象属于{@link org.apache.druid.segment.data.ColumnarLongs}中，
       * 也就是说是通过ColumnarLongs接口对象调用的makeVectorValueSelector()方法创建的此selector，
       * （一个ColumnarLongs对应一个列的所有数据）
       *
       * 而selector是在创建此聚合器时被传入的。
       * 由此可见selector与某个聚合器对象是一一绑定关系，
       * 而此处逻辑可看出：
       * “adapter对应一个聚合器，也对应一个selector，也对应一个{@link org.apache.druid.segment.data.ColumnarLongs}及其内部的某列的所有值”
       */
      adapter.asVectorAggregator().aggregate(buf, numRows, positions, rows, aggregatorPositions[i]);
    }
  }

  /**
   * Retrieve aggregation state from one of our aggregators.
   *
   * @param buf              aggregation buffer
   * @param position         position in buffer where our block of size {@link #spaceNeeded()} starts
   * @param aggregatorNumber which aggregator to retrieve state, from 0 to {@link #size()} - 1
   */
  @Nullable
  public Object get(final ByteBuffer buf, final int position, final int aggregatorNumber)
  {
    Adapter adapter = adapters.get(aggregatorNumber);
    log.info("!!!：AggregatorAdapters中获取聚合器类型为："+adapter.getClass());
    return adapter.get(buf, position + aggregatorPositions[aggregatorNumber]);
  }

  /**
   * Inform all of our aggregators that they are being relocated.
   */
  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    for (int i = 0; i < adapters.size(); i++) {
      adapters.get(i).relocate(
          oldPosition + aggregatorPositions[i],
          newPosition + aggregatorPositions[i],
          oldBuffer,
          newBuffer
      );
    }
  }

  /**
   * Close all of our aggregators.
   */
  @Override
  public void close()
  {
    for (Adapter adapter : adapters) {
      try {
        adapter.close();
      }
      catch (Exception e) {
        log.warn(e, "Could not close aggregator [%s], skipping.", adapter.getFactory().getName());
      }
    }
  }

  /**
   * The interface that allows this class to achieve its goals of partially unifying handling of
   * BufferAggregator and VectorAggregator. Private, since it doesn't escape this class and the
   * only two implementations are private static classes below.
   */
  private interface Adapter extends Closeable
  {
    void init(ByteBuffer buf, int position);

    @Nullable
    Object get(ByteBuffer buf, int position);

    void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer);

    @Override
    void close();

    AggregatorFactory getFactory();

    BufferAggregator asBufferAggregator();

    VectorAggregator asVectorAggregator();
  }

  private static class VectorAggregatorAdapter implements Adapter
  {
    private final AggregatorFactory factory;
    private final VectorAggregator aggregator;

    VectorAggregatorAdapter(final AggregatorFactory factory, final VectorAggregator aggregator)
    {
      this.factory = factory;
      this.aggregator = aggregator;
    }

    @Override
    public void init(final ByteBuffer buf, final int position)
    {
      aggregator.init(buf, position);
    }

    @Override
    public Object get(final ByteBuffer buf, final int position)
    {
      log.info("!!!：VectorAggregatorAdapter内部聚合器为："+aggregator.getClass());
      return aggregator.get(buf, position);
    }

    @Override
    public void close()
    {
      aggregator.close();
    }

    @Override
    public void relocate(
        final int oldPosition,
        final int newPosition,
        final ByteBuffer oldBuffer,
        final ByteBuffer newBuffer
    )
    {
      aggregator.relocate(oldPosition, newPosition, oldBuffer, newBuffer);
    }

    @Override
    public AggregatorFactory getFactory()
    {
      return factory;
    }

    @Override
    public BufferAggregator asBufferAggregator()
    {
      throw new ISE("Not a BufferAggregator!");
    }

    @Override
    public VectorAggregator asVectorAggregator()
    {
      return aggregator;
    }
  }

  private static class BufferAggregatorAdapter implements Adapter
  {
    private final AggregatorFactory factory;
    private final BufferAggregator aggregator;

    BufferAggregatorAdapter(final AggregatorFactory factory, final BufferAggregator aggregator)
    {
      this.factory = factory;
      this.aggregator = aggregator;
    }

    @Override
    public void init(final ByteBuffer buf, final int position)
    {
      aggregator.init(buf, position);
    }

    @Override
    public Object get(final ByteBuffer buf, final int position)
    {
      log.info("!!!：BufferAggregatorAdapter内部聚合器为："+aggregator.getClass());
      return aggregator.get(buf, position);
    }

    @Override
    public void close()
    {
      aggregator.close();
    }

    @Override
    public void relocate(
        final int oldPosition,
        final int newPosition,
        final ByteBuffer oldBuffer,
        final ByteBuffer newBuffer
    )
    {
      aggregator.relocate(oldPosition, newPosition, oldBuffer, newBuffer);
    }

    @Override
    public AggregatorFactory getFactory()
    {
      return factory;
    }

    @Override
    public BufferAggregator asBufferAggregator()
    {
      return aggregator;
    }

    @Override
    public VectorAggregator asVectorAggregator()
    {
      throw new ISE("Not a VectorAggregator!");
    }
  }
}
