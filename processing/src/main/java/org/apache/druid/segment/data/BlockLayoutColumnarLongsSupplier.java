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

package org.apache.druid.segment.data;

import com.google.common.base.Supplier;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.CompressedPools;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

public class BlockLayoutColumnarLongsSupplier implements Supplier<ColumnarLongs>
{
  private final GenericIndexed<ResourceHolder<ByteBuffer>> baseLongBuffers;

  // The number of rows in this column.
  private final int totalSize;

  // The number of longs per buffer.
  private final int sizePer;
  private final CompressionFactory.LongEncodingReader baseReader;

  public BlockLayoutColumnarLongsSupplier(
      int totalSize,
      int sizePer,
      ByteBuffer fromBuffer,
      ByteOrder order,
      CompressionFactory.LongEncodingReader reader,
      CompressionStrategy strategy
  )
  {
    baseLongBuffers = GenericIndexed.read(fromBuffer, new DecompressingByteBufferObjectStrategy(order, strategy));
    this.totalSize = totalSize;
    this.sizePer = sizePer;
    this.baseReader = reader;
  }

  @Override
  public ColumnarLongs get()
  {
    final int div = Integer.numberOfTrailingZeros(sizePer);
    final int rem = sizePer - 1;
    final boolean isPowerOf2 = sizePer == (1 << div);
    if (isPowerOf2) {
      // this provide slightly better performance than calling the LongsEncodingReader.read, probably because Java
      // doesn't inline the method call for some reason. This should be removed when test show that performance
      // of using read method is same as directly accessing the longbuffer
      if (baseReader instanceof LongsLongEncodingReader) {
        return new BlockLayoutColumnarLongs()
        {
          @Override
          public long get(int index)
          {
            /**
             * 请求第一次到达此逻辑，
             * 在下面loadBuffer逻辑中
             */
            log.info("!!!：调用BlockLayoutColumnarLongsSupplier中if成立get");
            // optimize division and remainder for powers of 2
            final int bufferNum = index >> div;

            if (bufferNum != currBufferNum) {
              loadBuffer(bufferNum);
            }

            final int bufferIndex = index & rem;
            return longBuffer.get(bufferIndex);
          }

          @Override
          protected void loadBuffer(int bufferNum)
          {
            //此处的close(holder)会调用holder.close，然后会尝试将holder装回StupidPool
            CloseQuietly.close(holder);
            /**
             * singleThreadedLongBuffers：{@link org.apache.druid.segment.data.GenericIndexed}$2
             *
             * 最终由此方法创建holder：
             * {@link org.apache.druid.segment.data.GenericIndexed.BufferIndexed#bufferedIndexedGet(ByteBuffer, int, int)}
             * |->{@link DecompressingByteBufferObjectStrategy#fromByteBuffer(ByteBuffer, int)}
             *
             * holder是一个容器，下面的get方法只是将holder中的对象（即buffer）拿出来而已。
             * holder全部都是从StupidPool队列中取出来的（队列为空的话则创建），
             * 此处用到的StupidPool队列就是{@link CompressedPools.LITTLE_ENDIAN_BYTE_BUF_POOL}
             * 这个buffer就是创建holder时放进去的。
             * 注意：
             * 虽然buffer是创建holder时放进去的，可创建holder时，放进去的是一个空buffer，
             * 而此处用来读取查询结果并传给vector数组的buffer明显不是空的，
             * 所以StupidPool中存在一个逻辑：“创建一个空buffer的holder后，将查询结果存入此buffer，然后又将此holder放回StupidPool队列”
             * （存在这个逻辑：此StupidPool初始化时不会创建任何holder，然后第一次调用take()会创建并返回出一个holder，
             * “然后调用holder.close时，又会将此holder放回StupidPool，然后下一次take时，就会直接拿取此holder”）
             */
            log.info("!!!：singleThreadedLongBuffers类型："+singleThreadedLongBuffers.getClass());
            holder = singleThreadedLongBuffers.get(bufferNum);
            log.info("!!!：loadBuffer中holder类型："+holder.getClass());
            buffer = holder.get();
            // asLongBuffer() makes the longBuffer's position = 0
            longBuffer = buffer.asLongBuffer();
            // 将buffer传入reader，后续将buffer中的数据读取到out数组中
            reader.setBuffer(buffer);
            currBufferNum = bufferNum;
          }
        };
      } else {
        return new BlockLayoutColumnarLongs()
        {
          @Override
          public long get(int index)
          {
            log.info("!!!：调用BlockLayoutColumnarLongsSupplier中if不成立get");
            // optimize division and remainder for powers of 2
            final int bufferNum = index >> div;

            if (bufferNum != currBufferNum) {
              loadBuffer(bufferNum);
            }

            final int bufferIndex = index & rem;
            return reader.read(bufferIndex);
          }
        };
      }

    } else {
      return new BlockLayoutColumnarLongs();
    }
  }

  private class BlockLayoutColumnarLongs implements ColumnarLongs
  {
    final CompressionFactory.LongEncodingReader reader = baseReader.duplicate();
    final Indexed<ResourceHolder<ByteBuffer>> singleThreadedLongBuffers = baseLongBuffers.singleThreaded();

    int currBufferNum = -1;
    ResourceHolder<ByteBuffer> holder;
    ByteBuffer buffer;
    /**
     * longBuffer's position must be 0
     */
    LongBuffer longBuffer;

    @Override
    public int size()
    {
      return totalSize;
    }

    @Override
    public long get(int index)
    {
      log.info("!!!：调用BlockLayoutColumnarLongs中get");
      // division + remainder is optimized by the compiler so keep those together
      final int bufferNum = index / sizePer;
      final int bufferIndex = index % sizePer;

      if (bufferNum != currBufferNum) {
        loadBuffer(bufferNum);
      }

      return reader.read(bufferIndex);
    }

    /**
     * 当前批次为连续时间范围
     */
    @Override
    public void get(final long[] out, final int start, final int length)
    {
      /**
       * out为long类型数组，也是我们一个adapter要查询的结果，
       * 可能是行记录的时间戳，也可能是long类型列中的每行记录
       */
      log.info("!!!：进入BlockLayoutColumnarLongsSupplier.get(final long[] out, final int start, final int length)");
      // division + remainder is optimized by the compiler so keep those together
      int bufferNum = start / sizePer;
      int bufferIndex = start % sizePer;

      int p = 0;

      while (p < length) {
        if (bufferNum != currBufferNum) {
          /**
           * 加载buffer
           * 后续out中的内容，就是从此buffer中读取的
           *
           * 此处的buffer由以下逻辑获得：
           * holder = singleThreadedLongBuffers.get(bufferNum);
           * buffer = holder.get();
           */
          loadBuffer(bufferNum);
        }

        final int limit = Math.min(length - p, sizePer - bufferIndex);
        log.info("!!!：read连续时间范围，p="+p+"...bufferIndex="+bufferIndex+"...limit="+limit);
        /**
         * 之前的loadBuffer(bufferNum);方法将buffer获取到后并装入此reader对象，
         * 此处的read就是将buffer中的内容读取到out数组中
         */
        reader.read(out, p, bufferIndex, limit);
        p += limit;
        bufferNum++;
        bufferIndex = 0;
      }
    }

    /**
     * 当前批次不为连续范围
     */
    @Override
    public void get(final long[] out, final int[] indexes, final int length)
    {
      int p = 0;

      while (p < length) {
        int bufferNum = indexes[p] / sizePer;
        if (bufferNum != currBufferNum) {
          loadBuffer(bufferNum);
        }

        final int numRead = reader.read(out, p, indexes, length - p, bufferNum * sizePer, sizePer);
        assert numRead > 0;
        p += numRead;
      }
    }

    protected void loadBuffer(int bufferNum)
    {
      CloseQuietly.close(holder);
      holder = singleThreadedLongBuffers.get(bufferNum);
      buffer = holder.get();
      currBufferNum = bufferNum;
      reader.setBuffer(buffer);
    }

    @Override
    public void close()
    {
      if (holder != null) {
        holder.close();
      }
    }

    @Override
    public String toString()
    {
      return "BlockCompressedColumnarLongs_Anonymous{" +
             "currBufferNum=" + currBufferNum +
             ", sizePer=" + sizePer +
             ", numChunks=" + singleThreadedLongBuffers.size() +
             ", totalSize=" + totalSize +
             '}';
    }
  }
}
