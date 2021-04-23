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

import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.collections.StupidPool;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.CompressedPools;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class DecompressingByteBufferObjectStrategy implements ObjectStrategy<ResourceHolder<ByteBuffer>>
{
  private static final Logger log = new Logger(DecompressingByteBufferObjectStrategy.class);

  private final ByteOrder order;
  private final CompressionStrategy.Decompressor decompressor;

  DecompressingByteBufferObjectStrategy(ByteOrder order, CompressionStrategy compression)
  {
    this.order = order;
    this.decompressor = compression.getDecompressor();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Class<ResourceHolder<ByteBuffer>> getClazz()
  {
    return (Class) ResourceHolder.class;
  }

  @Override
  public ResourceHolder<ByteBuffer> fromByteBuffer(ByteBuffer buffer, int numBytes)
  {
    log.info("!!!：DecompressingByteBufferObjectStrategy.fromByteBuffer，准备获取holder");
    /**
     * 从StupidPool队列（LITTLE_ENDIAN_BYTE_BUF_POOL）中取出一个holder，
     * 第一次取时holder中的buffer是空的
     */
    final ResourceHolder<ByteBuffer> bufHolder = CompressedPools.getByteBuf(order);

    /**
     * bufHolder:{@link org.apache.druid.collections.StupidPool.ObjectResourceHolder}
     * bufHolder是StupidPool中的一个“容器对象”
     * 其get方法只是将内部的buffer属性拿出来而已，
     * 所以真正的ByteBuffer是在创建bufHolder对象时作为参数传进去的，
     *
     * CompressedPools.getByteBuf(order);
     * 就是创建一个StupidPool池{@link CompressedPools.LITTLE_ENDIAN_BYTE_BUF_POOL}，用于返回bufHolder包装类。
     *
     * 每次创建StupidPool对象时，都要传入一个
     *
     * CompressedPools.getByteBuf(order)创建好StupidPool后，调用此方法获取ByteBuffer的包装类
     * {@link StupidPool#take()}
     *
     */
    final ByteBuffer buf = bufHolder.get();

    // 将position移到初始位置，且limit被设置成capacity，相当于还原了buffer的初始属性，但其中的数据没有清除
    buf.clear();

    log.info("!!!：fromByteBuffer取出holder");

    // 将buffer中的数据传给buf（第一次调用时buf为新建holder是空的，但是buffer中已经有了所有行的时间戳）
    decompressor.decompress(buffer, numBytes, buf);

    // Needed, because if e. g. if this compressed buffer contains 3-byte integers, it should be possible to getInt()
    // from the buffer, including padding. See CompressedVSizeColumnarIntsSupplier.bufferPadding().
    //由于上面的解压缩操作，重新设置
    buf.limit(buf.capacity());

    return new ResourceHolder<ByteBuffer>()
    {
      @Override
      public ByteBuffer get()
      {
        log.info("!!!：调用DecompressingByteBufferObjectStrategy中匿名ResourceHolder.get()");
        return buf;
      }

      @Override
      public void close()
      {
        bufHolder.close();
      }
    };
  }

  @Override
  public int compare(ResourceHolder<ByteBuffer> o1, ResourceHolder<ByteBuffer> o2)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] toBytes(ResourceHolder<ByteBuffer> holder)
  {
    throw new UnsupportedOperationException();
  }
}
