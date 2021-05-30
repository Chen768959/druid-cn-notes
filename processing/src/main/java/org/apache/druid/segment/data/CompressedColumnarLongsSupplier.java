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
import org.apache.druid.io.Channels;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.serde.MetaSerdeHelper;
import org.apache.druid.segment.serde.Serializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 */
public class CompressedColumnarLongsSupplier implements Supplier<ColumnarLongs>, Serializer
{
  public static final byte LZF_VERSION = 0x1;
  public static final byte VERSION = 0x2;

  private static final MetaSerdeHelper<CompressedColumnarLongsSupplier> META_SERDE_HELPER = MetaSerdeHelper
      .firstWriteByte((CompressedColumnarLongsSupplier x) -> VERSION)
      .writeInt(x -> x.totalSize)
      .writeInt(x -> x.sizePer)
      .maybeWriteByte(
          x -> x.encoding != CompressionFactory.LEGACY_LONG_ENCODING_FORMAT,
          x -> CompressionFactory.setEncodingFlag(x.compression.getId())
      )
      .writeByte(x -> {
        if (x.encoding != CompressionFactory.LEGACY_LONG_ENCODING_FORMAT) {
          return x.encoding.getId();
        } else {
          return x.compression.getId();
        }
      });

  private final int totalSize;
  private final int sizePer;
  private final ByteBuffer buffer;
  private final Supplier<ColumnarLongs> supplier;
  private final CompressionStrategy compression;
  private final CompressionFactory.LongEncodingFormat encoding;

  /**
   *
   * @param totalSize 头部第一个int
   * @param sizePer 头部第二个int
   * @param buffer 其中包含了指定列的所有值，目前头部的size和压缩id等已读完
   * @param supplier 工厂对象，可创建<ColumnarLongs>，该工厂对象为：{@link BlockLayoutColumnarLongsSupplier}
   * @param compression 压缩策略
   * @param encoding 编码
   *
   * @return
   */
  CompressedColumnarLongsSupplier(
      int totalSize,
      int sizePer,
      ByteBuffer buffer,
      Supplier<ColumnarLongs> supplier,
      CompressionStrategy compression,
      CompressionFactory.LongEncodingFormat encoding
  )
  {
    this.totalSize = totalSize;
    this.sizePer = sizePer;
    this.buffer = buffer;
    this.supplier = supplier;
    this.compression = compression;
    this.encoding = encoding;
  }

  @Override
  public ColumnarLongs get()
  {
    return supplier.get();
  }

  @Override
  public long getSerializedSize()
  {
    return META_SERDE_HELPER.size(this) + (long) buffer.remaining();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    META_SERDE_HELPER.writeTo(channel, this);
    Channels.writeFully(channel, buffer.asReadOnlyBuffer());
  }

  /**
   * @param buffer bytebuffer，其中包含了指定列的所有值
   * @param order 该列排序格式。
   *
   * 该fromByteBuffer()方法中创建一个匿名函数包装类{@link BlockLayoutColumnarLongsSupplier#BlockLayoutColumnarLongsSupplier(int, int, ByteBuffer, ByteOrder, CompressionFactory.LongEncodingReader, CompressionStrategy)}
   * 该对象中包含了{@link GenericIndexed}，其是该列信息的包装类，后续聚合查询也是通过此对象查询指定列的各行信息。
   * 该匿名函数包装类对象的get()方法提供ColumnarLongs类型数据，也就是其对应的“列”的信息对象
   *
   * 最后将{@link BlockLayoutColumnarLongsSupplier}装入{@link CompressedColumnarLongsSupplier}并返回
   */
  public static CompressedColumnarLongsSupplier fromByteBuffer(ByteBuffer buffer, ByteOrder order)
  {
    // 第一位为version信息
    byte versionFromBuffer = buffer.get();

    if (versionFromBuffer == LZF_VERSION || versionFromBuffer == VERSION) {
      final int totalSize = buffer.getInt();
      final int sizePer = buffer.getInt();

      // 压缩策略，默认CompressionStrategy.LZF
      CompressionStrategy compression = CompressionStrategy.LZF;

      CompressionFactory.LongEncodingFormat encoding = CompressionFactory.LEGACY_LONG_ENCODING_FORMAT;
      if (versionFromBuffer == VERSION) {
        byte compressionId = buffer.get();
        if (CompressionFactory.hasEncodingFlag(compressionId)) {
          encoding = CompressionFactory.LongEncodingFormat.forId(buffer.get());
          compressionId = CompressionFactory.clearEncodingFlag(compressionId);
        }
        // 指定压缩策略
        compression = CompressionStrategy.forId(compressionId);
      }

      /**
       * 返回：匿名函数包装类{@link BlockLayoutColumnarLongsSupplier}
       * 该对象中包含了{@link GenericIndexed}，其是该列信息的包装类，后续聚合查询也是通过此对象查询指定列的各行信息。
       * 该匿名函数包装类对象的get()方法提供ColumnarLongs类型数据，也就是其对应的“列”的信息对象
       */
      Supplier<ColumnarLongs> supplier = CompressionFactory.getLongSupplier(
          totalSize,
          sizePer,
          buffer.asReadOnlyBuffer(),
          order,
          encoding,
          compression
      );

      // 还是一个简单工厂，其内部又包裹了supplier工厂
      return new CompressedColumnarLongsSupplier(
          totalSize,
          sizePer,
          buffer,
          supplier,
          compression,
          encoding
      );
    }

    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }
}
