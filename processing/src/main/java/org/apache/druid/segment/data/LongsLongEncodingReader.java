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

import org.apache.druid.java.util.common.logger.Logger;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

public class LongsLongEncodingReader implements CompressionFactory.LongEncodingReader
{
  private static final Logger log = new Logger(CompressionFactory.class);

  private LongBuffer buffer;

  public LongsLongEncodingReader(ByteBuffer fromBuffer, ByteOrder order)
  {
    log.info("!!!：reader中buffer出处1");
    this.buffer = fromBuffer.asReadOnlyBuffer().order(order).asLongBuffer();
  }

  private LongsLongEncodingReader(LongBuffer buffer)
  {
    log.info("!!!：reader中buffer出处2");
    this.buffer = buffer;
  }

  @Override
  public void setBuffer(ByteBuffer buffer)
  {
    log.info("!!!：reader中buffer出处3");
    this.buffer = buffer.asLongBuffer();
  }

  @Override
  public long read(int index)
  {
    return buffer.get(buffer.position() + index);
  }

  @Override
  public void read(final long[] out, final int outPosition, final int startIndex, final int length)
  {
    log.info("!!!调用LongsLongEncodingReader.read");
    final int oldPosition = buffer.position();
    try {
      buffer.position(oldPosition + startIndex);
      buffer.get(out, outPosition, length);
    }
    finally {
      buffer.position(oldPosition);
    }
  }

  @Override
  public CompressionFactory.LongEncodingReader duplicate()
  {
    return new LongsLongEncodingReader(buffer.duplicate());
  }
}
