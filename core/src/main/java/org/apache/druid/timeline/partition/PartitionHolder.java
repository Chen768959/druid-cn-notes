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

package org.apache.druid.timeline.partition;

import com.google.common.collect.Iterables;
import org.apache.druid.timeline.Overshadowable;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * An object that clumps together multiple other objects which each represent a shard of some space.
 *
 * 一个PartitionHolder，代表了一个时间区间的segment的“所有分片”
 */
public class PartitionHolder<T extends Overshadowable<T>> implements Iterable<PartitionChunk<T>>
{
  // 其中存储了该时间区间segment的所有分片
  private final OvershadowableManager<T> overshadowableManager;

  public static <T extends Overshadowable<T>> PartitionHolder<T> copyWithOnlyVisibleChunks(
      PartitionHolder<T> partitionHolder
  )
  {
    return new PartitionHolder<>(OvershadowableManager.copyVisible(partitionHolder.overshadowableManager));
  }

  public static <T extends Overshadowable<T>> PartitionHolder<T> deepCopy(PartitionHolder<T> partitionHolder)
  {
    return new PartitionHolder<>(OvershadowableManager.deepCopy(partitionHolder.overshadowableManager));
  }

  public PartitionHolder(PartitionChunk<T> initialChunk)
  {
    this.overshadowableManager = new OvershadowableManager<>();
    add(initialChunk);
  }

  public PartitionHolder(List<PartitionChunk<T>> initialChunks)
  {
    this.overshadowableManager = new OvershadowableManager<>();
    for (PartitionChunk<T> chunk : initialChunks) {
      add(chunk);
    }
  }

  protected PartitionHolder(OvershadowableManager<T> overshadowableManager)
  {
    this.overshadowableManager = overshadowableManager;
  }

  public ImmutablePartitionHolder<T> asImmutable()
  {
    return new ImmutablePartitionHolder<>(OvershadowableManager.copyVisible(overshadowableManager));
  }

  // 增加一个分片进来
  public boolean add(PartitionChunk<T> chunk)
  {
    return overshadowableManager.addChunk(chunk);
  }

  @Nullable
  public PartitionChunk<T> remove(PartitionChunk<T> chunk)
  {
    return overshadowableManager.removeChunk(chunk);
  }

  public boolean isEmpty()
  {
    return overshadowableManager.isEmpty();
  }

  public boolean isComplete()
  {
    if (overshadowableManager.isEmpty()) {
      return false;
    }

    Iterator<PartitionChunk<T>> iter = iterator();

    PartitionChunk<T> curr = iter.next();

    if (!curr.isStart()) {
      return false;
    }

    if (curr.isEnd()) {
      return overshadowableManager.isComplete();
    }

    while (iter.hasNext()) {
      PartitionChunk<T> next = iter.next();
      if (!curr.abuts(next)) {
        return false;
      }

      if (next.isEnd()) {
        return overshadowableManager.isComplete();
      }
      curr = next;
    }

    return false;
  }

  public PartitionChunk<T> getChunk(final int partitionNum)
  {
    return overshadowableManager.getChunk(partitionNum);
  }

  @Override
  public Iterator<PartitionChunk<T>> iterator()
  {
    return overshadowableManager.visibleChunksIterator();
  }

  public List<PartitionChunk<T>> getOvershadowed()
  {
    return overshadowableManager.getOvershadowedChunks();
  }

  public Iterable<T> payloads()
  {
    return Iterables.transform(this, PartitionChunk::getObject);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PartitionHolder<?> that = (PartitionHolder<?>) o;
    return Objects.equals(overshadowableManager, that.overshadowableManager);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(overshadowableManager);
  }

  @Override
  public String toString()
  {
    return "PartitionHolder{" +
           "overshadowableManager=" + overshadowableManager +
           '}';
  }
}
