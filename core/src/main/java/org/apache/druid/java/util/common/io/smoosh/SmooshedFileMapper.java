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

package org.apache.druid.java.util.common.io.smoosh;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.apache.druid.java.util.common.ByteBufferUtils;
import org.apache.druid.java.util.common.ISE;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Class that works in conjunction with FileSmoosher.  This class knows how to map in a set of files smooshed
 * by the FileSmoosher.
 */
public class SmooshedFileMapper implements Closeable
{
  private static final Interner<String> STRING_INTERNER = Interners.newWeakInterner();

  public static SmooshedFileMapper load(File baseDir) throws IOException
  {
    // meta.smoosh文件
    File metaFile = FileSmoosher.metaFile(baseDir);

    // 读取meta.smoosh文件，转化成SmooshedFileMapper对象返回
    BufferedReader in = null;
    try {
      in = new BufferedReader(new InputStreamReader(new FileInputStream(metaFile), StandardCharsets.UTF_8));

      String line = in.readLine();
      if (line == null) {
        throw new ISE("First line should be version,maxChunkSize,numChunks, got null.");
      }

      // 每行按逗号分隔，
      // meta.smoosh文件的每行都是固定格式，先是列名(也可能是index.drd和metadata.drd)，
      // 然后就是该信息对应的文件中位置始末偏移量
      String[] splits = line.split(",");
      // 第一行肯定是v1,2147483647,1
      if (!"v1".equals(splits[0])) {
        throw new ISE("Unknown version[%s], v1 is all I know.", splits[0]);
      }
      if (splits.length != 3) {
        throw new ISE("Wrong number of splits[%d] in line[%s]", splits.length, line);
      }
      // splits[2]表示当前文件夹下的smooth文件数
      final Integer numFiles = Integer.valueOf(splits[2]);
      // 由此可见outFiles集合中装了当前文件夹下的每个smooth文件对象
      List<File> outFiles = Lists.newArrayListWithExpectedSize(numFiles);

      // 填充所有smooth文件对象
      for (int i = 0; i < numFiles; ++i) {
        outFiles.add(FileSmoosher.makeChunkFile(baseDir, i));
      }

      Map<String, Metadata> internalFiles = new TreeMap<>();
      // 遍历meta.smoosh文件剩余行
      while ((line = in.readLine()) != null) {
        splits = line.split(",");

        if (splits.length != 4) {
          throw new ISE("Wrong number of splits[%d] in line[%s]", splits.length, line);
        }
        // key是列名(也可能是"index.drd"或"metadata.drd")
        // value是一个Metadata数据，其实就是存放对应始末偏移量的一个包装对象
        internalFiles.put(
            STRING_INTERNER.intern(splits[0]),
            new Metadata(Integer.parseInt(splits[1]), Integer.parseInt(splits[2]), Integer.parseInt(splits[3]))
        );
      }

      // 所以最后返回的SmooshedFileMapper对象中包含了：
      // outFiles：包含了meta.smoosh同文件夹下的所有smoosh文件
      // internalFiles为meta.smoosh内的所有数据信息
      return new SmooshedFileMapper(outFiles, internalFiles);
    }
    finally {
      Closeables.close(in, false);
    }
  }

  // outFiles：包含了meta.smoosh同文件夹下的所有smoosh文件
  private final List<File> outFiles;
  // internalFiles为meta.smoosh内的所有数据信息
  private final Map<String, Metadata> internalFiles;
  private final List<MappedByteBuffer> buffersList = new ArrayList<>();

  private SmooshedFileMapper(
      List<File> outFiles,
      Map<String, Metadata> internalFiles
  )
  {
    this.outFiles = outFiles;
    this.internalFiles = internalFiles;
  }

  public Set<String> getInternalFilenames()
  {
    return internalFiles.keySet();
  }

  /**
   * Returns a mapped buffer of the smooshed file with the given name. Buffer's contents from 0 to capacity() are the
   * whole mapped file contents, limit() is equal to capacity().
   *
   * 该方法作用：
   * 从某个smoosh文件中，找name名所指的信息，并以ByteBuffer的形式返回
   *
   * name是某个smoosh文件中的一个“字段名”，如某个列名、index.drd、metadata.drd等，其目的就是找到smoosh文件中这部分数据的具体内容
   * 该name所在的smoosh文件在该方法中都会利用内存映射技术被映射成bytebuffer对象，然后存放于{@link this#buffersList}
   * （已经映射过的smoosh文件不会重复映射）
   */
  public ByteBuffer mapFile(String name) throws IOException
  {
    // internalFiles为meta.smoosh内的所有数据信息，此处metadata是指定name在xxxx.smoosh中的始末偏移量
    final Metadata metadata = internalFiles.get(name);
    if (metadata == null) {
      return null;
    }

    // 获取name信息所属第几个文件
    final int fileNum = metadata.getFileNum();
    // buffersList中的index下标，映射的就是第几个文件（0开始）
    while (buffersList.size() <= fileNum) {
      buffersList.add(null);
    }

    // 根据name信息所属文件编号，找到buffersList中对应对象，新加载name信息，自然对应为null
    MappedByteBuffer mappedBuffer = buffersList.get(fileNum);
    if (mappedBuffer == null) {
      /**
       * 此处通过内存映射技术，
       * 将第fileNum位xxxx.smoosh文件“完全”映射到内存上，
       * （实际调用的是jdk中的MappedByteBuffer，会受到文件大小不能超过2G的限制，所以smoosh文件不能超过2G）
       */
      mappedBuffer = Files.map(outFiles.get(fileNum));
      // 填充回buffersList，下次无需重复映射该smoosh文件
      buffersList.set(fileNum, mappedBuffer);
    }

    // 创建个和mappedBuffer一样的内存缓冲区，对新缓冲区的任何操作，都会同样反映在原缓冲区上
    ByteBuffer retVal = mappedBuffer.duplicate();

    /**
     * 将整个smoosh文件的读取始末位置定位成“name名”所指数据的始末位置，
     * 接下来slice就直接单把这部分数据“切”下来，并返回
     */
    retVal.position(metadata.getStartOffset()).limit(metadata.getEndOffset());
    return retVal.slice();
  }

  @Override
  public void close()
  {
    Throwable thrown = null;
    for (MappedByteBuffer mappedByteBuffer : buffersList) {
      if (mappedByteBuffer == null) {
        continue;
      }
      try {
        ByteBufferUtils.unmap(mappedByteBuffer);
      }
      catch (Throwable t) {
        if (thrown == null) {
          thrown = t;
        } else {
          thrown.addSuppressed(t);
        }
      }
    }
    buffersList.clear();
    if (thrown != null) {
      throw new RuntimeException(thrown);
    }
  }
}
