/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.startree;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.utils.Pairs.IntPair;

import xerial.larray.mmap.MMapBuffer;
import xerial.larray.mmap.MMapMode;


public class StarTreeDataTableNew {
  private static final Logger LOGGER = LoggerFactory.getLogger(StarTreeDataTableNew.class);

  private File file;
  private int dimensionSizeInBytes;
  private int metricSizeInBytes;
  private int totalSizeInBytes;
 final MMapBuffer mappedByteBuffer;
  
  public StarTreeDataTableNew(File file, int dimensionSizeInBytes, int metricSizeInBytes) throws Exception{
    this.file = file;
    this.dimensionSizeInBytes = dimensionSizeInBytes;
    this.metricSizeInBytes = metricSizeInBytes;
    this.totalSizeInBytes = dimensionSizeInBytes + metricSizeInBytes;
    mappedByteBuffer = new MMapBuffer(file, 0, file.length(), MMapMode.READ_WRITE);

  }

  /**
   * 
   * @param startRecordId inclusive
   * @param endRecordId exclusive
   */
  public void sort(int startRecordId, int endRecordId, final int[] sortOrder) {
    sort(startRecordId, endRecordId, 0, dimensionSizeInBytes, sortOrder);
  }

  public void sort(int startRecordId, int endRecordId, final int startOffsetInRecord, final int endOffsetInRecord, final int[] sortOrder) {
    long start = System.currentTimeMillis();

    try {
      int length = endRecordId - startRecordId;
      if (length == 1) {
        return;
      }
      final int startOffset = startRecordId * totalSizeInBytes;
      
      List<Integer> idList = new ArrayList<Integer>();
      for (int i = startRecordId; i < endRecordId; i++) {
        idList.add(i - startRecordId);
      }
      Comparator<Integer> comparator = new Comparator<Integer>() {
        byte[] buf1 = new byte[dimensionSizeInBytes];
        byte[] buf2 = new byte[dimensionSizeInBytes];

        @Override
        public int compare(Integer o1, Integer o2) {
          int pos1 = (o1) * totalSizeInBytes;
          int pos2 = (o2) * totalSizeInBytes;
          //System.out.println("pos1="+ pos1 +" , pos2="+ pos2);
          mappedByteBuffer.copyTo(startOffset + pos1, buf1, 0, dimensionSizeInBytes);
          mappedByteBuffer.copyTo(startOffset + pos2, buf2, 0, dimensionSizeInBytes);
          IntBuffer bb1 = ByteBuffer.wrap(buf1).asIntBuffer();
          IntBuffer bb2 = ByteBuffer.wrap(buf2).asIntBuffer();
          for (int dimIndex : sortOrder) {
            int v1 = bb1.get(dimIndex);
            int v2 = bb2.get(dimIndex);
            if (v1 != v2) {
              return v1 - v2;
            }
          }
          return 0;
        }
      };
      Collections.sort(idList, comparator);
      //System.out.println("AFter sorting:" + idList);
      int[] currentPositions = new int[length];
      int[] indexToRecordIdMapping = new int[length];
      byte[] buf1 = new byte[totalSizeInBytes];
      byte[] buf2 = new byte[totalSizeInBytes];

      for (int i = 0; i < length; i++) {
        currentPositions[i] = i;
        indexToRecordIdMapping[i] = i;
      }
      for (int i = 0; i < length; i++) {
        int thisRecordId = indexToRecordIdMapping[i];
        int thisRecordIdPos = currentPositions[thisRecordId];

        int thatRecordId = idList.get(i);
        int thatRecordIdPos = currentPositions[thatRecordId];

        //swap the buffers
        mappedByteBuffer.copyTo(startOffset + thisRecordIdPos * totalSizeInBytes, buf1, 0, totalSizeInBytes);
        mappedByteBuffer.copyTo(startOffset + thatRecordIdPos * totalSizeInBytes, buf2, 0, totalSizeInBytes);
        //        mappedByteBuffer.position(thisRecordIdPos * totalSizeInBytes);
        //        mappedByteBuffer.get(buf1);
        //        mappedByteBuffer.position(thatRecordIdPos * totalSizeInBytes);
        //        mappedByteBuffer.get(buf2);
        mappedByteBuffer.readFrom(buf2, 0, startOffset + thisRecordIdPos * totalSizeInBytes, totalSizeInBytes);
        mappedByteBuffer.readFrom(buf1, 0, startOffset + thatRecordIdPos * totalSizeInBytes, totalSizeInBytes);
        //        mappedByteBuffer.position(thisRecordIdPos * totalSizeInBytes);
        //        mappedByteBuffer.put(buf2);
        //        mappedByteBuffer.position(thatRecordIdPos * totalSizeInBytes);
        //        mappedByteBuffer.put(buf1);
        //update the positions
        indexToRecordIdMapping[i] = thatRecordId;
        indexToRecordIdMapping[thatRecordIdPos] = thisRecordId;

        currentPositions[thatRecordId] = i;
        currentPositions[thisRecordId] = thatRecordIdPos;
      }
      
    } catch (Exception e) {
      LOGGER.error("Exception while sorting", e);
    } finally {
      //      IOUtils.closeQuietly(randomAccessFile);
    }
    long end = System.currentTimeMillis();
    LOGGER.info("Sorted {} docs from {} to {} in {} ms", endRecordId -startRecordId, startRecordId, endRecordId, (end-start));    
  }

  /**
   * 
   * @param startDocId inclusive
   * @param endDocId exclusive
   * @param colIndex
   * @return start,end for each value. inclusive start, exclusive end
   */
  public Map<Integer, IntPair> groupByIntColumnCount(int startDocId, int endDocId, Integer colIndex) {
    try {
      int length = endDocId - startDocId;
      Map<Integer, IntPair> rangeMap = new LinkedHashMap<>();
      final int startOffset = startDocId * totalSizeInBytes;
      int prevValue = -1;
      int prevStart = 0;
      byte[] dimBuff = new byte[dimensionSizeInBytes];
      for (int i = 0; i < length; i++) {
        mappedByteBuffer.copyTo(startOffset + i * totalSizeInBytes, dimBuff, 0, dimensionSizeInBytes);
        int value = ByteBuffer.wrap(dimBuff).asIntBuffer().get(colIndex);
        if (prevValue != -1 && prevValue != value) {
          rangeMap.put(prevValue, new IntPair(startDocId + prevStart, startDocId + i));
          prevStart = i;
        }
        prevValue = value;
      }
      rangeMap.put(prevValue, new IntPair(startDocId + prevStart, endDocId));
      return rangeMap;
    } catch (Exception e) {
      e.printStackTrace();
    } 
    return Collections.emptyMap();
  }
  public void flush() {
    mappedByteBuffer.flush();    
  }
  public void close() {
    mappedByteBuffer.flush();    
    try {
      mappedByteBuffer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
