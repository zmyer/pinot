/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.io.reader.impl.v1;

import com.linkedin.pinot.core.io.reader.impl.ChunkReaderContext;
import com.linkedin.pinot.core.io.writer.impl.v1.FixedByteChunkSingleValueWriter;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * Reader class for data written out by {@link FixedByteChunkSingleValueWriter}.
 * For data layout, please refer to the documentation for {@link FixedByteChunkSingleValueReader}
 *
 */
public class FixedByteChunkSingleValueReader extends BaseChunkSingleValueReader {

  // Thread local (reusable) byte[] to read bytes from data file.
  private final ThreadLocal<byte[]> _reusableBytes = ThreadLocal.withInitial(() -> new byte[_lengthOfLongestEntry]);

  /**
   * Constructor for the class.
   *
   * @param pinotDataBuffer Data buffer to read from
   * @throws IOException
   */
  public FixedByteChunkSingleValueReader(PinotDataBuffer pinotDataBuffer)
      throws IOException {
    super(pinotDataBuffer);
  }

  @Override
  public int getInt(int row) {
    if (!isCompressed()) {
      return getRawData().getInt(row * INT_SIZE);
    } else {
      throw new UnsupportedOperationException("Read without context not supported for compressed data.");
    }
  }

  @Override
  public int getInt(int row, ChunkReaderContext context) {
    assert _lengthOfLongestEntry == INT_SIZE;
    int chunkRowId = row % _numDocsPerChunk;
    ByteBuffer chunkBuffer = getChunkForRow(row, context);
    return chunkBuffer.getInt(chunkRowId * INT_SIZE);
  }

  @Override
  public float getFloat(int row) {
    if (!isCompressed()) {
      return getRawData().getFloat(row * FLOAT_SIZE);
    } else {
      throw new UnsupportedOperationException("Read without context not supported for compressed data.");
    }
  }

  @Override
  public float getFloat(int row, ChunkReaderContext context) {
    assert _lengthOfLongestEntry == FLOAT_SIZE;
    int chunkRowId = row % _numDocsPerChunk;
    ByteBuffer chunkBuffer = getChunkForRow(row, context);
    return chunkBuffer.getFloat(chunkRowId * FLOAT_SIZE);
  }

  @Override
  public long getLong(int row) {
    if (!isCompressed()) {
      return getRawData().getLong(row * LONG_SIZE);
    } else {
      throw new UnsupportedOperationException("Read without context not supported for compressed data.");
    }
  }

  @Override
  public long getLong(int row, ChunkReaderContext context) {
    assert _lengthOfLongestEntry == LONG_SIZE;
    int chunkRowId = row % _numDocsPerChunk;
    ByteBuffer chunkBuffer = getChunkForRow(row, context);
    return chunkBuffer.getLong(chunkRowId * LONG_SIZE);
  }

  @Override
  public double getDouble(int row) {
    if (!isCompressed()) {
      return getRawData().getDouble(row * DOUBLE_SIZE);
    } else {
      throw new UnsupportedOperationException("Read without context not supported for compressed data.");
    }
  }

  @Override
  public double getDouble(int row, ChunkReaderContext context) {
    assert _lengthOfLongestEntry == DOUBLE_SIZE;
    int chunkRowId = row % _numDocsPerChunk;
    ByteBuffer chunkBuffer = getChunkForRow(row, context);
    return chunkBuffer.getDouble(chunkRowId * DOUBLE_SIZE);
  }

  @Override
  public byte[] getBytes(int row) {
    if (!isCompressed()) {
      byte[] bytes = _reusableBytes.get();
      getRawData().copyTo(row * _lengthOfLongestEntry, bytes, 0, _lengthOfLongestEntry);
      return bytes;
    } else {
      throw new UnsupportedOperationException("Read without context not supported for compressed data.");
    }
  }

  @Override
  public byte[] getBytes(int row, ChunkReaderContext context) {
    int chunkRowId = row % _numDocsPerChunk;
    ByteBuffer chunkBuffer = getChunkForRow(row, context);

    byte[] bytes = _reusableBytes.get();
    chunkBuffer.position(chunkRowId * _lengthOfLongestEntry );
    chunkBuffer.get(bytes);
    return bytes;
  }

  @Override
  public ChunkReaderContext createContext() {
    return new ChunkReaderContext(_chunkSize);
  }
}
