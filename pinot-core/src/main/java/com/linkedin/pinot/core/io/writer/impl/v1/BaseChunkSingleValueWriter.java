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
package com.linkedin.pinot.core.io.writer.impl.v1;

import com.linkedin.pinot.core.io.compression.ChunkCompressor;
import com.linkedin.pinot.core.io.writer.SingleColumnSingleValueWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Abstract implementation for {@link SingleColumnSingleValueWriter}
 * Base class for fixed and variable byte writer implementations.
 */
public abstract class BaseChunkSingleValueWriter implements SingleColumnSingleValueWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseChunkSingleValueWriter.class);

  protected static final int INT_SIZE = Integer.SIZE / Byte.SIZE;
  protected static final int LONG_SIZE = Long.SIZE / Byte.SIZE;
  protected static final int FLOAT_SIZE = Float.SIZE / Byte.SIZE;
  protected static final int DOUBLE_SIZE = Double.SIZE / Byte.SIZE;

  protected final FileChannel _dataFile;
  protected final ByteBuffer _header;
  protected final ByteBuffer _chunkBuffer;
  protected final ByteBuffer _compressedBuffer;
  protected final ChunkCompressor _chunkCompressor;

  protected int _chunkSize;
  protected int _dataOffset;

  /**
   * Constructor for the class.
   *
   * @param file Data file to write into
   * @param compressor Data compressor
   * @param totalDocs Total docs to write
   * @param numDocsPerChunk Number of docs per data chunk
   * @param chunkSize Size of chunk
   * @param sizeOfEntry Size of entry (in bytes), max size for variable byte implementation.
   * @param version Version of file
   * @throws FileNotFoundException
   */
  protected BaseChunkSingleValueWriter(File file, ChunkCompressor compressor, int totalDocs, int numDocsPerChunk,
      int chunkSize, int sizeOfEntry, int version)
      throws FileNotFoundException {
    _chunkSize = chunkSize;
    _chunkCompressor = compressor;

    int numChunks = (totalDocs + numDocsPerChunk - 1) / numDocsPerChunk;
    int headerSize = (numChunks + 4) * INT_SIZE; // 4 items written before chunk indexing.

    _header = ByteBuffer.allocateDirect(headerSize);
    _header.putInt(version);
    _header.putInt(numChunks);
    _header.putInt(numDocsPerChunk);
    _header.putInt(sizeOfEntry);
    _dataOffset = headerSize;

    _chunkBuffer = ByteBuffer.allocateDirect(chunkSize);
    _compressedBuffer = ByteBuffer.allocateDirect(chunkSize * 2);
    _dataFile = new RandomAccessFile(file, "rw").getChannel();
  }

  @Override
  public void setChar(int row, char ch) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setInt(int row, int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setShort(int row, short s) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setLong(int row, long l) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setFloat(int row, float f) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setDouble(int row, double d) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setString(int row, String string) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBytes(int row, byte[] bytes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close()
      throws IOException {

    // Write the chunk if it is non-empty.
    if (_chunkBuffer.position() > 0) {
      writeChunk();
    }

    // Write the header and close the file.
    _header.flip();
    _dataFile.write(_header, 0);
    _dataFile.close();
  }

  /**
   * Helper method to compress and write the current chunk.
   * <ul>
   *   <li> Chunk header is of fixed size, so fills out any remaining offsets for partially filled chunks. </li>
   *   <li> Compresses and writes the chunk to the data file. </li>
   *   <li> Updates the header with the current chunks offset. </li>
   *   <li> Clears up the buffers, so that they can be reused. </li>
   * </ul>
   *
   */
  protected void writeChunk() {
    _chunkBuffer.flip();
    _compressedBuffer.flip();

    int compressedSize;
    try {
      compressedSize = _chunkCompressor.compress(_chunkBuffer, _compressedBuffer);
      _dataFile.write(_compressedBuffer, _dataOffset);
    } catch (IOException e) {
      LOGGER.error("Exception caught while compressing/writing data chunk", e);
      throw new RuntimeException(e);
    }

    _header.putInt(_dataOffset);
    _dataOffset += compressedSize;

    _chunkBuffer.clear();
    _compressedBuffer.clear();
  }
}
