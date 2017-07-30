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

package com.linkedin.pinot.core.operator.docvaliterators;

import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.io.readerwriter.RealtimeIndexOffHeapMemoryManager;
import com.linkedin.pinot.core.io.readerwriter.impl.FixedByteSingleColumnSingleValueReaderWriter;
import com.linkedin.pinot.core.io.writer.impl.DirectMemoryManager;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;


public class RealtimeSingleValueIteratorTest {
  private static final int NUM_ROWS = 1000;
  private int[] _intVals = new int[NUM_ROWS];
  private long[] _longVals = new long[NUM_ROWS];
  private float[] _floatVals = new float[NUM_ROWS];
  private double[] _doubleVals = new double[NUM_ROWS];
  FixedByteSingleColumnSingleValueReaderWriter _intReader;
  FixedByteSingleColumnSingleValueReaderWriter _longReader;
  FixedByteSingleColumnSingleValueReaderWriter _floatReader;
  FixedByteSingleColumnSingleValueReaderWriter _doubleReader;
  private Random _random;
  private long _seed;
  private RealtimeIndexOffHeapMemoryManager _memoryManager;

  @AfterSuite
  public void tearDown() throws Exception {
    _memoryManager.close();
  }

  @BeforeClass
  public void initData() {
    _memoryManager = new DirectMemoryManager(RealtimeSingleValueIteratorTest.class.getName());
    final long _seed = new Random().nextLong();
    _random = new Random(_seed);
    _intReader = new FixedByteSingleColumnSingleValueReaderWriter(_random.nextInt(NUM_ROWS)+1, V1Constants.Numbers.INTEGER_SIZE,
        _memoryManager, "intReader");
    _longReader = new FixedByteSingleColumnSingleValueReaderWriter(_random.nextInt(NUM_ROWS)+1, V1Constants.Numbers.LONG_SIZE,
        _memoryManager, "longReader");
    _floatReader = new FixedByteSingleColumnSingleValueReaderWriter(_random.nextInt(NUM_ROWS)+1, V1Constants.Numbers.FLOAT_SIZE,
        _memoryManager, "floatReader");
    _doubleReader = new FixedByteSingleColumnSingleValueReaderWriter(_random.nextInt(NUM_ROWS)+1, V1Constants.Numbers.DOUBLE_SIZE,
        _memoryManager, "doubleReader");

    for (int i = 0; i < NUM_ROWS; i++) {
      _intVals[i] = _random.nextInt();
      _intReader.setInt(i, _intVals[i]);
      _longVals[i]  = _random.nextLong();
      _longReader.setLong(i, _longVals[i]);
      _floatVals[i] = _random.nextFloat();
      _floatReader.setFloat(i, _floatVals[i]);
      _doubleVals[i] = _random.nextDouble();
      _doubleReader.setDouble(i, _doubleVals[i]);
    }
  }


  @Test
  public void testIntReader() throws Exception {
    try {
      RealtimeSingleValueIterator iterator = new RealtimeSingleValueIterator(_intReader, NUM_ROWS, FieldSpec.DataType.INT);
      // Test all values
      iterator.reset();
      for (int i = 0; i < NUM_ROWS; i++) {
        Assert.assertEquals(iterator.nextIntVal(), _intVals[i], " at row " + i);
      }
      Assert.assertEquals(iterator.nextIntVal(), Constants.EOF);

      final int startDocId = _random.nextInt(NUM_ROWS);
      iterator.skipTo(startDocId);
      for (int i = startDocId; i < NUM_ROWS; i++) {
        Assert.assertEquals(iterator.nextIntVal(), _intVals[i], " at row " + i);
      }
      Assert.assertEquals(iterator.nextIntVal(), Constants.EOF);
    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Failed with seed " + _seed);
    }
  }

  @Test
  public void testLongReader() throws Exception {
    try {
      RealtimeSingleValueIterator iterator = new RealtimeSingleValueIterator(_longReader, NUM_ROWS, FieldSpec.DataType.LONG);
      // Test all values
      iterator.reset();
      for (int i = 0; i < NUM_ROWS; i++) {
        Assert.assertEquals(iterator.nextLongVal(), _longVals[i], " at row " + i);
      }
      Assert.assertEquals(iterator.nextLongVal(), (long)Constants.EOF);

      final int startDocId = _random.nextInt(NUM_ROWS);
      iterator.skipTo(startDocId);
      for (int i = startDocId; i < NUM_ROWS; i++) {
        Assert.assertEquals(iterator.nextLongVal(), _longVals[i], " at row " + i);
      }
      Assert.assertEquals(iterator.nextLongVal(), (long)Constants.EOF);
    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Failed with seed " + _seed);
    }
  }

  @Test
  public void testFloatReader() throws Exception {
    try {
      RealtimeSingleValueIterator iterator = new RealtimeSingleValueIterator(_floatReader, NUM_ROWS, FieldSpec.DataType.FLOAT);
      // Test all values
      iterator.reset();
      for (int i = 0; i < NUM_ROWS; i++) {
        Assert.assertEquals(iterator.nextFloatVal(), _floatVals[i], " at row " + i);
      }
      Assert.assertEquals(iterator.nextFloatVal(), (float) Constants.EOF);

      final int startDocId = _random.nextInt(NUM_ROWS);
      iterator.skipTo(startDocId);
      for (int i = startDocId; i < NUM_ROWS; i++) {
        Assert.assertEquals(iterator.nextFloatVal(), _floatVals[i], " at row " + i);
      }
      Assert.assertEquals(iterator.nextFloatVal(), (float) Constants.EOF);
    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Failed with seed " + _seed);
    }
  }

    @Test
    public void testDoubleReader() throws Exception {
      try {
        RealtimeSingleValueIterator iterator = new RealtimeSingleValueIterator(_doubleReader, NUM_ROWS, FieldSpec.DataType.DOUBLE);
        // Test all values
        iterator.reset();
        for (int i = 0; i < NUM_ROWS; i++) {
          Assert.assertEquals(iterator.nextDoubleVal(), _doubleVals[i], " at row " + i);
        }
        Assert.assertEquals(iterator.nextDoubleVal(), (double)Constants.EOF);

        final int startDocId = _random.nextInt(NUM_ROWS);
        iterator.skipTo(startDocId);
        for (int i = startDocId; i < NUM_ROWS; i++) {
          Assert.assertEquals(iterator.nextDoubleVal(), _doubleVals[i], " at row " + i);
        }
        Assert.assertEquals(iterator.nextDoubleVal(), (double)Constants.EOF);
      } catch (Throwable t) {
        t.printStackTrace();
        Assert.fail("Failed with seed " + _seed);
      }
  }
}
