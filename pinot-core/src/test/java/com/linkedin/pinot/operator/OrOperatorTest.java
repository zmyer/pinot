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
package com.linkedin.pinot.operator;

import com.google.common.collect.Lists;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.operator.blocks.BaseFilterBlock;
import com.linkedin.pinot.core.operator.filter.BaseFilterOperator;
import com.linkedin.pinot.core.operator.filter.OrOperator;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import org.apache.commons.lang3.ArrayUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class OrOperatorTest {

  @Test
  public void testIntersectionForTwoLists() {
    int[] list1 = new int[] { 2, 3, 10, 15, 16, 28 };
    int[] list2 = new int[] { 3, 6, 8, 20, 28 };

    List<BaseFilterOperator> operators = new ArrayList<>();
    operators.add(makeFilterOperator(list1));
    operators.add(makeFilterOperator(list2));
    final OrOperator orOperator = new OrOperator(operators);

    orOperator.open();
    Block block;
    TreeSet<Integer> set = new TreeSet<Integer>();
    set.addAll(Lists.newArrayList(ArrayUtils.toObject(list1)));
    set.addAll(Lists.newArrayList(ArrayUtils.toObject(list2)));
    Iterator<Integer> expectedIterator = set.iterator();
    while ((block = orOperator.nextBlock()) != null) {
      final BlockDocIdSet blockDocIdSet = block.getBlockDocIdSet();
      final BlockDocIdIterator iterator = blockDocIdSet.iterator();
      int docId;
      while ((docId = iterator.next()) != Constants.EOF) {
        Assert.assertEquals(expectedIterator.next().intValue(), docId);
      }
    }
    orOperator.close();
  }

  @Test
  public void testIntersectionForThreeLists() {
    int[] list1 = new int[] { 2, 3, 6, 10, 15, 16, 28 };
    int[] list2 = new int[] { 3, 6, 8, 20, 28 };
    int[] list3 = new int[] { 1, 2, 3, 6, 30 };

    List<BaseFilterOperator> operators = new ArrayList<>();
    operators.add(makeFilterOperator(list1));
    operators.add(makeFilterOperator(list2));
    operators.add(makeFilterOperator(list3));

    final OrOperator orOperator = new OrOperator(operators);

    orOperator.open();
    Block block;
    TreeSet<Integer> set = new TreeSet<Integer>();
    set.addAll(Lists.newArrayList(ArrayUtils.toObject(list1)));
    set.addAll(Lists.newArrayList(ArrayUtils.toObject(list2)));
    set.addAll(Lists.newArrayList(ArrayUtils.toObject(list3)));
    Iterator<Integer> expectedIterator = set.iterator();
    while ((block = orOperator.nextBlock()) != null) {
      final BlockDocIdSet blockDocIdSet = block.getBlockDocIdSet();
      final BlockDocIdIterator iterator = blockDocIdSet.iterator();
      int docId;
      while ((docId = iterator.next()) != Constants.EOF) {
        Assert.assertEquals(expectedIterator.next().intValue(), docId);
      }
    }
    orOperator.close();
  }

  @Test
  public void testComplex() {
    int[] list1 = new int[] { 2, 3, 6, 10, 15, 16, 28 };
    int[] list2 = new int[] { 3, 6, 8, 20, 28 };
    int[] list3 = new int[] { 1, 2, 3, 6, 30 };

    TreeSet<Integer> set = new TreeSet<Integer>();
    set.addAll(Lists.newArrayList(ArrayUtils.toObject(list1)));
    set.addAll(Lists.newArrayList(ArrayUtils.toObject(list2)));
    set.addAll(Lists.newArrayList(ArrayUtils.toObject(list3)));
    Iterator<Integer> expectedIterator = set.iterator();

    List<BaseFilterOperator> operators = new ArrayList<>();
    operators.add(makeFilterOperator(list1));
    operators.add(makeFilterOperator(list2));

    final OrOperator orOperator1 = new OrOperator(operators);
    List<BaseFilterOperator> operators1 = new ArrayList<>();
    operators1.add(orOperator1);
    operators1.add(makeFilterOperator(list3));

    final OrOperator orOperator = new OrOperator(operators1);

    orOperator.open();
    BaseFilterBlock block;
    while ((block = orOperator.getNextBlock()) != null) {
      final BlockDocIdSet blockDocIdSet = block.getBlockDocIdSet();
      final BlockDocIdIterator iterator = blockDocIdSet.iterator();
      int docId;
      while ((docId = iterator.next()) != Constants.EOF) {
//        System.out.println(docId);
        Assert.assertEquals(expectedIterator.next().intValue(), docId);
      }
    }
    orOperator.close();
  }

  public BaseFilterOperator makeFilterOperator(final int[] list) {
    return new BaseFilterOperator() {
      public static final String OPERATOR_NAME = "Anonymous";

      @Override
      public String getOperatorName() {
        return OPERATOR_NAME;
      }

      boolean alreadyInvoked = false;

      @Override
      public boolean open() {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public boolean close() {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public BaseFilterBlock nextFilterBlock(BlockId blockId) {

        if (alreadyInvoked) {
          return null;
        }
        alreadyInvoked = true;
        return new ArrayBasedFilterBlock(list);
      }

      @Override
      public boolean isResultEmpty() {
        return false;
      }
    };
  }
}
