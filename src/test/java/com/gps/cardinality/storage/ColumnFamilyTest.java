/**
 * Copyright (c) 2018 George Stathis <gstathis@gmail.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package com.gps.cardinality.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * @author gstathis
 * Created on: 2018-11-04
 */
public class ColumnFamilyTest {

  @Test
  public void updateTest() {
    List<String> primaryKeys = List.of("partitionKey1", "partitionKey2");
    List<String> clusteringKeys = List.of("clusterKey1", "clusterKey2");
    ColumnDefinition definition = new ColumnDefinition(primaryKeys, clusteringKeys);
    ColumnFamily data = new ColumnFamily("testTable", definition);
    // Insert separate keys and values
    data.update(
        Map.of("partitionKey1", "partitionVal1",
            "partitionKey2", "partitionVal2",
            "clusterKey1", "clusterVal1",
            "clusterKey2", "clusterVal2"),
        Map.of("normalField1", "normalVal1",
            "normalField2", 5));
    // Partition key and clustering keys should be automatically set
    assertEquals("Column Family: 'testTable'\n"
                 + "Partition Key: 'partitionVal1:partitionVal2'\n"
                 + "=>(column='clusterVal1:clusterVal2:normalField1', value='normalVal1')\n"
                 + "=>(column='clusterVal1:clusterVal2:normalField2', value='5')\n"
        , data.toString());
  }

  @Test
  public void updateIfExistsTest() {
    ColumnDefinition definition = new ColumnDefinition(List.of("pKey"), List.of());
    ColumnFamily data = new ColumnFamily("testTable", definition);
    assertFalse(data.updateIfExists(
        Map.of("pKey", "pVal"),
        Map.of("colKey1", "colVal1")));
    data.update(Map.of("pKey", "pVal"), Map.of("colKey1", "colVal1"));
    assertTrue(data.updateIfExists(
        Map.of("pKey", "pVal"),
        Map.of("colKey2", "colVal2")));
  }

  @Test
  public void selectOneTest() {
    ColumnDefinition definition = new ColumnDefinition(List.of("pKey"), List.of("clusterKey"));
    ColumnFamily data = new ColumnFamily("testTable", definition);
    Map<String, Object> keys = Map.of("pKey", "pVal", "clusterKey", "clusterVal");
    data.update(keys, Map.of("colKey1", "colVal1"));
    data.update(keys, Map.of("colKey2", "colVal2"));
    data.update(keys, Map.of("colKey3", "colVal3"));
    data.update(keys, Map.of("colKey4", "colVal4"));
    data.update(keys, Map.of("colKey5", "colVal5"));
    assertEquals("colVal3", data.selectOne(keys, "colKey3"));
  }

  @Test
  public void selectRangeTest() {
    ColumnDefinition definition = new ColumnDefinition(List.of("pKey"), List.of("clusterKey"));
    ColumnFamily data = new ColumnFamily("testTable", definition);
    Map<String, Object> keys = Map.of("pKey", "pVal", "clusterKey", "clusterVal");
    data.update(keys, Map.of("day_total_1535846400", 1));
    data.update(keys, Map.of("day_total_1535932800", 1));
    data.update(keys, Map.of("day_total_1536364800", 1));
    data.update(keys, Map.of("day_total_1536451200", 1));
    data.update(keys, Map.of("day_total_1536537600", 1));
    data.update(keys, Map.of("day_total_1536710400", 1));
    data.update(keys, Map.of("day_total_1537142400", 1));
    data.update(keys, Map.of("day_total_1537488000", 1));
    data.update(keys, Map.of("day_total_1537574400", 1));
    data.update(keys, Map.of("day_total_1537660800", 5));
    data.update(keys, Map.of("day_total_1538092800", 1));
    data.update(keys, Map.of("day_total_", 0));
    data.update(keys, Map.of("day_total_end", 0));
    assertEquals(
        "{clusterVal:day_total_1535846400=1, "
        + "clusterVal:day_total_1535932800=1, "
        + "clusterVal:day_total_1536364800=1, "
        + "clusterVal:day_total_1536451200=1, "
        + "clusterVal:day_total_1536537600=1, "
        + "clusterVal:day_total_1536710400=1, "
        + "clusterVal:day_total_1537142400=1, "
        + "clusterVal:day_total_1537488000=1, "
        + "clusterVal:day_total_1537574400=1, "
        + "clusterVal:day_total_1537660800=5, "
        + "clusterVal:day_total_1538092800=1}",
        data.selectRange(keys, "day_total_", "day_total_end").toString());
  }

}
