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
        Map.of("partitionKey1", "partitionVal1", "partitionKey2", "partitionVal2"),
        Map.of("clusterKey1", "clusterVal1", "clusterKey2", "clusterVal2", "normalField1",
            "normalVal1", "normalField2",
            5));/* Partition key and clustering keys should be automatically set*/
    assertEquals(
        "Column Family: 'testTable'\nPartition Key: 'partitionVal1:partitionVal2'\n=>"
        + "(column='clusterVal1:clusterVal2:normalField1', value='normalVal1')\n=>"
        + "(column='clusterVal1:clusterVal2:normalField2', value='5')\n",
        data.toString());
  }

  @Test
  public void updateIfExistsTest() {
    ColumnDefinition definition = new ColumnDefinition(List.of("pKey"), List.of());
    ColumnFamily data = new ColumnFamily("testTable", definition);
    assertFalse(
        data.updateIfExists(Map.of("pKey", "pVal"), Map.of("colKey1", "colVal1")));
    data.update(Map.of("pKey", "pVal"), Map.of("colKey1", "colVal1"));
    assertTrue(data.updateIfExists(Map.of("pKey", "pVal"), Map.of("colKey2", "colVal2")));
  }

  @Test
  public void selectOneTest() {
    ColumnDefinition definition = new ColumnDefinition(List.of("pKey"), List.of("clusterKey"));
    ColumnFamily data = new ColumnFamily("testTable", definition);
    Map<String, Object> keys = Map.of("pKey", "pVal");
    data.update(keys, Map.of("clusterKey", "clusterVal", "colKey1", "colVal1"));
    data.update(keys, Map.of("clusterKey", "clusterVal", "colKey2", "colVal2"));
    data.update(keys, Map.of("clusterKey", "clusterVal", "colKey3", "colVal3"));
    data.update(keys, Map.of("clusterKey", "clusterVal", "colKey4", "colVal4"));
    data.update(keys, Map.of("clusterKey", "clusterVal", "colKey5", "colVal5"));
    assertEquals("colVal3", data.selectOne(keys, Map.of("clusterKey", "clusterVal", "colKey3", "")));
  }

  @Test
  public void selectRangeTest() {
    ColumnDefinition definition = new ColumnDefinition(List.of("pKey"), List.of(
        "metric",
        "interval_start"));
    ColumnFamily data = new ColumnFamily("testTable", definition);
    Map<String, Object> keys = Map.of("pKey", "pVal");
    data.update(keys, Map.of("metric", "day_total", "interval_start", "1535846400", "value", 1));
    data.update(keys, Map.of("metric", "day_total", "interval_start", "1535932800", "value", 1));
    data.update(keys, Map.of("metric", "day_total", "interval_start", "1536364800", "value", 1));
    data.update(keys, Map.of("metric", "day_total", "interval_start", "1536451200", "value", 1));
    data.update(keys, Map.of("metric", "day_total", "interval_start", "1536537600", "value", 1));
    data.update(keys, Map.of("metric", "day_total", "interval_start", "1536710400", "value", 1));
    data.update(keys, Map.of("metric", "day_total", "interval_start", "1537142400", "value", 1));
    data.update(keys, Map.of("metric", "day_total", "interval_start", "1537488000", "value", 1));
    data.update(keys, Map.of("metric", "day_total", "interval_start", "1537574400", "value", 1));
    data.update(keys, Map.of("metric", "day_total", "interval_start", "1537660800", "value", 5));
    data.update(keys, Map.of("metric", "day_total", "interval_start", "1538092800", "value", 1));
    assertEquals(
        "{day_total:1535846400:value=1, day_total:1535932800:value=1, "
        + "day_total:1536364800:value=1, day_total:1536451200:value=1, "
        + "day_total:1536537600:value=1, day_total:1536710400:value=1, "
        + "day_total:1537142400:value=1, day_total:1537488000:value=1, "
        + "day_total:1537574400:value=1, day_total:1537660800:value=5, "
        + "day_total:1538092800:value=1}",
        data.selectRange(keys, Map.of("metric", "day_total", "interval_start", "-1"),
            Map.of("metric", "day_total", "interval_start", "∞")).toString());
  }

}
