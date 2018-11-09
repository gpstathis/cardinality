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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Cassandra inspired data structure for storing partitions or ordered key/values.
 *
 * @author gstathis
 * Created on: 2018-11-03
 */
public class ColumnFamily {

  private String name;
  private ColumnDefinition columnDefinition;
  private Map<String, ColumnFamilyData> data;

  public ColumnFamily(String name, ColumnDefinition columnDefinition) {
    this.name = name;
    this.columnDefinition = columnDefinition;
    this.data = new HashMap<>();
  }

  /**
   * Updates a single record or creates a new one if none ecists (upsert).
   *
   * @param keys
   *     the partitioning and clustering keys
   * @param data
   *     the data
   */
  public void update(Map<String, Object> keys, Map<String, Object> data) {
    String partitionKey = buildCompositeKey(keys, this.columnDefinition.getCompositeKeys());
    String clusteringKey = buildCompositeKey(keys, this.columnDefinition.getClusteringKeys());
    clusteringKey = clusteringKey.isBlank() ? "" : clusteringKey.concat(":");

    Map<String, Object> prefixedKeyValues = new HashMap<>();
    for (Map.Entry<String, Object> keyValue : data.entrySet()) {
      String newKey = clusteringKey.concat(keyValue.getKey());
      prefixedKeyValues.put(newKey, keyValue.getValue());
    }
    put(partitionKey, prefixedKeyValues);
  }

  /**
   * Updates a single record only if it already exists.
   *
   * @param keys
   *     the partitioning and clustering keys
   * @param data
   *     the data
   * @return true if the record was updated, false if no matching record was found
   */
  public Boolean updateIfExists(Map<String, Object> keys, Map<String, Object> data) {
    String partitionKey = buildCompositeKey(keys, this.columnDefinition.getCompositeKeys());
    if (this.data.containsKey(partitionKey)) {
      update(keys, data);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Selects a single record.
   *
   * @param keys
   *     the partitioning and clustering keys
   * @param column
   *     the data column name
   * @return the column value
   */
  public Object selectOne(Map<String, Object> keys, String column) {
    String partitionKey = buildCompositeKey(keys, this.columnDefinition.getCompositeKeys());
    String clusteringKey = buildCompositeKey(keys, this.columnDefinition.getClusteringKeys());
    clusteringKey = clusteringKey.isBlank() ? "" : clusteringKey.concat(":");
    return data.get(partitionKey).get(clusteringKey.concat(column));
  }

  /**
   * Returns a contiguous range of column rows.
   *
   * @param keys
   *     the partitioning and clustering keys
   * @param fromColumn
   *     the starting column name
   * @param toColumn
   *     the end column name
   * @return the sorted key/values of the matching colunm range.
   */
  public Map<String, Object> selectRange(
      Map<String, Object> keys, String fromColumn,
      String toColumn) {
    String partitionKey = buildCompositeKey(keys, this.columnDefinition.getCompositeKeys());
    String clusteringKey = buildCompositeKey(keys, this.columnDefinition.getClusteringKeys());
    clusteringKey = clusteringKey.isBlank() ? "" : clusteringKey.concat(":");
    return data.get(partitionKey).getRange(clusteringKey.concat(fromColumn),
        false, clusteringKey.concat(toColumn), false);
  }

  /**
   * Composite key builder. For a given composite key such as key1:key2, this method will build
   * the corresponding key string such as value1:value2.
   *
   * @param keyValues
   *     a map of key values
   * @param keys
   *     the key names that should be used to build the composite key, ordered
   * @return a composite key
   */
  private String buildCompositeKey(Map<String, Object> keyValues, List<String> keys) {
    List<String> keyParts = new ArrayList<>();
    for (String key : keys) {
      Object value = keyValues.get(key);
      if (null == value) {
        throw new IllegalArgumentException(String.format("Required key '%s' missing", key));
      }
      keyParts.add(value.toString());
    }
    return String.join(":", keyParts);
  }

  /**
   * Inserts a single record.
   *
   * @param key
   *     the partitioning key
   * @param keyValues
   *     the key/values
   * @return the old values that this operation replaced, or null if none existed before.
   */
  private Map<String, Object> put(String key, Map<String, Object> keyValues) {
    Map<String, Object> oldKeyValues = null;
    ColumnFamilyData columnFamily = data.get(key);
    if (null == columnFamily) {
      columnFamily = new ColumnFamilyData();
      data.put(key, columnFamily);
    } else {
      oldKeyValues = columnFamily.getDataAsMap();
    }
    columnFamily.putAll(keyValues);
    return oldKeyValues;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Column Family: '");
    sb.append(name);
    sb.append("'\n");
    for (Map.Entry<String, ColumnFamilyData> entry : data.entrySet()) {
      sb.append("Partition Key: '");
      sb.append(entry.getKey());
      sb.append("'\n");
      sb.append(entry.getValue());
    }
    return sb.toString();
  }
}
