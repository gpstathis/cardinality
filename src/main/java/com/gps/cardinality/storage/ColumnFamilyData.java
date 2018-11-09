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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents the data (columns) of a {@link ColumnFamily} as an ordered hash map
 * ({@link ConcurrentSkipListMap}).
 *
 * @author gstathis
 * Created on: 2018-11-03
 */
public class ColumnFamilyData {

  private static final String NON_INTEGER_COUNTER_TYPE
      = "Non integer counter type for counter '%s': %s";
  /**
   * This hash map orders keys by natural ordering and supports extracting contiguous ranges of
   * key/value pairs. It's also thread safe.
   */
  protected ConcurrentSkipListMap<String, Object> data;

  ColumnFamilyData() {
    data = new ConcurrentSkipListMap<>();
  }

  /**
   * Checks whether the specified value is a counter increment for the specified key. If the
   * value is expressed in the form of {@code counter_column_name + | - counter_offset}, the
   * method will extract the increment (or decrement) value as well as the increment (or
   * decrement) operator (i.e. "+" or "-").
   *
   * @param key
   *     the counter key name
   * @param value
   *     the increment/decrement expression
   * @return a {@link CounterMatch} representing the match state and if applicable, the operator
   * and increment.
   */
  static CounterMatch isCounter(String key, Object value) {
    int pos = key.lastIndexOf(":");
    String fieldName = key.substring(pos < 0 ? 0 : pos);
    Matcher m =
        Pattern.compile("(" + fieldName + ")(\\s*)(\\+|-)(\\s*)(\\d+)").matcher(value.toString());
    if (m.matches()) {
      return new CounterMatch(true, m.group(3), Integer.parseInt(m.group(5)));
    } else {
      return new CounterMatch(false);
    }
  }

  /**
   * Inner class, encapsulating the result of a counter expression match.
   */
  static class CounterMatch {
    private Boolean match;
    private String op;
    private Integer inc;

    CounterMatch(Boolean match, String op, Integer inc) {
      this.match = match;
      this.op = op;
      this.inc = inc;
    }

    CounterMatch(Boolean match) {
      this.match = match;
    }

    Boolean isMatch() {
      return match;
    }

    String getOp() {
      return op;
    }

    Integer getInc() {
      return inc;
    }
  }

  /**
   * <p>
   * Associates the specified value with the specified key in this map. If the map previously
   * contained a mapping for the key, the old value is replaced.
   * </p>
   *
   * <p>
   * This method also handles updating counters. If the value is expressed in the form of {@code
   * counter_column_name + | - counter_offset}, the method will inc (or decrement) the
   * value of the column by {@code counter_offset} depending on whether {@code counter_offset} is
   * preceded by "+" or "-".
   * </p>
   *
   * @param key
   *     the key
   * @param value
   *     the new value
   * @return the previous value associated with the specified key, or null if there was no
   * mapping for the key
   * @throws ClassCastException
   *     if the specified key cannot be compared with the keys currently
   *     in the map
   * @throws NullPointerException
   *     if the specified key or value is null
   */
  Object put(String key, Object value) {
    CounterMatch cm = isCounter(key, value);
    if (cm.isMatch()) {
      int inc = cm.getInc();
      String op = cm.getOp();
      Object oldVal, returnVal;
      oldVal = returnVal = data.get(key);
      if (null == oldVal) {
        oldVal = 0;
      }
      if (!(oldVal instanceof Integer)) {
        throw new RuntimeException(String.format(NON_INTEGER_COUNTER_TYPE, key, oldVal));
      }
      data.put(key, op.equals("+") ? (int) oldVal + inc : (int) oldVal - inc);
      return returnVal;
    } else {
      return data.put(key, value);
    }
  }

  /**
   * Returns the value to which the specified key is mapped,
   * or {@code null} if there is no mapping for the key.
   *
   * @param key
   *     the key for which to fetch the associated value
   * @return the value associated to the key parameter or null if there is no key mapping
   * @throws ClassCastException
   *     if the specified key cannot be compared
   *     with the keys currently in the map
   * @throws NullPointerException
   *     if the specified key is null
   */
  Object get(String key) {
    Object ret = data.get(key);
    return ret;
  }

  /**
   * @param fromKey
   *     low endpoint of the keys in the returned map
   * @param fromInclusive
   *     true if the low endpoint is to be included in the returned view
   * @param toKey
   *     high endpoint of the keys in the returned map
   * @param toInclusive
   *     true if the high endpoint is to be included in the returned view
   * @return a view of the portion of this map whose keys range from fromKey to toKey
   * @see https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/NavigableMap
   * .html#subMap(K,boolean,K,boolean)
   */
  Map<String, Object> getRange(
      String fromKey, boolean fromInclusive, String toKey, boolean toInclusive) {
    return data.subMap(fromKey, fromInclusive, toKey, toInclusive);
  }

  /**
   * Adds all the entries of the specified map to this map.
   *
   * @param newData
   *     the data entries to add
   */
  void putAll(Map<String, Object> newData) {
    for (Map.Entry<String, Object> e : newData.entrySet()) {
      put(e.getKey(), e.getValue());
    }
  }

  Map<String, Object> getDataAsMap() {
    return new HashMap<>(data);
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, Object> entry : data.entrySet()) {
      sb.append("=>(column='");
      sb.append(entry.getKey());
      sb.append("', value='");
      sb.append(entry.getValue());
      sb.append("')");
      sb.append('\n');
    }
    return sb.toString();
  }
}
