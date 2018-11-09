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

package com.gps.cardinality;

import com.gps.cardinality.storage.ColumnDefinition;
import com.gps.cardinality.storage.ColumnFamily;
import com.gps.cardinality.utils.DataGenerator;
import com.gps.cardinality.utils.Throttle;
import com.gps.cardinality.utils.Timestamps;
import com.gps.cardinality.utils.Timestamps.Intervals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Entry point for the simulation.
 *
 * @author gstathis
 * Created on: 2018-11-05
 */
public class Cardinality {

  private static String CF_RAW_DATA = "cf_raw_data";
  private static String CF_MONTH_ALL = "cf_month_all";
  private Map<String, ColumnFamily> keySpace;

  private Cardinality() {
    defineColumnFamilies();
  }

  public static void main(String[] args) {
    Cardinality cardinality = new Cardinality();
    cardinality.run();
  }

  /**
   * Scratchpad for rapid development.
   *
   * TODO: remove
   */
  private void defineColumnFamilies() {
    keySpace = new HashMap<>();
    ColumnDefinition definition = new ColumnDefinition(
        List.of("site", "month_interval_start", "guid"),
        List.of("week_interval_start", "day_interval_start", "timestamp"));
    keySpace.put(CF_RAW_DATA, new ColumnFamily(CF_RAW_DATA, definition));

    definition = new ColumnDefinition(List.of("site", "month_interval_start"), List.of());
    keySpace.put(CF_MONTH_ALL, new ColumnFamily(CF_MONTH_ALL, definition));
  }

  /**
   * Scratchpad for rapid development. For now, it combines in one place generating mock data
   * populating some tables and printing them to the console to help visualize the results.
   *
   * TODO: remove
   */
  private void run() {
    Supplier<DataGenerator.GeneratedData> randomDataSupplier = () -> DataGenerator
        .generate(1530403200, 1538352000, Throttle.create(100));
    Stream.generate(randomDataSupplier).limit(50).forEach(data -> {

      Intervals intervals = Timestamps.getIntervals(data.timestamp);
      Map<String, Object> keys = Map
          .of("site", "site1",
              "month_interval_start", intervals.getMonthStart(),
              "guid", data.guid,
              "week_interval_start", intervals.getWeekStart(),
              "day_interval_start", intervals.getDayStart(),
              "timestamp", data.timestamp);
      Map<String, Object> values = Map.of("feature1", data.feature1, "feature2", data.feature2);

      boolean unique = true;
      if (unique = !keySpace.get(CF_RAW_DATA).updateIfExists(keys, values)) {
        keySpace.get(CF_RAW_DATA).update(keys, values);
      }

      keys = Map
          .of("site", "site1",
              "month_interval_start", intervals.getMonthStart());
      values = Map.of("month_total", "month_total+1", "month_unique", "month_unique+" + (unique ? 1 : 0),
          String.format("week_total_%d", intervals.getWeekStart()),
          String.format("week_total_%d+1", intervals.getWeekStart()),
          String.format("day_total_%d", intervals.getDayStart()),
          String.format("day_total_%d+1", intervals.getDayStart()));
      keySpace.get(CF_MONTH_ALL).update(keys, values);
    });
    System.out.println(keySpace.get(CF_RAW_DATA));
    System.out.println(keySpace.get(CF_MONTH_ALL));
  }
}
