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

import static com.gps.cardinality.utils.DataGenerator.generateUUIDs;
import static com.gps.cardinality.utils.Timestamps.toEpoch;

import com.gps.cardinality.storage.Database;
import com.gps.cardinality.utils.DataGenerator;
import com.gps.cardinality.utils.DataGenerator.GeneratedData;
import com.gps.cardinality.utils.Throttle;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Entry point for the simulation.
 *
 * @author gstathis
 * Created on: 2018-11-05
 */
public class Cardinality {

  private Database db;

  private Cardinality() {
    db = new Database();
  }

  public static void main(String[] args) {
    Cardinality cardinality = new Cardinality();
    cardinality.run("site1");
  }

  /**
   * Scratchpad for rapid development. For now, it combines in one place generating mock data
   * populating some tables and printing them to the console to help visualize the results.
   *
   * TODO: remove
   */
  private void run(String siteId) {
    db.createTables(siteId, new TreeSet<>(List.of("feature1", "feature2")));
    List<UUID> guids = generateUUIDs(50);
    Supplier<GeneratedData> randomDataSupplier = () -> DataGenerator
        .generate(guids, List.of("facebook.com", "google.com"),
            List.of("/index.html", "/product1.html"),
            toEpoch(2018, 10, 1),
            toEpoch(2018, 11, 1), Throttle.create(100));
    Stream.generate(randomDataSupplier).limit(100).forEach(data -> {
      db.track(
          siteId, data.timestamp, data.guid,
          new TreeMap<>(Map.of("feature1", data.feature1, "feature2", data.feature2)));
    });
    System.out.println(db.getGuidDataTable(siteId));
    System.out.println(db.getMonthlyCountsTable(siteId));
  }
}
