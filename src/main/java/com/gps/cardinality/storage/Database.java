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
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Manages all in-memory data store column families.
 *
 * @author gstathis
 * Created on: 2018-11-07
 */
public class Database {

  private static String CF_RAW_DATA = "%s_cf_raw_data";
  private static String CF_MONTHLY_DATA = "%s_cf_monthly_data_%s";

  private Map<String, ColumnFamily> siteTables;

  public Database() {
    this.siteTables = new HashMap<>();
  }

  /**
   * Scratchpad main for rapid development.
   *
   * TODO: remove
   */
  public static void main(String[] args) {
    Database db = new Database();
    db.createTables("site1", Set.of("feature1", "feature2"));
    db.keyCombinations("site1", System.currentTimeMillis(), new TreeMap<>(
        Map.of("feature1", "foo", "feature2", "bar", "feature3", "zoo", "feature4", "zaa")))
        .stream().forEach(System.out::println);
  }

  /**
   * Audo generates tables for a given site and set of supported features.
   *
   * TODO: finish
   *
   * @param siteId
   * @param features
   */
  public void createTables(String siteId, Set<String> features) {
    // TODO: fill placeholder method
  }

  /**
   * Generates all possible key combinations (n!)/(k!(n-k)!) from the given key/values, sorted in
   * natural ordering. E.g. for set feature1=foo, feature2=bar, feature3=zoo, feature4=zaa, this
   * will generate:
   *
   * <pre>
   * site1:1541739316983:feature1=foo
   * site1:1541739316983:feature1=foo:feature2=bar
   * site1:1541739316983:feature1=foo:feature2=bar:feature3=zoo
   * site1:1541739316983:feature1=foo:feature2=bar:feature3=zoo:feature4=zaa
   * site1:1541739316983:feature1=foo:feature3=zoo
   * site1:1541739316983:feature1=foo:feature4=zaa
   * site1:1541739316983:feature2=bar
   * site1:1541739316983:feature2=bar:feature3=zoo
   * site1:1541739316983:feature2=bar:feature3=zoo:feature4=zaa
   * site1:1541739316983:feature2=bar:feature4=zaa
   * site1:1541739316983:feature3=zoo
   * site1:1541739316983:feature3=zoo:feature4=zaa
   * site1:1541739316983:feature4=zaa
   * </pre>
   *
   * @param siteId
   *     the site id
   * @param monthInterval
   *     the interval start
   * @param features
   *     the key/values of the features
   * @return all combinations of the specified features, sorted by natural order
   */
  public Set<String> keyCombinations(
      String siteId, long monthInterval, NavigableMap<String, String> features) {
    Set<String> keyCombinations = new TreeSet<>();
    String[] keyParts = features.entrySet().stream().map(e ->
        String.format("%s=%s", e.getKey(), e.getValue())).toArray(String[]::new);
    System.out.println(features);
    for (int increment = 0; increment < keyParts.length; increment++) {
      for (int j = 0 + increment; j < keyParts.length; j++) {
        String adjacentKeys = "";
        for (int i = j - increment; i <= j; i++) {
          adjacentKeys = String.format("%s:%s", adjacentKeys, keyParts[i]);
        }
        keyCombinations.add(String.format("%s:%s%s", siteId, monthInterval, adjacentKeys));
        if (increment > 0) {
          String distantKeys = String.format(":%s:%s", keyParts[j - increment], keyParts[j]);
          keyCombinations.add(String.format("%s:%s%s", siteId, monthInterval, distantKeys));
        }
      }
    }
    return keyCombinations;
  }
}
