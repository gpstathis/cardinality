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

import static com.gps.cardinality.utils.Maps.combineMaps;

import com.gps.cardinality.utils.Timestamps;
import com.gps.cardinality.utils.Timestamps.Intervals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.stream.IntStream;

/**
 * Manages all in-memory data store column families.
 *
 * @author gstathis
 * Created on: 2018-11-07
 */
public class Database {

  private static final String CF_SITE_ID = "site_id";
  private static final String CF_INTERVAL_SIZE = "interval_size";
  private static final String CF_INTERVAL_START = "interval_start";
  private static final String CF_GUID = "guid";
  private static final String CF_MONTH_START = "month_start";
  private static final String CF_METRIC = "metric";
  private static String CF_GUID_DATA = "%s_cf_guid_data";
  private static String CF_MONTHLY_COUNTS = "%s_cf_monthly_data";

  private Map<String, Map<String, ColumnFamily>> siteTables;
  private Map<String, NavigableSet<String>> siteFeatures;
  private Map<String, List<List<String>>> siteFeatureNameCombinations;

  public Database() {
    this.siteTables = new HashMap<>();
    this.siteFeatures = new HashMap<>();
    this.siteFeatureNameCombinations = new HashMap<>();
  }

  /**
   * Used to generate dynamic partition keys. Generates maps of all possible combinations
   * (n!)/(k!(n-k)!) from the given feature key values. E.g. for key/values feature1=facebook.com"
   * and feature2=/index.html of feature set feature1, feature2, feature3, feature4, this will
   * generate:
   *
   * [{feature1=, feature2=, feature3=, feature4=},
   * {feature1=facebook.com, feature2=, feature3=, feature4=},
   * {feature1=, feature2=/index.html, feature3=, feature4=},
   * {feature1=facebook.com, feature2=/index.html, feature3=, feature4=}]
   *
   * @param allFeaturesNames
   *     list of all supported site features
   * @param featureNameCombinations
   *     available combinations of feature names
   * @param featureValues
   *     feature values to be combined
   * @return
   */
  List<Map<String, Object>> featureNameValueCombinations(
      NavigableSet<String> allFeaturesNames,
      List<List<String>> featureNameCombinations,
      NavigableMap<String, String> featureValues) {
    List<Map<String, Object>> ret = new ArrayList<>();
    for (List<String> nameCombo : featureNameCombinations) {
      Map<String, Object> nameValueCombo = new TreeMap<>();
      for (String featureName : allFeaturesNames) {
        if (!nameCombo.contains(featureName)) {
          nameValueCombo.put(featureName, "");
          continue;
        }
        if (nameCombo.contains(featureName) && featureValues.containsKey(featureName)) {
          nameValueCombo.put(featureName, featureValues.get(featureName));
          continue;
        }
        nameValueCombo = null;
        break;
      }
      if (null != nameValueCombo) {
        ret.add(nameValueCombo);
      }
    }
    return ret;
  }

  /**
   * Used to generate dynamic partition keys. Generates lists of all possible combinations
   * (n!)/(k!(n-k)!) from the given feature names. E.g. for set feature1, feature2, feature3,
   * feature4, this will generate:
   *
   * <pre>
   * [feature1, , , ]
   * [, feature2, , ]
   * [, , feature3, ]
   * [, , , feature4]
   * [feature1, feature2, , ]
   * [, feature2, feature3, ]
   * [, , feature3, feature4]
   * [feature1, feature2, feature3, ]
   * [feature1, , feature3, ]
   * [, feature2, feature3, feature4]
   * [, feature2, , feature4]
   * [feature1, feature2, feature3, feature4]
   * [feature1, , , feature4]]
   * </pre>
   *
   * @param featureList
   *     the ordered list of feature names
   * @return all combinations of the specified features
   */
  List<List<String>> featureNameCombinations(NavigableSet<String> featureList) {
    List<List<String>> featureCombinations = new ArrayList<>();

    // Default combo is no features present
    List<String> noFeaturesPresent = new ArrayList<>();
    IntStream.range(0, featureList.size()).forEach(i -> noFeaturesPresent.add(""));
    featureCombinations.add(noFeaturesPresent);

    String[] orderedFeatures = featureList.toArray(new String[0]);
    int numFeatures = orderedFeatures.length;
    for (int increment = 0; increment < numFeatures; increment++) {
      for (int j = increment; j < numFeatures; j++) {
        List<String> adjacentKeys = new ArrayList<>();
        for (int i = 0; i < j - increment; i++) {
          adjacentKeys.add("");
        }
        for (int i = j - increment; i <= j; i++) {
          adjacentKeys.add(orderedFeatures[i]);
        }
        for (int i = 0; i < numFeatures - j - 1; i++) {
          adjacentKeys.add("");
        }
        featureCombinations.add(adjacentKeys);
        if (increment > 1) {
          List<String> distantKeys = new ArrayList<>();
          for (int i = 0; i < j - increment; i++) {
            distantKeys.add("");
          }
          distantKeys.add(orderedFeatures[j - increment]);
          for (int i = 0; i < increment - 1; i++) {
            distantKeys.add("");
          }
          distantKeys.add(orderedFeatures[j]);
          for (int i = 0; i < numFeatures - j - 1; i++) {
            distantKeys.add("");
          }
          featureCombinations.add(distantKeys);
        }
      }
    }
    return featureCombinations;
  }

  /**
   * Records a single site event into two tables, one tracking guids and one
   * tracking the monthly cardinality counts.
   *
   * @param siteId
   *     the site to be tracked
   * @param timestamp
   *     the timestamp of the event
   * @param guid
   *     the guid of the visitor
   * @param features
   *     the features key/values
   */
  public void track(
      String siteId, long timestamp, String guid, NavigableMap<String, String> features) {
    Intervals intervals = Timestamps.getIntervals(timestamp);

    List<Map<String, Object>> featureCombos = featureNameValueCombinations(
        siteFeatures.get(siteId), siteFeatureNameCombinations.get(siteId), features);

    featureCombos.forEach(fc -> {
      Map<String, Object> rawTableKeys = combineMaps(
          Map.of(CF_SITE_ID, siteId, CF_INTERVAL_SIZE, "month", CF_INTERVAL_START,
              intervals.getMonthStart(), CF_GUID, guid), fc);
      boolean unique = updateAndReportIfUnique(
          getGuidDataTable(siteId), rawTableKeys, Map.of("visits", "visits+1"));

      if (unique) {
        Map<String, Object> countsTableKeys = combineMaps(
            Map.of(CF_SITE_ID, siteId, CF_MONTH_START, intervals.getMonthStart()), fc);
        getMonthlyCountsTable(siteId).update(
            countsTableKeys,
            Map.of(CF_METRIC, "month_unique", CF_INTERVAL_START, intervals.getMonthStart(),
                "visits", "visits+1"));
      }
    });
  }

  /**
   * Updates a table and reports if a previous record existed for that key. Used to track unique
   * visits.
   *
   * @param cf
   *     the table
   * @param keys
   *     the row key
   * @param data
   *     the data for the row
   * @return true if the record is a new unique record, false if there was already an entry for
   * that row key
   */
  private boolean updateAndReportIfUnique(
      ColumnFamily cf, Map<String, Object> keys, Map<String, Object> data) {
    boolean unique;
    if (unique = !cf.updateIfExists(keys, data)) {
      cf.update(keys, data);
    }
    return unique;
  }

  /**
   * Auto generates tables for a given site and set of supported features.
   *
   * @param siteId
   *     the site
   * @param features
   *     the names of features supported by the site
   */
  public void createTables(String siteId, NavigableSet<String> features) {
    Map<String, ColumnFamily> tables = new HashMap<>();

    // Raw data table
    List<String> rawTableKeys = new ArrayList<>(features);
    rawTableKeys.addAll(0, List.of(CF_SITE_ID, CF_INTERVAL_SIZE, CF_INTERVAL_START, CF_GUID));
    ColumnDefinition rawDataDefinition = new ColumnDefinition(
        rawTableKeys,
        List.of());
    String tableName = String.format(CF_GUID_DATA, siteId);
    tables.put(tableName, new ColumnFamily(tableName, rawDataDefinition));

    // Counts table
    List<String> countsTableKeys = new ArrayList<>(features);
    countsTableKeys.addAll(0, List.of(CF_SITE_ID, CF_MONTH_START));
    ColumnDefinition countsDefinition = new ColumnDefinition(
        countsTableKeys,
        List.of(CF_METRIC, CF_INTERVAL_START));
    tableName = String.format(CF_MONTHLY_COUNTS, siteId);
    tables.put(tableName, new ColumnFamily(tableName, countsDefinition));

    this.siteTables.put(siteId, tables);
    this.siteFeatures.put(siteId, features);
    this.siteFeatureNameCombinations.put(siteId, featureNameCombinations(features));
  }

  public ColumnFamily getGuidDataTable(String siteId) {
    return this.siteTables.get(siteId).get(String.format(CF_GUID_DATA, siteId));
  }

  public ColumnFamily getMonthlyCountsTable(String siteId) {
    return this.siteTables.get(siteId).get(String.format(CF_MONTHLY_COUNTS, siteId));
  }
}
