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

package com.gps.cardinality.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;

/**
 * A site visitor mock data stream generator.
 *
 * @author gstathis
 * Created on: 2018-11-05
 */
public class DataGenerator {

  private static Random rand = new Random();

  /**
   * Generates a single data payload using the sample top referrer sites and finite guid list.
   * This method allows specifying an epoch date interval for the mock data timestamps.
   * Timestamps are randomly generated withing that interval. This helps generate mock data
   * spanning a specific number of months.
   *
   * @param rand
   *     a random number generator
   * @param guids
   *     a list of UUIDs from which to randomly select
   * @param referers
   *     a list of referer sites from which to randomly select
   * @param landingPages
   *     a list of landing pages from which to randomly select
   * @param intervalStart
   *     the mock data timestamp interval start
   * @param intervalEnd
   *     the mock data timestamp interval end
   * @param throttle
   *     a rate limiter
   * @return an instance of {@link GeneratedData}  containing the generated guid, timestamp and
   * feature1 and 2 values.
   */
  public static GeneratedData generate(
      Random rand, List<UUID> guids, List<String> referers, List<String> landingPages,
      int intervalStart, int intervalEnd, Throttle throttle) {
    if (null != throttle) {
      throttle.acquire();
    }
    return new GeneratedData(
        guids.get(rand.nextInt(guids.size())).toString(),
        (long) (rand.nextInt((intervalEnd - intervalStart) + 1) + intervalStart),
        referers.get(rand.nextInt(referers.size())),
        landingPages.get(rand.nextInt(landingPages.size())));
  }

  /**
   * Generates a single data payload using the sample top referrer sites and finite guid list.
   * This method allows specifying an epoch date interval for the mock data timestamps.
   * Timestamps are randomly generated withing that interval. This helps generate mock data
   * spanning a specific number of months.
   *
   * @param guids
   *     a list of UUIDs from which to randomly select
   * @param referers
   *     a list of referer sites from which to randomly select
   * @param landingPages
   *     a list of landing pages from which to randomly select
   * @param intervalStart
   *     the mock data timestamp interval start
   * @param intervalEnd
   *     the mock data timestamp interval end
   * @param throttle
   *     a rate limiter
   * @return an instance of {@link GeneratedData}  containing the generated guid, timestamp and
   * feature1 and 2 values.
   */
  public static GeneratedData generate(
      List<UUID> guids, List<String> referers, List<String> landingPages, int intervalStart,
      int intervalEnd, Throttle throttle) {
    return generate(rand, guids, referers, landingPages, intervalStart, intervalEnd, throttle);
  }

  public static List<UUID> generateUUIDs(int count) {
    List<UUID> ret = new ArrayList<>();
    IntStream.range(0, count).forEach(i -> ret.add(UUID.randomUUID()));
    return ret;
  }

  public static class GeneratedData {
    public String guid;
    public Long timestamp;
    public String feature1;
    public String feature2;

    GeneratedData(String guid, Long timestamp, String feature1, String feature2) {
      this.guid = guid;
      this.timestamp = timestamp;
      this.feature1 = feature1;
      this.feature2 = feature2;
    }

    public String toString() {
      return String.format("guid: %s, timestamp: %d, feature1: %s, feature2: %s", guid, timestamp
          , feature1, feature2);
    }

    public String toCsv() {
      return String.format("\"%s\",\"%d\",\"%s\",\"%s\"", guid, timestamp, feature1, feature2);
    }
  }
}
