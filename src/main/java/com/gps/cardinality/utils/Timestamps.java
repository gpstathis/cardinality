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

import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Interval calculation helpers.
 *
 * @author gstathis
 * Created on: 2018-11-06
 */
public class Timestamps {

  /**
   * For a given timestamp, this method generates the timestamps for the corresponding month
   * (00:00 UTC of the first day of the month), week (00:00 UTC of that week's Monday) and day
   * (00:00 UTC of that same day).
   *
   * @param timestamp
   *     the timestamp
   * @return a {@link Intervals} instance
   */
  public static Intervals getIntervals(long timestamp) {
    LocalDateTime dateTime = LocalDateTime.ofEpochSecond(timestamp, 0, ZoneOffset.UTC);
    int year = dateTime.getYear();
    int month = dateTime.getMonthValue();
    int day = dateTime.getDayOfMonth();
    int weekDay = dateTime.getDayOfWeek().getValue();
    LocalDateTime weekDateTime = dateTime.minusDays(weekDay - 1);

    return new Intervals(
        timestamp, toEpoch(year, month, 1),
        toEpoch(weekDateTime.getYear(), weekDateTime.getMonthValue(), weekDateTime.getDayOfMonth()),
        toEpoch(year, month, day));
  }

  /**
   * Generates a timestamp from the provided year, month and day.
   *
   * @param year
   *     the year
   * @param month
   *     the month
   * @param day
   *     the day
   * @return a corresponding unix epoch time
   */
  public static int toEpoch(int year, int month, int day) {
    return (int) LocalDateTime.of(year, month, day, 0, 0).toEpochSecond(ZoneOffset.UTC);
  }

  public static class Intervals {
    long timestamp;
    long monthStart;
    long weekStart;
    long dayStart;

    public Intervals(long timestamp, long monthStart, long weekStart, long dayStart) {
      this.timestamp = timestamp;
      this.monthStart = monthStart;
      this.weekStart = weekStart;
      this.dayStart = dayStart;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public long getMonthStart() {
      return monthStart;
    }

    public long getWeekStart() {
      return weekStart;
    }

    public long getDayStart() {
      return dayStart;
    }
  }
}
