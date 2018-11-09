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

import static com.gps.cardinality.utils.Timestamps.getIntervals;
import static com.gps.cardinality.utils.Timestamps.toEpoch;
import static org.junit.Assert.assertEquals;

import com.gps.cardinality.utils.Timestamps.Intervals;

import org.junit.Test;

/**
 * @author gstathis
 * Created on: 2018-11-06
 */
public class TimestampsTest {

  @Test
  public void intervalsTest() {
    Intervals intervals = getIntervals(1541562050); // 11/07/2018 @ 3:40am (UTC)
    assertEquals(toEpoch(2018, 11, 1), intervals.getMonthStart());
    assertEquals(toEpoch(2018, 11, 5), intervals.getWeekStart());
    assertEquals(toEpoch(2018, 11, 7), intervals.getDayStart());

    // Test having the week cross over a month and year
    intervals = getIntervals(1546434000); // 01/02/2019 @ 1:00pm (UTC)
    assertEquals(toEpoch(2019, 1, 1), intervals.getMonthStart());
    assertEquals(toEpoch(2018, 12, 31), intervals.getWeekStart()); // different month and year
    assertEquals(toEpoch(2019, 1, 2), intervals.getDayStart());
  }

}
