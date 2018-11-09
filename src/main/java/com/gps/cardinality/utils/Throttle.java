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

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A basic rate limiter for controlling the rate of mock data generation.
 *
 * @author gstathis
 * Created on: 2018-11-05
 */
public class Throttle {

  private Long maxWaitIntervalNanos;

  private Throttle(Double permitsPerSecond) {
    if (null == permitsPerSecond) {
      throw new IllegalArgumentException("Null parameter");
    }
    this.maxWaitIntervalNanos = (long)(1000000000.0 / permitsPerSecond);
  }

  public static Throttle create(final double permitsPerSecond) {
    return new Throttle(permitsPerSecond);
  }

  public void acquire() {
    try {
      NANOSECONDS.sleep(this.maxWaitIntervalNanos);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
