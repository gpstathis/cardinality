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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.gps.cardinality.storage.ColumnFamilyData.CounterMatch;

import org.junit.Test;

/**
 * @author gstathis
 * Created on: 2018-11-03
 */
public class ColumnFamilyDataTest {

  @Test
  public void naturalOrderingTest() {
    ColumnFamilyData data = new ColumnFamilyData();
    // Insert values without following the natural ordering
    assertNull(data.put("testkey_10", "testValue10"));
    assertNull(data.put("testkey_03", "testValue03"));
    assertNull(data.put("testkey_07", "testValue07"));
    assertNull(data.put("testkey_05", "testValue05"));
    assertNull(data.put("testkey_04", "testValue04"));
    assertNull(data.put("testkey_09", "testValue09"));
    assertNull(data.put("testkey_01", "testValue01"));
    assertNull(data.put("testkey_08", "testValue08"));
    assertNull(data.put("testkey_02", "testValue02"));
    assertNull(data.put("testkey_06", "testValue06"));
    // Re-add an existing value and confirm that we get the old value back
    assertEquals("testValue06", data.put("testkey_06", "testValue06'"));
    // Print values with keys following the natural ordering
    assertEquals("=>(column='testkey_01', value='testValue01')\n"
                 + "=>(column='testkey_02', value='testValue02')\n"
                 + "=>(column='testkey_03', value='testValue03')\n"
                 + "=>(column='testkey_04', value='testValue04')\n"
                 + "=>(column='testkey_05', value='testValue05')\n"
                 + "=>(column='testkey_06', value='testValue06'')\n"
                 + "=>(column='testkey_07', value='testValue07')\n"
                 + "=>(column='testkey_08', value='testValue08')\n"
                 + "=>(column='testkey_09', value='testValue09')\n"
                 + "=>(column='testkey_10', value='testValue10')\n", data.toString());
  }

  @Test
  public void getSingleRecordTest() {
    ColumnFamilyData data = new ColumnFamilyData();
    data.put("testKey", "testVal");
    assertEquals("testVal", data.get("testKey"));
  }

  @Test
  public void getRecordRangeTest() {
    ColumnFamilyData data = new ColumnFamilyData();
    data.put("week_total_1535328000", "week_total_1535328000+1");
    data.put("week_total_1535932800", "week_total_1535932800+3");
    data.put("week_total_1536537600", "week_total_1536537600+2");
    data.put("week_total_1537142400", "week_total_1537142400+8");
    data.put("week_total_1537747200", "week_total_1537747200+1");
    data.put("week_total_", "0");
    data.put("week_total_end", "0");
    data.put("month_total", "month_total+15");
    data.put("month_unique", "month_unique+14");
    data.put("day_total_1535846400", "day_total_1535846400+1");
    data.put("day_total_1535932800", "day_total_1535932800+1");
    data.put("day_total_1536364800", "day_total_1536364800+1");
    data.put("day_total_1536451200", "day_total_1536451200+1");
    data.put("day_total_1536537600", "day_total_1536537600+1");
    data.put("day_total_1536710400", "day_total_1536710400+1");
    data.put("day_total_1537142400", "day_total_1537142400+1");
    data.put("day_total_1537488000", "day_total_1537488000+1");
    data.put("day_total_1537574400", "day_total_1537574400+1");
    data.put("day_total_1537660800", "day_total_1537660800+5");
    data.put("day_total_1538092800", "day_total_1538092800+1");
    data.put("day_total_", "0");
    data.put("day_total_end", "0");
    assertEquals(
        "{day_total_1535846400=1, day_total_1535932800=1, day_total_1536364800=1, "
        + "day_total_1536451200=1, day_total_1536537600=1, day_total_1536710400=1, "
        + "day_total_1537142400=1, day_total_1537488000=1, day_total_1537574400=1, "
        + "day_total_1537660800=5, day_total_1538092800=1}",
        data.getRange("day_total_", false, "day_total_end", false).toString());
  }

  @Test
  public void counterTest() {
    ColumnFamilyData data = new ColumnFamilyData();
    assertNull(data.put("testCol:testField", "testField+1"));
    assertEquals("=>(column='testCol:testField', value='1')\n", data.toString());
    assertEquals(1, data.put("testCol:testField", "testField+9"));
    assertEquals("=>(column='testCol:testField', value='10')\n", data.toString());
    assertEquals(10, data.put("testCol:testField", "testField-5"));
    assertEquals("=>(column='testCol:testField', value='5')\n", data.toString());
  }

  @Test(expected = RuntimeException.class)
  public void nonIntegerCounterExceptionTest() {
    ColumnFamilyData data = new ColumnFamilyData();
    data.put("testkey", "testVal");
    data.put("testkey", "testkey + 1");
  }

  @Test
  public void counterExpressionMatchTest() {
    assertFalse(ColumnFamilyData.isCounter("testCol:testField", "testValue").isMatch());

    CounterMatch m = ColumnFamilyData.isCounter("testCol:testField", "testField+1");
    assertTrue(m.isMatch());
    assertEquals("+", m.getOp());
    assertEquals(Integer.valueOf(1), m.getInc());

    m = ColumnFamilyData.isCounter("testCol:testField", "testField + 3124");
    assertTrue(m.isMatch());
    assertEquals("+", m.getOp());
    assertEquals(Integer.valueOf(3124), m.getInc());

    m = ColumnFamilyData.isCounter("testCol:testField", "testField - 32");
    assertTrue(m.isMatch());
    assertEquals("-", m.getOp());
    assertEquals(Integer.valueOf(32), m.getInc());

    m = ColumnFamilyData.isCounter("testCol:testField", "testField -4");
    assertTrue(m.isMatch());
    assertEquals("-", m.getOp());
    assertEquals(Integer.valueOf(4), m.getInc());
  }
}
