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

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * @author gstathis
 * Created on: 2018-11-12
 */
public class FileWriter {
  private BufferedWriter csvWriter;
  private BufferedWriter tablesWriter;
  private String csvFilePath;
  private String tablesFilePath;

  public FileWriter(String siteId) {
    long now = System.currentTimeMillis();
    this.csvFilePath = String.format("%d_%s_visits.csv", now, siteId);
    try {
      this.csvWriter = Files.newBufferedWriter(Paths.get(this.csvFilePath));
    } catch (IOException e) {
      System.err.println(String.format("Could not create CSV file:\n%s", e.getMessage()));
    }
    this.tablesFilePath = String.format("%d_%s_tables.txt", now, siteId);
    try {
      this.tablesWriter = Files.newBufferedWriter(Paths.get(this.tablesFilePath));
    } catch (IOException e) {
      System.err.println(String.format("Could not create tables file:\n%s", e.getMessage()));
    }
  }

  public String getCsvFilePath() {
    return csvFilePath;
  }

  public String getTablesFilePath() {
    return tablesFilePath;
  }

  public void writeCsv(String line) {
    if (null != this.csvWriter) {
      try {
        this.csvWriter.write(String.format("%s%n", line));
      } catch (IOException e) {
        System.err.println(String.format("Could not writeCsv data to csv: %s", line));
      }
    }
  }

  public void writeTable(ColumnFamily table) {
    if (null != this.tablesWriter) {
      try {
        table.write(this.tablesWriter);
      } catch (IOException e) {
        System.err.println("Could not table data");
      }
    }
  }

  public void close() {
    if (null != this.csvWriter) {
      try {
        this.csvWriter.close();
      } catch (IOException e) {
        System.err.println(String.format("Could not close CSV file:\n%s", e.getMessage()));
      }
    }
    if (null != this.tablesWriter) {
      try {
        this.tablesWriter.close();
      } catch (IOException e) {
        System.err.println(String.format("Could not close tables file:\n%s", e.getMessage()));
      }
    }
  }
}
