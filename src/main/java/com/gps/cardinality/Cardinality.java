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
import static picocli.CommandLine.Option;

import com.gps.cardinality.storage.FileWriter;
import com.gps.cardinality.storage.Database;
import com.gps.cardinality.utils.DataGenerator;
import com.gps.cardinality.utils.DataGenerator.GeneratedData;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;

import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Entry point for the simulation.
 *
 * @author gstathis
 * Created on: 2018-11-05
 */
@Command(name = "java -jar build/libs/cardinality.jar",
    mixinStandardHelpOptions = true,
    version = "Cardinality by George Stathis, v1")
public class Cardinality implements Runnable {

  @Option(names = {"-r", "--referers"},
      arity = "1..*",
      required = true,
      description = "A list of referers")
  String[] referers;

  @Option(names = {"-p", "--landing_pages"},
      arity = "1..*",
      required = true,
      description = "A list of landing pages")
  String[] landingPages;

  private Database db;

  @Option(names = {"-s", "--site_id"},
      required = true,
      description = "Provide a sample site id")
  private String siteId;

  @Option(names = {"-g", "--num_guids"},
      required = true,
      description = "The number of random guids to select from")
  private int numGuids;

  @Option(names = {"-f", "--from"},
      required = true,
      description = "A 'yyyy-MM-dd' formatted date representing the date from which the random "
                    + "timestamps should start")
  private String from;

  @Option(names = {"-t", "--to"},
      required = true,
      description = "A 'yyyy-MM-dd' formatted date representing the date when the random "
                    + "timestamps should stop")
  private String to;

  @Option(names = {"-n", "--num_samples"},
      required = true,
      description = "The number of samples to generate")
  private int numSamples;

  private Cardinality() {
    db = new Database();
  }

  public static void main(String[] args) {
    CommandLine.run(new Cardinality(), args);
  }

  /**
   * Generates mock data, populating some tables and printing them to the console to help
   * visualize the results.
   */
  public void run() {
    db.createTables(siteId, new TreeSet<>(List.of("feature1", "feature2")));
    List<UUID> guids = generateUUIDs(numGuids);
    Supplier<GeneratedData> randomDataSupplier = () -> DataGenerator
        .generate(guids, List.of(referers),
            List.of(landingPages),
            toEpoch(from),
            toEpoch(to), null);
    FileWriter fileWriter = new FileWriter(siteId);
    fileWriter.writeCsv("guid,timestamp,feature1,feature2");
    Stream.generate(randomDataSupplier).limit(numSamples).forEach(data -> {
      db.track(
          siteId, data.timestamp, data.guid,
          new TreeMap<>(Map.of("feature1", data.feature1, "feature2", data.feature2)));
      fileWriter.writeCsv(data.toCsv());
    });
    fileWriter.writeTable(db.getGuidDataTable(siteId));
    fileWriter.writeTable(db.getMonthlyCountsTable(siteId));
    fileWriter.close();
    System.out.println(String.format("Simulation complete. Check %s and %s for results.",
        fileWriter.getTablesFilePath(), fileWriter.getCsvFilePath()));
  }
}
