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

import java.util.List;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * A site visitor mock data stream generator.
 *
 * @author gstathis
 * Created on: 2018-11-05
 */
public class DataGenerator {

  private static Random rand = new Random();

  /**
   * A few top sites on the internet likely to send traffic.
   */
  private static List<String> TOP_REFERRER_SITES = List
      .of("google.com", "youtube.com", "facebook.com", "baidu.com", "search.yahoo.com",
          "instagram.com", "twitter.com", "weibo.com", "pinterest.com", "reddit.com", "yandex.com",
          "blogspot.com", "bing.com", "ask.com", "search.aol.com", "duckduckgo.com",
          "wolframalpha.com", "webcrawler.com", "search.com", "dogpile.com", "ixquick.com",
          "excite.com", "info.com", "qq.com", "ask.fm", "tumblr.com", "flickr.com", "linkedin.com",
          "vk.com", "odnoklassniki.ru", "meetup.com");

  /**
   * A static list of randomly generated guids. The guids are randomly picked from this list but
   * since its finite, we can generate traffic from repeat visitors. This helps simulate both
   * unique and repeat traffic.
   */
  private static List<String> GUIDS = List
      .of("7c744ced-e65b-4bb9-8d10-b5549a00abf3", "3e1391ed-87e8-41bc-9c94-0b647826236b",
          "8894922e-3758-4691-b762-87fba2a23dc6", "6211397e-47e4-4614-956d-079e97ba4448",
          "ba4d71a2-fe24-4469-bf81-851dfeb203ae", "b6a5ee25-eaf5-4c69-b8ab-735fba56e879",
          "36fe6802-bec8-4735-b433-82edbac6161f", "df48e668-4654-448e-a490-7978c2cc56c3",
          "3acd3b4a-a41f-4db5-84c1-99bfd1d03883", "5fce1f95-ee02-4766-a803-2e670450dad5",
          "b8ba9321-b151-4c8d-b880-28f4c9f04557", "2f008802-142d-4ce8-b7e3-27fc1f897e79",
          "b003119e-f47e-4dbf-be71-4d18fcbbfc81", "2fda4a32-430a-4d4a-8bef-ae131df264fb",
          "cc4b311e-32be-4f41-a0a2-e294a3b371c0", "653dea83-771f-45b9-8e6e-a06792f6b65a",
          "739d4a58-02b9-4cec-b258-1a2f01552e74", "eca97074-2613-429d-837d-d61f1aaddd9a",
          "65f96d03-6e9a-4403-bccc-3aff2ba0e160", "88baf488-6e58-433b-a142-7cef29cd5b49",
          "2559799b-303b-445e-baf6-c25fcd872246", "fd08ba65-9111-4b39-8fa6-d0dc30d0c7fa",
          "4f5a1849-c190-447b-86e0-b953adf8ddcc", "7c1ef6a0-7a9a-42cd-abcf-7d17276c7236",
          "2a4f4761-ccbb-4d2a-996a-ef83f7c43f78", "018565b4-4306-4d75-bcb8-d07b9e3d2462",
          "a4fe99db-3a85-46c9-bc65-c158e5e39a6c", "cb009bd4-51f3-4a02-a7c1-cf83ec6343db",
          "ce6d9f4b-27ef-49ed-a471-afea0c281466", "946990d1-eca7-41d6-ac87-76cbc5c1c87c",
          "8f60e2b9-5ec4-42d7-b26f-d1edf0022fd8", "6859deca-67a8-4262-9e59-cf2e9baf3e1b",
          "4c5ad26f-ca45-469e-b6f5-d05e51c0b7f9", "e72c9da8-818f-4a99-a168-6be531f00af9",
          "e819847c-6ca2-4ea2-bd1d-7f300dee898f", "ae4ed6a3-a0e4-49b4-aef5-b2cdf9c10c63",
          "56f21a09-aaae-4bc5-b4dc-b5e3864a0167", "91d3d678-57cc-4125-a36e-429e4a07195e",
          "0a682bcb-4287-4de1-ba9e-5e61b4f4902d", "b99b78fb-2f3a-4b60-906c-70c3adea7ea4",
          "acd9cc5b-165b-4d5d-bbcc-50c840038b63", "6c0f9c85-b5f8-4883-9887-b120f006bdc0",
          "be833636-27ce-4093-856b-8d7598fbac42", "46088206-c690-4d9b-ac4e-bac5b038a149",
          "525847d5-6baa-4168-93ea-e2d900668dae", "6c4bb547-7fc1-483b-8842-9a388f94ce7c",
          "dc850806-acb0-4770-a275-de5d135d871e", "8aa055d6-c3f3-4bb2-846b-15cf26b8e883",
          "a854d7b6-b51a-42a5-a4ed-05d7e719b977", "3d4d0464-dc0b-470e-a3b0-dcb5422a6938");

  /**
   * Generates a single data payload using the sample top referrer sites and finite guid list.
   * This method allows specifying an epoch date interval for the mock data timestamps.
   * Timestamps are randomly generated withing that interval. This helps generate mock data
   * spanning a specific number of months.
   *
   * @param rand
   *     a random number generator
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
      Random rand, int intervalStart, int intervalEnd, Throttle throttle) {
    throttle.acquire();
    return new GeneratedData(
        GUIDS.get(rand.nextInt(GUIDS.size())),
        (long) (rand.nextInt((intervalEnd - intervalStart) + 1) + intervalStart),
        TOP_REFERRER_SITES.get(rand.nextInt(TOP_REFERRER_SITES.size())),
        String.format("/product_%d.html", rand.nextInt(100)));
  }

  /**
   * Generates a single data payload using the sample top referrer sites and finite guid list.
   * This method allows specifying an epoch date interval for the mock data timestamps.
   * Timestamps are randomly generated withing that interval. This helps generate mock data
   * spanning a specific number of months.
   *
   * @param intervalStart
   *     the mock data timestamp interval start
   * @param intervalEnd
   *     the mock data timestamp interval end
   * @param throttle
   *     a rate limiter
   * @return an instance of {@link GeneratedData}  containing the generated guid, timestamp and
   * feature1 and 2 values.
   */
  public static GeneratedData generate(int intervalStart, int intervalEnd, Throttle throttle) {
    return generate(rand, intervalStart, intervalEnd, throttle);
  }

  /**
   * Scratchpad main for rapid development.
   *
   * TODO: remove
   */
  public static void main(String[] args) {
    Random rand = new Random();
    Throttle throttle = Throttle.create(2);
    Supplier<GeneratedData> randomDataSupplier = () -> DataGenerator.generate(rand,
        1530403200, 1538352000, throttle);
    Stream.generate(randomDataSupplier).limit(20).forEach(System.out::println);
  }

  public static class GeneratedData {
    public String guid;
    public Long timestamp;
    public String feature1;
    public String feature2;

    public GeneratedData(String guid, Long timestamp, String feature1, String feature2) {
      this.guid = guid;
      this.timestamp = timestamp;
      this.feature1 = feature1;
      this.feature2 = feature2;
    }

    public String toString() {
      return String.format("guid: %s, timestamp: %d, feature1: %s, feature2: %s", guid, timestamp
          , feature1, feature2);
    }
  }
}
