# Cardinality

Web analytics platforms typically have to content with counting cardinality. An example of a query they need to answer is the number of people who view a page or product each month. This can be thought of as counting the cardinality of the set of visitors who viewed at least one page.

We can imagine data streaming into a web analytics platform from `script` tags placed on customer sites in the form of a `guid`, a `timestamp` and segmentation dimensions ("features") such as the referer header data and the page that was visited. An example `POST` request made into the platform by one of those script tags could looke like this:

```
POST /track
Accept: */*
guid=962e0c3a-6c08-4160-9196-f0050faffe62&timestamp=1522802128&feature1=facebook.com&feature2=/index.html
```

The challenge is to figure out a scalable solution to keeping track of all this information across thousands of sites, each with tens of millions of page views a month across millions of unique visitors and be able to answer question such as "how many people visited my website this month where feature1 is 'facebook.com'" in real-time. There are critical design choices -- both storage engine and data modeling -- that can have significant impacts on how the throughput and storage costs will scale as the system grows.

## Project Goals

This project is a simple command line simulation that generates mock visitor data and tracks it in-memory. It is a data modeling demo of what a possible tracking solution could be.

## Storage Engine

The target storage engine in this example would be Apache Cassandra. Cassandra is a good candidate for this type of use case and the workload it entails due to the following reasons:

* Cassandra is built for fast writes: it's built as an append-only log where all writes are done sequentially. That implies a lower write latency. Cassandra also partitions the key space allowing for machines to write in parallel, achieving higher write throughputs. Large organizations such a Netflix have posted write ups demonstrating the type of write speeds that can be seen at scale: https://medium.com/netflix-techblog/revisiting-1-million-writes-per-second-c191a84864cc
* It can handle very large data sets
* It's known to scale linearly which makes it ideal for predicting capacity based on current/predicted throughput

## Data Modeling

### Guid data tables

Purpose: track the guids encountered per selected time interval.

Design goals:

* Should allow to quickly find whether a given visitor `guid` has been seen for a given site on a given month and feature combination.

If we were to model this as a Cassandra table, the schema might look like this:

```
CREATE TABLE GuidData (
  siteId text,
  interval_size text,
  interval_start timestamp,
  guid text,
  feature1 text,
  feature2 text,
  PRIMARY KEY (
      (siteId, interval_size, interval_start, guid, feature1, feature2)
      )
  );
```

The composite partition key helps quickly locate single rows for a visitor's monthly activity on a given site and for a given feature value combination. If such row doesn't already exist, then this visit for this combination for features is unique for that month.

E.g.

```
          interval type     interval start
          site id     |     |          guid             feature combinations
                |     |     |          |                                   |  
Partition Key: 'site1:month:1530403200:acd9cc5b-165b-4d5d-bbcc-50c840038b63:::'
=>(column='visits', value='1')
Partition Key: 'site1:month:1530403200:acd9cc5b-165b-4d5d-bbcc-50c840038b63:facebook.com::'
=>(column='visits', value='1')
Partition Key: 'site1:month:1530403200:acd9cc5b-165b-4d5d-bbcc-50c840038b63::/index.html:'
=>(column='visits', value='1')
Partition Key: 'site1:month:1530403200:acd9cc5b-165b-4d5d-bbcc-50c840038b63:facebook.com:/index.html:'
=>(column='visits', value='1')
[...]
```

Note that the rows in this table grow increasingly large with each site feature tracked. Each visit results in one write for the `guid` alone and `N` writes for each combination `(n!)/(k!(n-k)!)` of features, where `n` is the total number of features and `k` is the number of features to combine at a time. While this may seem wasteful at first, it leverages two of Cassandra's core features: fast writes and large data set ingestion. So we opt to incur several writes per visit and to pre-compute and store every combination of feature at write time to speed up reads.

Note also that if storage is an issue, a table per month can be created and dropped once the cardinality numbers have been obtained (see next section for Count tables). The raw logs from the visits, which are smaller in size, can be kept in cold storage if needed and replayed back to re-build the table if necessary.

### Count tables

Purpose: store the monthly unique visit counts for each site, for every combination of feature values.

Design goals:

* Trade storage for query speed by pre-computing the results using counters as the raw data comes in.

```
CREATE TABLE Counts (
  siteId text,
  month_interval_start timestamp,
  feature1 text,
  feature2 text,
  metric_type text,
  interval_start counter,
  PRIMARY KEY (
      (siteId, month_interval_start, feature1, feature2),
      metric_type, interval_start
      )
  );
```

This schema helps quickly answer questions such as how many unique visits were there for a given site and month where `feature1` is 'facebook.com'. The composite partition key helps fetch the columns for a given site, month and feature value combination. 

E.g.

```
          site id     month      feature1     empty feature2
                |     |          |            |
Partition Key: 'site1:1530403200:facebook.com:'
           metric       interval start             counter
           |            |                          |
=>(column='month_unique:1530403200:visits', value='13')
                                 feature1     feature2
                                 |            |
Partition Key: 'site1:1530403200:facebook.com:/index.html'
=>(column='month_unique:1530403200:visits', value='5')
                      feature2 only
                                  |            
Partition Key: 'site1:1530403200::/index.html'
=>(column='month_unique:1530403200:visits', value='15')
[...]
```

Records in this table are pupulated each time a new row is inserted in the `guid` data tables but not when an existing `guid` row's counter is incremented.

Note that the clustering keys sort the columns by unique counts for the month but could easily do it by week or day. Combined with a row scan, this can quickly yield a histogram of unique visits per week or day in a month.

## Project Requirements

* Java 11
* Gradle

## Building Fat Jar

Run this first to build a jar file. It will be located in `build/libs/cardinality.jar`.

```
$ gradle clean customFatJar

BUILD SUCCESSFUL in 0s
3 actionable tasks: 2 executed, 1 up-to-date
```

## Usage

```
$ java -jar build/libs/cardinality.jar -h
Usage: java -jar build/libs/cardinality.jar [-hV] -f=<from> -g=<numGuids>
                                            -n=<numSamples> -s=<siteId> -t=<to>
                                            -p=<landingPages>...
                                            [-p=<landingPages>...]...
                                            -r=<referers>...
                                            [-r=<referers>...]...
  -f, --from=<from>        A 'yyyy-MM-dd' formatted date representing the date from
                             which the random timestamps should start
  -g, --num_guids=<numGuids>
                           The number of random guids to select from
  -h, --help               Show this help message and exit.
  -n, --num_samples=<numSamples>
                           The number of samples to generate
  -p, --landing_pages=<landingPages>...
                           A list of landing pages
  -r, --referers=<referers>...
                           A list of referers
  -s, --site_id=<siteId>   Provide a sample site id
  -t, --to=<to>            A 'yyyy-MM-dd' formatted date representing the date when
                             the random timestamps should stop
  -V, --version            Print version information and exit.
```

## Sample run

The following run simulates 500 visits from 100 unique visitors to a site with id `site1` in the month of October 2018, distributed among three different landing pages:

```
$ java -jar build/libs/cardinality.jar -s site1 -g 100 -r facebook.com google.com -p /index.html /index2.html /index3.html -f 2018-10-01 -t 2018-11-01 -n 500
Simulation complete. Check 1542082422933_site1_tables.txt and 1542082422933_site1_visits.csv for results.
```
Each run outputs two files:

* a `{timestamp}_{site_id}_tables.txt` file where the `guid` visits and counts tables are output as text
* a `{timestamp}_{site_id}_visits.csv` file where the visit data was recorded

You can find the files under the `samples` directory. The counts table for the sample was:

```
Column Family: 'site1_cf_monthly_data'
Partition Key: 'site1:1538352000::'
=>(column='month_unique:1538352000:visits', value='100')
Partition Key: 'site1:1538352000:google.com:/index3.html'
=>(column='month_unique:1538352000:visits', value='56')
Partition Key: 'site1:1538352000:facebook.com:/index.html'
=>(column='month_unique:1538352000:visits', value='54')
Partition Key: 'site1:1538352000::/index2.html'
=>(column='month_unique:1538352000:visits', value='81')
Partition Key: 'site1:1538352000:facebook.com:/index2.html'
=>(column='month_unique:1538352000:visits', value='63')
Partition Key: 'site1:1538352000:google.com:/index.html'
=>(column='month_unique:1538352000:visits', value='53')
Partition Key: 'site1:1538352000:google.com:/index2.html'
=>(column='month_unique:1538352000:visits', value='52')
Partition Key: 'site1:1538352000:facebook.com:'
=>(column='month_unique:1538352000:visits', value='92')
Partition Key: 'site1:1538352000::/index3.html'
=>(column='month_unique:1538352000:visits', value='78')
Partition Key: 'site1:1538352000:facebook.com:/index3.html'
=>(column='month_unique:1538352000:visits', value='56')
Partition Key: 'site1:1538352000:google.com:'
=>(column='month_unique:1538352000:visits', value='96')
Partition Key: 'site1:1538352000::/index.html'
=>(column='month_unique:1538352000:visits', value='78')
```

To verify the numbers, we uploaded the accompanying CSV to Google Sheets (see https://docs.google.com/spreadsheets/d/13P8CtUyHm6PBHNqa09xhEA4RyUiAA_1lusfg71IP6wg/edit#gid=1892714881) and created a Pivot Table:

<img src="https://github.com/gpstathis/cardinality/blob/master/samples/pivot_table_screenie.png?raw=true" width="400">

For instance, we can see both in the the counts rable row `site1:1538352000:facebook.com:` and in the pivot table that the unique number of visits where `feature1` is `facebook.com` in the month of October was 92.

## Running Tests

```
$ gradle clean test

> Task :test

com.gps.cardinality.utils.TimestampsTest > numsToEpoch PASSED

com.gps.cardinality.utils.TimestampsTest > intervalsTest PASSED

com.gps.cardinality.utils.TimestampsTest > stringToEpoch PASSED

com.gps.cardinality.storage.ColumnFamilyDataTest > counterTest PASSED

com.gps.cardinality.storage.ColumnFamilyDataTest > getRecordRangeTest PASSED

com.gps.cardinality.storage.ColumnFamilyDataTest > counterExpressionMatchTest PASSED

com.gps.cardinality.storage.ColumnFamilyDataTest > naturalOrderingTest PASSED

com.gps.cardinality.storage.ColumnFamilyDataTest > nonIntegerCounterExceptionTest PASSED

com.gps.cardinality.storage.ColumnFamilyDataTest > getSingleRecordTest PASSED

com.gps.cardinality.storage.ColumnFamilyTest > selectRangeTest PASSED

com.gps.cardinality.storage.ColumnFamilyTest > updateIfExistsTest PASSED

com.gps.cardinality.storage.ColumnFamilyTest > selectOneTest PASSED

com.gps.cardinality.storage.ColumnFamilyTest > updateTest PASSED

com.gps.cardinality.storage.DatabaseTest > featureNameCombinationsTest PASSED

com.gps.cardinality.storage.DatabaseTest > featureNameValueCombinationsTest PASSED

BUILD SUCCESSFUL in 1s
4 actionable tasks: 4 executed
```
