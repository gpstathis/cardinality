# Cardinality

Web analytics platforms typically have to content with counting cardinality. An example of a query they need to answer is the number of people who view a page or product each month. This can be thought of as counting the cardinality of the set of visitors who viewed at least one page.

We can imagine data streaming into a web analytics platform from `script` tags placed on customer sites in the form of a `guid`, a `timestamp` and segmentation dimensions ("features") such as the referer header data and the page that was visited. An example `POST` request made into the platform by one of those script tags could looke like this:

```
POST /track
Accept: */*
guid=962e0c3a-6c08-4160-9196-f0050faffe62&timestamp=1522802128&feature1=facebook.com&feature2=/index.html
```

The challenge is to figure out a scalable solution to keeping track of all this information across thousands of sites, each with tens of millions of page views a month across millions of unique visitors and be able to answer question such as "how many people visited my website this month where feature1 is 'facebook.com'" in real-time. There are critical design choices -- both storage engine and data modeling -- that can have significant impacts on how the throughput and storage costs will scale as the system grows.

## Storage Engine

TODO: discuss why Cassandra might be a good choice.

## Data Modeling

### Raw data tables

Purpose: store the original raw requests to allow discerning uniqe visits vs. all page views.

Design goals:

* Should allow to quickly find whether a given visitor `guid` has been seen for a given site on a given month, week or day..

If we were to model this as a Cassandra table, the schema might look like this:

```
CREATE TABLE RawData (
  siteId text,
  month_interval_start timestamp,
  guid text,
  week_interval_start timestamp,
  day_interval_start timestamp,
  timestamp timestamp,
  feature1 text,
  feature2 text,
  PRIMARY KEY (
      (siteId, month_interval_start, guid),
      week_interval_start, day_interval_start, timestamp
      )
  );
```

The composite partition key helps quickly locate single rows for a visitor's monthly activity on a given site. If such row doesn't already exist, then this is a new unique visitor for that month. Similarly, the week, day and timestamp clustering keys help sort monthly visits such that we can quickly scan looking for specific week or day. The absence of columns for a given week or day signals a unique visit from that `guid`.

E.g.

```
          site id     month      guid
                |     |          |
Partition Key: 'site1:1530403200:acd9cc5b-165b-4d5d-bbcc-50c840038b63'
           week       day        timestamp  feature name      feature value
           |          |          |          |                 |
=>(column='1530489600:1530835200:1530895602:feature1', value='facebook.com')
=>(column='1530489600:1530835200:1530895602:feature2', value='/index.html')
=>(column='1531699200:1532044800:1532103123:feature1', value='facebook.com')
=>(column='1531699200:1532044800:1532103123:feature2', value='/product_74.html')
[...]
```

### Count tables

Purpose: store the monthly, weekly and daily visit counts for each site, for every combination of feature values.

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

This schema helps quickly answer questions such as how many unique visits were there for a given site and month (or week or day) where `feature1` is 'facebook.com'. The composite partition key helps fetch the columns for a give site, month and feature value combination. The clustering keys sort the columns by total or unique counts for the month, week or day.

E.g.

```
          site id     month      feature1     empty feature2
                |     |          |            |
Partition Key: 'site1:1530403200:facebook.com:'
           metric    interval start      counter
           |         |                   |
=>(column='day_total:1530403200', value='1')
[...]
=>(column='day_total:1532476800', value='1')
[...]
=>(column='day_unique:1532476800', value='1')
=>(column='month_total:1530403200', value='19')
=>(column='month_unique:1530403200', value='13')
=>(column='week_total:1529884800', value='1')
[...]
=>(column='week_total:1532304000', value='1')
=>(column='week_unique:1529884800', value='1')
[...]
=>(column='week_unique:1532304000', value='1')
```

## Project Requirements

* Java 11
* Gradle

## Running Tests

```
$ gradle clean test

> Task :test

com.gps.cardinality.utils.TimestampsTest > intervalsTest PASSED

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

BUILD SUCCESSFUL in 1s
4 actionable tasks: 4 executed
```
