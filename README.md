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

This schema helps quickly answer questions such as how many unique visits were there for a given site and month (or week or day) where `feature1` is 'facebook.com'. The composite partition key helps fetch the columns for a give site, month and feature value combination. The clustering keys sort the columns by total or unique counts for the month, week or day.

E.g.

```
          site id     month      feature1     empty feature2
                |     |          |            |
Partition Key: 'site1:1530403200:facebook.com:'
           metric    interval start             counter
           |         |                          |
=>(column='day_unique:1530403200:visits', value='1')
[...]
=>(column='day_unique:1532476800:visits', value='1')
=>(column='month_unique:1530403200:visits', value='13')
[...]
=>(column='week_unique:1529884800:visits', value='1')
[...]
=>(column='week_unique:1532304000:visits', value='1')
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
