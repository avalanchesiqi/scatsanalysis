ScatsAnalysis
=============
Software used to ingest SCATS traffic volume data into Accumulo/GeoMesa.

About
-----
ScatsAnalysis is service that ingests SCATS traffic volume, aggregates vehicle_count and loads result into Accumulo database or stores in HDFS.
Intuitively it is built to process Australian SCATS traffic volume, data I/O may be different according to different input format and requires specific preprocessing.

Data Architecture
-----------------
An big data architecture called "STACK" is implemented here. It uses Apache Hadoop as file systems, Apache Spark as computation engine. On the top of that, NoSQL database Accumulo is set up with plugin GeoMesa to optimise geo-spatial data indexing. This architecture could be set up in standalone or cluster mode.

A more detailed slides about ScatsAnalysis and STACK architecture could be found here:
[An Architecture for Big Data Processing and Visualisation of Traffic Data](https://goo.gl/XITC0H)

Build
-----
```mvn clean install -DskipTests=true```

Usage
-----
1. Set up STACK architecture, create an Accumulo instance "tweeter", user "tweeter", table "tweet", assign WRITE and READ permission to user "tweeter" on table "tweet"

2. Load Sample data into HDFS
   ```${HADOOP_HOME}/bin/hadoop fs -put ./data/VolumeDataSample.CSV / ```

3. Modify run script according to your system setting
   - Change Spark master address to your hostname
   - Change partition number to large number if process large file (e.g. 1000 for 20GB dataset)
   - Allocate more executer memory and driver memory if needed


Look at the results
-------------------
```${ACCUMULO_HOME}/bin/accumulo shell -u root```
```scan -table tweet_records```

