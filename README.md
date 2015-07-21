# ScatsAnalysis
Software used to ingest SCATS traffic volume data into Accumulo/GeoMesa.

## Installation steps


### Build the environment on your local machine

See this Wiki page to install Hadoop, Spark, and Accumulo/GeoMesa:
http://wiki.aurin.org.au/display/TWIT/Analytical+stack+installation+procedure

Start HDFS, Spark, Zookeeper and Accumulo.


### Load test data into HDFS
  `${HADOOP_HOME}/bin/hadoop fs -put ./data/vs15min_2012-05-17.csv /`


### Load Adelaide intersection coordinates data into HDFS
  `${HADOOP_HOME}/bin/hadoop fs -put ./data/dictionary.csv /`


### Build the project

  `mvn clean install`


### Run the project

  `${SPARK_HOME}/bin/spark-submit  \`
  `  --name "ScatsAnalysis" \`
  `  --class au.org.aurin.ScatsAnalysis \`
  `  --master "spark://vaneyck:7077" \`
  `  --deploy-mode client \`
  `  ./target/scats-<version>.jar \`
  `  --instanceId atraffic \`
  `  --zookeepers "localhost:2181" \`
  `  --user root --password atraffic \`
  `  --tableName atraffic \`
  `  --overwrite \`
  `  --readingsFile "hdfs://localhost:9000/sample.csv" \`

### Look at the results

  `${ACCUMULO_HOME}/bin/accumulo shell -u root`
  `scan -table atraffic_records`

