${SPARK_HOME}/bin/spark-submit  \
  --name "ScatsAnslysis" \
  --class au.org.aurin.scatsanalysis.ScatsAnalysis \
  --master "spark://{hostname}:7077" \
  --deploy-mode client \
   ./target/scatsanalysis-0.2.0-SNAPSHOT.jar \
  --instanceId tweeter \
  --zookeepers "localhost:2181" \
  --user root --password tweeter \
  --tableName tweet \
  --overwrite \
  --readingsFile "hdfs://localhost:9000/VolumeDataSample.CSV" \
  --partitionNumber 10
