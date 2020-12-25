##Spark RDD lab steps

gsutil cp gs://globallogic-procamp-bigdata-datasets_bogdan/2015_Flight_Delays_and_Cancellations/flights.csv .
gsutil cp gs://globallogic-procamp-bigdata-datasets_bogdan/2015_Flight_Delays_and_Cancellations/airports.csv .
gsutil cp gs://globallogic-procamp-bigdata-datasets_bogdan/2015_Flight_Delays_and_Cancellations/airlines.csv .

hdfs dfs -mkdir /user/bogdan
hdfs dfs -put *.csv /user/bogdan/


# Run on a YARN cluster
./spark-submit \
--master yarn \
--deploy-mode cluster \
--num-executors 20 \
--executor-memory 1G \
--driver-memory 1G \
--conf spark.yarn.appMasterEnv.SPARK_HOME=/dev/null \
--conf spark.executorEnv.SPARK_HOME=/dev/null \
~/spark-df_2.12-1.0.jar


#To check results
hdfs dfs -ls /user/bogdan/Waco_Regional_Airport_CSV
 hdfs dfs -ls /user/bogdan/flights_canceled_json

###########################################################3
#Yarn Log:
Log Type: prelaunch.err

Log Upload Time: Fri Dec 25 01:47:47 +0000 2020

Log Length: 0


Log Type: prelaunch.out

Log Upload Time: Fri Dec 25 01:47:47 +0000 2020

Log Length: 70

Setting up env variables
Setting up job resources
Launching container

Log Type: stderr

Log Upload Time: Fri Dec 25 01:47:47 +0000 2020

Log Length: 2133

SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/usr/lib/spark/jars/slf4j-log4j12-1.7.16.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/usr/lib/hadoop/lib/slf4j-log4j12-1.7.25.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
20/12/25 01:45:51 INFO org.spark_project.jetty.util.log: Logging initialized @6438ms
20/12/25 01:45:51 INFO org.spark_project.jetty.server.Server: jetty-9.3.z-SNAPSHOT, build timestamp: unknown, git hash: unknown
20/12/25 01:45:51 INFO org.spark_project.jetty.server.Server: Started @6652ms
20/12/25 01:45:51 INFO org.spark_project.jetty.server.AbstractConnector: Started ServerConnector@61b6114d{HTTP/1.1,[http/1.1]}{0.0.0.0:43473}
20/12/25 01:45:54 WARN org.apache.spark.scheduler.cluster.YarnSchedulerBackend$YarnSchedulerEndpoint: Attempted to request executors before the AM has registered!
20/12/25 01:45:54 INFO org.apache.hadoop.yarn.client.RMProxy: Connecting to ResourceManager at procamp-cluster-m/10.142.0.49:8030
20/12/25 01:45:55 INFO org.apache.hadoop.conf.Configuration: resource-types.xml not found
20/12/25 01:45:55 INFO org.apache.hadoop.yarn.util.resource.ResourceUtils: Unable to find 'resource-types.xml'.
20/12/25 01:45:55 INFO org.apache.hadoop.yarn.util.resource.ResourceUtils: Adding resource type - name = memory-mb, units = Mi, type = COUNTABLE
20/12/25 01:45:55 INFO org.apache.hadoop.yarn.util.resource.ResourceUtils: Adding resource type - name = vcores, units = , type = COUNTABLE
20/12/25 01:46:50 WARN org.apache.spark.util.Utils: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.debug.maxToStringFields' in SparkEnv.conf.
20/12/25 01:47:44 INFO org.spark_project.jetty.server.AbstractConnector: Stopped Spark@61b6114d{HTTP/1.1,[http/1.1]}{0.0.0.0:0}
20/12/25 01:47:45 INFO org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl: Waiting for application to be successfully unregistered.

Log Type: stdout

Log Upload Time: Fri Dec 25 01:47:47 +0000 2020

Log Length: 387

Spirit Air Lines -> 34
Frontier Airlines Inc. -> 63
Alaska Airlines Inc. -> 67
American Eagle Airlines Inc. -> 133
JetBlue Airways -> 64
Atlantic Southeast Airlines -> 181
American Airlines Inc. -> 98
United Air Lines Inc. -> 94
Southwest Airlines Co. -> 86
Skywest Airlines Inc. -> 211
Virgin America -> 21
Delta Air Lines Inc. -> 156
US Airways Inc. -> 79
Hawaiian Airlines Inc. -> 17
