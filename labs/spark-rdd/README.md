##Spark RDD lab steps

gsutil cp gs://globallogic-procamp-bigdata-datasets_bogdan/2015_Flight_Delays_and_Cancellations/flights.csv .
gsutil cp gs://globallogic-procamp-bigdata-datasets_bogdan/2015_Flight_Delays_and_Cancellations/airports.csv .

hdfs dfs -put flights.csv /user/yarn/
hdfs dfs -put airports.csv /user/yarn/

# Run on a YARN cluster
./spark-submit \
--master yarn-cluster \
--deploy-mode cluster \
--num-executors 20 \
--executor-memory 1G \
--driver-memory 1G \
--conf spark.yarn.appMasterEnv.SPARK_HOME=/dev/null \
--conf spark.executorEnv.SPARK_HOME=/dev/null \
~/spark-rdd-assembly-1.0.jar


#To check results
hdfs dfs -text /output/*

sapozhnikov_bogdan_gmail_com@procamp-cluster-m:/usr/lib/spark/bin$ hdfs dfs -text /output/*
1       Hartsfield-Jackson Atlanta International Airport       29492
2       Hartsfield-Jackson Atlanta International Airport       27366
3       Hartsfield-Jackson Atlanta International Airport       32775
4       Hartsfield-Jackson Atlanta International Airport       31383
5       Hartsfield-Jackson Atlanta International Airport       32425
6       Hartsfield-Jackson Atlanta International Airport       32739
7       Hartsfield-Jackson Atlanta International Airport       33735
8       Hartsfield-Jackson Atlanta International Airport       33725
9       Hartsfield-Jackson Atlanta International Airport       31331
11      Hartsfield-Jackson Atlanta International Airport       30903
12      Hartsfield-Jackson Atlanta International Airport       31030