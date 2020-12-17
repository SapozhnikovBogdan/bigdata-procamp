##Steps to be done for flights-mapreduce task run

1) Open ssh cloud console on cluster name-node and upload flights-mapreduce-1.0-jar-with-dependencies.jar to root folder 
2) chmod +x flights-mapreduce-1.0-jar-with-dependencies.jar 

3) gsutil cp gs://globallogic-procamp-bigdata-datasets_bogdan/2015_Flight_Delays_and_Cancellations/airlines.csv .
4) gsutil cp gs://globallogic-procamp-bigdata-datasets_bogdan/2015_Flight_Delays_and_Cancellations/flights.csv .

5) hdfs dfs -mkdir -p /flights-mapreduce/input/data 
6) hdfs dfs -mkdir -p /flights-mapreduce/input/dict
7) hdfs dfs -mkdir -p /flights-mapreduce/output

8) hdfs dfs -put ./flights.csv /flights-mapreduce/input/data
9) hdfs dfs -put ./airlines.csv /flights-mapreduce/input/dict

10) yarn jar flights-mapreduce-1.0-jar-with-dependencies.jar com.sapozhnikov.flights.mapreduce.FlightsDriver /flights-mapreduce/input/data /flights-mapreduce/output/result /flights-mapreduce/input/dict/airlines.csv /flights-mapreduce/output/temp

11) hdfs dfs -text /flights-mapreduce/output/result/*

