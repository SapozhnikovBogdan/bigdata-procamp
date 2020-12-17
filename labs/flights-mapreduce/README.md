##Steps to be done for flights-mapreduce task run
1)Download https://github.com/SapozhnikovBogdan/bigdata-procamp/tree/main/labs/flights-mapreduce and build project 

2) Open ssh cloud console on cluster name-node and upload ./target/flights-mapreduce-1.0-jar-with-dependencies.jar to root folder 
3) chmod +x flights-mapreduce-1.0-jar-with-dependencies.jar 

4) gsutil cp gs://globallogic-procamp-bigdata-datasets_bogdan/2015_Flight_Delays_and_Cancellations/airlines.csv .
5) gsutil cp gs://globallogic-procamp-bigdata-datasets_bogdan/2015_Flight_Delays_and_Cancellations/flights.csv .

6) hdfs dfs -mkdir -p /flights-mapreduce/input/data 
7) hdfs dfs -mkdir -p /flights-mapreduce/input/dict
8) hdfs dfs -mkdir -p /flights-mapreduce/output

9) hdfs dfs -put ./flights.csv /flights-mapreduce/input/data
10) hdfs dfs -put ./airlines.csv /flights-mapreduce/input/dict

11) yarn jar flights-mapreduce-1.0-jar-with-dependencies.jar com.sapozhnikov.flights.mapreduce.FlightsDriver /flights-mapreduce/input/data /flights-mapreduce/output/result /flights-mapreduce/input/dict/airlines.csv /flights-mapreduce/output/temp

12) hdfs dfs -text /flights-mapreduce/output/result/*

