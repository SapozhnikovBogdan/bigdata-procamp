#!/bin/bash
export HADOOP_HOME=~/Install/hadoop-3.1.4
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

hadoop version
echo "map-reduce job start"
hadoop jar ./target/flights-mapreduce-1.0-jar-with-dependencies.jar com.sapozhnikov.flights.mapreduce.FlightsDriver ~/project/flights-mapreduce/test/input/data ~/project/flights-mapreduce/test/output/result ~/project/flights-mapreduce/test/input/dict/airlines.csv ~/project/flights-mapreduce/test/output/temp

echo "map-reduce job complete"
