#!/bin/bash
  
KAFKA_HOME=${1}

if [ -z "$KAFKA_HOME" ]
then
  echo "KAFKA_HOME parameter of $0 file is empty. Please specify it"
else
  echo "kafka_home = ${KAFKA_HOME}"
  ${KAFKA_HOME}/bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic bitcoin-transactions --partitions 1 --replication-factor 3
fi
exit 0