##Instruction how to build and run kafka project

1) Dowload Bitcoins_Kafka kafka consumer project to local machine 
2) Clear and package by maven Bitcoins_Kafka. It will be created "/target/bitcoins-kafka-consumer-1.0-jar-with-dependencies.jar" artifact
3) Upload the artifact to root user folder of procamp-cluster master node
4) Upload "/src/main/resources/create_bitcoin_transactions_topic.sh" shell-file to the folder from step 3
5) Make shell-file executable. Run command "chmod +x create_bitcoin_transactions_topic.sh"
6) Run shell-file to create bitcoin-transactions topic "./create_bitcoin_transactions_topic.sh /usr/lib/kafka" where "/usr/lib/kafka" - folder to Kafka
7) Run bitcoin-transactions topic consumer "java -Xms256m -Xmx512m -jar bitcoins-kafka-consumer-1.0-jar-with-dependencies.jar"
8) Upload "/src/main/resources/nifi_kafka_template.xml" template to NiFi cluster  and run it.   
