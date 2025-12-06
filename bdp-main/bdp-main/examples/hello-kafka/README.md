# Hello Kafka

## Commands
```
kafka-topics.sh --bootstrap-server kafka:9092 --create --topic test-topic --partitions 1 --replication-factor 1 --if-not-exists
kafka-topics.sh --bootstrap-server kafka:9092 --list --topic test-topic

kafka-console-producer.sh --bootstrap-server kafka:9092 --topic test-topic 

kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic test-topic --from-beginning 
kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic test-topic --group test-group --from-beginning --max-messages 1
kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic test-topic --group test-group --from-beginning 
kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic test-topic --group test-group2 --from-beginning 
 
kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic test-topic



cd hello-kafka
sbt run
kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic hello-output-topic --from-beginning
```