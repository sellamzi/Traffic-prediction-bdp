# Flink + Kafka integration example

Example Flink *streaming* program that reads strings from an input topic, capitalizes them, and writes them to an output topic.
Make sure the input and output topic(s) for this program exist when running.

## Commands
These are some (groups of) commands useful for setting up, understanding, experimenting, testing, ...

```
cd hello-flink-kafka

kafka-topics.sh --bootstrap-server kafka:9092 --create --topic hello-input-topic --partitions 1 --replication-factor 1 --if-not-exists
kafka-topics.sh --bootstrap-server kafka:9092 --create --topic hello-output-topic -- partitions 1 --replication-factor 1 --if-not-exists

kafka-console-producer.sh --bootstrap-server kafka:9092 --topic hello-input-topic 

sbt run

sbt clean assembly

start-cluster.sh
flink run target/scala-2.13/myProject.jar
stop-cluster.sh
tail /opt/flink/log/flink-*-taskexecutor-*.out

flink run -m flink-jobmanager:8081 target/scala-2.13/myProject.jar
```