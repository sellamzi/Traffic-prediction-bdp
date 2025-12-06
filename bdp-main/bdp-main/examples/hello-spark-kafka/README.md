# Spark + Kafka integration example

Example Spark program that reads strings from an input topic in *batch*, capitalizes them, and writes them to an output topic.
Make sure the input and output topic(s) for this program exist when running.

## Commands
These are some (groups of) commands useful for setting up, understanding, experimenting, testing, ...

```
cd hello-spark-kafka

kafka-topics.sh --bootstrap-server kafka:9092 --create --topic hello-input-topic --partitions 1 --replication-factor 1 --if-not-exists
kafka-topics.sh --bootstrap-server kafka:9092 --create --topic hello-output-topic -- partitions 1 --replication-factor 1 --if-not-exists

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 --class example.Hello --master local[*] target/scala-2.13/myProject.jar
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 --class example.Hello --master spark://spark-master:7077 target/scala-2.13/myProject.jar
```