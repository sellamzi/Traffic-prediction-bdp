# Hello Flink Batch

Example Flink *batch* program that reads strings from an input topic, capitalizes them, and writes them to an output topic.
Make sure the input and output topic(s) for this program exist when running.

## Commands
These are some (groups of) commands useful for setting up, understanding, experimenting, testing, ...

```
cd hello-flink-batch

start-cluster.sh
flink run /opt/flink/examples/streaming/WordCount.jar
stop-cluster.sh
tail /opt/flink/log/flink-*-taskexecutor-*.out

flink run -m flink-jobmanager:8081 /opt/flink/examples/streaming/WordCount.jar

sbt run
sbt clean assembly
jar -tf target/scala-2.13/myProject.jar | grep "org/apache/flink/"

start-cluster.sh
flink run target/scala-2.13/myProject.jar
stop-cluster.sh
tail /opt/flink/log/flink-*-taskexecutor-*.out

flink run -m flink-jobmanager:8081 target/scala-2.13/myProject.jar
```