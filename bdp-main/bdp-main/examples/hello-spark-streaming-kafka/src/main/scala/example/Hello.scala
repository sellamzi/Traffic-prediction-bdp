package example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, upper} 
// import org.apache.spark.sql.streaming.Trigger // additional!

// https://spark.apache.org/docs/latest/streaming/structured-streaming-kafka-integration.html#creating-a-kafka-source-for-streaming-queries

object Hello {

  val kafkaBootstrapServers = "kafka:9092"
  val inputTopic = "hello-input-topic"
  val outputTopic = "hello-output-topic"

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    if (!conf.contains("spark.master")) {
      conf.setMaster("local[*]")
    }

    val spark = SparkSession.builder
      .appName("hello-spark-streaming-kafka")
      .config(conf)
      .getOrCreate();

    // Checkpointing is essential for fault-tolerant, stateful streaming.
    // It stores processed offsets and, for stateful operations, intermediate state.
    // For a simple demo, a temp directory is fine. In production, this must be a
    // reliable distributed filesystem like HDFS or S3.
    val checkpointDir = s"/tmp/spark-streaming-checkpoints/${spark.sparkContext.applicationId}"

    println(s"Starting Spark streaming job on ${spark.sparkContext.master}:")
    println(s"  Consuming from Kafka topic: $inputTopic")
    println(s"  Producing to Kafka topic: $outputTopic")
    println(s"  Using Kafka bootstrap servers: $kafkaBootstrapServers")
    println(s"  Checkpointing to directory: $checkpointDir")

    // 1. Read data from the 'hello-input-topic' Kafka topic
    // Use readStream for continuous input
    val inputStreamDf = spark.readStream 
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", inputTopic)
      .option("startingOffsets", "earliest") // Process historical data if available when starting for the first time (no "endingOffsets" for streaming!)
      .load()

    // 2. Process the stream: Extract value, cast to string, and capitalize
    val processedStreamDf = inputStreamDf
      .select(col("value").cast("string").alias("original_message"))
      .filter(col("original_message").isNotNull)
      .select(upper(col("original_message")).alias("capitalized_message"))

    // 3. Prepare the stream for writing to the output Kafka topic
    // The Kafka sink requires a 'value' column for the message content.
    val outputStreamDf = processedStreamDf.select(col("capitalized_message").alias("value"))

    // 4. Write the processed stream to the 'hello-output-topic' Kafka topic
    val query = outputStreamDf.writeStream // Use writeStream for continuous output
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("topic", outputTopic)
      .option("checkpointLocation", checkpointDir) // Critical for recovery and exactly-once semantics
      .outputMode("append") // Append new processed rows to the sink
      .start() // Start the streaming query in the background

    println(s"Streaming query '${query.id}' started. Running in the background.")
    println("Waiting for termination... (press Ctrl+C to stop)")

    // 5. Keep the application running while the stream processes data
    query.awaitTermination()
  }}