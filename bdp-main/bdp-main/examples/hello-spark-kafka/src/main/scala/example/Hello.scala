package example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, upper} 

// https://spark.apache.org/docs/latest/streaming/structured-streaming-kafka-integration.html#creating-a-kafka-source-for-batch-queries

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
      .appName("hello-spark-kafka")
      .config(conf)
      .getOrCreate();

    println(s"Starting Spark batch job on ${spark.sparkContext.master}:")
    println(s"  Consuming from Kafka topic: $inputTopic")
    println(s"  Producing to Kafka topic: $outputTopic")
    println(s"  Using Kafka bootstrap servers: $kafkaBootstrapServers")

    // 1. Read data from the 'hello-input-topic' Kafka topic
    // This configuration defines a batch: reads messages from the earliest offset
    // up to the latest available offset at the time the query starts.
    val inputKafkaDf = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", inputTopic)
      .option("startingOffsets", "earliest") // Process from the beginning of the topic
      .option("endingOffsets", "latest")     // Process up to the latest messages available at query start
      .load()

    // Check if any data was read before proceeding
    if (inputKafkaDf.isEmpty) {
      println(s"No messages found in input topic '$inputTopic'. Spark job will exit.")
      spark.stop()
      return // Exit if no data to process
    }

    println(s"Schema of DataFrame read from '$inputTopic':")
    inputKafkaDf.printSchema() // Shows key, value, topic, partition, offset, etc.

    // 2. Process the messages:
    //    - Kafka messages are in the 'value' column as binary. Cast to string.
    //    - Capitalize the string messages.
    val processedDf = inputKafkaDf
      .select(col("value").cast("string").alias("original_message"))
      .filter(col("original_message").isNotNull) // Handle potential nulls after casting
      .select(upper(col("original_message")).alias("capitalized_message"))

    println("Preview of processed (capitalized) messages to be written:")
    processedDf.show(truncate = false)

    // 3. Prepare the DataFrame for writing to the output Kafka topic
    // The Kafka sink primarily expects a 'value' column for the message content.
    // You can also include 'key', 'topic', 'partition' columns if needed.
    val outputToKafkaDf = processedDf.select(col("capitalized_message").alias("value"))

    // 4. Write the processed DataFrame to the 'hello-output-topic' Kafka topic
    println(s"Writing processed messages to Kafka topic: '$outputTopic'...")
    outputToKafkaDf.write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("topic", outputTopic) // Specify the output topic for the sink
      .save() // This is a batch write operation

    println(s"Successfully wrote messages to Kafka topic: '$outputTopic'")

    spark.stop()
    println("Spark job finished.")
  }
}