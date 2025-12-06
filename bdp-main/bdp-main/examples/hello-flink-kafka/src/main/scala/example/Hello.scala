package example

import org.apache.flink.api.common.functions.MapFunction 
import org.apache.flink.streaming.api.datastream.DataStream 
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.base.DeliveryGuarantee 
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer


object Hello {

  val kafkaBootstrapServers = "kafka:9092"
  val inputTopic = "hello-input-topic"
  val outputTopic = "hello-output-topic"
  val consumerGroupId = "flink-group" // Using a different group ID

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    println("Starting Flink streaming job...")

    val kafkaSource: KafkaSource[String] = KafkaSource.builder[String]()
      .setBootstrapServers(kafkaBootstrapServers)
      .setTopics(inputTopic)
      .setGroupId(consumerGroupId)
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val inputDataStream: DataStream[String] = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
    val transformedDataStream: DataStream[String] = inputDataStream.map(new MapFunction[String, String]() {
      @Override
      def map(value: String): String = {
        println(s"Processing message: $value")
        value.toUpperCase()
      }
    })

    // val transformedDataStream: DataStream[String] = inputDataStream.map((value: String) => value.toUpperCase())
    
    val kafkaSink: KafkaSink[String] = KafkaSink.builder[String]()
      .setBootstrapServers(kafkaBootstrapServers)
      .setRecordSerializer(KafkaRecordSerializationSchema.builder()
        .setTopic(outputTopic)
        .setValueSerializationSchema(new SimpleStringSchema())
        .build()
      )
      .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .build()

    transformedDataStream.sinkTo(kafkaSink)

    env.execute("hello-flink-kafka")

    println("--- Flink streaming job finished ---\n")
  }
}