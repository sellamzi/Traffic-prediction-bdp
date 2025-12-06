package example

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import java.time.Duration
import java.util.{Collections, Properties}
import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success, Try}

object HelloKafka {

  val bootstrapServers = "kafka:9092" 
  val inputTopic = "hello-input-topic"
  val outputTopic = "hello-output-topic"
  val consumerGroupId = "hello-kafka-group"

  def createTopicsIfNotExists(adminClient: AdminClient): Unit = {
    println(s"Attempting to create topics: $inputTopic, $outputTopic")
    val topicsToCreate = List(
      new NewTopic(inputTopic, 1, 1.toShort), // topicName, numPartitions, replicationFactor
      new NewTopic(outputTopic, 1, 1.toShort)
    )

    try {
      val existingTopics = adminClient.listTopics().names().get()
      val newTopics = topicsToCreate.filterNot(nt => existingTopics.contains(nt.name()))
      if (newTopics.nonEmpty) {
        val createTopicsResult = adminClient.createTopics(newTopics.asJavaCollection)
        // Wait for topic creation to complete
        Await.result(Future.sequence(newTopics.map(nt => toScalaFuture(createTopicsResult.values().get(nt.name())))), scala.concurrent.duration.Duration("30s"))
        println(s"Successfully created topics: ${newTopics.map(_.name()).mkString(", ")}")
      } else {
        println("Topics already exist or no new topics specified.")
      }
    } catch {
      case e: TopicExistsException =>
        println(s"One or more topics already exist (caught TopicExistsException, which is fine here).")
      case e: Exception =>
        System.err.println(s"Error creating topics: ${e.getMessage}")
        throw e 
    }
  }

  // Helper to convert Kafka's KafkaFuture to Scala Future
  def toScalaFuture[T](kafkaFuture: org.apache.kafka.common.KafkaFuture[T]): Future[T] = {
    val promise = scala.concurrent.Promise[T]()
    kafkaFuture.whenComplete((result, throwable) => {
      if (throwable != null) promise.failure(throwable)
      else promise.success(result)
    })
    promise.future
  }


  def produceInitialMessages(producer: KafkaProducer[String, String], messages: Seq[String]): Unit = {
    println(s"Producing ${messages.length} messages to topic '$inputTopic'...")
    messages.zipWithIndex.foreach { case (msg, index) =>
      val record = new ProducerRecord[String, String](inputTopic, s"key-$index", msg)
      val sendFuture = producer.send(record, (metadata: RecordMetadata, exception: Exception) => {
        if (exception != null) {
          System.err.println(s"Failed to send message '$msg': ${exception.getMessage}")
        } else {
          println(s"Sent message '$msg' to topic ${metadata.topic()} partition ${metadata.partition()} @ offset ${metadata.offset()}")
        }
      })
    }
    producer.flush()
    println("Finished producing initial messages.")
  }

  def consumeProcessAndRepublish(consumer: KafkaConsumer[String, String], producer: KafkaProducer[String, String], expectedMessages: Int): Unit = {
    println(s"Subscribing to topic '$inputTopic' and waiting for messages...")
    consumer.subscribe(Collections.singletonList(inputTopic))

    var messagesProcessed = 0
    val startTime = System.currentTimeMillis()
    val maxWaitTimeMs = 30000 // Max wait 30 seconds for messages

    try {
      while (messagesProcessed < expectedMessages && (System.currentTimeMillis() - startTime) < maxWaitTimeMs) {
        val records = consumer.poll(Duration.ofMillis(1000)) // Poll with a timeout
        if (!records.isEmpty) {
          records.forEach { record =>
            println(s"Consumed message from ${record.topic()}: key=${record.key()}, value='${record.value()}', offset=${record.offset()}")
            val originalValue = record.value()
            val capitalizedValue = originalValue.toUpperCase()

            val outputRecord = new ProducerRecord[String, String](outputTopic, record.key(), capitalizedValue)
            producer.send(outputRecord, (metadata: RecordMetadata, exception: Exception) => {
              if (exception != null) {
                System.err.println(s"Failed to send capitalized message '$capitalizedValue': ${exception.getMessage}")
              } else {
                println(s"Sent capitalized message '$capitalizedValue' to topic ${metadata.topic()} partition ${metadata.partition()} @ offset ${metadata.offset()}")
              }
            })
            messagesProcessed += 1
          }
          producer.flush() // Flush after processing a batch of records
        } else {
          // println("No records received in this poll interval.")
        }
      }
    } catch {
      case e: Exception => System.err.println(s"Error during consumption/republishing: ${e.getMessage}")
    } finally {
      println(s"Finished consuming. Processed $messagesProcessed messages.")
    }
  }

  def main(args: Array[String]): Unit = {
    val adminProps = new Properties()
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all") // For stronger delivery guarantees

    val consumerProps = new Properties()
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId)
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") // Read from beginning if group is new

    var adminClientOpt: Option[AdminClient] = None
    var initialProducerOpt: Option[KafkaProducer[String, String]] = None
    var consumerOpt: Option[KafkaConsumer[String, String]] = None
    var republishProducerOpt: Option[KafkaProducer[String, String]] = None

    try {
      adminClientOpt = Some(AdminClient.create(adminProps))
      createTopicsIfNotExists(adminClientOpt.get)

      initialProducerOpt = Some(new KafkaProducer[String, String](producerProps))
      val messagesToProduce = Seq("hello", "world", "kafka", "scala", "topics")
      produceInitialMessages(initialProducerOpt.get, messagesToProduce)

      // Create a separate producer for republishing to avoid conflicts if settings were different
      republishProducerOpt = Some(new KafkaProducer[String, String](producerProps))
      consumerOpt = Some(new KafkaConsumer[String, String](consumerProps))
      consumeProcessAndRepublish(consumerOpt.get, republishProducerOpt.get, messagesToProduce.length)

      println("\n--- Verification ---")
      println(s"Check topic '$inputTopic' for original messages and '$outputTopic' for capitalized messages using a Kafka tool (e.g., Redpanda Console or kafka-console-consumer.sh).")

    } catch {
      case e: Exception => System.err.println(s"An error occurred: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      println("Closing Kafka clients...")
      Try(initialProducerOpt.foreach(_.close(Duration.ofSeconds(5))))
      Try(republishProducerOpt.foreach(_.close(Duration.ofSeconds(5))))
      Try(consumerOpt.foreach(_.close(Duration.ofSeconds(5))))
      Try(adminClientOpt.foreach(_.close(Duration.ofSeconds(5))))
      println("Program finished.")
    }
  }
}