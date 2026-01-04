package example

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.HashPartitioner

import java.util.Properties
import scala.jdk.CollectionConverters._
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.apache.spark.ml.regression.LinearRegressionModel


object TrafficPredictor {

  // ------------------------- Schema / Types -------------------------
  object SchemaDefs {

    /**
     * Event = one fully enriched traffic event (speed+volume+static+time features)
     * Used as typed input for stateful streaming.
     */
    case class Event(
      node_id: Long,
      event_ts: java.sql.Timestamp,
      speed: Double,
      volume: Double,
      speed_limit: Double,
      length: Double,
      num_segments: Double,
      region_id: Double,
      hour_of_day: Int,
      day_of_week: Int
    )

    /**
     * Per-node streaming state
     */
    case class NodeState(lastSpeeds: List[Double])

    /**
     * Features = the row that will be fed to the ML model
     * includes lag features + rolling mean + static/time features.
     */ 
    case class Features(
      node_id: Long,
      event_ts: java.sql.Timestamp,
      speed: Double,
      volume: Double,
      lag1_speed: Double,
      lag6_speed: Double,
      rolling_mean_1h_speed: Double,
      speed_limit: Double,
      length: Double,
      num_segments: Double,
      region_id: Double,
      hour_of_day: Int,
      day_of_week: Int
    )

    /**
     * Incoming Kafka message schema
     */
    val kafkaMsgSchema: StructType = new StructType()
      .add("timestamp", StringType)
      .add("node_id", StringType)
      .add("value", DoubleType)
  }

  // ------------------------- Config -------------------------
  object Config {
    // Kafka settings / topics
    val bootstrap = "kafka:9092"
    val speedTopic = "speed"
    val volumeTopic = "volume"
    val outTopic = "traffic"

    // Dataset paths
    val staticPath = "/workspace/data/dataset/city_M_static_features.parquet"
    val speedPath  = "/workspace/data/dataset/city_M_speed_5000_100.parquet"
    val volumePath = "/workspace/data/dataset/city_M_volume_5000_100.parquet"

    // Saved model path (the 5-minute horizon model is persisted here for streaming inference)
    val modelH5Path = "/workspace/models/lr_h5"

    // Checkpoints for structured streaming
    val ckptConsole = "/workspace/checkpoints/preds5_console"
    val ckptKafka   = "/workspace/checkpoints/preds5_kafka"

    // Simple arg switch: run streaming pipeline when --streaming is present
    def isStreaming(args: Array[String]): Boolean =
      args.contains("--streaming") || args.contains("streaming")
  }

  // ------------------------- Kafka Admin -------------------------
  object KafkaAdmin {
    def ensureTopic(bootstrap: String, topic: String, partitions: Int = 3, rf: Short = 1): Unit = {
      val props = new Properties()
      props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
      val admin = AdminClient.create(props)
      try {
        admin.createTopics(List(new NewTopic(topic, partitions, rf)).asJava).all().get()
        println(s"Created topic '$topic'")
      } catch {
        case _: TopicExistsException =>
          println(s"Topic '$topic' already exists")
        case e: java.util.concurrent.ExecutionException
          if Option(e.getCause).exists(_.isInstanceOf[TopicExistsException]) =>
          println(s"Topic '$topic' already exists")
      } finally admin.close()
    }
  }

  // ------------------------- Static Features -------------------------
  object StaticFeatures {

    /**
     * Static parquet has no exlicit node_id column; I assign node_id by row index
     */
    def loadWithNodeId(spark: SparkSession): org.apache.spark.sql.DataFrame = {
      val staticDF = spark.read.parquet(Config.staticPath)
      val rddWithIndex = staticDF.rdd.zipWithIndex().map { case (row, idx) =>
        Row.fromSeq(idx.toLong +: row.toSeq)
      }
      val schema = StructType(StructField("node_id", LongType, false) +: staticDF.schema.fields)
      spark.createDataFrame(rddWithIndex, schema)
    }
  }

  // ------------------------- Batch Pipeline (Part 1) -------------------------
  object BatchPipeline {

    /**
     * VectorAssembler defines the exact feature order used for training and inference.
     * Reused in streaming to guarantee feature vector compatibility with the saved model.
     */
    def buildAssembler(): VectorAssembler = {
      val featureCols = Array(
        "speed", "volume",
        "lag1_speed", "lag6_speed", "rolling_mean_1h_speed",
        "speed_limit", "length", "num_segments", "region_id",
        "hour_of_day", "day_of_week"
      )
      new VectorAssembler()
        .setInputCols(featureCols)
        .setOutputCol("features")
    }

    def run(spark: SparkSession): Unit = {
      import spark.implicits._

      // ---- Task 1: Load + join + unpivot + join static ----
      val speedDF  = spark.read.parquet(Config.speedPath)
      val volumeDF = spark.read.parquet(Config.volumePath)

      val speedRenamed =
        speedDF.select(col("timestamp") +: speedDF.columns.filter(_ != "timestamp").map(c => col(c).as(s"${c}_speed")): _*)

      val volumeRenamed =
        volumeDF.select(col("timestamp") +: volumeDF.columns.filter(_ != "timestamp").map(c => col(c).as(s"${c}_volume")): _*)

      val dynamicDF = speedRenamed.join(volumeRenamed, Seq("timestamp"), "inner").persist()

      val staticWithId = StaticFeatures.loadWithNodeId(spark)

      val speedColumns = dynamicDF.columns.filter(_.endsWith("_speed"))
      val nodeEntries = speedColumns.map { speedCol =>
        val nodeIdStr = speedCol.stripPrefix("node_").stripSuffix("_speed")
        val nodeId    = nodeIdStr.toInt
        val volumeCol = s"node_${nodeIdStr}_volume"
        struct(lit(nodeId).as("node_id"), col(speedCol).as("speed"), col(volumeCol).as("volume"))
      }

      val dynamicLong = dynamicDF
        .select(col("timestamp"), explode(array(nodeEntries: _*)).as("entry"))
        .select(col("timestamp"), col("entry.node_id"), col("entry.speed"), col("entry.volume"))

      val fullDF = dynamicLong.join(broadcast(staticWithId), Seq("node_id"), "inner").persist() //avoids shuffling dynamicLong entirely for the join.

      // ---- Task 2: Feature engineering (lags, rolling mean, time features) ----
      val baseDF = fullDF
        .withColumn("ts_long", unix_timestamp(col("timestamp")))
        .withColumn("hour_of_day", hour(col("timestamp")))
        .withColumn("day_of_week", dayofweek(col("timestamp")))

      // Repartition by node_id before converting to RDD, helps downstream per-node processing
      val tsDF = baseDF.select(
        $"node_id", $"ts_long", $"speed", $"volume",
        $"speed_limit", $"length", $"num_segments", $"region_id",
        $"hour_of_day", $"day_of_week"
      ).repartition(col("node_id")).persist() // ensuring the data is already partitioned by node_id before converting to RDD, so Spark does less work during the shuffle.

      // Convert to pair RDD keyed by (node_id, ts) to sort within partitions
      val keyed = tsDF.rdd.map { row =>
        val nodeId = row.getAs[Number]("node_id").longValue()
        val ts     = row.getAs[Number]("ts_long").longValue()
        val speed  = row.getAs[Double]("speed")
        val volume = row.getAs[Double]("volume")
        val speedLimit = row.getAs[Double]("speed_limit")
        val length     = row.getAs[Double]("length")
        val numSeg     = row.getAs[Double]("num_segments")
        val regionId   = row.getAs[Double]("region_id")
        val hour       = row.getAs[Int]("hour_of_day")
        val dow        = row.getAs[Int]("day_of_week")
        ((nodeId, ts), (speed, volume, speedLimit, length, numSeg, regionId, hour, dow))
      }

      val numParts = spark.sparkContext.defaultParallelism
      val partitioner = new HashPartitioner(numParts)

      // Key insight:
      // repartition by node_id (via key hashing),
      // sort by (node_id, ts) within each partition,
      // This allows a single-pass scan per node in mapPartitions to compute rolling/lag features efficiently.
      val sorted = keyed.repartitionAndSortWithinPartitions(partitioner)

      val windowSize = 12
      val featuresRDD = sorted.mapPartitions { it =>
        val buff = scala.collection.mutable.ArrayBuffer[Row]()

        var currentNode: Long = Long.MinValue

        // rolling buffers for speeds
        val lastSpeeds = scala.collection.mutable.ArrayBuffer[Double]() 

        
        it.foreach { case ((nodeId, ts),
          (speed, volume, speedLimit, length, numSeg, regionId, hour, dow)) =>

          // node boundary -> reset state
          if (nodeId != currentNode) {
            currentNode = nodeId
            lastSpeeds.clear()
          }

          // compute features once we have enough history
          if (lastSpeeds.length >= windowSize) {
            val lag1 = lastSpeeds.last
            val lag6 = lastSpeeds(lastSpeeds.length - 6)
            val rollingMean = lastSpeeds.takeRight(windowSize).sum / windowSize.toDouble

            buff += Row(
              nodeId, ts, speed, volume,
              lag1, lag6, rollingMean,
              speedLimit, length, numSeg, regionId,
              hour, dow
            )
          }

          // update state
          lastSpeeds += speed
          if (lastSpeeds.length > windowSize) lastSpeeds.remove(0)
        }

        buff.iterator
      }

      val schema = StructType(Seq(
        StructField("node_id", LongType, false),
        StructField("ts_long", LongType, false),
        StructField("speed", DoubleType, false),
        StructField("volume", DoubleType, false),
        StructField("lag1_speed", DoubleType, false),
        StructField("lag6_speed", DoubleType, false),
        StructField("rolling_mean_1h_speed", DoubleType, false),
        StructField("speed_limit", DoubleType, true),
        StructField("length", DoubleType, true),
        StructField("num_segments", DoubleType, true),
        StructField("region_id", DoubleType, true),
        StructField("hour_of_day", IntegerType, true),
        StructField("day_of_week", IntegerType, true)
      ))

      val featureDF = spark.createDataFrame(featuresRDD, schema).persist()

      // ---- Task 3: Train models, forecast ----
      val assembler = buildAssembler()

      val speedAtTime = featureDF.select($"node_id", $"ts_long".as("ts_future"), $"speed".as("future_speed")).persist()

      def trainingForH(h: Int): org.apache.spark.sql.DataFrame = {
        val offset = h * 300
        featureDF.withColumn("ts_future", $"ts_long" + lit(offset))
          .join(speedAtTime, Seq("node_id", "ts_future"), "inner")
          .withColumnRenamed("future_speed", "label")
      }

      val w = Window.partitionBy("node_id").orderBy(col("ts_long").desc)
      val latestRows = featureDF.withColumn("rn", row_number().over(w)).filter($"rn" === 1).drop("rn").persist()
      val latestX = assembler.transform(latestRows).select($"node_id", $"ts_long", $"features").persist()

      val horizons = 1 to 6
      val predPerH = horizons.map { h =>
        val train = assembler.transform(trainingForH(h)).select($"label", $"features")
        val Array(tr, te) = train.randomSplit(Array(0.8, 0.2), seed = 42L)

        val lr = new LinearRegression().setLabelCol("label").setFeaturesCol("features").setMaxIter(50).setRegParam(0.1)
        val model = lr.fit(tr)

        if (h == 1) model.write.overwrite().save(Config.modelH5Path) // save 5-min model for Part 2

        model.transform(latestX).select($"node_id", $"ts_long", $"prediction".as(s"pred_${h*5}"))
      }

      val joined = predPerH.reduce((a,b) => a.join(b, Seq("node_id","ts_long"), "inner"))
      val forecast = joined.select(
        $"node_id",
        concat_ws(",", $"pred_5", $"pred_10", $"pred_15", $"pred_20", $"pred_25", $"pred_30").as("predictions_csv")
      )

      forecast.orderBy("node_id").select("predictions_csv")
        .coalesce(1)
        .write.mode("overwrite")
        .text("/workspace/output/forecast.txt")
    }
  }

  // ------------------------- Streaming Pipeline (Part 2) -------------------------
  object StreamingPipeline {
    import SchemaDefs._

    /**
     * Parse Kafka messages:
     * - cast binary value -> string
     * - parse JSON with kafkaMsgSchema
     * - extract node_id numeric part
     * - expose as (event_ts, node_id, speed/volume)
     */
    def parseKafka(raw: org.apache.spark.sql.DataFrame, valueCol: String): org.apache.spark.sql.DataFrame =
      raw.selectExpr("CAST(value AS STRING) AS json")
        .select(from_json(col("json"), kafkaMsgSchema).as("m"))
        .select(
          to_timestamp(col("m.timestamp")).as("event_ts"),
          regexp_extract(col("m.node_id"), "(\\d+)", 1).cast("long").as("node_id"),
          col("m.value").as(valueCol)
        )

    def run(spark: SparkSession): Unit = {
      import spark.implicits._

      // 1) Read speed + volume topics
      val speedRaw = spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", Config.bootstrap)
        .option("subscribe", Config.speedTopic)
        .option("startingOffsets", "latest")
        .load()

      val volumeRaw = spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", Config.bootstrap)
        .option("subscribe", Config.volumeTopic)
        .option("startingOffsets", "latest")
        .load()

      val speedStream  = parseKafka(speedRaw,  "speed")
      val volumeStream = parseKafka(volumeRaw, "volume")

      // 2) Normalize timestamps to 5-minute buckets and join
      val speedNorm = speedStream.withColumn("event_ts_5m", from_unixtime((unix_timestamp($"event_ts")/300)*300).cast("timestamp"))
      val volNorm   = volumeStream.withColumn("event_ts_5m", from_unixtime((unix_timestamp($"event_ts")/300)*300).cast("timestamp"))

      val trafficStream =
        speedNorm.as("s").join(volNorm.as("v"), Seq("node_id","event_ts_5m"), "inner")
          .select($"node_id", $"event_ts_5m".as("event_ts"), $"s.speed".as("speed"), $"v.volume".as("volume"))

      // 3) Join static
      val staticWithId = StaticFeatures.loadWithNodeId(spark)
      val enriched = trafficStream.join(broadcast(staticWithId), Seq("node_id"), "left")
        .withColumn("hour_of_day", hour($"event_ts"))
        .withColumn("day_of_week", dayofweek($"event_ts"))

      val eventsDS = enriched.select(
        $"node_id", $"event_ts", $"speed", $"volume",
        $"speed_limit", $"length", $"num_segments", $"region_id",
        $"hour_of_day", $"day_of_week"
      ).as[Event]

      // 4) Online lag features using state
      val featuresDS = eventsDS.groupByKey(_.node_id)
        .flatMapGroupsWithState[NodeState, Features](
          outputMode = org.apache.spark.sql.streaming.OutputMode.Append(),
          timeoutConf = GroupStateTimeout.NoTimeout
        ) { case (nodeId, rows, state) =>
          val sorted = rows.toList.sortBy(_.event_ts.getTime)
          var st = state.getOption.getOrElse(NodeState(Nil))
          val out = scala.collection.mutable.ListBuffer[Features]()

          sorted.foreach { e =>
            val hist = st.lastSpeeds
            if (hist.length >= 12) {
              val lag1 = hist.head
              val lag6 = hist.lift(5).getOrElse(lag1)
              val mean = hist.take(12).sum / 12.0
              out += Features(nodeId, e.event_ts, e.speed, e.volume, lag1, lag6, mean,
                e.speed_limit, e.length, e.num_segments, e.region_id,
                e.hour_of_day, e.day_of_week
              )
            }
            st = NodeState((e.speed :: hist).take(12))
          }

          state.update(st)
          out.iterator
        }

      val featureDFStream = featuresDS.toDF()

      // 5) Assemble features + load pretrained 5-min model + predict
      val assembler = BatchPipeline.buildAssembler().setHandleInvalid("keep")
      val assembled = assembler.transform(featureDFStream)

      val model5 = LinearRegressionModel.load(Config.modelH5Path)

      val preds = model5.transform(assembled)
        .select(
          $"node_id",
          $"event_ts".as("ts_now"),
          (col("event_ts") + expr("INTERVAL 5 MINUTES")).as("ts_plus_5m"),
          $"prediction".as("pred_speed_5m")
        )

      // 6) Output: console first (debug), later Kafka topic 'traffic'
      //val qConsole = preds.writeStream
      //  .format("console")
      //  .option("truncate", "false")
      //  .option("checkpointLocation", Config.ckptConsole)
      //  .start()

      // Write output to Kafka:
      KafkaAdmin.ensureTopic(Config.bootstrap, Config.outTopic)
      val outKafka = preds.selectExpr(
        "CAST(node_id AS STRING) AS key",
        "to_json(named_struct('node_id', node_id, 'ts_now', ts_now, 'ts_plus_5m', ts_plus_5m, 'pred_speed_5m', pred_speed_5m)) AS value"
      )
      val qKafka = outKafka.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", Config.bootstrap)
        .option("topic", Config.outTopic)
        .option("checkpointLocation", Config.ckptKafka)
        .start()

      qKafka.awaitTermination()
    }
  }

  // ------------------------- Main entrypoint -------------------------
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    if (!conf.contains("spark.master")) conf.setMaster("local[*]")

    val spark = SparkSession.builder.appName("hello-spark").config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    if (Config.isStreaming(args)) {
      StreamingPipeline.run(spark)
    } else BatchPipeline.run(spark)

    spark.stop()
  }
}
