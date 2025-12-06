package example

import java.sql.Timestamp

import scala.collection.mutable.Queue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.execution.streaming.MemoryStream

object Hello {

def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    // If no master is set (e.g., via spark-submit), default to local[*]
    if (!conf.contains("spark.master")) {
      conf.setMaster("local[*]")
    }

    val spark = SparkSession.builder
      .appName("hello-spark")
      .config(conf)
      .getOrCreate();

    println(s"Starting Spark job on ${spark.sparkContext.master}:")

    val sc = spark.sparkContext;
    sc.setLogLevel("ERROR") // less logging

    import spark.implicits._

    implicit val sqlContext = spark.sqlContext
    val inputStream = MemoryStream[(String, Timestamp)]

    val result = inputStream.toDS()
      .flatMap { case (line, ts) => line.split(" ").map(word => (word, ts)) }
      .toDF("word", "timestamp")
      .withWatermark("timestamp", "10 minutes")
      .groupBy(window($"timestamp", "10 minutes", "5 minutes"), $"word")
      .count()

    val query = result.writeStream
      .outputMode("update") 
      .format("console")
      .option("truncate", false) // ensure full timestamp is displayed in column
      .start()

    def ts(str: String): Timestamp = Timestamp.valueOf(str)

    inputStream.addData(Seq(("dog", ts("2025-01-01 12:07:00")))) 
    inputStream.addData(Seq(("owl", ts("2025-01-01 12:08:00")))) 
    query.processAllAvailable()
    // +------------------------------------------+----+-----+
    // |window                                    |word|count|
    // +------------------------------------------+----+-----+
    // |{2025-01-01 12:00:00, 2025-01-01 12:10:00}|owl |1    |
    // |{2025-01-01 12:05:00, 2025-01-01 12:15:00}|owl |1    |
    // |{2025-01-01 12:05:00, 2025-01-01 12:15:00}|dog |1    |
    // |{2025-01-01 12:00:00, 2025-01-01 12:10:00}|dog |1    |
    // +------------------------------------------+----+-----+    
    println(s"watermark ${query.lastProgress.eventTime.get("watermark")}")
    // watermark 2025-01-01 11:58:00
    
    inputStream.addData(Seq(("dog", ts("2025-01-01 12:14:00")))) 
    inputStream.addData(Seq(("cat", ts("2025-01-01 12:09:00")))) 
    // inputStream.addData(Seq(("xxx", ts("2025-01-01 11:53:00"))))
    query.processAllAvailable()
    // +------------------------------------------+----+-----+
    // |window                                    |word|count|
    // +------------------------------------------+----+-----+
    // |{2025-01-01 12:00:00, 2025-01-01 12:10:00}|cat |1    |
    // |{2025-01-01 12:05:00, 2025-01-01 12:15:00}|cat |1    |
    // |{2025-01-01 12:05:00, 2025-01-01 12:15:00}|dog |2    |
    // |{2025-01-01 12:10:00, 2025-01-01 12:20:00}|dog |1    |
    // +------------------------------------------+----+-----+    
    println(s"watermark ${query.lastProgress.eventTime.get("watermark")}")
    // watermark 2025-01-01 12:04:00
    
    inputStream.addData(Seq(("cat", ts("2025-01-01 12:15:00")))) 
    inputStream.addData(Seq(("dog", ts("2025-01-01 12:08:00")))) 
    inputStream.addData(Seq(("owl", ts("2025-01-01 12:13:00")))) 
    inputStream.addData(Seq(("owl", ts("2025-01-01 12:21:00")))) 
    query.processAllAvailable()
    // +------------------------------------------+----+-----+
    // |window                                    |word|count|
    // +------------------------------------------+----+-----+
    // |{2025-01-01 12:00:00, 2025-01-01 12:10:00}|dog |2    |
    // |{2025-01-01 12:05:00, 2025-01-01 12:15:00}|dog |3    |
    // |{2025-01-01 12:05:00, 2025-01-01 12:15:00}|owl |2    |
    // |{2025-01-01 12:10:00, 2025-01-01 12:20:00}|cat |1    |
    // |{2025-01-01 12:10:00, 2025-01-01 12:20:00}|owl |1    |
    // |{2025-01-01 12:15:00, 2025-01-01 12:25:00}|cat |1    |
    // |{2025-01-01 12:15:00, 2025-01-01 12:25:00}|owl |1    |
    // |{2025-01-01 12:20:00, 2025-01-01 12:30:00}|owl |1    |
    // +------------------------------------------+----+-----+    
    println(s"watermark ${query.lastProgress.eventTime.get("watermark")}")
    // watermark 2025-01-01 12:11:00
    
    inputStream.addData(Seq(("donkey", ts("2025-01-01 12:04:00")))) 
    inputStream.addData(Seq(("bee", ts("2025-01-01 12:10:00")))) 
    inputStream.addData(Seq(("owl", ts("2025-01-01 12:17:00")))) 
    query.processAllAvailable()
    // +------------------------------------------+----+-----+
    // |window                                    |word|count|
    // +------------------------------------------+----+-----+
    // |{2025-01-01 12:05:00, 2025-01-01 12:15:00}|bee |1    |
    // |{2025-01-01 12:10:00, 2025-01-01 12:20:00}|bee |1    |
    // |{2025-01-01 12:10:00, 2025-01-01 12:20:00}|owl |2    |
    // |{2025-01-01 12:15:00, 2025-01-01 12:25:00}|owl |2    |
    // +------------------------------------------+----+-----+    
    println(s"watermark ${query.lastProgress.eventTime.get("watermark")}")
    // watermark 2025-01-01 12:11:00

    println("Spark Session stopped. Program finished.\n")
  }

}
