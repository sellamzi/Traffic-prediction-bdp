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
      .groupBy(window($"timestamp", "10 minutes", "5 minutes"), $"word")
      .count()
      .orderBy("window")

    val query = result.writeStream
      .outputMode("complete") // because we don't set a watermark
      .format("console")
      .option("truncate", false) // ensure full timestamp is displayed in column
      .start()

    def ts(str: String): Timestamp = Timestamp.valueOf(str)

    inputStream.addData(Seq(("cat dog", ts("2025-01-01 12:02:00")))) 

    inputStream.addData(Seq(("dog dog", ts("2025-01-01 12:03:00")))) 
    query.processAllAvailable()

    inputStream.addData(Seq(("owl cat", ts("2025-01-01 12:07:00")))) 
    query.processAllAvailable()

    inputStream.addData(Seq(("dog", ts("2025-01-01 12:11:00")))) 

    inputStream.addData(Seq(("owl", ts("2025-01-01 12:13:00")))) 

    query.awaitTermination()
    println("Spark Session stopped. Program finished.\n")
  }

}
