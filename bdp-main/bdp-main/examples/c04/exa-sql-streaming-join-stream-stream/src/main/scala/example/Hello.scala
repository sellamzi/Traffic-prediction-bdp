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

    val purchaseStream = MemoryStream[(Int, String, Timestamp)]
    val clickStream = MemoryStream[(Int, String, Timestamp)]

    val purchases = purchaseStream.toDS()
      .toDF("p_id", "item", "p_ts")
      .withWatermark("p_ts", "10 seconds")

    val clicks = clickStream.toDS()
      .toDF("c_id", "ad", "c_ts")
      .withWatermark("c_ts", "10 seconds")

    val joinCondition = expr("""
      p_id = c_id AND
      c_ts <= p_ts AND
      c_ts >= p_ts - interval 5 seconds
    """)

    val joined = purchases.join(clicks, joinCondition)

    val query = joined.writeStream
      .format("console")
      .outputMode("append") 
      .start()

    // Helper for timestamps
    def ts(s: String): Timestamp = Timestamp.valueOf(s)

    purchaseStream.addData(Seq((101, "lamp", ts("2025-01-01 12:00:05"))))
    clickStream.addData(Seq(
      (101, "ad235", ts("2025-01-01 12:00:05")), 
      (102, "ad38",  ts("2025-01-01 12:00:04")), 
      (101, "ad47",  ts("2025-01-01 12:00:02"))  
    ))

    query.processAllAvailable()

    purchaseStream.addData(Seq((102, "desk", ts("2025-01-01 12:00:06"))))
    clickStream.addData(Seq.empty)

    query.awaitTermination()
    println("Spark Session stopped. Program finished.\n")
  }

}
