package example

import scala.collection.mutable.Queue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, Dataset, DataFrame}
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
    val inputStream = MemoryStream[String]

    val lines: Dataset[String] = inputStream.toDS()
    val words: Dataset[String] = lines.flatMap(_.split(" "))
    val wordCounts: DataFrame = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    inputStream.addData(Seq("hello world", "hello spark"))
    query.processAllAvailable()
    // +-----+-----+
    // |value|count|
    // +-----+-----+
    // |hello|    2|
    // |spark|    1|
    // |world|    1|
    // +-----+-----+    

    inputStream.addData(Seq("spark streaming", "structured streaming"))
    // +----------+-----+
    // |     value|count|
    // +----------+-----+
    // |     hello|    2|
    // | streaming|    2|
    // |     spark|    2|
    // |structured|    1|
    // |     world|    1|
    // +----------+-----+    

    query.awaitTermination()
    println("Spark Session stopped. Program finished.\n")
  }

}
