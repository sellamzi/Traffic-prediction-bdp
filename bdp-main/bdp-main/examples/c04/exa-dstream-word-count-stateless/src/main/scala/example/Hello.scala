package example

import scala.collection.mutable.Queue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.streaming.dstream.DStream

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

    println("\n--- Spark Scala DStream stateless word count example ---")
    println(s"Starting Spark job on ${spark.sparkContext.master}:")

    val sc = spark.sparkContext;
    sc.setLogLevel("ERROR") // less logging
    val ssc = new StreamingContext(sc, Seconds(1))

    val rddQueue = new Queue[RDD[String]]()

    val lines: DStream[String] = ssc.queueStream(rddQueue)

    val wordCounts: DStream[(String, Int)] = lines.flatMap(_.split(" "))
                     .map(word => (word, 1))
                     .reduceByKey(_ + _)

    wordCounts.print()                     

    ssc.start()

    println("--- micro-batch 1 ---")
    rddQueue.enqueue(sc.parallelize(Seq("hello spark", "hello world")))
    Thread.sleep(2000)

    println("--- micro-batch 2 ---")
    rddQueue.enqueue(sc.parallelize(Seq("spark streaming", "hello spark")))
    Thread.sleep(2000)

    ssc.stop()
    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
