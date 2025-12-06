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

    println("\n--- Spark Scala DStream transformation that is RDD action ---")
    println(s"Starting Spark job on ${spark.sparkContext.master}:")

    val sc = spark.sparkContext;
    sc.setLogLevel("ERROR") // less logging
    val ssc = new StreamingContext(sc, Seconds(1))

    val rddQueue = new Queue[RDD[String]]()

    val words: DStream[String] = ssc.queueStream(rddQueue).flatMap(_.split(" "))
    val wordLength: DStream[Int] = words.map(_.length).reduce(_ + _)
    // java.lang.IllegalArgumentException: requirement failed: No output operations registered, so nothing to execute

    ssc.start()

    rddQueue.enqueue(sc.parallelize(Seq("hello spark", "hello world")))
    rddQueue.enqueue(sc.parallelize(Seq("spark streaming", "hello spark")))

    Thread.sleep(3000)
    ssc.stop()
    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
