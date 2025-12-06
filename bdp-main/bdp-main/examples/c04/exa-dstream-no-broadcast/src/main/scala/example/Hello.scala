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

    println("\n--- Spark Scala DStream broadcast example ---")
    println(s"Starting Spark job on ${spark.sparkContext.master}:")

    val sc = spark.sparkContext;
    sc.setLogLevel("ERROR") // less logging
    val ssc = new StreamingContext(sc, Seconds(1))


    val blacklistWords: Seq[String] = Seq("the", "a", "an", "is", "in", "of")
    val rddQueue = new Queue[RDD[String]]()
    val words: DStream[String] = ssc.queueStream(rddQueue).flatMap(_.split(" "))
    val filteredWords: DStream[String] = words.filter(word => !blacklistWords.contains(word.toLowerCase))
    val wordPairs: DStream[(String, Int)] = filteredWords.map((_, 1))
    val wordCounts: DStream[(String, Int)] = wordPairs.reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()

    rddQueue.enqueue(sc.parallelize(Seq("hello world", "the dog is a good dog")))
    // (hello,1)
    // (world,1)
    // (dog,2)
    // (good,1)

    rddQueue.enqueue(sc.parallelize(Seq("hello spark", "a new world of spark")))
    // (hello,1)
    // (world,1)
    // (spark,2)
    // (new,1)

    Thread.sleep(3000)
    ssc.stop()
    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
