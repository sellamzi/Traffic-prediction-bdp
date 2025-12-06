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

    println("\n--- Spark Scala DStream groupByKeyAndWindow ---")
    println(s"Starting Spark job on ${spark.sparkContext.master}:")

    val sc = spark.sparkContext;
    sc.setLogLevel("ERROR") // less logging
    val ssc = new StreamingContext(sc, Seconds(1))

    val rddQueue = new Queue[RDD[String]]()
    val lines: DStream[String] = ssc.queueStream(rddQueue)
    val wordPairs: DStream[(String, Int)] = lines
      .flatMap(_.split(" "))
      .map(word => (word.toLowerCase, 1))
    
    val windowedWordCounts: DStream[(String, Int)] = wordPairs
                                      .groupByKeyAndWindow(Seconds(3), Seconds(1))
                                      .mapValues(_.reduce(_ + _))                                

    windowedWordCounts.print()

    ssc.start()

    rddQueue.enqueue(sc.parallelize(Seq("one", "two", "three")))
    // (two,1)
    // (one,1)
    // (three,1)

    rddQueue.enqueue(sc.parallelize(Seq("two", "three", "four")))
    // (two,2)
    // (one,1)
    // (four,1)
    // (three,2)

    rddQueue.enqueue(sc.parallelize(Seq("three", "four", "five")))
    // (two,2)
    // (one,1)
    // (four,2)
    // (five,1)
    // (three,3)

    rddQueue.enqueue(sc.parallelize(Seq("four", "five", "six")))
    // (two,1)
    // (six,1)
    // (four,3)
    // (five,2)
    // (three,2)

    // (six,1)
    // (four,2)
    // (five,2)
    // (three,1)
        
    // (six,1)
    // (four,1)
    // (five,1)

    // ()

    Thread.sleep(8000)
    ssc.stop()
    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
