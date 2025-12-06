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

    println("\n--- Spark Scala DStream updateStateByKey ---")
    println(s"Starting Spark job on ${spark.sparkContext.master}:")

    val sc = spark.sparkContext;
    sc.setLogLevel("ERROR") // less logging
    val ssc = new StreamingContext(sc, Seconds(1))

    ssc.checkpoint("checkpointDir")

    def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
        println(s"${newValues} ${runningCount} => ${Option(runningCount.sum + newValues.size)}")
         Option(runningCount.sum + newValues.size)
    }

    val rddQueue = new Queue[RDD[String]]()
    val lines: DStream[String] = ssc.queueStream(rddQueue)
    val wordPairs: DStream[(String, Int)] = lines
      .flatMap(_.split(" "))
      .map(word => (word.toLowerCase, 1))
    val runningWordCounts: DStream[(String, Int)] = wordPairs.updateStateByKey[Int](updateFunction _)

    runningWordCounts.print()

    ssc.start()

    rddQueue.enqueue(sc.parallelize(Seq("hello world", "dog eat dog")))
    // (eat,1)
    // (hello,1)
    // (world,1)
    // (dog,2)

    rddQueue.enqueue(sc.parallelize(Seq("hello spark", "new world")))
    // (eat,1)
    // (hello,2)
    // (world,2)
    // (dog,2)
    // (spark,1)
    // (new,1)

    Thread.sleep(3000)
    ssc.stop()
    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
