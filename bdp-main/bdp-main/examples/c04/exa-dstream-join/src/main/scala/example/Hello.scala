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

    println("\n--- Spark Scala DStream join ---")
    println(s"Starting Spark job on ${spark.sparkContext.master}:")

    val sc = spark.sparkContext;
    sc.setLogLevel("ERROR") // less logging
    val ssc = new StreamingContext(sc, Seconds(1))

    // ssc.checkpoint("checkpointDir")
    
    val purchaseQueue = new Queue[RDD[(Int, String)]]()
    val clickQueue = new Queue[RDD[(Int, String)]]()    

    val purchases: DStream[(Int, String)] = ssc.queueStream(purchaseQueue)
    val clicks: DStream[(Int, String)] = ssc.queueStream(clickQueue)
    val joined = purchases.join(clicks)
    
    joined.print()
    
    ssc.start()

    purchaseQueue.enqueue(sc.parallelize(Seq((101, "lamp"))))
    clickQueue.enqueue(sc.parallelize(Seq((101, "ad235"), (102, "ad38"), (101, "ad47"))))
    // (101,(lamp,ad235))
    // (101,(lamp,ad47))

    purchaseQueue.enqueue(sc.parallelize(Seq((102, "desk"))))
    clickQueue.enqueue(sc.emptyRDD)
    // ()

    Thread.sleep(3000)
    ssc.stop()
    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
