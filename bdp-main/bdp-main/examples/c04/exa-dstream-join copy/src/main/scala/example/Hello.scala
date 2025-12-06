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

    println("\n--- Spark Scala DStream join with RDD ---")
    println(s"Starting Spark job on ${spark.sparkContext.master}:")

    val sc = spark.sparkContext;
    sc.setLogLevel("ERROR") // less logging
    val ssc = new StreamingContext(sc, Seconds(1))

    // ssc.checkpoint("checkpointDir")

    case class User(name: String, country: String) 
    case class Purchase(amount: Double)

    val usersList = Seq(
      (101, User("Alice", "BE")),
      (102, User("Bob", "FR")),
      (103, User("Charlie", "UK"))
      )
    val users: RDD[(Int, User)] = sc.parallelize(usersList).persist()

    val purchaseQueue = new Queue[RDD[(Int, Purchase)]]()
    val rawPurchases: DStream[(Int, Purchase)] = ssc.queueStream(purchaseQueue)
    val enrichedPurchases: DStream[(Int, (Purchase, User))] = rawPurchases.transform(_.join(users))
    
    enrichedPurchases.print()
    
    ssc.start()

    purchaseQueue.enqueue(sc.parallelize(Seq((101, Purchase(50.0)))))
    // (101,(Purchase(50.0),User(Alice,BE)))

    purchaseQueue.enqueue(sc.parallelize(Seq((102, Purchase(120.0)))))
    // (102,(Purchase(120.0),User(Bob,FR)))

    // () 
    
    Thread.sleep(4000)
    ssc.stop()
    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
