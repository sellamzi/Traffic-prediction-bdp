package example

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

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

    println("\n--- Spark Scala Exercise c02e01---")
    println(s"Starting Spark job on ${spark.sparkContext.master}:")

    val sc = spark.sparkContext;

    val base = sc.parallelize(Seq(1, 2, 3, 4, 5))

    val even = base.filter(_ % 2 == 0)   // branch 1
    val odd  = base.filter(_ % 2 == 1)   // branch 2

    val combined = even.union(odd)

    // println("Combined result: " + combined.collect().mkString(", "))

    println("\n=== RDD Lineage ===")
    println(combined.toDebugString)
    
    System.in.read() // pause

    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
