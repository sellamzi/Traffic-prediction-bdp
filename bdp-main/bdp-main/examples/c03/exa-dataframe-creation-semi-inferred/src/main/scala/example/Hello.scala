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

    println("\n--- Spark Scala DataFrame from semi-structured data with inferred schema ---")
    println(s"Starting Spark job on ${spark.sparkContext.master}:")

    val salesDf = spark.read.json("/workspace/data/dataset/salesData/salesData.json")
    salesDf.printSchema()
    // root
    // |-- amount: double (nullable = true)
    // |-- category: string (nullable = true)
    // |-- customer_id: string (nullable = true)
    // |-- store: string (nullable = true)

    salesDf.show()
    // +------+-----------+-----------+--------+
    // |amount|   category|customer_id|   store|
    // +------+-----------+-----------+--------+
    // |1200.0|Electronics|       NULL|Brussels|
    // |  30.0|      Books|       c123|   Ghent|
    // |  45.0|  Groceries|       NULL|Brussels|
    // |2000.0|Electronics|       c456| Antwerp|
    // |  70.0|      Books|       NULL|Brussels|
    // +------+-----------+-----------+--------+
        
    System.in.read()

    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
