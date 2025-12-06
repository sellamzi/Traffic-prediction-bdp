package example

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc, desc}

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

    import spark.implicits._      

    println("\n--- Spark Scala DataFrame sorting example ---")
    println(s"Starting Spark job on ${spark.sparkContext.master}:")

    val salesData = List(
      ("Electronics", "Brussels", 1300.0),
      ("Books", "Ghent", 30.0),
      ("Groceries", "Brussels", 45.0),
      ("Electronics", "Antwerp", 2000.0),
      ("Books", "Brussels", 70.0)
    )
    val salesDf = salesData.toDF("category", "store", "amount")

    val sortedDf = salesDf.orderBy($"category".asc, $"amount".desc).show()
    // +-----------+--------+------+
    // |   category|   store|amount|
    // +-----------+--------+------+
    // |      Books|Brussels|  70.0|
    // |      Books|   Ghent|  30.0|
    // |Electronics| Antwerp|2000.0|
    // |Electronics|Brussels|1300.0|
    // |  Groceries|Brussels|  45.0|
    // +-----------+--------+------+    

    System.in.read()

    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
