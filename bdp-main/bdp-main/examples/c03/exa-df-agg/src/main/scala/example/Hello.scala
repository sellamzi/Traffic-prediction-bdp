package example

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{round, sum, avg, count}

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

    println("\n--- Spark Scala DataFrame aggregation with agg example ---")
    println(s"Starting Spark job on ${spark.sparkContext.master}:")

    val salesData = List(
      ("Electronics", "Brussels", 1300.0),
      ("Books", "Ghent", 30.0),
      ("Groceries", "Brussels", 45.0),
      ("Electronics", "Antwerp", 2000.0),
      ("Books", "Brussels", 70.0)
    )
    val salesDf = salesData.toDF("category", "store", "amount")

    val groupedData = salesDf.groupBy("store")

    val summaryDf = groupedData.agg(
      sum("amount").as("total_sales"),
      round(avg("amount")).as("average_sales"),
      count("*").as("number_of_sales")
    ).show()
    // +--------+-----------+-------------+---------------+
    // |   store|total_sales|average_sales|number_of_sales|
    // +--------+-----------+-------------+---------------+
    // |Brussels|     1415.0|        472.0|              3|
    // |   Ghent|       30.0|         30.0|              1|
    // | Antwerp|     2000.0|       2000.0|              1|
    // +--------+-----------+-------------+---------------+    

    System.in.read()

    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
