package example

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.round

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

    println("\n--- Spark Scala unbound columns example ---")
    println(s"Starting Spark job on ${spark.sparkContext.master}:")

    val roundedWithVAT = round($"amount" * 0.21)

    val salesData = List(
      ("Electronics", 1300.0),
      ("Books", 30.0),
      ("Groceries", 45.0)
    )
    val salesDf = salesData.toDF("category", "amount")

    // completely unrelated data
    val equipmentData = List(
      ("Linux server", 67382.23),
      ("Server rack", 23462.99),
      ("Cables", 9834.63)
    )
    val equipmentDf = equipmentData.toDF("name", "amount")

    salesDf.select(roundedWithVAT).show();
    // +-------------------------+
    // |round((amount * 0.21), 0)|
    // +-------------------------+
    // |                    273.0|
    // |                      6.0|
    // |                      9.0|
    // +-------------------------+

    equipmentDf.select(roundedWithVAT).show();
    // +-------------------------+
    // |round((amount * 0.21), 0)|
    // +-------------------------+
    // |                  14150.0|
    // |                   4927.0|
    // |                   2065.0|
    // +-------------------------+    

    System.in.read()

    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
