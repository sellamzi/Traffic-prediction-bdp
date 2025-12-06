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

    import spark.implicits._      

    println("\n--- Spark Scala initial DataFrame operations example ---")
    println(s"Starting Spark job on ${spark.sparkContext.master}:")

    val salesData = List(
      ("Electronics", "Brussels", 1300.0),
      ("Books", "Ghent", 30.0),
      ("Groceries", "Brussels", 45.0),
      ("Electronics", "Antwerp", 2000.0),
      ("Books", "Brussels", 70.0)
    )

    val salesDf = salesData.toDF("category", "store", "amount")
                            .select($"category", $"amount")
                            .filter($"amount" > 100)
                            .orderBy($"amount".desc)

    println("--- Query result: sales > 100, ordered by amount descending ---")
    salesDf.show()
    // +-----------+------+
    // |   category|amount|
    // +-----------+------+
    // |Electronics|2000.0|
    // |Electronics|1300.0|
    // +-----------+------+

    System.in.read()

    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
