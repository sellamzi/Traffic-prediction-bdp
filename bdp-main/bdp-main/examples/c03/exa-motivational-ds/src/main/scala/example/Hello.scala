package example

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

    println("\n--- Spark Scala motivation example for DataSets ---")
    println(s"Starting Spark job on ${spark.sparkContext.master}:")

    val sc = spark.sparkContext;
    sc.setLogLevel("ERROR") // less logging

    val salesData = List(
      ("Electronics", "Brussels", 1300.0),
      ("Books", "Ghent", 30.0),
      ("Groceries", "Brussels", 45.0),
      ("Electronics", "Antwerp", 2000.0),
      ("Books", "Brussels", 70.0)
    )
    
    val salesDf = salesData.toDF("category", "store", "amount")
    val averageSalesDf = salesDf.groupBy($"store").avg("amount")
    // +--------+-----------------+
    // |   store|      avg(amount)|
    // +--------+-----------------+
    // |Brussels|471.6666666666667|
    // |   Ghent|             30.0|
    // | Antwerp|           2000.0|
    // +--------+-----------------+

    val mapped = averageSalesDf.map { row =>
      try {
        // val avgAmount: Double = row.get(0).asInstanceOf[Double] // wrong column index, so casting String to Double
        val avgAmount: Double = row.getDouble(0) // wrong column index, so casting String to Double
        s"Average amount: EUR ${avgAmount.round}"
      } catch {
        case e: Exception => s"Error: ${e.getMessage}"
        // Error: class java.lang.String cannot be cast to class java.lang.Double
      }
    }

    mapped.collect().foreach(println)

    System.in.read()

    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
