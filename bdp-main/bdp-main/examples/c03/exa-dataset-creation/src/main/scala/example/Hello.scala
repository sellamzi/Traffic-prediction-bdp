package example

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, Dataset}

case class Sale(category: String, store: String, amount: Double)

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

    println("\n--- Spark Scala RDD Dataset creation ---")
    println(s"Starting Spark job on ${spark.sparkContext.master}:")

    val sc = spark.sparkContext;

    val salesData  = List(
      ("Electronics", "Brussels", 1300.0),
      ("Books", "Ghent", 30.0),
      ("Groceries", "Brussels", 45.0),
      ("Electronics", "Antwerp", 2000.0),
      ("Books", "Brussels", 70.0)
    )

    // from collection
    val salesCollection: Seq[Sale] = salesData.map {
      case (category, store, amount) => Sale(category, store, amount)
    }
    val dsFromCollection: Dataset[Sale] = salesCollection.toDS()

    // from RDD
    val salesRdd: RDD[Sale] = sc.parallelize(salesCollection)
    val dsFromRdd: Dataset[Sale] = salesRdd.toDS()

    // from DataFrame
    val salesDf = salesData.toDF("category", "store", "amount")
    val dsFromDf: Dataset[Sale] = salesDf.as[Sale]

    System.in.read()

    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
