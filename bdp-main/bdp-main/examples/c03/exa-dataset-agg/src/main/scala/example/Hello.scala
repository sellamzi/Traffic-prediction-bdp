package example

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset, KeyValueGroupedDataset}
import org.apache.spark.sql.functions.sum


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

    println(s"Starting Spark job on ${spark.sparkContext.master}:")

    val sc = spark.sparkContext;

    val salesData = List[Sale](
      Sale("Electronics", "Brussels", 1300.0),
      Sale("Books", "Ghent", 30.0),
      Sale("Groceries", "Brussels", 45.0),
      Sale("Electronics", "Antwerp", 2000.0),
      Sale("Books", "Brussels", 70.0)
    )
    
    val salesDs: Dataset[Sale] = salesData.toDS();
    val totalSalesDs: Dataset[(String, Double)] = salesDs
          .groupByKey(_.store)
          .agg(sum($"amount").as[Double])
    totalSalesDs.show()
    
    System.in.read()

    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
