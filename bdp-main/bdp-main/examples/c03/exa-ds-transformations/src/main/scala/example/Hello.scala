package example

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset, KeyValueGroupedDataset}

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

    println("\n--- Spark Scala motivation example for DataSets ---")
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
    
    // does not change schema
    val highValueDs: Dataset[Sale] = salesDs.filter($"amount" > 1000)
    highValueDs.show()

    val groupedDs: KeyValueGroupedDataset[String, Sale] = salesDs.groupByKey(_.category)
    // groupedDs.show() NOT SUPPORTED

    // changes schema
    val categoryDf: DataFrame = salesDs.select($"category")
    categoryDf.show()

    // potentially changes schema
    val fullSelectDf: DataFrame = salesDs.select($"category", $"store", $"amount")
    fullSelectDf.show()

    // typed column
    val typedCategoryDf: Dataset[String] = salesDs.select($"category".as[String])
    typedCategoryDf.show()

    
    System.in.read()

    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
