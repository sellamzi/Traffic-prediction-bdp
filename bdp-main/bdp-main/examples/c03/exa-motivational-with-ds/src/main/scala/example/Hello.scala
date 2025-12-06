package example

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.functions.avg

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

    println("\n--- Spark Scala motivation example for DataSets, with Datasets ---")
    println(s"Starting Spark job on ${spark.sparkContext.master}:")

    val sc = spark.sparkContext;

    val salesData = List(
      ("Electronics", "Brussels", 1300.0),
      ("Books", "Ghent", 30.0),
      ("Groceries", "Brussels", 45.0),
      ("Electronics", "Antwerp", 2000.0),
      ("Books", "Brussels", 70.0)
    )

    val salesDf = salesData.toDF("category", "store", "amount");
    val salesDs: Dataset[Sale] = salesDf.as[Sale]
    
    // statically type-safe
    val averageSalesDs1: Dataset[(String, Double)] = 
      salesDs.groupByKey(_.store) // statically type-safe             
             .mapGroups { (storeName, salesIterator) =>  // statically type-safe
                val salesList = salesIterator.toList
                val totalAmount = salesList.map(_.amount).sum
                val totalCount = salesList.length
                (storeName, totalAmount / totalCount)
              }    
    val mapped1 = averageSalesDs1.map { 
        case (store: String, avgAmount: Double) => s"1 Average amount: EUR ${avgAmount.round}"
    }

    // hybrid (ds-df)
    val averageSalesDs2: Dataset[(String, Double)] = 
      salesDs.groupByKey(_.store) // statically type-safe
             .agg(avg("amount").as[Double]) // not type-safe
    val mapped2 = averageSalesDs2.map { 
        case (store: String, avgAmount: Double) => s"2 Average amount: EUR ${avgAmount.round}"
    }

    // declarative (dynamic)
    val averageSalesDf3 = 
      salesDf.groupBy("store") 
             .agg(avg("amount")) 
    val mapped3 = averageSalesDf3.map { row =>
        val avgAmount: Double = row.get(1).asInstanceOf[Double]
        s"3 Average amount: EUR ${avgAmount.round}"
    }

    mapped1.collect().foreach(println)
    mapped2.collect().foreach(println)
    mapped3.collect().foreach(println)

    System.in.read()

    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
