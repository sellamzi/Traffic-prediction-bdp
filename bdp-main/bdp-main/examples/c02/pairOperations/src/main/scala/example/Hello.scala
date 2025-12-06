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

    println("\n--- Spark Scala pair operations ---")
    println(s"Starting Spark job on ${spark.sparkContext.master}:")

    val sc = spark.sparkContext;

    // (ProductCategory, SaleAmount)
    val salesData = List(
      ("Electronics", 500),
      ("Books", 20),
      ("Electronics", 1300),
      ("Groceries", 45),
      ("Books", 30),
      ("Electronics", 300)
    )

    val salesRdd: RDD[(String, Int)] = sc.parallelize(salesData)

    println("\n\n--- Aggregation ---")

    val groupedSalesRdd: RDD[(String, Iterable[Int])] = salesRdd.groupByKey()
    println("\n======")
    groupedSalesRdd.collect().foreach(println)

    val totalSalesRdd: RDD[(String, Int)] = salesRdd.reduceByKey(_ + _)
    println("\n======")
    totalSalesRdd.collect().foreach(println)
    
    val seqOp = (acc: (Int, Int), value: Int) => (acc._1 + value, acc._2 + 1)
    val combOp = (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    val salesSummaryRdd: RDD[(String, (Int, Int))] = salesRdd.aggregateByKey((0,0))(seqOp, combOp)
    println("\n======")
    salesSummaryRdd.collect().foreach(println)

    val averageSalesRdd: RDD[(String, Double)] = salesSummaryRdd.mapValues { case (sum, count) => sum.toDouble / count }
    println("\n======")
    averageSalesRdd.collect().foreach(println)

    System.in.read() // pause

    println("\n\n--- Sorting & other pair transformations  ---")
    val sortedSalesRdd: RDD[(String, Int)] = totalSalesRdd.sortByKey()
    println("\n====== sortByKey")
    sortedSalesRdd.collect().foreach(println) 

    val formattedSalesRdd: RDD[(String, String)] = sortedSalesRdd.mapValues(amount => s"EUR ${amount}.00")
    println("\n====== mapValues")
    formattedSalesRdd.collect().foreach(println)

    val detailedRecordsRdd: RDD[(String, (String, Double))] = formattedSalesRdd.flatMapValues { amountString =>
      val amount = amountString.stripPrefix("EUR").stripSuffix(".00").toDouble
      List(("Sales", amount), ("Tax", amount * 0.1))
    }
    println("\n====== flatMapValues")
    detailedRecordsRdd.collect().foreach(println)
    // (Books, (Sales, 50.0))
    // (Books, (Tax, 5.0))
    // (Electronics, (Sales, 2100.0))
    // (Electronics, (Tax, 210.0))
    // (Groceries, (Sales, 45.0))
    // (Groceries, (Tax, 4.5))

    val categoriesRdd: RDD[String] = detailedRecordsRdd.keys
    println("\n====== keys")
    categoriesRdd.distinct().collect().foreach(println)
    // Books
    // Electronics
    // Groceries


    val amountsRdd: RDD[(String, Double)] = detailedRecordsRdd.values
    println("\n====== values")
    amountsRdd.collect().foreach(println)
    // (Sales,50.0)
    // (Tax,5.0)
    // (Sales,2100.0)
    // (Tax,210.0)
    // (Sales,45.0)
    // (Tax,4.5)    

    System.in.read() // pause


    println("\n\n--- Joining ---")
    // Note: "Groceries" is missing, and "Apparel" is extra.
    val managerData = List(
      ("Electronics", "Alice"),
      ("Books", "Bob"),
      ("Apparel", "Charlie")
    )
    val categoryManagersRdd: RDD[(String, String)] = sc.parallelize(managerData)

    val innerJoinRdd: RDD[(String, (Int, String))] = totalSalesRdd.join(categoryManagersRdd)
    println("\n====== join")
    innerJoinRdd.collect().foreach(println)

    val leftOuterJoinRdd: RDD[(String, (Int, Option[String]))] = totalSalesRdd.leftOuterJoin(categoryManagersRdd)
    println("\n====== leftOuterJoin")
    leftOuterJoinRdd.collect().foreach(println)

    val rightOuterJoinRdd: RDD[(String, (Option[Int], String))] = totalSalesRdd.rightOuterJoin(categoryManagersRdd)
    println("\n====== rightOuterJoin")
    rightOuterJoinRdd.collect().foreach(println)

    System.in.read() // pause

    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
