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

    println("\n--- Spark Scala implicit range partitioning ---")
    println(s"Starting Spark job on ${spark.sparkContext.master}:")

    val sc = spark.sparkContext;

    // (ProductCategory, SaleAmount)
    val salesData = List(
      ("Electronics", 500),
      ("Books", 20),
      ("Zen-Decor", 385),
      ("Electronics", 1300),
      ("Groceries", 45),
      ("Books", 30),
      ("Electronics", 300),
      ("Groceries", 150),
      ("Apparel", 100),
      ("Books", 70)
    )

    val salesRdd: RDD[(String, Int)] = sc.parallelize(salesData)
    
    val numPartitions = 2

    // implicit range partitioning
    val sortedSalesRdd = salesRdd.sortByKey(ascending = true, numPartitions)
    println(s"sorted sales RDD Partitioner: ${sortedSalesRdd.partitioner}")
    // sorted sales RDD Partitioner: Some(org.apache.spark.RangePartitioner@ff058e19)
    
    val partitionContents = sortedSalesRdd.mapPartitionsWithIndex {
      (partitionId, iterator) =>
        iterator.map { case (key, value) => (partitionId, key, value) }.toList.iterator
    }

    println(s"Distribution of keys across $numPartitions partitions:")
    partitionContents.collect().foreach(println)
    // (0,Apparel,100)
    // (0,Books,20)
    // (0,Books,30)
    // (0,Books,70)
    // (0,Electronics,500)
    // (0,Electronics,1300)
    // (0,Electronics,300)
    // (1,Groceries,45)
    // (1,Groceries,150)
    // (1,Zen-Decor,385)

    System.in.read()

    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
