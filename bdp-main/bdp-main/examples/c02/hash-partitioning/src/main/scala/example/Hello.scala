package example

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner
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

    println("\n--- Spark Scala hash partitioning ---")
    println(s"Starting Spark job on ${spark.sparkContext.master}:")

    val sc = spark.sparkContext;

    // (ProductCategory, SaleAmount)
    val salesData = List(
      ("Electronics", 500),
      ("Books", 20),
      ("Electronics", 1300),
      ("Groceries", 45),
      ("Books", 30),
      ("Electronics", 300),
      ("Groceries", 150),
      ("Apparel", 100),
      ("Books", 70)
    )

    val salesRdd: RDD[(String, Int)] = sc.parallelize(salesData)
    
    val numPartitions = 4
    val partitioner = new HashPartitioner(numPartitions)

    val partitionedRdd = salesRdd.partitionBy(partitioner)

    val partitionContents = partitionedRdd.mapPartitionsWithIndex {
      (partitionId, iterator) =>
        iterator.map { case (key, value) => (partitionId, key, key.hashCode() % numPartitions, value) }.toList.iterator
    }

    println(s"Distribution of keys across $numPartitions partitions:")
    partitionContents.collect().foreach(println)
    // (1,Apparel,1,100)
    // (2,Books,2,20)
    // (2,Books,2,30)
    // (2,Books,2,70)
    // (3,Electronics,-1,500)
    // (3,Electronics,-1,1300)
    // (3,Groceries,-1,45)
    // (3,Electronics,-1,300)
    // (3,Groceries,-1,150)

    System.in.read()

    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
