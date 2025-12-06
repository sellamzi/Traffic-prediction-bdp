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

    println("\n--- Spark Scala implicit partitioning ---")
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

    val numPartitions = 4

    val salesRdd: RDD[(String, Int)] = sc.parallelize(salesData, numPartitions)

    println(s"Original RDD Partitioner: ${salesRdd.partitioner}")
    // Original RDD Partitioner: None

    val defaultPartitionedContents = salesRdd.mapPartitionsWithIndex {
      (partitionId, iterator) =>
        iterator.map { element => 
            (partitionId, element) }
    }

    println(s"Key-unaware distribution of keys across $numPartitions partitions:")
    defaultPartitionedContents.collect().foreach(println)
    // (0,(Electronics,500))
    // (0,(Books,20))
    // (1,(Zen-Decor,385))
    // (1,(Electronics,1300))
    // (1,(Groceries,45))
    // (2,(Books,30))
    // (2,(Electronics,300))
    // (3,(Groceries,150))
    // (3,(Apparel,100))
    // (3,(Books,70))    
        
    val groupedRdd: RDD[(String, Iterable[Int])] = salesRdd.groupByKey()
    println(s"Grouped RDD Partitioner: ${groupedRdd.partitioner}")
    // Grouped RDD Partitioner: Some(org.apache.spark.HashPartitioner@4)

    val partitionedContents = groupedRdd.mapPartitionsWithIndex {
      (partitionId, iterator) =>
        iterator.map { case (key, value) => 
            (partitionId, key, key.hashCode() % numPartitions, value) }.toList.iterator
    }

    println(s"Key-aware distribution of keys across $numPartitions partitions:")
    partitionedContents.collect().foreach(println)
    // (1,Apparel,1,Seq(100))
    // (2,Books,2,Seq(20, 30, 70))
    // (3,Electronics,-1,Seq(500, 1300, 300))
    // (3,Zen-Decor,3,Seq(385))
    // (3,Groceries,-1,Seq(45, 150))    

    System.in.read()

    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
