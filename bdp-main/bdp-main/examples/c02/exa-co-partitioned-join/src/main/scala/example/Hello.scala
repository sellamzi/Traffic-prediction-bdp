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

    println("\n--- Spark Scala co-partitioned join ---")
    println(s"Starting Spark job on ${spark.sparkContext.master}:")

    val sc = spark.sparkContext;

    val partitioner = new HashPartitioner(2);
    val a = sc.parallelize(Seq((1, "A"), (2, "B"))).partitionBy(partitioner)
    val b = sc.parallelize(Seq((1, "x"), (2, "y"))).partitionBy(partitioner)
    val joined = a.join(b)

    println(s"joined partitioner equal to parent a partitioner: ${joined.partitioner.equals(a.partitioner)}")
    println(s"joined partitioner equal to parent b partitioner: ${joined.partitioner.equals(b.partitioner)}")
    System.in.read()

    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
