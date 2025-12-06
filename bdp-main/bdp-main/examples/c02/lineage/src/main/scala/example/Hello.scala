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

    println("\n--- Spark Scala Exercise c02e01---")
    println(s"Starting Spark job on ${spark.sparkContext.master}:")

    val sc = spark.sparkContext;

    val data = Seq(
      "Apache Spark is fast",
      "Spark is powerful",
      "RDD lineage is a DAG"
    )

    val rdd0 = sc.parallelize(data)
    val rdd1 = rdd0.flatMap(_.split(" "))
    val rdd2 = rdd1.filter(_.length > 3)
    val rdd3 = rdd2.map(_.toLowerCase)
    // println("Result:")
    // rdd3.collect().foreach(println)

    println("\n=== RDD Lineage ===")
    println(rdd3.toDebugString)
    
     System.in.read() // pause

    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
