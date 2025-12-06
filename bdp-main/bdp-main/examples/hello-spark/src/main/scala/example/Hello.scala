package example

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

    println("\n--- Spark Scala Hello World Program ---")
    println(s"Starting Spark job on ${spark.sparkContext.master}:")

    val data = Seq("hello", "world", "spark", "scala")
    println(spark.sparkContext.parallelize(data).map(_.toUpperCase).collect().mkString("\n"));
    
    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
