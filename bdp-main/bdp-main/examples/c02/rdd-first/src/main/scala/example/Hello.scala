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

    // 2. Load data into an RDD
    val logLines: RDD[String] = sc.textFile("example.log") // TODO this can only be run on local master

    // 3. Transform the data to find error lines
    val errorLines: RDD[String] = logLines.filter(line => line.contains("ERROR"))

    // 4. Perform an action to get the result
    val numErrors: Long = errorLines.count()

    println(s"The log file contains ${numErrors} error lines.")
    
    // Example of another action to inspect the data
    println("First 5 error lines:")
    errorLines.take(5).foreach(println)
    println("^^^")
    
    System.in.read() // pause

    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
