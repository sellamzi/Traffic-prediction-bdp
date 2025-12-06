package example

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._

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

    // import spark.implicits._      

    println("\n--- Spark Scala RDD to dataframe with manually added schema ---")
    println(s"Starting Spark job on ${spark.sparkContext.master}:")

    val sc = spark.sparkContext;

    val schema = StructType(Seq(
      StructField("category", StringType, nullable = true),
      StructField("store", StringType, nullable = true),
      StructField("amount", DoubleType, nullable = false)
    ))

    val rowRdd: RDD[Row] = sc.textFile("/workspace/data/dataset/salesData/salesData.csv").map {
         case line =>
           val attributes = line.split(",")
           Row(attributes(0), attributes(1), attributes(2))
          }

    val salesDf = spark.createDataFrame(rowRdd, schema) 
    salesDf.printSchema()
    // root
    // |-- category: string (nullable = true)
    // |-- store: string (nullable = true)
    // |-- amount: double (nullable = false)
    
    System.in.read()

    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
