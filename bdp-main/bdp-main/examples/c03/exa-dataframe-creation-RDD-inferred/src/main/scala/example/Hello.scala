package example

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

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

    println("\n--- Spark Scala RDD to dataframe with inferred schema ---")
    println(s"Starting Spark job on ${spark.sparkContext.master}:")

    val sc = spark.sparkContext;

    val salesData = List(
      ("Electronics", "Brussels", 1300.0),
      ("Books", "Ghent", 30.0),
      ("Groceries", "Brussels", 45.0),
      ("Electronics", "Antwerp", 2000.0),
      ("Books", "Brussels", 70.0)
    )

    val salesRdd: RDD[(String, String, Double)] = sc.parallelize(salesData)

    val df = salesRdd.toDF()
    df.printSchema()
    // root
    // |-- _1: string (nullable = true)
    // |-- _2: string (nullable = true)
    // |-- _3: double (nullable = false)
    
    val dfNames = salesRdd.toDF("category", "store", "amount")
    dfNames.printSchema()
    // root
    // |-- category: string (nullable = true)
    // |-- store: string (nullable = true)
    // |-- amount: double (nullable = false)
    
    val salesRddCase: RDD[Sale] = 
      salesRdd.map { case (category, store, amount) => Sale(category, store, amount) }

    val dfRef = salesRddCase.toDF()
    dfRef.printSchema()
    // root
    // |-- category: string (nullable = true)
    // |-- store: string (nullable = true)
    // |-- amount: double (nullable = false)

    System.in.read()

    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
