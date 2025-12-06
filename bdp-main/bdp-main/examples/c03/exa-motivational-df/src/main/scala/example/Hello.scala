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

    import spark.implicits._      

    println("\n--- Spark Scala motivation example solved with DataFrames ---")
    println(s"Starting Spark job on ${spark.sparkContext.master}:")

    val sc = spark.sparkContext;

    val demographicsData = List(
      (1, 18, false, "Belgium", "m", false, false)
    )
    val financesData = List(
      (1, true, true, false, 4143.93)
    )

    val demographicsDf = demographicsData.toDF("id", "age", "codingBootcamp", "country", "gender", "isEthnicMinority", "servedMilitary");
    val financesDf = financesData.toDF("id", "hasDebt", "hasFinancialDependents", "hasStudentsLoans", "income");
    
    val result = demographicsDf.join(financesDf, demographicsDf("id") === financesDf("id"))
                  .filter($"hasDebt" && $"hasFinancialDependents")
                  .filter($"country" === "Belgium")
                  .count()

    println(s"result: ${result}")

    System.in.read()

    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
