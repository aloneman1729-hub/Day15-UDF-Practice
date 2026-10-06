package com.day15udf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class Transaction(
  transaction_id: String,
  customer_id: String,
  amount: Double
)

object CustomerRiskApp {

  // Scala function used as the basis for our UDF
  def classifyRisk(amount: Double): String = {
    if (amount < 2000) {
      "LOW"
    } else if (amount < 5000) {
      "MEDIUM"
    } else if (amount < 10000) {
      "HIGH"
    } else {
      "CRITICAL"
    }
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Customer Risk UDF")
      .master("local[*]")
      .config("spark.hadoop.fs.defaultFS", "file:///")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("\n========================================")
    println("DAY 15 - UDF PRACTICE")
    println("Customer Transaction Risk Analysis")
    println("========================================\n")

    // ------------------------------------------------
    // 1. Load transaction data
    // ------------------------------------------------

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/transactions.csv")

    println("===== ORIGINAL TRANSACTIONS =====")
    df.show(false)

    // ------------------------------------------------
    // 2. Create Scala UDF
    // ------------------------------------------------

    val riskUDF = udf(classifyRisk _)

    // ------------------------------------------------
    // 3. Add calculated column using withColumn
    // ------------------------------------------------

    val riskDF = df
      .withColumn("risk_category", riskUDF(col("amount")))

    println("===== UDF RISK CLASSIFICATION =====")
    riskDF.show(false)

    // ------------------------------------------------
    // 4. Compare UDF with built-in Spark function
    // ------------------------------------------------

    val builtInDF = df.withColumn(
      "built_in_category",
      when(col("amount") < 2000, "LOW")
        .when(col("amount") < 5000, "MEDIUM")
        .when(col("amount") < 10000, "HIGH")
        .otherwise("CRITICAL")
    )

    println("===== BUILT-IN SPARK FUNCTION =====")
    builtInDF.show(false)

    println("===== UDF VS BUILT-IN COMPARISON =====")

    val comparisonDF = riskDF
      .join(
        builtInDF,
        Seq("transaction_id", "customer_id", "amount")
      )
      .select(
        col("transaction_id"),
        col("customer_id"),
        col("amount"),
        col("risk_category"),
        col("built_in_category")
      )

    comparisonDF.show(false)

    // ------------------------------------------------
    // 5. Register UDF with Spark session/catalog
    // ------------------------------------------------

    spark.udf.register(
      "classify_risk",
      classifyRisk _
    )

    // ------------------------------------------------
    // 6. Use registered UDF through SQL
    // ------------------------------------------------

    df.createOrReplaceTempView("transactions")

    val sqlRiskDF = spark.sql(
      """
        |SELECT
        |  transaction_id,
        |  customer_id,
        |  amount,
        |  classify_risk(amount) AS sql_risk_category
        |FROM transactions
        |ORDER BY amount
        |""".stripMargin
    )

    println("===== REGISTERED UDF THROUGH SQL =====")
    sqlRiskDF.show(false)

    // ------------------------------------------------
    // 7. Customer-level risk summary
    // ------------------------------------------------

    val customerRiskDF = riskDF
      .groupBy("customer_id")
      .agg(
        count("*").alias("transaction_count"),
        sum("amount").alias("total_transaction_value"),
        max("amount").alias("maximum_transaction"),
        avg("amount").alias("average_transaction")
      )
      .withColumn(
        "customer_risk",
        when(col("maximum_transaction") >= 10000, "CRITICAL")
          .when(col("maximum_transaction") >= 5000, "HIGH")
          .when(col("maximum_transaction") >= 2000, "MEDIUM")
          .otherwise("LOW")
      )
      .orderBy(desc("total_transaction_value"))

    println("===== CUSTOMER RISK SUMMARY =====")
    customerRiskDF.show(false)

    // ------------------------------------------------
    // 8. Risk category counts
    // ------------------------------------------------

    val riskCounts = riskDF
      .groupBy("risk_category")
      .count()
      .orderBy("risk_category")

    println("===== RISK CATEGORY COUNTS =====")
    riskCounts.show(false)

    println("\n========================================")
    println("CUSTOMER RISK PIPELINE COMPLETED")
    println("========================================\n")

    spark.stop()
  }
}
