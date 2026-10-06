package com.day15udf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class CustomerRiskAppSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark = SparkSession.builder()
      .appName("Customer Risk UDF Tests")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.hadoop.fs.defaultFS", "file:///")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }

    super.afterAll()
  }

  test("Transaction data should contain 10 records") {

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/transactions.csv")

    assert(df.count() == 10)
  }

  test("classifyRisk should return correct risk categories") {

    assert(CustomerRiskApp.classifyRisk(500) == "LOW")
    assert(CustomerRiskApp.classifyRisk(2500) == "MEDIUM")
    assert(CustomerRiskApp.classifyRisk(5000) == "HIGH")
    assert(CustomerRiskApp.classifyRisk(8500) == "HIGH")
    assert(CustomerRiskApp.classifyRisk(15000) == "CRITICAL")
  }

  test("UDF should produce correct risk category counts") {

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/transactions.csv")

    val riskUDF = udf(CustomerRiskApp.classifyRisk _)

    val riskDF = df.withColumn(
      "risk_category",
      riskUDF(col("amount"))
    )

    val counts = riskDF
      .groupBy("risk_category")
      .count()
      .collect()
      .map(row => row.getString(0) -> row.getLong(1))
      .toMap

    assert(counts("LOW") == 2)
    assert(counts("MEDIUM") == 3)
    assert(counts("HIGH") == 3)
    assert(counts("CRITICAL") == 2)
  }

  test("UDF and built-in Spark logic should produce the same results") {

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/transactions.csv")

    val riskUDF = udf(CustomerRiskApp.classifyRisk _)

    val udfDF = df
      .withColumn(
        "risk_category",
        riskUDF(col("amount"))
      )
      .select("transaction_id", "risk_category")

    val builtInDF = df
      .withColumn(
        "risk_category",
        when(col("amount") < 2000, "LOW")
          .when(col("amount") < 5000, "MEDIUM")
          .when(col("amount") < 10000, "HIGH")
          .otherwise("CRITICAL")
      )
      .select("transaction_id", "risk_category")

    val udfResults = udfDF.collect().toSet
    val builtInResults = builtInDF.collect().toSet

    assert(udfResults == builtInResults)
  }

  test("Registered SQL UDF should classify transactions correctly") {

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/transactions.csv")

    df.createOrReplaceTempView("transactions_test")

    spark.udf.register(
      "classify_risk_test",
      CustomerRiskApp.classifyRisk _
    )

    val result = spark.sql(
      """
        SELECT
          transaction_id,
          classify_risk_test(amount) AS risk_category
        FROM transactions_test
      """
    )

    assert(result.count() == 10)

    val criticalCount = result
      .filter(col("risk_category") === "CRITICAL")
      .count()

    assert(criticalCount == 2)
  }
}
