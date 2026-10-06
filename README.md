# Day 15 - Spark Scala UDF Practice

A Spark Scala project demonstrating how to create and use User Defined Functions (UDFs) for customer transaction risk analysis.

## Project Objective

The goal of this project is to:

- Create a custom Scala function for transaction risk classification
- Convert the Scala function into a Spark UDF
- Use the UDF with `withColumn`
- Compare the UDF result with Spark built-in `when` logic
- Register the UDF with the Spark SQL catalog
- Use the registered UDF through Spark SQL
- Generate customer-level risk summaries
- Write unit tests using ScalaTest

## Risk Classification Rules

| Transaction Amount | Risk Category |
|---|---|
| `< 2000` | LOW |
| `2000 - 4999` | MEDIUM |
| `5000 - 9999` | HIGH |
| `>= 10000` | CRITICAL |

## Input Data

The project uses:

`src/main/resources/data/transactions.csv`

The dataset contains 10 transactions for 5 customers.

Example:

```text
transaction_id,customer_id,amount
T101,C101,500
T102,C102,2500
T103,C103,5000
T104,C104,8500
T105,C105,15000
UDF Implementation

The main Scala function is:

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

The function is converted into a Spark UDF:

val riskUDF = udf(classifyRisk _)

It is then applied using withColumn:

val riskDF = df
  .withColumn("risk_category", riskUDF(col("amount")))
UDF vs Built-in Spark Function

The project also implements the same business logic using Spark's built-in when function.

Both approaches produce the same classifications for all 10 transactions.

SQL UDF

The UDF is registered with Spark SQL:

spark.udf.register(
  "classify_risk",
  classifyRisk _
)

It can then be used inside SQL queries:

SELECT
  transaction_id,
  customer_id,
  amount,
  classify_risk(amount) AS sql_risk_category
FROM transactions
Risk Category Results

The project produces the following risk distribution:

Risk Category	Count
LOW	2
MEDIUM	3
HIGH	3
CRITICAL	2

Total transactions: 10

Customer Risk Summary

The project also calculates:

Transaction count
Total transaction value
Maximum transaction
Average transaction
Customer-level risk category

Results:

Customer	Total Value	Maximum Transaction	Customer Risk
C104	26500	18000	CRITICAL
C105	19500	15000	CRITICAL
C102	9700	7200	HIGH
C103	6200	5000	HIGH
C101	3700	3200	MEDIUM
Project Structure
Day15-UDF-Practice/
│
├── build.sbt
├── project.properties
├── README.md
│
├── project/
│   └── build.properties
│
└── src/
    ├── main/
    │   ├── resources/
    │   │   └── data/
    │   │       └── transactions.csv
    │   │
    │   └── scala/
    │       └── com/
    │           └── day15udf/
    │               └── CustomerRiskApp.scala
    │
    └── test/
        └── scala/
            └── com/
                └── day15udf/
                    └── CustomerRiskAppSpec.scala
Technologies Used
Scala 2.12.18
Apache Spark 3.5.6
Spark SQL
SBT 2.0.8
ScalaTest 3.2.18
Java 17
How to Run

Compile the project:

sbt compile

Run the Spark application:

sbt run

Run all tests:

sbt test
Testing

The project contains 5 automated tests covering:

Transaction data contains 10 records
classifyRisk returns the correct categories
UDF produces the expected risk counts
UDF and built-in Spark logic produce the same results
Registered SQL UDF classifies transactions correctly

All 5 tests pass successfully.

Total number of tests run: 5
Tests: succeeded 5, failed 0
All tests passed.
Conclusion

This project demonstrates how Spark Scala UDFs can be used when custom business logic is required beyond standard Spark functions.

The project also demonstrates how the same business rule can be implemented using both a custom UDF and Spark's built-in functions, and how a UDF can be registered and used directly in Spark SQL.
