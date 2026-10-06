# Day 15 - Spark Scala UDF Practice

A practical Spark Scala project demonstrating how to create and use
User Defined Functions (UDFs) for customer transaction risk classification.

## Project Objective

The goal of this project is to classify customer transactions into
different risk categories based on transaction amount.

The project demonstrates:

- Scala UDF creation
- Spark `withColumn`
- Spark built-in `when` function
- UDF vs built-in Spark comparison
- Registering a UDF with Spark SQL
- Using a registered UDF through SQL
- Customer-level aggregation
- Risk category analysis

---

## Technology Stack

- Scala 2.12.18
- Apache Spark 3.5.6
- Spark SQL
- SBT 2.0.8
- ScalaTest 3.2.18
- Java 17
- Ubuntu / WSL2

---

## Project Structure

```text
Day15-UDF-Practice/
│
├── build.sbt
├── .gitignore
├── README.md
│
├── project/
│   └── build.properties
│
└── src/
    └── main/
        ├── resources/
        │   └── data/
        │       └── transactions.csv
        │
        └── scala/
            └── com/
                └── day15udf/
                    └── CustomerRiskApp.scala
Input Data

The project uses the following transaction dataset:

Transaction ID	Customer ID	Amount
T101	C101	500
T102	C102	2500
T103	C103	5000
T104	C104	8500
T105	C105	15000
T106	C101	3200
T107	C102	7200
T108	C103	1200
T109	C104	18000
T110	C105	4500
Risk Classification Rules

Transactions are classified using the following business rules:

Transaction Amount	Risk Category
Less than 2000	LOW
2000 to less than 5000	MEDIUM
5000 to less than 10000	HIGH
10000 or greater	CRITICAL

The Scala function used for classification is:

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
UDF Implementation

The Scala function is converted into a Spark UDF:

val riskUDF = udf(classifyRisk _)

The UDF is then applied using withColumn:

val riskDF = df
  .withColumn("risk_category", riskUDF(col("amount")))
UDF Results

The transaction risk classification is:

Transaction	Amount	Risk
T101	500	LOW
T102	2500	MEDIUM
T103	5000	HIGH
T104	8500	HIGH
T105	15000	CRITICAL
T106	3200	MEDIUM
T107	7200	HIGH
T108	1200	LOW
T109	18000	CRITICAL
T110	4500	MEDIUM
UDF vs Built-in Spark Function

The project also performs the same classification using Spark's built-in
when function:

when(col("amount") < 2000, "LOW")
  .when(col("amount") < 5000, "MEDIUM")
  .when(col("amount") < 10000, "HIGH")
  .otherwise("CRITICAL")

The results of the custom UDF and the built-in Spark expression match
for all transactions.

This demonstrates that the custom UDF correctly implements the same
business logic.

Registering the UDF with Spark SQL

The UDF is registered with the Spark SQL catalog:

spark.udf.register(
  "classify_risk",
  classifyRisk _
)

It can then be used directly inside SQL:

SELECT
  transaction_id,
  customer_id,
  amount,
  classify_risk(amount) AS sql_risk_category
FROM transactions
ORDER BY amount
Customer Risk Summary

The project also aggregates transactions at customer level.

Customer	Transactions	Total Value	Maximum Transaction	Average Transaction	Risk
C104	2	26500	18000	13250.0	CRITICAL
C105	2	19500	15000	9750.0	CRITICAL
C102	2	9700	7200	4850.0	HIGH
C103	2	6200	5000	3100.0	HIGH
C101	2	3700	3200	1850.0	MEDIUM

Customer risk is determined from the maximum transaction amount.

Risk Category Counts

The final risk distribution is:

Risk Category	Count
LOW	2
MEDIUM	3
HIGH	3
CRITICAL	2

Total transactions analyzed: 10

How to Run

Make sure you are inside the project directory:

cd ~/Day15-UDF-Practice

Compile the project:

sbt compile

Run the Spark application:

sbt run
Expected Output

The application performs the following operations:

Loads transaction data.
Creates a Scala UDF.
Classifies transactions using the UDF.
Performs the same classification using Spark built-in functions.
Compares UDF and built-in results.
Registers the UDF with Spark SQL.
Executes the UDF through SQL.
Generates customer-level risk summaries.
Calculates risk category counts.

The final risk counts are:

CRITICAL  2
HIGH      3
LOW       2
MEDIUM    3
Key Concepts Learned
1. UDF

A User Defined Function allows custom business logic to be applied
to Spark DataFrame columns.

2. withColumn

withColumn is used to create a new column or replace an existing
column.

3. Spark Built-in Functions

Spark's built-in functions such as when can often perform logic
without creating a UDF.

4. SQL UDF Registration

A Scala function can be registered and then called from Spark SQL.

5. Aggregation

Spark aggregation functions such as:

count
sum
max
avg

are used to create customer-level analytics.

Conclusion

This project demonstrates how Spark Scala UDFs can be used to implement
custom business rules for transaction risk classification.

It also compares custom UDF logic with Spark's built-in functions and
demonstrates how a registered UDF can be used through Spark SQL.
