# 🚀 GB-Scale Data Processing with Apache Spark

<div align="center">

# ⚡ Apache Spark Large-Scale Data Processing 

### Process Millions of Records Efficiently with Spark DataFrames

![Apache Spark](https://img.shields.io/badge/Apache%20Spark-FF9900?style=for-the-badge\&logo=apachespark\&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge\&logo=python\&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge)
![Big Data](https://img.shields.io/badge/Big%20Data-Processing-blue?style=for-the-badge)
![Linux](https://img.shields.io/badge/Linux-Administration-FCC624?style=for-the-badge\&logo=linux\&logoColor=black)
![Data Engineering](https://img.shields.io/badge/Data-Engineering-green?style=for-the-badge)

### 📊 Analyze Millions of Records Using Apache Spark

</div>

---

# 📖 Overview

In this lab, you will learn how to process **GB-scale datasets** using Apache Spark on a single Linux machine.

You will generate millions of e-commerce transactions, load them into Spark DataFrames, perform large-scale analytics, optimize performance, and export results into multiple formats.

---

# 🎯 Learning Objectives

By completing this lab, you will be able to:

✅ Install and configure Apache Spark

✅ Generate large datasets

✅ Load GB-scale data using Spark DataFrames

✅ Apply transformations and aggregations

✅ Optimize Spark jobs using caching and partitioning

✅ Export processed data into CSV, JSON, and Parquet formats

---

# 📋 Prerequisites

Before starting this lab, ensure you have:

* Basic Linux command-line skills
* Understanding of Python programming
* Familiarity with filtering and aggregation concepts
* Basic knowledge of distributed computing

---

# 🖥️ System Requirements

| Component  | Requirement    |
| ---------- | -------------- |
| OS         | Ubuntu 20.04+  |
| RAM        | Minimum 4GB    |
| Java       | OpenJDK 11     |
| Python     | Python 3.x     |
| Disk Space | 5GB+ Available |

---

# 🏗️ Processing Architecture

```text
                   ┌─────────────────────┐
                   │  transactions.csv   │
                   │  5 Million Records  │
                   └──────────┬──────────┘
                              │
                              ▼

                 ┌────────────────────────┐
                 │     Apache Spark       │
                 │     DataFrame Engine   │
                 └──────────┬─────────────┘
                            │
         ┌──────────────────┼──────────────────┐
         │                  │                  │
         ▼                  ▼                  ▼

 ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
 │ Country KPI  │   │ Category     │   │ Top Customer │
 │ Aggregation  │   │ Trends       │   │ Analysis     │
 └──────┬───────┘   └──────┬───────┘   └──────┬───────┘
        │                  │                  │
        ▼                  ▼                  ▼

      CSV             Parquet             JSON
```

---

# ⚙️ Environment Setup

---

## 🔹 Install Java Development Kit

```bash
sudo apt update

sudo apt install -y openjdk-11-jdk

java -version
```

Expected Output:

```text
openjdk version "11.x.x"
```

---

## 🔹 Install Apache Spark

```bash
cd ~

wget https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz

tar -xzf spark-3.4.1-bin-hadoop3.tgz

sudo mv spark-3.4.1-bin-hadoop3 /opt/spark
```

---

## 🔹 Configure Environment Variables

```bash
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc

echo 'export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin' >> ~/.bashrc

echo 'export PYSPARK_PYTHON=python3' >> ~/.bashrc

source ~/.bashrc
```

---

## 🔹 Verify Spark Installation

```bash
spark-submit --version

pyspark --version
```

---

## 🔹 Install Python Dependencies

```bash
sudo apt install -y python3-pip

pip3 install pyspark pandas
```

---

# 🚀 Task 1: Generate Large Dataset

---

## 🔹 Step 1: Create Data Generator

Create:

```bash
nano generate_data.py
```

Add:

```python
import csv
import random
from datetime import datetime, timedelta

def generate_transactions(num_records, output_file):

    categories = [
        "Electronics",
        "Clothing",
        "Books",
        "Home",
        "Sports"
    ]

    countries = [
        "USA",
        "UK",
        "Canada",
        "Germany",
        "France",
        "Japan"
    ]

    with open(output_file, "w", newline="") as file:

        writer = csv.writer(file)

        writer.writerow([
            "transaction_id",
            "date",
            "customer_id",
            "product_category",
            "amount",
            "country"
        ])

        for i in range(num_records):

            transaction_id = i + 1

            random_date = (
                datetime.now() -
                timedelta(days=random.randint(0,365))
            )

            customer_id = random.randint(1000,99999)

            category = random.choice(categories)

            amount = round(
                random.uniform(10,1000),2
            )

            country = random.choice(countries)

            writer.writerow([
                transaction_id,
                random_date.strftime("%Y-%m-%d"),
                customer_id,
                category,
                amount,
                country
            ])

if __name__ == "__main__":

    generate_transactions(
        5000000,
        "transactions.csv"
    )

    print("Data generation complete")
```

---

## 🔹 Step 2: Generate Dataset

```bash
python3 generate_data.py
```

Verify:

```bash
ls -lh transactions.csv

du -h transactions.csv
```

Expected:

```text
~500MB CSV File
5 Million Records
```

---

# ⚡ Task 2: Create Spark Processing Pipeline

---

## 🔹 Step 1: Create Processing Script

```bash
nano process_data.py
```

---

## 🔹 Create Spark Session

```python
from pyspark.sql import SparkSession

def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("GB-Scale Processing")
        .master("local[*]")
        .config("spark.driver.memory","4g")
        .config(
            "spark.sql.shuffle.partitions",
            "8"
        )
        .getOrCreate()
    )

    return spark
```

---

## 🔹 Load CSV Dataset

```python
def load_data(spark,file_path):

    df = (
        spark.read
        .option("header",True)
        .option("inferSchema",True)
        .csv(file_path)
    )

    return df
```

---

## 🔹 Test Data Loading

```python
spark = create_spark_session()

df = load_data(
    spark,
    "transactions.csv"
)

df.printSchema()

df.show(10)

print(
    f"Total Records: {df.count()}"
)

spark.stop()
```

Execute:

```bash
python3 process_data.py
```

---

# 🔄 Task 3: Data Cleaning

---

## 🔹 Clean Data Function

```python
def clean_data(df):

    return (
        df.na.drop()
        .filter(col("amount") > 0)
    )
```

---

# 📊 Task 4: Country Metrics Analysis

---

## 🔹 Country-Level Aggregation

```python
def calculate_country_metrics(df):

    return (
        df.groupBy("country")
          .agg(
              count("*")
              .alias("total_transactions"),

              round(
                  sum("amount"),2
              ).alias("total_revenue"),

              round(
                  avg("amount"),2
              ).alias(
                  "avg_transaction_value"
              )
          )
          .orderBy(
              col("total_revenue")
              .desc()
          )
    )
```

Display:

```python
country_metrics.show(10)
```

Save:

```python
country_metrics.write.mode(
    "overwrite"
).csv(
    "output/country_metrics",
    header=True
)
```

---

# 📈 Task 5: Category Trend Analysis

---

## 🔹 Monthly Category Trends

```python
def calculate_category_trends(df):

    return (
        df.withColumn(
            "year",
            year("date")
        )
        .withColumn(
            "month",
            month("date")
        )
        .groupBy(
            "product_category",
            "year",
            "month"
        )
        .agg(
            round(
                sum("amount"),2
            ).alias("total_sales"),

            count("*")
            .alias(
                "transaction_count"
            )
        )
        .orderBy(
            "year",
            "month"
        )
    )
```

Save:

```python
category_trends.write.mode(
    "overwrite"
).parquet(
    "output/category_trends"
)
```

---

# 👑 Task 6: Find Top Customers

---

## 🔹 Customer Spending Analysis

```python
def find_top_customers(
        df,
        top_n=100
):

    return (
        df.groupBy(
            "customer_id"
        )
        .agg(
            round(
                sum("amount"),2
            ).alias("total_spent"),

            count("*")
            .alias(
                "purchase_count"
            )
        )
        .orderBy(
            col("total_spent")
            .desc()
        )
        .limit(top_n)
    )
```

Save:

```python
top_customers.write.mode(
    "overwrite"
).json(
    "output/top_customers"
)
```

---

# 🚀 Task 7: Performance Optimization

Create:

```bash
nano process_data_optimized.py
```

---

## 🔹 Cache DataFrame

```python
clean_df = clean_df.cache()
```

---

## 🔹 Repartition by Country

```python
clean_df = clean_df.repartition(
    "country"
)
```

Benefits:

✅ Faster aggregations

✅ Reduced disk reads

✅ Better parallel execution

---

# 🔥 Run Optimized Pipeline

```bash
python3 process_data_optimized.py
```

---

# ✅ Verification

---

## Verify Output Structure

```bash
ls -R output/
```

Expected:

```text
output/
├── country_metrics
├── category_trends
└── top_customers
```

---

## Verify CSV Results

```bash
head output/country_metrics/part-*.csv
```

---

## Verify Parquet Output

```bash
ls -lh output/category_trends/
```

---

## Verify JSON Output

```bash
head output/top_customers/part-*.json
```

---

# 🔍 Data Verification Script

Create:

```bash
nano verify_results.py
```

Add:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Verify")
    .master("local[*]")
    .getOrCreate()
)

country_df = spark.read.csv(
    "output/country_metrics",
    header=True
)

print(
    f"Country Metrics: "
    f"{country_df.count()}"
)

spark.stop()
```

Run:

```bash
python3 verify_results.py
```

---

# 📈 Performance Benchmarking

Measure execution time:

```bash
time python3 process_data.py

time python3 process_data_optimized.py
```

Monitor Spark UI:

```text
http://localhost:4040
```

Observe:

✅ DAG Execution

✅ Task Duration

✅ Shuffle Operations

✅ Storage Usage

---

# 🛠️ Troubleshooting

---

## ❌ Out of Memory

Reduce memory:

```bash
export SPARK_DRIVER_MEMORY=2g

export SPARK_EXECUTOR_MEMORY=2g
```

---

## ❌ Slow Performance

Increase partitions:

```python
.config(
    "spark.sql.shuffle.partitions",
    "16"
)
```

Use:

```python
df.cache()
```

Inspect execution plans:

```python
df.explain()
```

---

## ❌ File Not Found

Verify:

```bash
ls -lh transactions.csv
```

Check working directory:

```bash
pwd
```

---

# 🎯 Expected Outcomes

After completing this lab you will have:

### 📦 Dataset Generated

* 5 Million Transactions
* Approximately 500MB CSV

### 📊 Analytics Generated

* Country Revenue Metrics (CSV)
* Monthly Category Trends (Parquet)
* Top 100 Customers (JSON)

### ⚡ Optimization Skills

* DataFrame Caching
* Repartitioning
* Spark Performance Tuning

---

# 🎓 Key Takeaways

### 🔹 Spark DataFrames

Provide optimized distributed processing for structured data.

### 🔹 Aggregations

Enable large-scale analytics on millions of records.

### 🔹 Caching

Improves performance when datasets are reused.

### 🔹 Partitioning

Enhances parallel processing efficiency.

### 🔹 Multiple Output Formats

Spark supports:

* CSV
* JSON
* Parquet
* ORC
* Avro

---

# 🚀 Next Steps

Explore:

* Spark SQL
* Advanced Joins
* Window Functions
* Structured Streaming
* MLlib Machine Learning
* Delta Lake
* Apache Iceberg
* Multi-Node Spark Clusters

---

<div align="center">

# 🎉 Congratulations!

You have successfully processed GB-scale datasets using Apache Spark.

### ⚡ Ready for Real-World Big Data Engineering Workloads

</div>
