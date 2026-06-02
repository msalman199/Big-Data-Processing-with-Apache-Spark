# ⚡ Performance Tuning Exercises with Apache Spark

<div align="center">

# 🚀 Apache Spark Performance Optimization 

### 📊 Partition Tuning • ⚡ Caching Strategies • 📈 Benchmarking • 🔍 Spark UI Analysis

![Spark](https://img.shields.io/badge/Apache-Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.x-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Java](https://img.shields.io/badge/Java-11-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white)
![Linux](https://img.shields.io/badge/Linux-Ubuntu-FCC624?style=for-the-badge&logo=linux&logoColor=black)
![PySpark](https://img.shields.io/badge/PySpark-3.4.1-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)

</div>

---

# 📖 Overview

This hands-on lab focuses on **Apache Spark Performance Optimization** techniques used in real-world big data environments. You will learn how to tune partition counts, implement caching strategies, benchmark workloads, and analyze Spark execution using Spark UI.

---

# 🎯 Learning Objectives

By completing this lab, you will be able to:

✅ Optimize Spark jobs through partition tuning

✅ Implement caching strategies for iterative computations

✅ Benchmark Spark performance under different configurations

✅ Analyze Spark UI to identify bottlenecks

✅ Compare execution metrics across multiple optimization techniques

---

# 📋 Prerequisites

- 🐧 Basic Linux command-line proficiency
- ☕ Understanding of Spark architecture (RDDs, DataFrames, Actions & Transformations)
- 🐍 Familiarity with Python 3.x
- ⚡ Basic distributed computing concepts
- 🧠 Knowledge of PySpark APIs

---

# 🛠️ Technology Stack

| Technology | Purpose |
|------------|----------|
| Apache Spark 3.4.1 | Distributed Processing Engine |
| PySpark | Python API for Spark |
| Java 11 | Spark Runtime Requirement |
| Linux | Execution Environment |
| Python 3.x | Performance Testing Scripts |

---

# ⚙️ Environment Setup

---

## 🔹 Step 1: Update System Packages

```bash
sudo apt update
```

---

## 🔹 Step 2: Install Java

```bash
sudo apt install -y openjdk-11-jdk

java -version
```

Expected Output:

```bash
openjdk version "11.x.x"
```

---

## 🔹 Step 3: Install Python

```bash
sudo apt install -y python3 python3-pip
```

Verify:

```bash
python3 --version
pip3 --version
```

---

## 🔹 Step 4: Install Apache Spark

```bash
cd ~

wget https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz

tar -xzf spark-3.4.1-bin-hadoop3.tgz

sudo mv spark-3.4.1-bin-hadoop3 /opt/spark
```

---

## 🔹 Step 5: Configure Environment Variables

```bash
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin' >> ~/.bashrc
echo 'export PYSPARK_PYTHON=python3' >> ~/.bashrc

source ~/.bashrc
```

---

## 🔹 Step 6: Install PySpark

```bash
pip3 install pyspark==3.4.1
```

Verify:

```bash
pyspark --version
```

---

# 🧪 Task 1: Partition Optimization

## 📚 Overview

Partitioning determines how Spark distributes data across executors.

### Benefits of Proper Partitioning

✅ Better CPU utilization

✅ Faster parallel execution

✅ Reduced task overhead

✅ Improved cluster efficiency

---

# 🔹 Step 1: Create Test Dataset

Create working directory:

```bash
mkdir -p ~/spark-performance-lab

cd ~/spark-performance-lab
```

Create file:

```bash
nano generate_data.py
```

---

## 📝 generate_data.py

```python
import random
import json

def generate_sales_data(num_records):
    """
    Generate sample sales data for performance testing.
    """

    categories = ['Electronics', 'Clothing', 'Food', 'Books', 'Toys']
    regions = ['North', 'South', 'East', 'West', 'Central']

    with open('sales_data.json', 'w') as f:
        for i in range(num_records):
            record = {
                'transaction_id': i,
                'category': random.choice(categories),
                'region': random.choice(regions),
                'amount': round(random.uniform(10, 1000), 2),
                'quantity': random.randint(1, 50)
            }

            f.write(json.dumps(record) + '\n')

if __name__ == '__main__':
    generate_sales_data(1000000)
    print("Data generation complete!")
```

---

## ▶️ Generate Data

```bash
python3 generate_data.py
```

Expected:

```bash
Data generation complete!
```

---

# 🔹 Step 2: Create Partition Benchmark Script

```bash
nano partition_tuning.py
```

---

## 📝 partition_tuning.py

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
import time

def create_spark_session(app_name):
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()

def benchmark_partitions(spark, input_file, num_partitions):

    start_time = time.time()

    df = spark.read.json(input_file)

    df_repartitioned = df.repartition(num_partitions)

    result = df_repartitioned.groupBy(
        "category",
        "region"
    ).agg(
        sum("amount").alias("total_sales")
    )

    result.count()

    end_time = time.time()

    return end_time - start_time

def main():

    spark = create_spark_session("Partition Tuning")

    input_file = "sales_data.json"

    partition_counts = [2, 4, 8, 16, 32]

    print("Partition Count | Execution Time (s)")
    print("-" * 40)

    for num_parts in partition_counts:

        exec_time = benchmark_partitions(
            spark,
            input_file,
            num_parts
        )

        print(f"{num_parts:15} | {exec_time:.2f}")

    spark.stop()

if __name__ == '__main__':
    main()
```

---

## ▶️ Run Benchmark

```bash
python3 partition_tuning.py
```

Example Output:

```text
Partition Count | Execution Time (s)
----------------------------------------
2               | 15.72
4               | 10.61
8               | 8.21
16              | 8.44
32              | 12.18
```

---

# 📊 Analysis Questions

### ❓ Which partition count performed best?

Usually:

```text
CPU Cores × 2 to CPU Cores × 4
```

---

### ❓ What happens with too few partitions?

- CPUs remain idle
- Poor parallelism
- Slower jobs

---

### ❓ What happens with too many partitions?

- Increased scheduling overhead
- Excessive task creation
- Slower execution

---

# 🚀 Task 2: Caching Strategies

## 📚 Overview

Caching stores intermediate data in memory to avoid recomputation.

Perfect for:

- Iterative Machine Learning
- Multiple Aggregations
- Interactive Analytics

---

# 🔹 Step 1: Create Cache Benchmark Script

```bash
nano caching_benchmark.py
```

---

## 📝 caching_benchmark.py

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.functions import sum as _sum
from pyspark import StorageLevel
import time

def create_spark_session():

    return SparkSession.builder \
        .appName("Caching Benchmark") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

def run_without_cache(spark, input_file):

    start = time.time()

    df = spark.read.json(input_file)

    for i in range(3):

        result1 = df.groupBy("category").agg(
            avg("amount")
        )

        result2 = df.groupBy("region").agg(
            _sum("quantity")
        )

        result1.count()
        result2.count()

    return time.time() - start

def run_with_cache(spark, input_file):

    start = time.time()

    df = spark.read.json(input_file)

    df.cache()

    df.count()

    for i in range(3):

        result1 = df.groupBy("category").agg(
            avg("amount")
        )

        result2 = df.groupBy("region").agg(
            _sum("quantity")
        )

        result1.count()
        result2.count()

    df.unpersist()

    return time.time() - start

def run_with_persist(spark, input_file):

    start = time.time()

    df = spark.read.json(input_file)

    df.persist(StorageLevel.MEMORY_AND_DISK)

    df.count()

    for i in range(3):

        result1 = df.groupBy("category").agg(
            avg("amount")
        )

        result2 = df.groupBy("region").agg(
            _sum("quantity")
        )

        result1.count()
        result2.count()

    df.unpersist()

    return time.time() - start

def main():

    spark = create_spark_session()

    input_file = "sales_data.json"

    print("Caching Strategy | Execution Time (s)")
    print("-" * 45)

    time_no_cache = run_without_cache(
        spark,
        input_file
    )

    print(f"No Cache         | {time_no_cache:.2f}")

    time_cache = run_with_cache(
        spark,
        input_file
    )

    print(f"With Cache       | {time_cache:.2f}")

    time_persist = run_with_persist(
        spark,
        input_file
    )

    print(f"Persist (M&D)    | {time_persist:.2f}")

    improvement = (
        (time_no_cache - time_cache)
        / time_no_cache
    ) * 100

    print(
        f"\nPerformance improvement: {improvement:.1f}%"
    )

    spark.stop()

if __name__ == '__main__':
    main()
```

---

## ▶️ Execute Benchmark

```bash
python3 caching_benchmark.py
```

Example Output:

```text
Caching Strategy | Execution Time (s)
---------------------------------------------
No Cache         | 55.42
With Cache       | 22.13
Persist (M&D)    | 25.64

Performance improvement: 60.1%
```

---

# 📈 Spark UI Analysis

Access Spark UI during execution:

```text
http://localhost:4040
```

---

## 🔍 Check Jobs Tab

Monitor:

- Job Duration
- Task Distribution
- Failed Jobs

---

## 🔍 Check Stages Tab

Look for:

- Shuffle Operations
- Data Skew
- Long Running Tasks

---

## 🔍 Check Storage Tab

Verify:

- Cached DataFrames
- Memory Consumption
- Storage Levels

---

# ✅ Verification Script

Create:

```bash
nano verify_results.py
```

---

## 📝 verify_results.py

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Verify") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.json("sales_data.json")

print(f"Total records: {df.count()}")

print(
    f"Default partitions: "
    f"{df.rdd.getNumPartitions()}"
)

df.show(5)

spark.stop()
```

Run:

```bash
python3 verify_results.py
```

---

# 🧰 Troubleshooting

---

## ❌ Out of Memory

Increase Spark memory:

```python
.config("spark.driver.memory", "4g")
```

Or reduce dataset size.

---

## ❌ Slow Performance

Check CPU cores:

```bash
nproc
```

Adjust partitions accordingly.

---

## ❌ Spark UI Not Available

Spark UI only exists while Spark jobs are running.

Use longer-running jobs or add:

```python
input("Press Enter to exit...")
```

---

## ❌ PySpark Import Errors

Verify installation:

```bash
pip3 show pyspark

python3 --version
```

---

# 🎯 Expected Outcomes

After completing this lab, you should have:

✅ Generated 1 Million Record Dataset

✅ Benchmarked Multiple Partition Counts

✅ Compared Cache vs Non-Cache Performance

✅ Analyzed Spark UI Metrics

✅ Improved Job Execution Speed

---

# 📚 Key Takeaways

### Partition Tuning

✔ Optimal partitions are generally:

```text
2–4 × CPU Core Count
```

---

### Caching

✔ Excellent for reused datasets

✔ Reduces repeated computation

✔ Consumes additional memory

---

### Persistence

✔ MEMORY_AND_DISK prevents OOM errors

✔ Trades speed for reliability

---

### Benchmarking

✔ Always test with realistic data

✔ Measure before optimizing

✔ Use Spark UI for evidence-based tuning

---

# 🚀 Next Steps

- Explore Broadcast Joins
- Test Dynamic Partition Pruning
- Experiment with Different Storage Levels
- Analyze Shuffle Performance
- Explore Spark SQL Optimizations
- Learn Adaptive Query Execution (AQE)
- Optimize Large-Scale ETL Pipelines

---

# 🏁 Conclusion

Congratulations! 🎉

You successfully learned how to:

- Configure Apache Spark for performance testing
- Tune partition counts for optimal parallelism
- Implement caching and persistence strategies
- Benchmark Spark workloads
- Analyze execution metrics through Spark UI

These optimization techniques are essential for building scalable, production-grade Spark applications capable of processing terabytes of data efficiently.

---

<div align="center">

### ⭐ Happy Learning & Happy Spark Tuning! ⭐

**Apache Spark Performance Engineering Lab Complete**

🚀📊⚡

</div>
