# 🚀 Distributed Spark Cluster Setup 

<div align="center">

# ⚡ Apache Spark Distributed Cluster Setup

### 🖥️ Master-Worker Architecture | Distributed Computing | Cluster Management

![Apache Spark](https://img.shields.io/badge/Apache%20Spark-FF9900?style=for-the-badge\&logo=apachespark\&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge\&logo=python\&logoColor=white)
![Scala](https://img.shields.io/badge/Scala-DC322F?style=for-the-badge\&logo=scala\&logoColor=white)
![Java](https://img.shields.io/badge/Java-ED8B00?style=for-the-badge\&logo=openjdk\&logoColor=white)
![Linux](https://img.shields.io/badge/Linux-FCC624?style=for-the-badge\&logo=linux\&logoColor=black)
![Distributed Computing](https://img.shields.io/badge/Distributed-Computing-blue?style=for-the-badge)

### 🎯 Learn How to Build and Manage a Distributed Apache Spark Cluster

</div>

---

# 📖 Overview

This lab demonstrates how to install, configure, and manage an **Apache Spark Distributed Cluster** using a **Master-Worker Architecture** on a Linux machine.

You will learn how Spark distributes workloads across workers, submits jobs to the cluster, and provides monitoring through its built-in Web UI.

---

# 🎯 Learning Objectives

By the end of this lab, you will be able to:

✅ Install Apache Spark in distributed mode

✅ Configure Spark Master and Worker nodes

✅ Set up cluster communication

✅ Submit distributed Spark jobs

✅ Monitor Spark applications through the Web UI

✅ Verify cluster health and connectivity

---

# 📋 Prerequisites

Before starting this lab, ensure you have:

* Basic Linux command-line knowledge
* Understanding of distributed computing concepts
* Familiarity with Java or Scala
* SSH and networking fundamentals

---

# 🖥️ System Requirements

| Requirement | Details                   |
| ----------- | ------------------------- |
| OS          | Ubuntu 20.04+ / CentOS 7+ |
| RAM         | Minimum 4GB               |
| Java        | Java 8 or Java 11         |
| Access      | Root or sudo privileges   |

---

# 🏗️ Spark Cluster Architecture

```text
                    ┌─────────────────┐
                    │   Spark Master  │
                    │   Port: 7077    │
                    └────────┬────────┘
                             │
          ┌──────────────────┼──────────────────┐
          │                                     │
          ▼                                     ▼

 ┌─────────────────┐                 ┌─────────────────┐
 │ Spark Worker 1  │                 │ Spark Worker 2  │
 │     2 Cores     │                 │     2 Cores     │
 │      2GB RAM    │                 │      2GB RAM    │
 └─────────────────┘                 └─────────────────┘

                 Spark Web UI
              http://localhost:8080
```

---

# ⚙️ Task 1: Install and Configure Apache Spark

## 🔹 Step 1: Verify Java Installation

```bash
java -version
```

### Install Java if Missing

```bash
sudo apt update
sudo apt install openjdk-11-jdk -y
```

---

## 🔹 Step 2: Configure Java Environment

```bash
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.bashrc

source ~/.bashrc
```

Verify:

```bash
echo $JAVA_HOME
```

---

## 🔹 Step 3: Download Apache Spark

```bash
cd ~

wget https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz

tar -xzf spark-3.4.1-bin-hadoop3.tgz

sudo mv spark-3.4.1-bin-hadoop3 /opt/spark
```

---

## 🔹 Step 4: Configure Spark Environment Variables

```bash
cat >> ~/.bashrc << 'EOF'
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYSPARK_PYTHON=python3
EOF

source ~/.bashrc
```

Verify:

```bash
echo $SPARK_HOME
```

---

# ⚙️ Task 2: Configure Spark Cluster Properties

## 🔹 Step 1: Configure spark-env.sh

```bash
cd $SPARK_HOME/conf

cp spark-env.sh.template spark-env.sh
```

Append:

```bash
cat >> spark-env.sh << 'EOF'
export SPARK_MASTER_HOST=localhost
export SPARK_MASTER_PORT=7077
export SPARK_WORKER_CORES=2
export SPARK_WORKER_MEMORY=2g
export SPARK_WORKER_INSTANCES=2
EOF
```

---

## 🔹 Step 2: Configure Spark Defaults

```bash
cp spark-defaults.conf.template spark-defaults.conf
```

Add:

```properties
spark.master                     spark://localhost:7077
spark.executor.memory            1g
spark.driver.memory              1g
spark.executor.cores             1
```

---

# 🚀 Task 3: Start Spark Cluster

## 🔹 Step 1: Start Spark Master

```bash
$SPARK_HOME/sbin/start-master.sh
```

Verify:

```bash
jps | grep Master
```

Expected:

```text
Master
```

---

## 🔹 Step 2: Open Spark Web UI

Access:

```text
http://localhost:8080
```

Verify:

✅ Master is active

✅ Cluster resources visible

---

## 🔹 Step 3: Start Worker Nodes

```bash
$SPARK_HOME/sbin/start-worker.sh spark://localhost:7077

sleep 5

$SPARK_HOME/sbin/start-worker.sh spark://localhost:7077
```

Verify:

```bash
jps | grep Worker
```

Expected:

```text
Worker
Worker
```

---

# 🐍 Task 4: Create Python Spark Application

## 🔹 Create Application Directory

```bash
mkdir -p ~/spark-apps

cd ~/spark-apps
```

---

## 🔹 Create WordCount Application

```python
from pyspark.sql import SparkSession

def create_spark_session():
    spark = SparkSession.builder \
        .appName("WordCount") \
        .master("spark://localhost:7077") \
        .getOrCreate()

    return spark

def perform_wordcount(spark, text_data):
    rdd = spark.sparkContext.parallelize(text_data)

    counts = (
        rdd.flatMap(lambda line: line.split())
           .map(lambda word: (word, 1))
           .reduceByKey(lambda a, b: a + b)
    )

    return counts.collect()

if __name__ == "__main__":

    sample_text = [
        "Apache Spark is a distributed computing framework",
        "Spark provides high-level APIs in Java Scala Python",
        "Distributed computing enables processing large datasets"
    ]

    spark = create_spark_session()

    results = perform_wordcount(spark, sample_text)

    for word, count in results:
        print(f"{word}: {count}")

    spark.stop()
```

Save as:

```text
wordcount.py
```

---

# 🔥 Task 5: Submit Distributed Job

```bash
spark-submit \
  --master spark://localhost:7077 \
  --executor-memory 1g \
  --total-executor-cores 2 \
  wordcount.py
```

---

# 📊 Monitor Job Execution

Open:

```text
http://localhost:8080
```

Observe:

✅ Active Applications

✅ Worker Utilization

✅ Executor Status

✅ Task Progress

---

# ⚡ Task 6: Test Spark Shell

Start Spark Shell:

```bash
spark-shell --master spark://localhost:7077
```

Execute:

```scala
val data = sc.parallelize(1 to 1000)

val result = data.map(x => x * 2).reduce(_ + _)

println(s"Result: $result")
```

Exit:

```scala
:quit
```

---

# ✅ Verification

## Check Cluster Processes

```bash
jps
```

Expected:

```text
Master
Worker
Worker
```

---

## Verify Web UI

Visit:

```text
http://localhost:8080
```

Expected:

✅ 2 Workers Registered

✅ Available Memory Displayed

✅ Available CPU Cores Displayed

---

## Verify Worker Logs

```bash
ls $SPARK_HOME/logs/

tail -f $SPARK_HOME/logs/spark-*-worker-*.out
```

---

## Run SparkPi Example

```bash
spark-submit \
  --master spark://localhost:7077 \
  --class org.apache.spark.examples.SparkPi \
  $SPARK_HOME/examples/jars/spark-examples*.jar 100
```

Expected:

```text
Pi is roughly 3.14
```

---

# 🔍 Troubleshooting

## ❌ Workers Not Connecting

Check firewall:

```bash
sudo ufw status
```

Verify ports:

```bash
netstat -tulpn | grep -E '7077|8080|8081'
```

---

## ❌ Out of Memory

Reduce worker memory:

```bash
export SPARK_WORKER_MEMORY=1g
```

Reduce executor memory:

```properties
spark.executor.memory=512m
```

---

## ❌ Port Conflicts

Check active ports:

```bash
netstat -tulpn | grep -E '7077|8080|8081'
```

Kill conflicting processes if required.

---

# 🧹 Cleanup

Stop Workers:

```bash
$SPARK_HOME/sbin/stop-worker.sh
```

Stop Master:

```bash
$SPARK_HOME/sbin/stop-master.sh
```

Verify shutdown:

```bash
jps
```

Expected:

```text
No Spark processes running
```

---

# 📈 Expected Outcomes

After completing this lab:

✅ Apache Spark Installed

✅ Master Node Running

✅ Multiple Worker Nodes Running

✅ Distributed Job Execution Working

✅ SparkPi Test Successful

✅ Web UI Monitoring Functional

---

# 🎓 Key Takeaways

### 🔹 Master-Worker Architecture

Spark distributes workloads through a centralized master and multiple workers.

### 🔹 Distributed Processing

Tasks execute in parallel across cluster resources.

### 🔹 Resource Management

Executors use allocated CPU and memory efficiently.

### 🔹 Monitoring

The Spark Web UI provides real-time operational visibility.

### 🔹 Multi-Language Support

Applications can be written using:

* Python (PySpark)
* Scala
* Java

---

# 🚀 Next Steps

Explore:

* Spark SQL
* DataFrames
* Structured Streaming
* MLlib Machine Learning
* Graph Processing
* Spark on Kubernetes
* High Availability Spark Clusters
* HDFS and Distributed Storage Integration

---

<div align="center">

# 🎉 Congratulations!

You have successfully deployed and tested a Distributed Apache Spark Cluster.

### ⚡ Ready for Large-Scale Distributed Data Processing

</div>
