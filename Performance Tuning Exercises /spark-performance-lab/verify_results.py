from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Verify").master("local[*]").getOrCreate()

# Verify data exists
df = spark.read.json("sales_data.json")
print(f"Total records: {df.count()}")
print(f"Default partitions: {df.rdd.getNumPartitions()}")

# Show sample
df.show(5)

spark.stop()
