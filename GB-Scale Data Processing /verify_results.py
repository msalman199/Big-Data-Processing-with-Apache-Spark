from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Verify").master("local[*]").getOrCreate()

# TODO: Read each output format
# TODO: Print record counts
# TODO: Show sample data from each output

# Verify country metrics
country_df = spark.read.csv("output/country_metrics", header=True)
print(f"Country metrics records: {country_df.count()}")

# TODO: Add verification for other outputs

spark.stop()
