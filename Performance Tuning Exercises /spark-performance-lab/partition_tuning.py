from pyspark.sql import SparkSession
import time

def create_spark_session(app_name):
    """Create and return a Spark session."""
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()

def benchmark_partitions(spark, input_file, num_partitions):
    """
    Benchmark Spark job with specified partition count.
    
    Args:
        spark: SparkSession object
        input_file: Path to input data
        num_partitions: Number of partitions to use
    
    Returns:
        Execution time in seconds
    """
    start_time = time.time()
    
    # TODO: Read JSON data
    df = # Read the JSON file
    
    # TODO: Repartition the DataFrame
    df_repartitioned = # Apply repartition with num_partitions
    
    # TODO: Perform aggregation (sum of amount by category and region)
    result = # Group by category and region, sum the amount
    
    # TODO: Trigger action to execute the job
    # Collect or count the results
    
    end_time = time.time()
    return end_time - start_time

def main():
    spark = create_spark_session("Partition Tuning")
    input_file = "sales_data.json"
    
    # Test different partition counts
    partition_counts = [2, 4, 8, 16, 32]
    
    print("Partition Count | Execution Time (s)")
    print("-" * 40)
    
    for num_parts in partition_counts:
        # TODO: Call benchmark_partitions and print results
        exec_time = # Call the benchmark function
        print(f"{num_parts:15} | {exec_time:.2f}")
    
    spark.stop()

if __name__ == '__main__':
    main()
