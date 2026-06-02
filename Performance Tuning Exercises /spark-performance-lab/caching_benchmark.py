from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as _sum
import time

def create_spark_session():
    return SparkSession.builder \
        .appName("Caching Benchmark") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

def run_without_cache(spark, input_file):
    """
    Run iterative operations WITHOUT caching.
    
    Args:
        spark: SparkSession
        input_file: Path to data
    
    Returns:
        Execution time in seconds
    """
    start_time = time.time()
    
    # TODO: Read the data
    df = 
    
    # Simulate iterative operations (3 iterations)
    for i in range(3):
        # TODO: Perform aggregation - average amount by category
        result1 = 
        
        # TODO: Perform another aggregation - sum quantity by region
        result2 = 
        
        # TODO: Trigger actions
        
    
    end_time = time.time()
    return end_time - start_time

def run_with_cache(spark, input_file):
    """
    Run iterative operations WITH caching.
    
    Args:
        spark: SparkSession
        input_file: Path to data
    
    Returns:
        Execution time in seconds
    """
    start_time = time.time()
    
    # TODO: Read the data
    df = 
    
    # TODO: Cache the DataFrame
    
    
    # Simulate iterative operations (3 iterations)
    for i in range(3):
        # TODO: Same aggregations as above
        result1 = 
        result2 = 
        
    
    # TODO: Unpersist when done
    
    
    end_time = time.time()
    return end_time - start_time

def run_with_persist(spark, input_file):
    """
    Run with MEMORY_AND_DISK persistence level.
    
    Args:
        spark: SparkSession
        input_file: Path to data
    
    Returns:
        Execution time in seconds
    """
    from pyspark import StorageLevel
    
    start_time = time.time()
    
    # TODO: Read and persist with MEMORY_AND_DISK
    df = 
    
    
    for i in range(3):
        result1 = 
        result2 = 
        
    
    
    
    end_time = time.time()
    return end_time - start_time

def main():
    spark = create_spark_session()
    input_file = "sales_data.json"
    
    print("Caching Strategy | Execution Time (s)")
    print("-" * 45)
    
    # TODO: Run all three scenarios and compare
    time_no_cache = 
    print(f"No Cache         | {time_no_cache:.2f}")
    
    time_cache = 
    print(f"With Cache       | {time_cache:.2f}")
    
    time_persist = 
    print(f"Persist (M&D)    | {time_persist:.2f}")
    
    # Calculate improvement
    improvement = ((time_no_cache - time_cache) / time_no_cache) * 100
    print(f"\nPerformance improvement with cache: {improvement:.1f}%")
    
    spark.stop()

if __name__ == '__main__':
    main()
