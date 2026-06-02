from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, year, month, round

def create_spark_session():
    """
    Create and configure Spark session for local processing.
    
    Returns:
        SparkSession object
    """
    # TODO: Create SparkSession with app name "GB-Scale Processing"
    # TODO: Configure master as "local[*]" to use all cores
    # TODO: Set spark.driver.memory to "4g"
    # TODO: Set spark.sql.shuffle.partitions to "8"
    
    pass

def load_data(spark, file_path):
    """
    Load CSV data into Spark DataFrame.
    
    Args:
        spark: SparkSession object
        file_path: Path to CSV file
    
    Returns:
        DataFrame with loaded data
    """
    # TODO: Read CSV with header=True and inferSchema=True
    # TODO: Return the DataFrame
    
    pass

def main():
    # Initialize Spark
    spark = create_spark_session()
    
    # Load data
    df = load_data(spark, "transactions.csv")
    
    # TODO: Print schema
    # TODO: Show first 10 rows
    # TODO: Print total record count
    
    spark.stop()

if __name__ == "__main__":
    main()
