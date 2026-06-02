from pyspark.sql import SparkSession

def create_spark_session():
    """
    TODO: Create and return a SparkSession with app name "WordCount"
    Use master URL: spark://localhost:7077
    """
    # Your code here
    pass

def perform_wordcount(spark, text_data):
    """
    TODO: Implement word count logic
    
    Steps:
    1. Create RDD from text_data list
    2. Split each line into words
    3. Map each word to (word, 1)
    4. Reduce by key to count occurrences
    5. Collect and return results
    
    Args:
        spark: SparkSession object
        text_data: List of text strings
    
    Returns:
        List of tuples (word, count)
    """
    # Your code here
    pass

if __name__ == "__main__":
    # Sample data
    sample_text = [
        "Apache Spark is a distributed computing framework",
        "Spark provides high-level APIs in Java Scala Python",
        "Distributed computing enables processing large datasets"
    ]
    
    # TODO: Call create_spark_session()
    # TODO: Call perform_wordcount()
    # TODO: Print results
    # TODO: Stop the Spark session
    
    pass
