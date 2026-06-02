import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]): Unit = {
    // TODO: Create SparkSession with master URL spark://localhost:7077
    
    // TODO: Create sample data RDD
    val textData = Seq(
      "Apache Spark is a distributed computing framework",
      "Spark provides high-level APIs in Java Scala Python"
    )
    
    // TODO: Implement word count logic:
    // 1. Create RDD from textData
    // 2. flatMap to split into words
    // 3. map to (word, 1) pairs
    // 4. reduceByKey to count
    // 5. collect and print results
    
    // TODO: Stop Spark session
  }
}
