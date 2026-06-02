def main():
    spark = create_spark_session()
    
    df = load_data(spark, "transactions.csv")
    clean_df = clean_data(df)
    
    # TODO: Cache the cleaned DataFrame for reuse
    # Hint: Use .cache() method
    
    # TODO: Repartition by country for better parallelism
    # Hint: Use .repartition() method
    
    # Perform all analyses on cached/repartitioned data
    country_metrics = calculate_country_metrics(clean_df)
    category_trends = calculate_category_trends(clean_df)
    top_customers = find_top_customers(clean_df, 100)
    
    # Save results
    # TODO: Implement saving logic with appropriate formats
    
    spark.stop()
