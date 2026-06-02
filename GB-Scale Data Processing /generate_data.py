import csv
import random
from datetime import datetime, timedelta

def generate_transactions(num_records, output_file):
    """
    Generate synthetic e-commerce transaction data.
    
    Args:
        num_records: Number of transaction records to generate
        output_file: Path to output CSV file
    """
    categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports']
    countries = ['USA', 'UK', 'Canada', 'Germany', 'France', 'Japan']
    
    # TODO: Open CSV file and create writer
    # TODO: Write header row: transaction_id, date, customer_id, product_category, amount, country
    # TODO: Generate num_records transactions with random data
    # Hint: Use random.randint() for IDs, random.choice() for categories
    # Hint: Generate dates within the last 365 days
    # Hint: Generate amounts between 10 and 1000
    
    pass

if __name__ == "__main__":
    # Generate 5 million records (~500MB)
    generate_transactions(5000000, "transactions.csv")
    print("Data generation complete")
