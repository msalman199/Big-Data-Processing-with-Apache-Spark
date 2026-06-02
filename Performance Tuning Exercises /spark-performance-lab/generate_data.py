import random
import json

def generate_sales_data(num_records):
    """
    Generate sample sales data for performance testing.
    
    Args:
        num_records: Number of records to generate
    """
    categories = ['Electronics', 'Clothing', 'Food', 'Books', 'Toys']
    regions = ['North', 'South', 'East', 'West', 'Central']
    
    with open('sales_data.json', 'w') as f:
        for i in range(num_records):
            record = {
                'transaction_id': i,
                'category': random.choice(categories),
                'region': random.choice(regions),
                'amount': round(random.uniform(10, 1000), 2),
                'quantity': random.randint(1, 50)
            }
            f.write(json.dumps(record) + '\n')

if __name__ == '__main__':
    generate_sales_data(1000000)  # 1 million records
    print("Data generation complete!")
