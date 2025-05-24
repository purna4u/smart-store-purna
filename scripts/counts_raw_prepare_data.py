import pandas as pd

# Paths to your raw and prepared CSV files
raw_customers_path = 'data/raw/customers_data.csv'
prepared_customers_path = 'data/prepared/customers_prepared.csv'

raw_products_path = 'data/raw/products_data.csv'
prepared_products_path = 'data/prepared/products_prepared.csv'

raw_sales_path = 'data/raw/sales_data.csv'
prepared_sales_path = 'data/prepared/sales_prepared.csv'

# Load data
raw_customers = pd.read_csv(raw_customers_path)
prepared_customers = pd.read_csv(prepared_customers_path)

raw_products = pd.read_csv(raw_products_path)
prepared_products = pd.read_csv(prepared_products_path)

raw_sales = pd.read_csv(raw_sales_path)
prepared_sales = pd.read_csv(prepared_sales_path)

# Count records
print(f"Customers: Raw records = {len(raw_customers)}, Prepared records = {len(prepared_customers)}")
print(f"Products: Raw records = {len(raw_products)}, Prepared records = {len(prepared_products)}")
print(f"Sales: Raw records = {len(raw_sales)}, Prepared records = {len(prepared_sales)}")
