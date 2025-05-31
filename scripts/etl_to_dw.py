import pandas as pd
import sqlite3
import pathlib
import sys

# For local imports, temporarily add project root to sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Constants
DW_DIR = pathlib.Path("data").joinpath("dw")
DB_PATH = DW_DIR.joinpath("smart_sales.db")
PREPARED_DATA_DIR = pathlib.Path("data").joinpath("prepared")

def create_schema(cursor: sqlite3.Cursor) -> None:
    """Create tables in the data warehouse if they don't exist."""
    print("DEBUG: Inside create_schema function.")
    print("Creating customer table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer (
            customer_id INTEGER PRIMARY KEY,
            name TEXT,
            region TEXT,
            join_date TEXT,
            loyalty_points INTEGER,
            customer_segment TEXT,
            membership_status TEXT
        )
    """)
    print("Customer table created.")

    print("Creating product table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS product (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT,
            category TEXT,
            unit_price REAL,
            stock_quantity INTEGER,
            subcategory TEXT,
            product_condition TEXT
        )
    """)
    print("Product table created.")

    print("Creating sale table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sale (
            sale_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            product_id INTEGER,
            sale_amount REAL,
            sale_date TEXT,
            store_id INTEGER,
            campaign_id REAL,
            discount_percent REAL,
            payment_type TEXT,
            sales_channel TEXT,
            FOREIGN KEY (customer_id) REFERENCES customer (customer_id),
            FOREIGN KEY (product_id) REFERENCES product (product_id)
        )
    """)
    print("Sale table created.")
    print("DEBUG: Exiting create_schema function.")

def delete_existing_records(cursor: sqlite3.Cursor) -> None:
    """Delete all existing records from the customer, product, and sale tables.
    Order of deletion matters due to foreign key constraints.
    """
    print("DEBUG: Inside delete_existing_records function.")
    print("Deleting existing records from tables...")
    cursor.execute("DELETE FROM sale")
    cursor.execute("DELETE FROM product")
    cursor.execute("DELETE FROM customer")
    print("Existing records deleted.")
    print("DEBUG: Exiting delete_existing_records function.")

def insert_customers(customers_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert customer data into the customer table."""
    print("DEBUG: Inside insert_customers function.")
    print("Inserting customer data...")
    customers_df.to_sql("customer", cursor.connection, if_exists="append", index=False)
    print(f"Inserted {len(customers_df)} customer records.")
    print("DEBUG: Exiting insert_customers function.")

def insert_products(products_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert product data into the product table."""
    print("DEBUG: Inside insert_products function.")
    print("Inserting product data...")
    products_df.to_sql("product", cursor.connection, if_exists="append", index=False)
    print(f"Inserted {len(products_df)} product records.")
    print("DEBUG: Exiting insert_products function.")

def insert_sales(sales_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert sales data into the sales table."""
    print("DEBUG: Inside insert_sales function.")
    print("Inserting sales data...")
    sales_df.to_sql("sale", cursor.connection, if_exists="append", index=False)
    print(f"Inserted {len(sales_df)} sale records.")
    print("DEBUG: Exiting insert_sales function.")

def load_data_to_db() -> None:
    conn = None
    print("DEBUG: Starting load_data_to_db function.")
    try:
        DW_DIR.mkdir(parents=True, exist_ok=True)
        print(f"Attempting to connect to database at: {DB_PATH}")
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        print("Database connection established.")

        create_schema(cursor)
        delete_existing_records(cursor)

        print(f"Loading prepared data from: {PREPARED_DATA_DIR}")

        # Load and rename columns for customer data
        customers_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("customers_prepared.csv"))
        customers_df.rename(columns={
            'CustomerID': 'customer_id',
            'Name': 'name',
            'Region': 'region',
            'JoinDate': 'join_date',
            'Loyalty Points': 'loyalty_points',
            'CustomerSegment': 'customer_segment',
            'membership_status': 'membership_status'
        }, inplace=True)
        customers_df.drop_duplicates(subset=['customer_id'], inplace=True)
        print(f"DEBUG: Customers DataFrame loaded and columns renamed. Rows: {len(customers_df)}")

        # Load and rename columns for product data
        products_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("products_prepared.csv"))
        products_df.rename(columns={
            'productid': 'product_id',
            'productname': 'product_name',
            'category': 'category',
            'unitprice': 'unit_price',
            'stockquantity': 'stock_quantity',
            'subcategory': 'subcategory',
            'product_condition': 'product_condition'
        }, inplace=True)
        print(f"DEBUG: Products DataFrame loaded and columns renamed. Rows: {len(products_df)}")

        # Load and rename columns for sales data
        sales_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("sales_prepared.csv"))
        sales_df.rename(columns={
            'TransactionID': 'sale_id',
            'SaleDate': 'sale_date',
            'CustomerID': 'customer_id',
            'ProductID': 'product_id',
            'SaleAmount': 'sale_amount',
            'StoreID': 'store_id',
            'CampaignID': 'campaign_id',
            'DiscountPercent': 'discount_percent',
            'PaymentType': 'payment_type',
            'sales_channel': 'sales_channel'
        }, inplace=True)
        sales_df['sale_amount'] = pd.to_numeric(sales_df['sale_amount'], errors='coerce')
        sales_df['sale_amount'].fillna(0, inplace=True)
        
        print("DEBUG: Filtering sales data for foreign key integrity...")
        initial_sales_rows = len(sales_df)
        
        valid_customer_ids = customers_df['customer_id'].tolist()
        valid_product_ids = products_df['product_id'].tolist()

        sales_df = sales_df[sales_df['customer_id'].isin(valid_customer_ids)]
        sales_df = sales_df[sales_df['product_id'].isin(valid_product_ids)]
        
        rows_after_fk_filter = len(sales_df)
        if initial_sales_rows != rows_after_fk_filter:
            print(f"DEBUG: Removed {initial_sales_rows - rows_after_fk_filter} sales rows due to invalid foreign keys.")
        else:
            print("DEBUG: No sales rows removed due to foreign key violations.")

        # --- NEW ADDITION: Drop duplicate sale_ids ---
        sales_df.drop_duplicates(subset=['sale_id'], inplace=True)
        # --- END NEW ADDITION ---

        print(f"DEBUG: Sales DataFrame loaded and columns renamed. Rows: {len(sales_df)}")

        print("Prepared data loaded into pandas DataFrames.")

        insert_customers(customers_df, cursor)
        insert_products(products_df, cursor)
        insert_sales(sales_df, cursor)

        print("DEBUG: Attempting to commit changes.")
        conn.commit()
        print("All data loaded successfully and committed to database.")

    except FileNotFoundError as e:
        print(f"ERROR: A required CSV file was not found. Please ensure your cleaned data files are in '{PREPARED_DATA_DIR}'.")
        print(f"Missing file: {e.filename}")
    except pd.errors.EmptyDataError:
        print(f"ERROR: One of the CSV files is empty. Please check your prepared data.")
    except Exception as e:
        print(f"AN UNEXPECTED ERROR OCCURRED DURING ETL PROCESS: {e}")
        if conn:
            conn.rollback()
            print("Transaction rolled back due to error.")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")
        print("DEBUG: Exiting load_data_to_db function.")

if __name__ == "__main__":
    print("DEBUG: Script started from main entry point.")
    load_data_to_db()
    print("DEBUG: Script finished.")