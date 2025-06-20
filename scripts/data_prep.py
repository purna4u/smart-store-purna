# scripts/data_prep.py

"""
Module 7: Custom BI Project - Data Preparation and Aggregation
This script loads prepared individual data files, merges them,
performs necessary calculations (Profit, Quarter, Year),
and aggregates data for BI analysis.
"""

#####################################
# Import Modules at the Top
#####################################

# Import from Python Standard Library
import pathlib
import sys

# Import from external packages
import pandas as pd

# Ensure project root is in sys.path for local imports
# This allows importing from 'utils' folder directly
sys.path.append(str(pathlib.Path(__file__).resolve().parent.parent))

# Import local modules (e.g. utils/logger.py)
from utils.logger import logger

# Constants (Paths)
SCRIPTS_DIR: pathlib.Path = pathlib.Path(__file__).resolve().parent
PROJECT_ROOT: pathlib.Path = SCRIPTS_DIR.parent
DATA_DIR: pathlib.Path = PROJECT_ROOT / "data"
# We'll now primarily work with 'prepared' and output to 'processed'
PREPARED_DATA_DIR: pathlib.Path = DATA_DIR / "prepared"
PROCESSED_DATA_DIR: pathlib.Path = DATA_DIR / "processed"

# Ensure output data directories exist or create them
PREPARED_DATA_DIR.mkdir(parents=True, exist_ok=True) # Ensure prepared exists for loading
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True) # Ensure processed exists for saving
logger.info(f"Data directories ensured: {PREPARED_DATA_DIR}, {PROCESSED_DATA_DIR}")

#####################################
# Define Functions - Reusable blocks of code / instructions
#####################################

def load_prepared_data() -> dict[str, pd.DataFrame]:
    """
    Loads prepared CSV files (sales, products, customers) into DataFrames.
    Assumes these files are in the 'data/prepared/' directory.
    Returns:
        dict: A dictionary of DataFrames with keys 'sales', 'products', 'customers'.
    """
    dataframes = {}
    files_to_load = {
        'sales': 'sales_prepared.csv',
        'products': 'products_prepared.csv',
        'customers': 'customers_prepared.csv'
    }

    for key, filename in files_to_load.items():
        file_path = PREPARED_DATA_DIR / filename
        try:
            logger.info(f"Loading prepared {key} data from: {file_path}")
            df = pd.read_csv(file_path)
            dataframes[key] = df
            logger.info(f"Loaded {len(df)} rows for {key}.")
            logger.debug(f"{key} head:\n{df.head()}") # Use debug for verbose output
        except FileNotFoundError:
            logger.error(f"Error: Prepared file not found at {file_path}. Returning empty DataFrame for '{key}'.")
            dataframes[key] = pd.DataFrame()
        except pd.errors.EmptyDataError:
            logger.error(f"Error: The file {file_path} is empty. Returning empty DataFrame for '{key}'.")
            dataframes[key] = pd.DataFrame()
        except Exception as e:
            logger.error(f"An unexpected error occurred while loading {key} data from {file_path}: {e}")
            dataframes[key] = pd.DataFrame()
    return dataframes

def merge_and_process_data(dataframes: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Merges prepared sales, products, and customer data, then performs final calculations
    and standardization for BI analysis.
    Args:
        dataframes (dict): Dictionary containing 'sales', 'products', 'customers' DataFrames.
    Returns:
        pd.DataFrame: A single, fully processed DataFrame ready for aggregation.
    """
    sales_df = dataframes.get('sales')
    products_df = dataframes.get('products')
    customers_df = dataframes.get('customers')

    if sales_df.empty:
        logger.error("Sales data is empty or not loaded. Cannot proceed with merging and processing.")
        return pd.DataFrame()

    logger.info("Starting data merging and final processing...")

    # --- IMPORTANT: ACTUAL COLUMN NAMES FROM YOUR *PREPARED* CSV FILES ---
    # These names are derived from your previous log output and directory structure.
    # sales_prepared.csv columns
    SALES_DATE_COL = 'SaleDate'
    SALES_REVENUE_COL = 'SaleAmount'
    # NOTE: No explicit 'Cost' column found in your sales_prepared.csv head.
    # We will derive a 'Total_Cost' based on an assumption.
    SALES_UNITS_COL = 'Quantity' # If this column exists in sales_prepared.csv
    SALES_CHANNEL_COL = 'sales_channel'
    SALES_CUSTOMER_ID_COL = 'CustomerID'
    SALES_PRODUCT_ID_COL = 'ProductID'
    # No 'Region' column observed in sales_prepared.csv head. Will get from customers.

    # products_prepared.csv columns
    PRODUCTS_PRODUCT_ID_COL = 'productid' # Lowercase
    PRODUCTS_CATEGORY_COL = 'category'    # Lowercase

    # customers_prepared.csv columns
    CUSTOMERS_CUSTOMER_ID_COL = 'CustomerID'
    CUSTOMERS_REGION_COL = 'Region'
    # -------------------------------------------------------------------

    merged_df = sales_df.copy() # Start with sales data

    # 3.1 Merge with Products Data
    if not products_df.empty and PRODUCTS_PRODUCT_ID_COL in products_df.columns and PRODUCTS_CATEGORY_COL in products_df.columns:
        # Before merging, rename productid in products_df to match ProductID in sales_df for consistent join key
        products_df.rename(columns={PRODUCTS_PRODUCT_ID_COL: SALES_PRODUCT_ID_COL}, inplace=True) 
        
        merged_df = pd.merge(merged_df, products_df[[SALES_PRODUCT_ID_COL, PRODUCTS_CATEGORY_COL]],
                             left_on=SALES_PRODUCT_ID_COL, right_on=SALES_PRODUCT_ID_COL, how='left')
        logger.info(f"Merged sales with products data on '{SALES_PRODUCT_ID_COL}'.")
        
        # After merge, rename the 'category' column (from products_df) to 'ProductCategory'
        if PRODUCTS_CATEGORY_COL in merged_df.columns:
            merged_df.rename(columns={PRODUCTS_CATEGORY_COL: 'ProductCategory'}, inplace=True)
            logger.info(f"Renamed product column '{PRODUCTS_CATEGORY_COL}' to 'ProductCategory'.")
        else:
            logger.warning("Product category column not found after products merge. 'ProductCategory' will be 'Unknown'.")
            merged_df['ProductCategory'] = 'Unknown'
    else:
        logger.warning(f"Products data missing or required columns ('{PRODUCTS_PRODUCT_ID_COL}', '{PRODUCTS_CATEGORY_COL}') not found. 'ProductCategory' will be 'Unknown'.")
        merged_df['ProductCategory'] = 'Unknown'

    # 3.2 Merge Region Data from Customers (as it's not in sales_df based on log)
    if not customers_df.empty and CUSTOMERS_CUSTOMER_ID_COL in customers_df.columns and CUSTOMERS_REGION_COL in customers_df.columns:
        merged_df = pd.merge(merged_df, customers_df[[CUSTOMERS_CUSTOMER_ID_COL, CUSTOMERS_REGION_COL]],
                             left_on=SALES_CUSTOMER_ID_COL, right_on=CUSTOMERS_CUSTOMER_ID_COL, how='left')
        # Ensure the final column is consistently named 'Region'
        if CUSTOMERS_REGION_COL != 'Region' and 'Region' not in merged_df.columns: # Prevent renaming if already 'Region' or if conflict
            merged_df.rename(columns={CUSTOMERS_REGION_COL: 'Region'}, inplace=True)
        logger.info(f"Merged customer data for 'Region' on '{SALES_CUSTOMER_ID_COL}'.")
    else:
        logger.warning("Customer data missing or required columns ('CustomerID', 'Region') not found. 'Region' will be 'Unknown'.")
        merged_df['Region'] = 'Unknown'


    # 3.3 Date Transformation: Convert to datetime, extract Year and Quarter
    if SALES_DATE_COL in merged_df.columns:
        initial_rows = len(merged_df)
        merged_df[SALES_DATE_COL] = pd.to_datetime(merged_df[SALES_DATE_COL], errors='coerce')
        # Filter out rows where date conversion failed (NaT) *before* extracting Year/Quarter
        merged_df.dropna(subset=[SALES_DATE_COL], inplace=True)
        if len(merged_df) < initial_rows:
            logger.warning(f"Dropped {initial_rows - len(merged_df)} rows due to invalid/missing dates in '{SALES_DATE_COL}'.")
        
        if not merged_df.empty: # Only proceed if data is not empty after dropping NaTs
            merged_df['Year'] = merged_df[SALES_DATE_COL].dt.year.astype(int)
            merged_df['Quarter'] = merged_df[SALES_DATE_COL].dt.quarter.astype(int)
            logger.info("Extracted 'Year' and 'Quarter' from date column.")
        else:
            logger.error("No valid dates remaining after conversion. Year/Quarter will be 0.")
            merged_df['Year'] = 0
            merged_df['Quarter'] = 0
    else:
        logger.error(f"Date column '{SALES_DATE_COL}' not found after merge. Cannot extract Year/Quarter.")
        merged_df['Year'] = 0
        merged_df['Quarter'] = 0


    # 3.4 Profit and Profit Margin Calculation
    if SALES_REVENUE_COL in merged_df.columns:
        # Ensure SaleAmount is numeric
        merged_df['Total_Revenue'] = pd.to_numeric(merged_df[SALES_REVENUE_COL], errors='coerce').fillna(0)

        # --- Handle Missing Cost Data: ASSUMPTION ---
        # Since 'Cost' column was not found in your sales_prepared.csv head.
        # This is a critical assumption for your homework.
        ASSUMED_COST_PERCENTAGE = 0.70 # Meaning 70% of revenue is cost, 30% is profit
        merged_df['Total_Cost'] = merged_df['Total_Revenue'] * ASSUMED_COST_PERCENTAGE
        logger.warning(f"Cost column not found. Assuming Total_Cost = Total_Revenue * {ASSUMED_COST_PERCENTAGE*100:.0f}% for demonstration.")

        merged_df['Profit'] = merged_df['Total_Revenue'] - merged_df['Total_Cost']
        merged_df['Profit_Margin'] = merged_df.apply(lambda row: (row['Profit'] / row['Total_Revenue'] * 100) if row['Total_Revenue'] > 0 else 0, axis=1)
        logger.info("Calculated 'Profit' and 'Profit_Margin'.")
    else:
        logger.warning(f"Revenue column ('{SALES_REVENUE_COL}') not found. Cannot calculate Profit/Profit Margin. Setting to 0.")
        merged_df['Total_Revenue'] = 0
        merged_df['Total_Cost'] = 0
        merged_df['Profit'] = 0
        merged_df['Profit_Margin'] = 0


    # 3.5 Units Sold Renaming/Conversion
    # This will check for 'Quantity' from your SALES_UNITS_COL. If not found, Units_Sold will be 0.
    if SALES_UNITS_COL in merged_df.columns:
        merged_df['Units_Sold'] = pd.to_numeric(merged_df[SALES_UNITS_COL], errors='coerce').fillna(0).astype(int)
        logger.info(f"Renamed '{SALES_UNITS_COL}' to 'Units_Sold' and ensured numeric.")
    else:
        logger.warning(f"Units Sold column '{SALES_UNITS_COL}' not found. 'Units_Sold' will be 0. Please verify your sales data or document this limitation.")
        merged_df['Units_Sold'] = 0


    # 3.6 Standardize Categorical Dimensions (using the *final* column names)
    # These column names should now be consistently 'ProductCategory', 'Region', 'sales_channel'
    categorical_cols_to_process = ['ProductCategory', 'Region', SALES_CHANNEL_COL]
    for col in categorical_cols_to_process:
        if col in merged_df.columns:
            # Fill NaNs with 'Unknown' BEFORE string operations, then clean and title case
            merged_df[col] = merged_df[col].fillna('Unknown').astype(str).str.strip().str.title()
            logger.info(f"Standardized categorical column: '{col}'.")
        else:
            logger.warning(f"Final categorical column '{col}' not found in merged DataFrame. Setting to 'Unknown'. Check merge logic/source columns if unexpected.")
            merged_df[col] = 'Unknown'


    # --- Select and reorder final columns for clarity ---
    # This ensures your final DataFrame has only the relevant columns in a logical order.
    # Adjust this list if you want to include other columns from your original sales_df.
    final_columns_order = [
        SALES_DATE_COL, 'Year', 'Quarter', 'Region', 'ProductCategory', SALES_CHANNEL_COL,
        'Total_Revenue', 'Total_Cost', 'Profit', 'Profit_Margin', 'Units_Sold',
        SALES_CUSTOMER_ID_COL, SALES_PRODUCT_ID_COL, # Keep IDs for potential drill-down in Power BI
        # Example of other columns you might want to keep if they exist in your merged_df:
        # 'TransactionID', 'StoreID', 'CampaignID', 'DiscountPercent', 'PaymentType'
    ]
    # Filter to only include columns that actually exist in the DataFrame after all processing
    merged_df = merged_df[[col for col in final_columns_order if col in merged_df.columns]]


    logger.info("Data merging and final processing complete.")
    logger.info("Final Processed DataFrame info:\n%s", merged_df.info(verbose=True, show_counts=True))
    logger.info("Final Processed DataFrame head:\n%s", merged_df.head())
    return merged_df

def aggregate_final_data(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Aggregates the fully processed sales data into the required formats for BI analysis.
    Returns:
        tuple: (main_profit_agg_df, sales_channel_share_df, yearly_product_revenue_df)
    """
    if df.empty:
        logger.warning("No data to aggregate. Returning empty DataFrames.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    logger.info("Starting data aggregation for final analysis...")

    # Filter out rows where Year/Quarter might be 0 (from date processing errors or missing data)
    # This ensures we only aggregate valid time periods.
    df_filtered = df[(df['Year'] > 0) & (df['Quarter'] > 0)].copy()
    if df_filtered.empty:
        logger.error("No valid data for aggregation after filtering for Year/Quarter (Year > 0, Quarter > 0). Returning empty DFs.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # 4.1 Aggregation for the main goal: Profit by Product Category, Region, Quarter
    # Ensure column names match the final names from merge_and_process_data
    main_profit_agg_df = df_filtered.groupby(['Year', 'Quarter', 'Region', 'ProductCategory']).agg(
        Total_Revenue=('Total_Revenue', 'sum'),
        Total_Profit=('Profit', 'sum'),
        Units_Sold=('Units_Sold', 'sum')
    ).reset_index()

    # Calculate Avg_Profit_Margin for the aggregated data (sum of profit / sum of revenue for the group)
    main_profit_agg_df['Avg_Profit_Margin'] = (
        main_profit_agg_df['Total_Profit'] / main_profit_agg_df['Total_Revenue'] * 100
    ).fillna(0) # Handle potential division by zero for groups with 0 revenue
    logger.info("Main aggregated profit data head:\n%s", main_profit_agg_df.head())

    # 4.2 Sales Channel Share
    logger.info("Aggregating for Sales Channel Share...")
    # Use the final column name 'sales_channel'
    # Ensure SALES_CHANNEL_COL (which is 'sales_channel') is used consistently
    sales_channel_share_df = df_filtered.groupby('sales_channel').agg(
        Total_Revenue=('Total_Revenue', 'sum')
    ).reset_index()
    sales_channel_share_df['Share_Percent'] = (sales_channel_share_df['Total_Revenue'] / sales_channel_share_df['Total_Revenue'].sum()) * 100
    logger.info("Sales Channel Share data:\n%s", sales_channel_share_df)

    # 4.3 Year-over-Year Growth (requires data spanning multiple years)
    logger.info("Aggregating for Year-over-Year Growth by Product Category...")
    yearly_product_revenue_df = df_filtered.groupby(['Year', 'ProductCategory'])['Total_Revenue'].sum().reset_index()
    yearly_product_revenue_df = yearly_product_revenue_df.sort_values(by=['ProductCategory', 'Year'])

    yearly_product_revenue_df['Previous_Year_Revenue'] = yearly_product_revenue_df.groupby('ProductCategory')['Total_Revenue'].shift(1)
    yearly_product_revenue_df['YoY_Growth_Percent'] = (
        (yearly_product_revenue_df['Total_Revenue'] - yearly_product_revenue_df['Previous_Year_Revenue']) / yearly_product_revenue_df['Previous_Year_Revenue'] * 100
    ).fillna(0) # Fill NaN for the first year of each product category (as there's no 'previous' year)
    logger.info("Year-over-Year Growth data head:\n%s", yearly_product_revenue_df.head())

    return main_profit_agg_df, sales_channel_share_df, yearly_product_revenue_df

#####################################
# Define Main Function - The main entry point of the script
#####################################

def main() -> None:
    """
    Main function to orchestrate the loading, merging, processing,
    and aggregation of sales, product, and customer data for BI analysis.
    """
    logger.info("--- Starting custom BI project data preparation and aggregation script ---")

    # Step 1: Load prepared individual data files
    prepared_data_dfs = load_prepared_data()

    # Step 2: Merge the loaded dataframes and perform final calculations/transformations
    fully_processed_df = merge_and_process_data(prepared_data_dfs)

    if not fully_processed_df.empty:
        # Step 3: Aggregate the fully processed data into the required formats for BI
        main_agg_df, channel_share_agg_df, yoy_growth_agg_df = aggregate_final_data(fully_processed_df)

        # Step 4: Save the aggregated DataFrames to the data/processed/ directory
        if not main_agg_df.empty:
            main_agg_path = PROCESSED_DATA_DIR / 'profit_by_category_region_quarter_agg.csv'
            main_agg_df.to_csv(main_agg_path, index=False)
            logger.info(f"Main aggregated data saved to: {main_agg_path}")
        else:
            logger.warning("Main aggregated DataFrame is empty after aggregation. Not saving.")

        if not channel_share_agg_df.empty:
            channel_share_path = PROCESSED_DATA_DIR / 'sales_channel_share_agg.csv'
            channel_share_agg_df.to_csv(channel_share_path, index=False)
            logger.info(f"Sales channel share data saved to: {channel_share_path}")
        else:
            logger.warning("Sales channel share DataFrame is empty after aggregation. Not saving.")

        if not yoy_growth_agg_df.empty:
            yoy_growth_path = PROCESSED_DATA_DIR / 'yoy_growth_agg.csv'
            yoy_growth_agg_df.to_csv(yoy_growth_path, index=False)
            logger.info(f"Year-over-Year growth data saved to: {yoy_growth_path}")
        else:
            logger.warning("Year-over-Year growth DataFrame is empty after aggregation. Not saving.")

        logger.info("All data processing and aggregation steps completed successfully.")
    else:
        logger.error("No fully processed data available. Aggregation and saving skipped.")

    logger.info("--- Script Finished ---")

#####################################
# Conditional Execution Block
# Ensures the script runs only when executed directly (not imported as a module)
# This is a common Python convention.
#####################################

if __name__ == "__main__":
    main()