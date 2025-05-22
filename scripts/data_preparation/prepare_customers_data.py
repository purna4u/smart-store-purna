"""
scripts/data_preparation/prepare_products_data.py

This script reads product data from the data/raw folder, cleans the data,
and writes the cleaned version to the data/prepared folder.

Tasks:
- Remove duplicates
- Handle missing values
- Remove outliers
- Ensure consistent formatting
"""

#####################################
# Import Modules at the Top
#####################################

import pathlib
import sys
import pandas as pd

# Add project root to sys.path for local imports
sys.path.append(str(pathlib.Path(__file__).resolve().parent.parent.parent))

from utils.logger import logger
from utils.data_scrubber import DataScrubber

# Paths
SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PREPARED_DATA_DIR = DATA_DIR / "prepared"

# Ensure required directories exist
DATA_DIR.mkdir(exist_ok=True)
RAW_DATA_DIR.mkdir(exist_ok=True)
PREPARED_DATA_DIR.mkdir(exist_ok=True)

#####################################
# Define Reusable Functions
#####################################

def read_raw_data(file_name: str) -> pd.DataFrame:
    file_path = RAW_DATA_DIR / file_name
    try:
        logger.info(f"Reading data from {file_path}")
        return pd.read_csv(file_path)
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        return pd.DataFrame()

def save_prepared_data(df: pd.DataFrame, file_name: str) -> None:
    file_path = PREPARED_DATA_DIR / file_name
    df.to_csv(file_path, index=False)
    logger.info(f"Cleaned data saved to {file_path}")

def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Removing duplicate product records...")
    scrubber = DataScrubber(df)
    df_clean = scrubber.remove_duplicate_records()
    logger.info(f"Shape after deduplication: {df_clean.shape}")
    return df_clean

def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Handling missing values in product data...")
    before = df.isna().sum().sum()
    logger.info(f"Missing values before: {before}")

    # Example logic:
    if 'ProductName' in df.columns:
        df['ProductName'].fillna('Unnamed Product', inplace=True)
    if 'ProductID' in df.columns:
        df.dropna(subset=['ProductID'], inplace=True)

    after = df.isna().sum().sum()
    logger.info(f"Missing values after: {after}")
    return df

def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Removing outliers from product data...")

    if 'Price' in df.columns:
        df = df[(df['Price'] >= 0) & (df['Price'] <= 10000)]  # Example price range

    if 'Stock' in df.columns:
        df = df[(df['Stock'] >= 0) & (df['Stock'] <= 100000)]  # Example stock range

    logger.info(f"Shape after removing outliers: {df.shape}")
    return df

#####################################
# Main Function
#####################################

def main() -> None:
    logger.info("====================================")
    logger.info("STARTING prepare_products_data.py")
    logger.info("====================================")

    input_file = "products_data.csv"
    output_file = "products_prepared.csv"

    df = read_raw_data(input_file)

    if df.empty:
        logger.warning("Input data is empty. Exiting.")
        return

    logger.info(f"Initial shape: {df.shape}")
    df.columns = df.columns.str.strip()

    df = remove_duplicates(df)
    df = handle_missing_values(df)
    df = remove_outliers(df)

    save_prepared_data(df, output_file)

    logger.info("====================================")
    logger.info(f"Final shape: {df.shape}")
    logger.info("FINISHED prepare_products_data.py")
    logger.info("====================================")

#####################################
# Run Script When Executed Directly
#####################################

if __name__ == "__main__":
    main()
