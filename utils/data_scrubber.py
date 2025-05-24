"""
utils/data_scrubber.py

Reusable utility class for performing common data cleaning and 
preparation tasks on a pandas DataFrame.

This class provides methods for:
- Checking data consistency
- Removing duplicates
- Handling missing values
- Filtering outliers
- Renaming and reordering columns
- Formatting strings
- Parsing date fields

Use this class to perform similar cleaning operations across multiple files.  
You are not required to use this class, but it shows how we can organize 
reusable data cleaning logic - or you can use the logic examples in your own code.

Example:
    from utils.data_scrubber import DataScrubber
    scrubber = DataScrubber(df)
    df = scrubber.remove_duplicates().handle_missing_data(fill_value="N/A")
"""

import io
import pandas as pd
from typing import Dict, Tuple, Union, List

class DataScrubber:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def check_data_consistency_before_cleaning(self) -> Dict[str, Union[pd.Series, int]]:
        null_counts = self.df.isnull().sum()
        duplicate_count = self.df.duplicated().sum()
        return {'null_counts': null_counts, 'duplicate_count': duplicate_count}

    def check_data_consistency_after_cleaning(self) -> Dict[str, Union[pd.Series, int]]:
        null_counts = self.df.isnull().sum()
        duplicate_count = self.df.duplicated().sum()
        assert null_counts.sum() == 0, "Data still contains null values after cleaning."
        assert duplicate_count == 0, "Data still contains duplicate records after cleaning."
        return {'null_counts': null_counts, 'duplicate_count': duplicate_count}

    def convert_column_to_type(self, column: str, new_type: type) -> pd.DataFrame:
        if column not in self.df.columns:
            raise ValueError(f"Column name '{column}' not found in the DataFrame.")
        self.df[column] = self.df[column].astype(new_type)
        return self.df

    def drop_columns(self, columns: List[str]) -> pd.DataFrame:
        for column in columns:
            if column not in self.df.columns:
                raise ValueError(f"Column name '{column}' not found in the DataFrame.")
        self.df = self.df.drop(columns=columns)
        return self.df

    def filter_column_outliers(self, column: str, lower_bound: Union[float, int], upper_bound: Union[float, int]) -> pd.DataFrame:
        if column not in self.df.columns:
            raise ValueError(f"Column name '{column}' not found in the DataFrame.")
        self.df = self.df[(self.df[column] >= lower_bound) & (self.df[column] <= upper_bound)]
        return self.df

    def format_column_strings_to_lower_and_trim(self, column: str) -> pd.DataFrame:
        if column not in self.df.columns:
            raise ValueError(f"Column name '{column}' not found in the DataFrame.")
        self.df[column] = self.df[column].str.lower().str.strip()
        return self.df

    def format_column_strings_to_upper_and_trim(self, column: str) -> pd.DataFrame:
        if column not in self.df.columns:
            raise ValueError(f"Column name '{column}' not found in the DataFrame.")
        self.df[column] = self.df[column].str.upper().str.strip()
        return self.df

    def handle_missing_data(self, drop: bool = False, fill_value: Union[None, float, int, str] = None) -> pd.DataFrame:
        if drop:
            self.df = self.df.dropna()
        elif fill_value is not None:
            self.df = self.df.fillna(fill_value)
        return self.df

    def inspect_data(self) -> Tuple[str, str]:
        buffer = io.StringIO()
        self.df.info(buf=buffer)
        info_str = buffer.getvalue()
        describe_str = self.df.describe().to_string()
        return info_str, describe_str

    def parse_dates_to_add_standard_datetime(self, column: str) -> pd.DataFrame:
        if column not in self.df.columns:
            raise ValueError(f"Column name '{column}' not found in the DataFrame.")
        self.df['StandardDateTime'] = pd.to_datetime(self.df[column])
        return self.df

    def remove_duplicates(self) -> pd.DataFrame:
        self.df = self.df.drop_duplicates()
        return self.df

    def rename_columns(self, column_mapping: Dict[str, str]) -> pd.DataFrame:
        for old_name in column_mapping.keys():
            if old_name not in self.df.columns:
                raise ValueError(f"Column '{old_name}' not found in the DataFrame.")
        self.df = self.df.rename(columns=column_mapping)
        return self.df

    def reorder_columns(self, columns: List[str]) -> pd.DataFrame:
        for column in columns:
            if column not in self.df.columns:
                raise ValueError(f"Column name '{column}' not found in the DataFrame.")
        self.df = self.df[columns]
        return self.df

    def standardize_column_names(self) -> pd.DataFrame:
        self.df.columns = [col.strip().lower().replace(' ', '_') for col in self.df.columns]
        return self.df

    def convert_column_types(self, column_types: Dict[str, type]) -> pd.DataFrame:
        for col, dtype in column_types.items():
            if col not in self.df.columns:
                raise ValueError(f"Column '{col}' not found in the DataFrame.")
            self.df[col] = self.df[col].astype(dtype)
        return self.df
