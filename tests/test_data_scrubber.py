import unittest
import pandas as pd
from utils.data_scrubber import DataScrubber

class TestDataScrubber(unittest.TestCase):

    def setUp(self):
        self.df = pd.DataFrame({
            'Name': [' Alice ', 'Bob', 'alice', 'Bob'],
            'Age': [25, 30, 25, 30],
            'Date': ['2023-01-01', '2023-01-02', None, '2023-01-02']
        })
        self.scrubber = DataScrubber(self.df)

    def test_remove_duplicates(self):
        cleaned_df = self.scrubber.remove_duplicates()
        self.assertEqual(len(cleaned_df), 3)

    def test_handle_missing_data(self):
        cleaned_df = self.scrubber.handle_missing_data(fill_value="N/A")
        self.assertFalse(cleaned_df.isnull().values.any())

    def test_standardize_column_names(self):
        df = pd.DataFrame(columns=[' First Name ', 'Last Name', 'AGE'])
        scrubber = DataScrubber(df)
        cleaned_df = scrubber.standardize_column_names()
        expected_columns = ['first_name', 'last_name', 'age']
        self.assertListEqual(cleaned_df.columns.tolist(), expected_columns)

    def test_convert_column_types(self):
        df = pd.DataFrame({'Age': ['25', '30', '35']})
        scrubber = DataScrubber(df)
        column_types = {'Age': 'int'}
        cleaned_df = scrubber.convert_column_types(column_types)
        self.assertTrue(pd.api.types.is_integer_dtype(cleaned_df['Age']))


if __name__ == '__main__':
    unittest.main()
