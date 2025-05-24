from scripts.data_scrubber import DataScrubber

scrubber = DataScrubber("data/raw_sales_data.csv")
scrubber.remove_duplicates()
scrubber.handle_missing_values()
# etc.

scrubber.save_clean_data("data/clean_sales_data.csv")
