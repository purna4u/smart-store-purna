# p6_olap_age_analysis.py
#
# This script addresses the business goal:
# "How does sales performance differ across customer age groups and product categories, 
#  and when are our key customer segments most active?"

# =========================================
# =========================================
# 1. SETUP & IMPORTS
# =========================================
import sqlite3
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# =========================================
# 2. DATA LOADING
# =========================================
print("Connecting to database...")
# The path navigates up two directories from 'scripts/olap' to the project root
conn = sqlite3.connect("C:/Repos/project1/smart-store-purna/data/dw/smart_sales.db")

# SQL query to join the necessary tables
# Corrected SQL Query
query = """
SELECT
    s.sale_date,
    s.sale_amount,
    p.category,
    c.customer_segment
FROM sale s
JOIN customer c ON s.customer_id = c.customer_id
JOIN product p ON s.product_id = p.product_id;
"""

print("Loading and joining data...")
df = pd.read_sql_query(query, conn)
conn.close()

# Validate by printing the first few rows
print("Data loaded successfully. Here are the first 5 rows:")
print(df.head())

# =========================================
# 3. DATA TRANSFORMATION
# =========================================
print("\nTransforming data...")
# Convert 'sale_date' to datetime
# TO: (add the format='mixed' argument)
# TO: (Use errors='coerce' to turn bad dates into NaT instead of crashing)
df['sale_date'] = pd.to_datetime(df['sale_date'], errors='coerce')
# Check if any dates failed to convert
invalid_dates_count = df['sale_date'].isnull().sum()
if invalid_dates_count > 0:
    print(f"\nWarning: Found and removed {invalid_dates_count} rows with invalid date formats.")
    # Drop rows where sale_date could not be parsed
    df.dropna(subset=['sale_date'], inplace=True)

# Now that bad data is removed, we can safely continue with transformations
df['day_of_week'] = df['sale_date'].dt.day_name()

# Create 'day_of_week' column from the correct date column
df['day_of_week'] = df['sale_date'].dt.day_name()

# Rename the 'customer_segment' column to 'age_segment' so the rest of the script works without changes
df.rename(columns={'customer_segment': 'age_segment'}, inplace=True)

# Validate the new columns
print("Data transformed. Here are the first 5 rows with new columns:")
print(df.head())

# =========================================
# 4. OLAP ANALYSIS (SLICE, DICE, DRILL-DOWN)
# =========================================
# - SLICE: Filter the DataFrame to create a "slice" containing only the 'Young Adult' data.
# - DICE: On that slice, group by 'day_of_week' and 'category' to get total sales. 
#   This is the core analysis for our specific question.

# =========================================
# 5. VISUALIZATION & OUTPUT
# =========================================
# - Generate a bar chart showing sales by day of the week for the Young Adult segment.
# - Print the key findings to the console.



# =======================================================
# 4. OLAP ANALYSIS (SLICE, DICE, DRILL-DOWN)
# =======================================================
print("\nPerforming OLAP analysis...")
# SLICE: Filter the DataFrame to get a slice for a specific customer segment.
# Let's assume 'Regular' is a key segment we want to analyze.
# You can change 'Regular' to 'New' or 'Loyal' if those exist in your data.
target_segment_slice = df[df['age_segment'] == 'Regular'].copy()
print(f"Sliced data for '{target_segment_slice['age_segment'].iloc[0]}' segment. Found {len(target_segment_slice)} sales records.")

# DICE: On that slice, group by 'day_of_week' to find their busiest shopping days
daily_sales_by_segment = target_segment_slice.groupby('day_of_week')['sale_amount'].sum()

# Sort the results by day for correct plotting
days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
daily_sales_by_segment = daily_sales_by_segment.reindex(days_order)

# This is the final aggregated data we need for our insight
print("\n--- Insight: Total Sales for the Target Segment by Day ---")
print(daily_sales_by_segment)


# =======================================================
# 5. VISUALIZATION & OUTPUT (Task 4)
# =======================================================
print("\nGenerating visualization...")
plt.figure(figsize=(10, 6))
sns.barplot(x=daily_sales_by_segment.index, y=daily_sales_by_segment.values, palette='viridis')

plt.title('Total Sales for "Regular" Customers by Day of Week', fontsize=16)
plt.xlabel('Day of the Week', fontsize=12)
plt.ylabel('Total Sales ($)', fontsize=12)
plt.xticks(rotation=45)
plt.tight_layout() # Adjusts plot to ensure everything fits without overlapping

# This command will display the chart in a new window
plt.show()

print("\n--- Script Finished ---")