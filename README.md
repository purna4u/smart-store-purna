# smart-store-purna


## Smart Sales Project

## Observations after exploring the data:

### Most common customer location:
- The most common customer location is **New York**, appearing 3 times in the dataset.

### Highest/Lowest product price:
- The highest product price is **150.00**, and the lowest product price is **25.75**.

### Estimated Average, Minimum, and Maximum Sales:
- **Average sales**: 160
- **Minimum sales**: 50
- **Maximum sales**: 300

### P2. BI Python w/External Packages (.venv, execute, add-commit-push)
Module 2 Assignment for CSIS 44632

Module 2 Changes
- Added logger script to utils folder

- Added data preparation script to scripts folder

Ran the following scripts

    git pull
    py -m venv .venv
    .venv\Scripts\activate
    py -m pip install --upgrade pip setuptools wheel
    py -m pip install --upgrade -r requirements.txt --timeout 100
    py scripts/data_prep.py

## Installed Pandas and loguru. Here are the commands
    pip install loguru
    pip install pandas

To run the data_prep.py script in Windows PowerShell terminal:

```bash
py scripts\data_prep.py


Ran the following scripts to push changes to Github repo:


git add .
git commit -m "ran initial data_prep.py"
git push -u origin main


### Module 3:

## Updates Made

As part of this assignment, additional columns were added to each dataset:

### Customers Data (`customers_data.csv`)
- **Numeric Column**: `LoyaltyPoints` – Represents points accumulated by each customer
- **Category Column**: `PreferredContactMethod` – Indicates whether the customer prefers Email, Phone, or Text

### Products Data (`products_data.csv`)
- **Numeric Column**: `StockQuantity` – Number of units available in inventory
- **Category Column**: `Subcategory` – Product grouping such as Electronics, Apparel, or Grocery

### Sales Data (`sales_data.csv`)
- **Numeric Column**: `DiscountPercent` – Percentage of discount applied to the transaction
- **Category Column**: `PaymentType` – Method of payment such as CreditCard, Cash, or PayPal

## Tools Used

- Python
- VS Code
- Git & GitHub
- Excel (for simple data editing)






