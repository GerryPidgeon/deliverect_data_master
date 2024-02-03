import pandas as pd
import numpy as np
import os
import sys
import datetime as dt
from datetime import datetime, timedelta

# Update system path to include parent directories for module access
# This allows the script to import modules from two directories up in the folder hierarchy
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Import specific data and functions from external modules
from deliverect.D02a_clean_raw_data import cleaned_deliverect_order_data

def process_clean_item_level_detail():
    # Initialize DataFrame for data processing
    order_item_df = cleaned_deliverect_order_data.copy()

    # Data Cleaning for 'ProductName' and 'ProductPLU'
    # First, remove rows where 'ProductName' is null to ensure data quality
    order_item_df['ProductName'] = np.where(order_item_df['ProductName'].str.strip() == '', np.nan, order_item_df['ProductName'])
    order_item_df = order_item_df[order_item_df['ProductName'].notnull()]

    # Split the 'ProductPLU' and 'ProductName' columns into lists based on commas
    order_item_df['ProductPLU'] = order_item_df['ProductPLU'].str.split(', ')
    order_item_df['ProductName'] = order_item_df['ProductName'].str.split(', ')

    # Replace Non-Standard ProductPLUs with 'Missing'
    order_item_df['ProductPLU'] = order_item_df['ProductPLU'].replace('', 'Missing').fillna('Missing')
    order_item_df['ProductPLU'] = order_item_df['ProductPLU'].apply(lambda x: 'Missing' if isinstance(x, str) and not x.startswith(('P', 'M')) else x)

    # Combine 'ProductPLU' and 'ProductName' into tuples in a new column 'ProductPLUName'
    # This creates a pair of PLU and ProductName for each product in an order
    order_item_df['ProductPLUName'] = order_item_df.apply(lambda x: list(zip(x['ProductPLU'], x['ProductName'])), axis=1)

    # 'Explode' the tuples to create separate rows for each product in an order
    # This helps in breaking down orders into individual product entries
    order_item_df = order_item_df.explode('ProductPLUName')

    # Split the tuples back into 'ProductPLU' and 'ProductName' columns
    # This separates the product data back into distinct columns
    order_item_df[['ProductPLU', 'ProductName']] = pd.DataFrame(order_item_df['ProductPLUName'].tolist(), index=order_item_df.index)

    # Drop the temporary column used for tuples
    order_item_df = order_item_df.drop(columns='ProductPLUName')

    # Split 'ProductName' to separate the quantity from the product name
    # This assumes the quantity is always the first word in the product name
    order_item_df[['Quantity', 'Product_Name']] = order_item_df['ProductName'].str.split(' ', n=1, expand=True)

    # Split 'ProductPLU' to separate the PLU from the quantity (if present)
    # This assumes the format 'PLU: Quantity' in the 'ProductPLU' field
    order_item_df[['PLU_Name', 'Quantity']] = order_item_df['ProductPLU'].str.split(': ', n=1, expand=True)

    # Drop the original 'ProductPLU' and 'ProductName' columns as they are no longer needed
    order_item_df = order_item_df.drop(columns=['ProductPLU', 'ProductName'])

    # Rename the new columns to 'ProductPLU' and 'ProductName' for clarity
    order_item_df = order_item_df.rename(columns={'PLU_Name': 'ProductPLU', 'Product_Name': 'ProductName'})

    # TODO: Create this as a shared function
    # Fill missing or empty 'ProductPLU' entries with 'Missing'
    # Also apply a condition to tag entries not starting with 'P' or 'M' as 'Missing'
    order_item_df['ProductPLU'] = order_item_df['ProductPLU'].replace('', 'Missing').fillna('Missing')
    order_item_df['ProductPLU'] = order_item_df['ProductPLU'].apply(lambda x: 'Missing' if isinstance(x, str) and not x.startswith(('P', 'M')) else x)

    # Standardize specific terms in 'ProductName' for consistency, e.g., capitalizing 'Plant-Based'
    order_item_df['ProductName'] = order_item_df['ProductName'].str.replace('plant-based', 'Plant-Based', case=False, regex=True)
    order_item_df['ProductName'] = order_item_df['ProductName'].str.replace('plant based', 'Plant-Based', case=False, regex=True)

    # Clean ProductName and ProductPLU
    order_item_df['ProductName'] = order_item_df['ProductName'].str.strip()
    order_item_df['ProductPLU'] = order_item_df['ProductPLU'].str.strip()

    # Convert the 'Quantity' column to a numeric type (int or float) to ensure proper summation
    # 'coerce' will set invalid parsing as NaN
    order_item_df['Quantity'] = pd.to_numeric(order_item_df['Quantity'], errors='coerce')

    # Create item level PrimaryKey
    order_item_df['PrimaryKeyItem'] = np.where(order_item_df['ProductPLU'] == 'Missing', order_item_df['PrimaryKeyAlt'] + ' ' + order_item_df['ProductName'], order_item_df['PrimaryKeyAlt'] + ' ' + order_item_df['ProductPLU'])

    # Save main DataFrame for checking
    os.chdir(r'H:\Shared drives\97 - Finance Only\20 - New Python Code\03 - Cleaned Data')
    order_item_df.to_csv('D02B - Broken Out Item Level Data.csv', index=False)

    return order_item_df

broken_out_order_data = process_clean_item_level_detail()