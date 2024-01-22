import pandas as pd
import numpy as np
import os
import sys

# Update system path to include parent directories for module access
# This allows the script to import modules from two directories up in the folder hierarchy
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Import specific data and functions from external modules
from _02a_clean_order_data import cleaned_deliverect_order_data
from _02b_clean_item_level_data import cleaned_deliverect_item_level_detail_data

def process_clean_item_level_detail():
    # Initialize DataFrame for data processing
    # The 'imported_deliverect_item_level_detail_data' is assigned to 'df' for processing
    order_df = cleaned_deliverect_order_data.copy()
    item_df = cleaned_deliverect_item_level_detail_data.copy()

    # Data Cleaning for 'ProductName' and 'ProductPLU'
    # First, remove rows where 'ProductName' is null to ensure data quality
    order_df['ProductName'] = np.where(order_df['ProductName'].str.strip() == '', np.nan, order_df['ProductName'])
    order_df = order_df[order_df['ProductName'].notnull()]

    # Split the 'ProductPLU' and 'ProductName' columns into lists based on commas
    order_df['ProductPLU'] = order_df['ProductPLU'].str.split(', ')
    order_df['ProductName'] = order_df['ProductName'].str.split(', ')

    # Define the function to check and replace PLU codes
    def check_and_replace_plu_codes(plu_list):
        new_plu_list = []
        for item in plu_list:
            if ': ' in item:  # Check if the item follows the pattern 'code: quantity'
                plu, quantity = item.split(': ')
                if not plu.startswith(('P-', 'M-')):
                    plu = 'Missing'
                new_plu_list.append(f'{plu}: {quantity}')
            else:
                new_plu_list.append('Missing')  # Handle the case where there is no ': '
        return new_plu_list

    # Apply the function to each list in the 'ProductPLU' column
    order_df['ProductPLU'] = order_df['ProductPLU'].apply(check_and_replace_plu_codes)

    # Fill missing or empty 'ProductPLU' entries with 'Missing'
    # This is done after the custom cleaning because the lists have been transformed
    order_df['ProductPLU'] = order_df['ProductPLU'].apply(lambda lst: ['Missing'] if not lst else lst)

    # Combine 'ProductPLU' and 'ProductName' into tuples in a new column 'ProductPLUName'
    # This creates a pair of PLU and ProductName for each product in an order
    order_df['ProductPLUName'] = order_df.apply(lambda x: list(zip(x['ProductPLU'], x['ProductName'])), axis=1)

    # Rename the new columns to 'ProductPLU' and 'ProductName' for clarity
    order_df = order_df.rename(columns={'PLU_Name': 'ProductPLU', 'Product_Name': 'ProductName'})



    # Change Directory
    os.chdir(r'H:\Shared drives\97 - Finance Only\10 - Cleaned Data\02 - Processed Data\01 - Data Checking')
    order_df.to_csv('Cleaned Item List.csv', index=False)

    return order_df, item_df

process_clean_item_level_detail()