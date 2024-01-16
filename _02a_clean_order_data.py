import pandas as pd
import numpy as np
import os
import sys
import csv

# Update system path to include parent directories for module access
# This allows the script to import modules from two directories up in the folder hierarchy
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Import specific data and functions from external modules
from _00_shared_functions import column_name_cleaner, convert_to_custom_format, process_deliverect_shared_data, clean_deliverect_product_name, process_deliverect_remove_duplicates
from _01_raw_import import imported_deliverect_order_data

def process_deliverect_order_data():
    # Initialize DataFrame for data processing
    # The 'imported_deliverect_item_level_detail_data' is assigned to 'df' for processing
    df = imported_deliverect_order_data

    # Apply 'column_name_cleaner', 'convert_to_custom_format', 'process_deliverect_shared_data', 'clean_deliverect_product_name' and 'process_deliverect_remove_duplicates' functions to DataFrame 'df'
    df = column_name_cleaner(df)
    df['OrderID'] = df['OrderID'].apply(convert_to_custom_format)

    # TEMP - Export for Checking
    os.chdir(r'C:\Users\gerry\Downloads')
    df.to_csv('Order Data Checker 1.csv', index=False)
    df = process_deliverect_shared_data(df) # Lost 22 Records
    os.chdir(r'C:\Users\gerry\Downloads')
    df.to_csv('Order Data Checker 2.csv', index=False)
    df = clean_deliverect_product_name(df)
    df = process_deliverect_remove_duplicates(df)

    # Sort the DataFrame by 'OrderPlacedDate', 'OrderPlacedTime', and 'PrimaryKey' to meet your sorting requirements
    df.sort_values(['OrderPlacedDate', 'OrderPlacedTime', 'PrimaryKey'], inplace=True)

    # Create Master List of PrimaryKey to apply to_02b_clean_item_level_data
    def create_master_list_of_primary_keys(df):
        master_list_of_primary_keys = df['PrimaryKey'].unique().tolist()
        return master_list_of_primary_keys

    # TEMP - Export for Checking
    os.chdir(r'C:\Users\gerry\Downloads')
    df.to_csv('Order Data Checker.csv', index=False)

    return df, create_master_list_of_primary_keys

df, master_list_of_primary_keys = process_deliverect_order_data()