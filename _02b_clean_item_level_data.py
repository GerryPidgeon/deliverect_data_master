import pandas as pd
import numpy as np
import os
import sys

# Update system path to include parent directories for module access
# This allows the script to import modules from two directories up in the folder hierarchy
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Import specific data and functions from external modules
from _00_shared_functions import column_name_cleaner, convert_to_custom_format, process_deliverect_shared_data, clean_deliverect_product_name
from _01_raw_import import imported_deliverect_item_level_detail_data
from _02a_clean_order_data import master_list_of_primary_keys

def process_deliverect_item_level_detail_data():
    # Initialize DataFrame for data processing
    # The 'imported_deliverect_item_level_detail_data' is assigned to 'df' for processing
    df = imported_deliverect_item_level_detail_data

    # Apply 'column_name_cleaner', 'convert_to_custom_format' and 'process_deliverect_shared_data' functions to DataFrame 'df'
    df = column_name_cleaner(df)
    df['OrderID'] = df['OrderID'].apply(convert_to_custom_format)
    df = process_deliverect_shared_data(df)


    df = clean_deliverect_product_name(df)

    # Correct
    df['Location'] = np.where(df['Location'] == '')

    # Sort the DataFrame by 'OrderPlacedDate', 'OrderPlacedTime', and 'PrimaryKey' to meet your sorting requirements
    df.sort_values(['OrderPlacedDate', 'OrderPlacedTime', 'PrimaryKey'], inplace=True)

    # Implement the master_list_of_primary_keys filter to this DataFrame
    primary_key_filter = master_list_of_primary_keys(df)
    df = df[df['PrimaryKey'].isin(primary_key_filter)]

    unique_count = df['PrimaryKey'].nunique()
    print("Count of unique records in 'PrimaryKey' column:", unique_count)

    # This processing is specific to this DataFrame and not shared with other
    # TEMP - Export for Checking
    os.chdir(r'C:\Users\gerry\Downloads')
    df.to_csv('Item Detail Data Checker.csv', index=False)

    return df

process_deliverect_item_level_detail_data()