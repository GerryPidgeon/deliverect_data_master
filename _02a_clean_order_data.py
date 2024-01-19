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
    df = process_deliverect_shared_data(df)
    df = clean_deliverect_product_name(df)
    df = process_deliverect_remove_duplicates(df)

    # Sort the DataFrame by 'OrderPlacedDate', 'OrderPlacedTime', and 'PrimaryKey' to meet your sorting requirements
    df.sort_values(['OrderPlacedDate', 'OrderPlacedTime', 'PrimaryKey'], inplace=True)

    # Create Unique List of PrimaryKey, Location and Brand to filter and correct Item Level Detail
    def unique_primary_key_list():
        unique_primary_key_df = df[['PrimaryKey', 'PrimaryKeyAlt', 'Location', 'Brand', 'OrderStatus', 'GrossAOV']]
        unique_primary_key_df = unique_primary_key_df.drop_duplicates()
        return unique_primary_key_df

    # Exclude Single Duplicate of Order 865 at Friedrichshain in Jan 23
    df = df[(df['OrderID'] != '#865') & (df['Location'] != 'Friedrichshain') & (df['OrderPlacedTime'] != '20:05:00')]

    return df, unique_primary_key_list

cleaned_deliverect_order_data, unique_primary_keys = process_deliverect_order_data()