import pandas as pd
import os
import sys

# Update system path to include parent directories for module access
# This allows the script to import modules from two directories up in the folder hierarchy
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Import specific data and functions from external modules
from _00_shared_functions import column_name_cleaner, convert_to_custom_format, process_deliverect_shared_data, clean_deliverect_product_name
from _01_raw_import import imported_deliverect_item_level_detail_data
from _02a_clean_order_data import unique_primary_keys

def process_deliverect_item_level_detail_data():
    # Initialize DataFrame for data processing
    # The 'imported_deliverect_item_level_detail_data' is assigned to 'df' for processing
    df = imported_deliverect_item_level_detail_data

    # Apply 'column_name_cleaner', 'convert_to_custom_format' and 'process_deliverect_shared_data' functions to DataFrame 'df'
    df = column_name_cleaner(df)
    df['OrderID'] = df['OrderID'].apply(convert_to_custom_format)
    df = process_deliverect_shared_data(df)
    df = clean_deliverect_product_name(df)

    # Correct ItemPrice from Euro cents to Euros
    df['ItemPrice'] = (df['ItemPrice'] / 100).round(2)

    # Sort the DataFrame by 'OrderPlacedDate', 'OrderPlacedTime', and 'PrimaryKey' to meet your sorting requirements
    df.sort_values(['OrderPlacedDate', 'OrderPlacedTime', 'PrimaryKey'], inplace=True)

    # Implement the master_list_of_primary_keys filter to this DataFrame
    primary_key_filter_df = unique_primary_keys()

    # Correct PrimaryKey, LocWithBrand and Location from Order Data
    df = pd.merge(df, primary_key_filter_df[['PrimaryKey', 'PrimaryKeyAlt', 'Location', 'Brand', 'GrossAOV']], on='PrimaryKeyAlt', how='left', suffixes=('', '_temp'))

    # Extract Missing Records
    missing_records_df = df.loc[df['Location_temp'].isna()]
    missing_records_df = missing_records_df.drop(columns={'PrimaryKey_temp', 'Location_temp', 'Brand_temp', 'GrossAOV_temp', 'ItemPrice', 'ItemQuantity', 'ProductPLU', 'ProductName'})
    missing_records_df = missing_records_df.drop_duplicates()

    # Check AOV
    aov_check_df = df[['PrimaryKey', 'PrimaryKeyAlt', 'ItemPrice', 'ItemQuantity', 'GrossAOV', 'GrossAOV_temp']].copy()
    aov_check_df['TotalItemCost'] = aov_check_df['ItemPrice'] * aov_check_df['ItemQuantity']



    print(aov_check_df)

    return df

cleaned_deliverect_item_level_detail_data = process_deliverect_item_level_detail_data()