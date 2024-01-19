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

    # Exclude Single Duplicate of Order 865 at Friedrichshain in Jan 23
    df = df[~((df['OrderID'] == '#865') & (df['Location'] == 'Friedrichshain') & (df['OrderPlacedTime'] == '20:05:00'))]

    # Implement the master_list_of_primary_keys filter to this DataFrame
    primary_key_filter_df = unique_primary_keys()

    # Change Directory
    os.chdir(r'H:\Shared drives\97 - Finance Only\10 - Cleaned Data\02 - Processed Data\01 - Data Checking')

    # Filter to align with cleaned order list
    df = pd.merge(df, primary_key_filter_df[['PrimaryKey', 'PrimaryKeyAlt', 'Location', 'Brand', 'OrderStatus', 'GrossAOV']], on=['PrimaryKeyAlt', 'OrderStatus'], how='left', suffixes=('', '_temp'))
    df = df.loc[df['Location_temp'].notna()]
    df.to_csv('Cleaned Item List.csv', index=False)

    # Extract Missing Records
    missing_records_df = df.loc[df['Location_temp'].isna()]
    missing_records_df = missing_records_df.drop(columns={'PrimaryKey_temp', 'Location_temp', 'Brand_temp', 'GrossAOV_temp', 'ItemPrice', 'ItemQuantity', 'ProductPLU', 'ProductName'})
    missing_records_df = missing_records_df.drop_duplicates()
    missing_records_df.to_csv('Missing Records.csv', index=False)

    # Check AOV
    aov_check_df = df[['PrimaryKey', 'PrimaryKeyAlt', 'ItemPrice', 'ItemQuantity', 'GrossAOV', 'GrossAOV_temp']].copy()
    aov_check_df['TotalItemCost'] = aov_check_df['ItemPrice'] * aov_check_df['ItemQuantity']
    summed_costs = aov_check_df.groupby('PrimaryKey')['TotalItemCost'].sum().reset_index()
    first_gross_aov = aov_check_df.drop_duplicates(subset='PrimaryKey')[['PrimaryKey', 'GrossAOV']]
    comparison_df = pd.merge(summed_costs, first_gross_aov, on='PrimaryKey')
    comparison_df['TotalItemCost'] = comparison_df['TotalItemCost'].round(2)
    comparison_df['GrossAOV'] = comparison_df['GrossAOV'].round(2)
    comparison_df['AOVCheck'] = np.where(comparison_df['TotalItemCost'] == comparison_df['GrossAOV'], 'OK', 'Price Discrepancies')
    comparison_df['AOVCheck'] = np.where((comparison_df['TotalItemCost'] % comparison_df['GrossAOV'] == 0) & (comparison_df['TotalItemCost'] != comparison_df['GrossAOV']), 'Multiple Entries', comparison_df['AOVCheck'])
    price_discrepancies_df = comparison_df.loc[comparison_df['AOVCheck'] == 'Price Discrepancies']
    multiple_entry_df = comparison_df.loc[comparison_df['AOVCheck'] == 'Multiple Entries']

    # Export CSV for checking
    multiple_entry_df.to_csv('Multiple Entries.csv', index=False)
    price_discrepancies_df.to_csv('Price Discrepancies.csv', index=False)

    return df

cleaned_deliverect_item_level_detail_data = process_deliverect_item_level_detail_data()