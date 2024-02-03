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
from shared_functions.S00_shared_functions import clean_rx_names, convert_to_custom_format
from deliverect.D00_shared_functions import column_name_cleaner, process_deliverect_shared_data, clean_deliverect_product_name, process_deliverect_remove_duplicates, column_name_sorter
from deliverect.D01_raw_data_import import imported_deliverect_order_data, imported_deliverect_item_level_detail_data

# Define Unique Functions, only used in this .py files
def unique_primary_key_list(df):
    df = df[['PrimaryKey', 'PrimaryKeyAlt', 'Location', 'Brand', 'OrderStatus', 'GrossAOV']]
    df = df.drop_duplicates()
    return df

def process_deliverect_order_data():
    # Initialize DataFrame for data processing
    df = imported_deliverect_order_data

    # Apply 'column_name_cleaner', 'convert_to_custom_format', 'process_deliverect_shared_data', 'clean_deliverect_product_name' and 'process_deliverect_remove_duplicates' functions to DataFrame 'df'
    df = column_name_cleaner(df)
    df = clean_rx_names(df)
    df['OrderID'] = df['OrderID'].apply(convert_to_custom_format)
    df = process_deliverect_shared_data(df)
    df = clean_deliverect_product_name(df)
    df = process_deliverect_remove_duplicates(df)

    # Arrange Columns and sort the DataFrame by 'OrderPlacedDate', 'OrderPlacedTime', and 'PrimaryKey' to meet your sorting requirements
    df = column_name_sorter(df)
    df.sort_values(['OrderPlacedDate', 'OrderPlacedTime', 'PrimaryKey'], inplace=True)

    # Create a Unique List of PrimaryKeys
    unique_primary_key_df = unique_primary_key_list(df)

    # Export data for checking
    os.chdir(r'H:\Shared drives\97 - Finance Only\20 - New Python Code\04 - Cleaned Data')
    df.to_csv('D02A - Cleaned Raw Order Data.csv', index=False)

    return df, unique_primary_key_df

cleaned_deliverect_order_data, unique_primary_keys = process_deliverect_order_data()

def process_deliverect_item_level_detail_data():
    # Initialize DataFrame for data processing
    # The 'imported_deliverect_item_level_detail_data' is assigned to 'df' for processing
    df = imported_deliverect_item_level_detail_data.copy()

    # Apply 'column_name_cleaner', 'convert_to_custom_format', 'process_deliverect_shared_data', 'clean_deliverect_product_name' and 'process_deliverect_remove_duplicates' functions to DataFrame 'df'
    df = column_name_cleaner(df)
    df = clean_rx_names(df)
    df['OrderID'] = df['OrderID'].apply(convert_to_custom_format)
    df = process_deliverect_shared_data(df)
    df = clean_deliverect_product_name(df)

    # Correct ItemPrice from Euro cents to Euros
    df['ItemPrice'] = (df['ItemPrice'] / 100).round(2)
    df['TotalItemCost'] = (df['ItemPrice'] * df['ItemQuantity'])
    df['TotalItemCost'] = df['TotalItemCost'].round(2)
    df['GrossAOV'] = df['GrossAOV'].round(2)

    # Replace Non-Standard ProductPLUs with 'Missing'
    # df['ProductPLU'] = np.where(df['ProductPLU'].str.startswith(('P-', 'M-')), df['ProductPLU'], 'Missing')
    df['ProductPLU'] = df['ProductPLU'].replace('', 'Missing').fillna('Missing')
    df['ProductPLU'] = df['ProductPLU'].apply(lambda x: 'Missing' if isinstance(x, str) and not x.startswith(('P', 'M')) else x)

    # Standardize specific terms in 'ProductName' for consistency, e.g., capitalizing 'Plant-Based'
    df['ProductName'] = df['ProductName'].str.replace('plant-based', 'Plant-Based', case=False, regex=True)
    df['ProductName'] = df['ProductName'].str.replace('plant based', 'Plant-Based', case=False, regex=True)

    # Exclude Single Duplicate of Order 865 at Friedrichshain in Jan 23
    # TODO: Work out why this isn't removing the duplicate
    df = df[~((df['OrderID'] == '#865') & (df['Location'] == 'Friedrichshain') & (df['OrderPlacedTime'] == '20:05:00'))]

    # Arrange Columns and sort the DataFrame by 'OrderPlacedDate', 'OrderPlacedTime', and 'PrimaryKey' to meet your sorting requirements
    df = column_name_sorter(df)
    df.sort_values(['OrderPlacedDate', 'OrderPlacedTime', 'PrimaryKey'], inplace=True)

    # Create item level PrimaryKey
    df['PrimaryKeyItem'] = np.where(df['ProductPLU'] == 'Missing', df['PrimaryKeyAlt'] + ' ' + df['ProductName'], df['PrimaryKeyAlt'] + ' ' + df['ProductPLU'])

    # Implement the master_list_of_primary_keys filter to this DataFrame
    primary_key_filter_df = unique_primary_keys

    # Filter to align with cleaned order list
    df = pd.merge(df, primary_key_filter_df[['PrimaryKey', 'PrimaryKeyAlt', 'Location', 'Brand', 'OrderStatus', 'GrossAOV']], on=['PrimaryKeyAlt', 'OrderStatus'], how='left', suffixes=('', '_temp'))
    df = df.loc[df['Location_temp'].notna()]

    # Extract Missing Records to save to Data Checking Folder
    missing_records_df = df.loc[df['Location_temp'].isna()]
    missing_records_df = missing_records_df.drop(columns={'PrimaryKey_temp', 'Location_temp', 'Brand_temp', 'GrossAOV_temp', 'ItemPrice', 'ItemQuantity', 'ProductPLU', 'ProductName'})
    missing_records_df = missing_records_df.drop_duplicates()

    # Create a new DataFrame 'aov_check_df' containing specific columns from 'df' for checking the Average Order Value (AOV)
    aov_check_df = df[['PrimaryKey', 'PrimaryKeyAlt', 'ItemPrice', 'ItemQuantity', 'GrossAOV', 'GrossAOV_temp', 'TotalItemCost']].copy()

    # Sum the 'TotalItemCost' for each unique 'PrimaryKey' and reset the index to turn the grouped data back into a DataFrame.
    summed_costs = aov_check_df.groupby('PrimaryKey')['TotalItemCost'].sum().reset_index()

    # Extract the first occurrence of each 'PrimaryKey' along with its 'GrossAOV'.
    first_gross_aov = aov_check_df.drop_duplicates(subset='PrimaryKey')[['PrimaryKey', 'GrossAOV']]

    # Merge 'summed_costs' with 'first_gross_aov' on 'PrimaryKey' to combine the summed costs and gross AOVs.
    comparison_df = pd.merge(summed_costs, first_gross_aov, on='PrimaryKey')

    # Calculate the price difference between 'GrossAOV' and 'TotalItemCost' and round to 2 decimal places.
    comparison_df['GrossAOV'] = comparison_df['GrossAOV'].round(2)
    comparison_df['TotalItemCost'] = comparison_df['TotalItemCost'].round(2)
    comparison_df['PriceDifference'] = comparison_df['GrossAOV'] - comparison_df['TotalItemCost']
    comparison_df['PriceDifference'] = comparison_df['PriceDifference'].round(2)

    # Determine the AOV check status:
    comparison_df['AOVCheck'] = np.where(comparison_df['TotalItemCost'] == comparison_df['GrossAOV'], 'OK', 'Price Discrepancies')

    # If 'TotalItemCost' is a multiple of 'GrossAOV' but not equal, mark as 'Multiple Entries'.
    comparison_df['AOVCheck'] = np.where((comparison_df['TotalItemCost'] % comparison_df['GrossAOV'] == 0) & (comparison_df['TotalItemCost'] != comparison_df['GrossAOV']), 'Multiple Entries', comparison_df['AOVCheck'])

    # Create two new DataFrames for specific AOV check statuses:
    multiple_entry_df = comparison_df.loc[comparison_df['AOVCheck'] == 'Multiple Entries']
    price_discrepancies_df = comparison_df.loc[comparison_df['AOVCheck'] == 'Price Discrepancies']

    # Save Data Checking CSV Files
    os.chdir(r'H:\Shared drives\97 - Finance Only\20 - New Python Code\02 - Data Checkers')
    missing_records_df.to_csv('Missing Records.csv', index=False)
    multiple_entry_df.to_csv('Multiple Entries.csv', index=False)
    price_discrepancies_df.to_csv('Price Discrepancies.csv', index=False)

    # Save main DataFrame for checking
    os.chdir(r'H:\Shared drives\97 - Finance Only\20 - New Python Code\04 - Cleaned Data')
    df.to_csv('D02A - Cleaned Raw Item Level Data.csv', index=False)

    return df, price_discrepancies_df

cleaned_deliverect_item_level_detail_data, price_discrepancies_data = process_deliverect_item_level_detail_data()