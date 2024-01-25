import pandas as pd
import numpy as np
import os
import sys

# Update system path to include parent directories for module access
# This allows the script to import modules from two directories up in the folder hierarchy
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Import specific data and functions from external modules
from _02a_clean_order_data import cleaned_deliverect_order_data
from _02b_clean_item_level_data import cleaned_deliverect_item_level_detail_data, price_discrepancies_data

def output_deliverect_data():
    # Initialize DataFrame for data processing
    # The 'imported_deliverect_item_level_detail_data' is assigned to 'df' for processing
    item_df = cleaned_deliverect_item_level_detail_data
    price_discrepancies_df = price_discrepancies_data

    # Merge DataFrames together
    item_df = pd.merge(item_df, price_discrepancies_df[['PrimaryKey', 'PriceDifference', 'AOVCheck']], on='PrimaryKey', how='left')

    # This line filters 'item_df' to include only those rows where the 'AOVCheck' column has the value 'Price Discrepancies'.
    # These filtered rows are then assigned to a new DataFrame 'amendment_df'.
    amendment_df = item_df[item_df['AOVCheck'] == 'Price Discrepancies']

    # This line removes duplicate rows from 'amendment_df' based on the 'PrimaryKey' column.
    # 'keep='first'' ensures that only the first occurrence of each duplicate is kept.
    amendment_df = amendment_df.drop_duplicates(subset='PrimaryKey', keep='first')

    # The following lines are modifying certain columns of 'amendment_df'.
    # 'ProductPLU' is set to a fixed value 'x-xx-xxxx-x' for all rows.
    amendment_df['ProductPLU'] = 'x-xx-xxxx-x'
    amendment_df['ProductPLU'] = np.where(amendment_df['ProductPLU'].str.startswith(('P', 'M'), na=False), amendment_df['ProductPLU'], 'x-xx-xxxx-x')

    # 'ProductName' is set to a fixed value 'Balancing Item' for all rows.
    amendment_df['ProductName'] = 'Balancing Item'

    # 'ItemPrice' is set to the values from 'PriceDifference' column of 'amendment_df'.
    # This implies that the price discrepancy is being accounted for in 'ItemPrice'.
    amendment_df['ItemPrice'] = amendment_df['PriceDifference']

    # 'ItemQuantity' is set to 1 for all rows, indicating a single unit for each item.
    amendment_df['ItemQuantity'] = 1

    # Add Price Difference Rows to item_df
    item_df['PriceDifference'] = 0
    item_df = pd.concat([item_df, amendment_df])
    item_df['TotalItemCost'] = (item_df['ItemPrice'] * item_df['ItemQuantity'])
    item_df['TotalItemCost'] = item_df['TotalItemCost'].round(2)
    item_df.sort_values(['OrderPlacedDate', 'OrderPlacedTime', 'PrimaryKey'], inplace=True)

    # Drop records that aren't needed
    item_df = item_df.drop(columns=(['PriceDifference', 'AOVCheck']))

    os.chdir(r'H:\Shared drives\97 - Finance Only\10 - Cleaned Data\02 - Processed Data\01 - Data Checking')
    item_df.to_csv('Processed Item Detail Data With Balancing Items.csv', index=False)

    return item_df

adjusted_item_level_detail = output_deliverect_data()