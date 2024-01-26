import pandas as pd
import numpy as np
import os
import sys

# Update system path to include parent directories for module access
# This allows the script to import modules from two directories up in the folder hierarchy
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Import specific data and functions from external modules
from _02a_clean_order_data import cleaned_deliverect_order_data

def process_clean_item_level_detail():
    # Initialize DataFrame for data processing
    # The 'imported_deliverect_item_level_detail_data' is assigned to 'df' for processing
    order_item_df = cleaned_deliverect_order_data.copy()

    # Data Cleaning for 'ProductName' and 'ProductPLU'
    # First, remove rows where 'ProductName' is null to ensure data quality
    order_item_df['ProductName'] = np.where(order_item_df['ProductName'].str.strip() == '', np.nan, order_item_df['ProductName'])
    order_item_df = order_item_df[order_item_df['ProductName'].notnull()]

    # Split the 'ProductPLU' and 'ProductName' columns into lists based on commas
    order_item_df['ProductPLU'] = order_item_df['ProductPLU'].str.split(', ')
    order_item_df['ProductName'] = order_item_df['ProductName'].str.split(', ')

    # # Define the function to check and replace PLU codes
    # def check_and_replace_plu_codes(plu_list):
    #     new_plu_list = []
    #     for item in plu_list:
    #         if ': ' in item:  # Check if the item follows the pattern 'code: quantity'
    #             plu, quantity = item.split(': ')
    #             if not plu.startswith(('P-', 'M-')):
    #                 plu = 'Missing'
    #             new_plu_list.append(f'{plu}: {quantity}')
    #         else:
    #             new_plu_list.append('Missing')  # Handle the case where there is no ': '
    #     return new_plu_list
    #
    # # Apply the function to each list in the 'ProductPLU' column
    # order_item_df['ProductPLU'] = order_item_df['ProductPLU'].apply(check_and_replace_plu_codes)
    #
    # # Fill missing or empty 'ProductPLU' entries with 'Missing'
    # # This is done after the custom cleaning because the lists have been transformed
    # order_item_df['ProductPLU'] = order_item_df['ProductPLU'].apply(lambda lst: ['Missing'] if not lst else lst)

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

    # Fill missing or empty 'ProductPLU' entries with 'Missing'
    # Also apply a condition to tag entries not starting with 'P' or 'M' as 'Missing'
    order_item_df['ProductPLU'] = order_item_df['ProductPLU'].replace('', 'Missing').fillna('Missing')
    order_item_df['ProductPLU'] = order_item_df['ProductPLU'].apply(lambda x: 'Missing' if isinstance(x, str) and not x.startswith(('P', 'M')) else x)

    # Standardize specific terms in 'ProductName' for consistency, e.g., capitalizing 'Plant-Based'
    order_item_df['ProductName'] = order_item_df['ProductName'].str.replace('plant-based', 'Plant-Based', case=False, regex=True)
    order_item_df['ProductName'] = order_item_df['ProductName'].str.replace('plant based', 'Plant-Based', case=False, regex=True)

    # # Copy the original DataFrame to a new one for adjustments
    # adj_order_item_df = order_item_df.copy()
    #
    # # Convert the 'Quantity' column to a numeric type (int or float) to ensure proper summation
    # # 'coerce' will set invalid parsing as NaN
    # adj_order_item_df['Quantity'] = pd.to_numeric(adj_order_item_df['Quantity'], errors='coerce')
    #
    # # Create a list of all column names from the DataFrame
    # adj_order_item_list = adj_order_item_df.columns.tolist()
    #
    # # Remove 'Quantity', 'ProductName', and 'ProductPLU' from the list of columns to be concatenated
    # adj_order_item_list = [item for item in adj_order_item_list if item not in ['Quantity', 'ProductName', 'ProductPLU']]
    #
    # # Concatenate the values from the remaining columns into a single column
    # # Use '|' as a separator to later facilitate splitting
    # concatenated_column_name = '|'.join(adj_order_item_list)
    # adj_order_item_df[concatenated_column_name] = adj_order_item_df[adj_order_item_list].apply(lambda x: '|'.join(x.astype(str)), axis=1)
    #
    # # Drop the original columns that have been concatenated into the new column
    # adj_order_item_df = adj_order_item_df.drop(columns=adj_order_item_list)
    #
    # # Group the DataFrame by the concatenated column, 'ProductPLU', and 'ProductName'
    # # Sum the 'Quantity' for each group, effectively aggregating data
    # adj_order_item_df = adj_order_item_df.groupby([concatenated_column_name, 'ProductPLU', 'ProductName'], as_index=False)['Quantity'].sum()
    #
    # # Split the concatenated column back into separate columns using the '|' separator
    # split_columns = adj_order_item_df[concatenated_column_name].str.split('|', expand=True)
    #
    # # Rename the split columns back to their original names
    # split_columns.columns = adj_order_item_list
    #
    # # Drop the concatenated column from the DataFrame as it's no longer needed
    # adj_order_item_df = adj_order_item_df.drop(columns=[concatenated_column_name])
    #
    # # Concatenate the DataFrame consisting of split columns back with the grouped DataFrame
    # # This restores the original structure of the DataFrame with adjusted 'Quantity'
    # adj_order_item_df = pd.concat([split_columns, adj_order_item_df], axis=1)

    # Create item level PrimaryKey
    # adj_order_item_df['PrimaryKeyItem'] = np.where(adj_order_item_df['ProductPLU'] == 'Missing',
    #                                                adj_order_item_df['PrimaryKeyAlt'] + ' ' + adj_order_item_df['ProductName'],
    #                                                adj_order_item_df['PrimaryKeyAlt'] + ' ' + adj_order_item_df['ProductPLU'])

    # Create item level PrimaryKey
    order_item_df['PrimaryKeyItem'] = np.where(order_item_df['ProductPLU'] == 'Missing',
                                               order_item_df['PrimaryKeyAlt'] + ' ' + order_item_df['ProductName'],
                                               order_item_df['PrimaryKeyAlt'] + ' ' + order_item_df['ProductPLU'])

    # Export dataframe for checking
    os.chdir(r'H:\Shared drives\97 - Finance Only\10 - Cleaned Data\02 - Processed Data\01 - Data Checking')
    order_item_df.to_csv('Expanded Order Data.csv', index=False)
    # adj_order_item_df.to_csv('Expanded Order Data (adj).csv', index=False)

    return order_item_df

broken_out_order_data = process_clean_item_level_detail()