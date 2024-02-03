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
from external_data.E01_raw_import import cleaned_item_name_csv
from deliverect.D00_shared_functions import column_name_sorter
from deliverect.D02a_clean_raw_data import cleaned_deliverect_item_level_detail_data
from deliverect.D02b_breakout_item_level_data import broken_out_order_data

def output_deliverect_data():
    # Initialize DataFrame for data processing
    # The 'imported_deliverect_item_level_detail_data' is assigned to 'df' for processing
    order_df = broken_out_order_data
    item_df = cleaned_deliverect_item_level_detail_data

    # Create Output DataFrame
    output_df = order_df.copy()

    # Filter to exclude records from before 1st Jan 2023
    filter_date = datetime(2023, 1, 1).date()
    output_df = output_df.loc[output_df['OrderPlacedDate'] >= filter_date]

    # Merge the dataframes together
    output_df = pd.merge(output_df, item_df[['PrimaryKeyItem', 'ItemPrice', 'ItemQuantity']], on='PrimaryKeyItem', how='left')
    output_df['Quantity'] = pd.to_numeric(output_df['Quantity'], errors='coerce')

    # Calculate the total item cost
    output_df['TotalItemCost'] = output_df['ItemPrice'] * output_df['Quantity']

    # Add a running index based on 'PrimaryKeyAlt'
    mask = output_df['PrimaryKeyAlt'] != output_df['PrimaryKeyAlt'].shift()
    output_df['PrimaryKeyIndex'] = (mask.cumsum() + 1).astype(int)
    output_df['PrimaryKeyIndex'] = output_df['PrimaryKeyIndex'] - 1

    # Add an item index within each 'PrimaryKeyAlt' group
    output_df['ItemIndex'] = output_df.groupby('PrimaryKeyAlt').cumcount() + 1

    # Convert required columns to floats
    output_df['PromotionsOnItems'] = output_df['PromotionsOnItems'].astype(float)
    output_df['DriverTip'] = output_df['DriverTip'].astype(float)
    output_df['ItemQuantity'] = output_df['ItemQuantity'].astype(float)

    # Convert to string and then replace empty strings
    output_df['ItemPrice'] = output_df['ItemPrice'].astype(str).replace('', 0)
    output_df['ItemQuantity'] = output_df['ItemQuantity'].astype(str).replace('', 0)
    output_df['TotalItemCost'] = output_df['TotalItemCost'].astype(str).replace('', 0)

    # Convert back to the original (likely numerical) data type
    output_df['ItemPrice'] = pd.to_numeric(output_df['ItemPrice'], errors='coerce')
    output_df['ItemQuantity'] = pd.to_numeric(output_df['ItemQuantity'], errors='coerce')
    output_df['TotalItemCost'] = pd.to_numeric(output_df['TotalItemCost'], errors='coerce')

    # Fill NaN values with zeros
    output_df['ItemPrice'] = output_df['ItemPrice'].fillna(0)
    output_df['ItemQuantity'] = output_df['ItemQuantity'].fillna(0)
    output_df['TotalItemCost'] = output_df['TotalItemCost'].fillna(0)

    # Export the DataFrame to a CSV file for checking
    os.chdir(r'H:\Shared drives\97 - Finance Only\20 - New Python Code\03 - Cleaned Data')
    output_df.to_csv('D03 - Final Item Detail Master.csv', index=False)

    return output_df, order_df, item_df

# Call the function and assign the returned DataFrames to variables
output_df, order_df, item_df = output_deliverect_data()

def add_balancing_items():
    # Copying the original DataFrame for safe manipulation without altering the original data
    price_discrepancies_df = output_df.copy()

    # Grouping the data by 'PrimaryKeyAlt' and summing 'TotalItemCost' for each group
    summed_costs = price_discrepancies_df.groupby('PrimaryKeyAlt')[['PrimaryKeyIndex', 'ItemIndex', 'TotalItemCost']].sum().reset_index()

    # Extracting the first occurrence of GrossAOV for each PrimaryKeyAlt
    first_gross_aov = price_discrepancies_df.drop_duplicates(subset='PrimaryKeyAlt')[['PrimaryKeyAlt', 'GrossAOV']]

    # Merging the summed costs and first gross AOV on PrimaryKeyAlt
    merged_df = pd.merge(summed_costs, first_gross_aov, on='PrimaryKeyAlt')

    # Converting GrossAOV and TotalItemCost to float and rounding to 2 decimal places
    merged_df['GrossAOV'] = merged_df['GrossAOV'].astype(float).round(2)
    merged_df['TotalItemCost'] = merged_df['TotalItemCost'].astype(float).round(2)

    # Identifying rows where GrossAOV and TotalItemCost don't match
    merged_df['AOVCheck'] = np.where(merged_df['GrossAOV'] != merged_df['TotalItemCost'], 'Price Discrepancies', '')
    merged_df = merged_df[merged_df['AOVCheck'] == 'Price Discrepancies']

    # Calculating the difference in price
    merged_df['PriceDifference'] = merged_df['GrossAOV'] - merged_df['TotalItemCost']

    # Filtering the original DataFrame to keep only those entries with discrepancies
    price_discrepancies_df = price_discrepancies_df[price_discrepancies_df['PrimaryKeyAlt'].isin(merged_df['PrimaryKeyAlt'])]

    # Selecting only the first item in each group for adjustment
    price_discrepancies_df = price_discrepancies_df.loc[price_discrepancies_df['ItemIndex'] == 1]

    # Merging the price difference information back into the discrepancies DataFrame
    price_discrepancies_df = pd.merge(price_discrepancies_df, merged_df[['PrimaryKeyAlt', 'PriceDifference']], on='PrimaryKeyAlt', how='left')

    # Setting values for the balancing item to adjust discrepancies
    price_discrepancies_df['ProductPLU'] = 'x-xx-xxxx-x'
    price_discrepancies_df['ProductName'] = 'Balancing Item'
    price_discrepancies_df['ItemPrice'] = price_discrepancies_df['PriceDifference']
    price_discrepancies_df['Quantity'] = 1
    price_discrepancies_df['ItemQuantity'] = 1
    price_discrepancies_df['TotalItemCost'] = price_discrepancies_df['PriceDifference']
    price_discrepancies_df['ItemIndex'] = 500

    # Combining the original DataFrame with the adjusted discrepancy rows
    price_discrepancies_output_df = pd.concat([output_df, price_discrepancies_df], ignore_index=True)

    # Sorting the DataFrame based on PrimaryKeyIndex and ItemIndex
    price_discrepancies_output_df.sort_values(by=['PrimaryKeyIndex', 'ItemIndex'], inplace=True)

    # Calling an external function to sort columns
    price_discrepancies_output_df = column_name_sorter(price_discrepancies_output_df)

    # Export the DataFrame to a CSV file for checking
    os.chdir(r'H:\Shared drives\97 - Finance Only\20 - New Python Code\03 - Cleaned Data')
    price_discrepancies_output_df.to_csv('D03 - Processed Item Detail Data With Balancing Items.csv', index=False)

    return price_discrepancies_output_df

price_discrepancies_output_df = add_balancing_items()

def item_cleaning():
    # Copying the original DataFrame for safe manipulation without altering the original data
    output_cleaned_names_df = price_discrepancies_output_df.copy()

    # Create Check Columns
    output_cleaned_names_df['OffsetOrderID'] = output_cleaned_names_df['OrderID'].shift(-1)
    output_cleaned_names_df['OffsetProductName'] = output_cleaned_names_df['ProductName'].shift(-1)
    output_cleaned_names_df['OffsetProductPLU'] = output_cleaned_names_df['ProductPLU'].shift(-1)
    output_cleaned_names_df['OffsetItemPrice'] = output_cleaned_names_df['ItemPrice'].shift(-1)
    output_cleaned_names_df['OffsetTotalItemCost'] = output_cleaned_names_df['TotalItemCost'].shift(-1)

    # Order ID Check
    output_cleaned_names_df['Order Check'] = np.where(output_cleaned_names_df['OrderID'] == output_cleaned_names_df['OffsetOrderID'], 'No', 'Yes')

    # Clear Last Entry for Each Order
    output_cleaned_names_df['OffsetProductName'] = np.where(output_cleaned_names_df['Order Check'] == 'Yes', '', output_cleaned_names_df['OffsetProductName'])
    output_cleaned_names_df['OffsetProductPLU'] = np.where(output_cleaned_names_df['Order Check'] == 'Yes', '', output_cleaned_names_df['OffsetProductPLU'])

    # Populate Last Row
    output_cleaned_names_df.iloc[-1, output_cleaned_names_df.columns.get_loc('OffsetOrderID')] = 'Last Row'
    output_cleaned_names_df.iloc[-1, output_cleaned_names_df.columns.get_loc('OffsetProductName')] = 'Last Row'
    output_cleaned_names_df.iloc[-1, output_cleaned_names_df.columns.get_loc('OffsetProductPLU')] = 'Last Row'

    # Clean up Incorrect Regular Records
    output_cleaned_names_df['ProductName'] = np.where(output_cleaned_names_df['OffsetProductName'] == 'Regular', output_cleaned_names_df['ProductName'] + ' [Regular]', output_cleaned_names_df['ProductName'])
    output_cleaned_names_df['ItemPrice'] = np.where(output_cleaned_names_df['OffsetProductName'] == 'Regular', output_cleaned_names_df['ItemPrice'] + output_cleaned_names_df['OffsetItemPrice'], output_cleaned_names_df['ItemPrice'])
    output_cleaned_names_df['TotalItemCost'] = np.where(output_cleaned_names_df['OffsetProductName'] == 'Regular', output_cleaned_names_df['TotalItemCost'] + output_cleaned_names_df['OffsetTotalItemCost'], output_cleaned_names_df['TotalItemCost'])
    output_cleaned_names_df = output_cleaned_names_df.loc[output_cleaned_names_df['ProductName'] != 'Regular']

    # Clean up Incorrect Large Records
    output_cleaned_names_df['ProductName'] = np.where(output_cleaned_names_df['OffsetProductName'] == 'Large', output_cleaned_names_df['ProductName'] + ' [Large]', output_cleaned_names_df['ProductName'])
    output_cleaned_names_df['ItemPrice'] = np.where(output_cleaned_names_df['OffsetProductName'] == 'Large', output_cleaned_names_df['ItemPrice'] + output_cleaned_names_df['OffsetItemPrice'], output_cleaned_names_df['ItemPrice'])
    output_cleaned_names_df['TotalItemCost'] = np.where(output_cleaned_names_df['OffsetProductName'] == 'Large', output_cleaned_names_df['TotalItemCost'] + output_cleaned_names_df['OffsetTotalItemCost'], output_cleaned_names_df['TotalItemCost'])
    output_cleaned_names_df = output_cleaned_names_df.loc[output_cleaned_names_df['ProductName'] != 'Large']

    # Remove ProductPLU that start with 'M-' and ItemPrice = 0
    output_cleaned_names_df = output_cleaned_names_df[~((output_cleaned_names_df['ProductPLU'].str.startswith('M-')) & (output_cleaned_names_df['ItemPrice'] == 0))]

    # Clean ProductNames to standardise output
    output_cleaned_names_df = pd.merge(output_cleaned_names_df, cleaned_item_name_csv[['ProductName', 'ProductPLU', 'CleanedName', 'Price', 'RevShare', 'DishType', 'ItemBrand']], on=['ProductName', 'ProductPLU'], how='left')

    # Populate CSV With Missing Items
    missing_items_list = output_cleaned_names_df[['ProductPLU', 'ProductName', 'CleanedName']]
    missing_items_list['CleanedName'].replace('', np.nan)
    missing_items_list = missing_items_list.loc[missing_items_list['CleanedName'].isna()]
    missing_items_list = missing_items_list.drop_duplicates()
    output_csv = pd.concat([cleaned_item_name_csv, missing_items_list], ignore_index=True)

    # Save Files
    os.chdir(r'H:\Shared drives\97 - Finance Only\20 - New Python Code\01 - Supporting Files')
    try:
        os.remove('Cleaned Item Name List (Old).csv')
    except FileNotFoundError:
        pass
    os.rename('Cleaned Item Name List.csv', 'Cleaned Item Name List (Old).csv')
    output_csv.to_csv('Cleaned Item Name List.csv', index=False)

    # Clean ProductNames to standardise output
    output_cleaned_names_df['ProductName'] = output_cleaned_names_df['CleanedName']
    output_cleaned_names_df = output_cleaned_names_df.drop(columns=(['CleanedName', 'OffsetOrderID', 'OffsetProductName', 'OffsetProductPLU', 'OffsetItemPrice',	'OffsetTotalItemCost', 'Order Check']))

    # Export the DataFrame to a CSV file for checking
    os.chdir(r'H:\Shared drives\97 - Finance Only\20 - New Python Code\03 - Cleaned Data')
    output_cleaned_names_df.to_csv('D03 - Processed Item Detail Data With Cleaned Names.csv', index=False)

    return output_cleaned_names_df

cleaned_names_output_df = item_cleaning()

def combine_like_items():
    combine_like_items_df = cleaned_names_output_df.copy()

    # Specify the columns to keep separate
    all_columns = combine_like_items_df.columns.to_list()
    first_columns = ['PrimaryKeyAlt', 'ProductPLU', 'ProductName', 'ItemPrice', 'ItemQuantity', 'TotalItemCost']
    remaining_columns = [item for item in all_columns if item not in first_columns]

    # Concatenate the values from the remaining columns into a single column
    concatenated_column_name = '|'.join(remaining_columns)
    combine_like_items_df[concatenated_column_name] = combine_like_items_df[remaining_columns].apply(lambda x: '|'.join(x.astype(str)), axis=1)

    # Group by 'PrimaryKeyAlt', 'ProductPLU', 'ProductName', and sum 'ItemPrice', 'ItemQuantity', 'TotalItemCost'
    group_columns = ['PrimaryKeyAlt', 'ProductPLU', 'ProductName', concatenated_column_name]
    aggregated_columns = {'ItemPrice': 'sum', 'ItemQuantity': 'sum', 'TotalItemCost': 'sum'}
    combine_like_items_df = combine_like_items_df.groupby(group_columns, as_index=False).agg(aggregated_columns)

    # Split the concatenated column back into individual columns
    split_columns = combine_like_items_df[concatenated_column_name].str.split('|', expand=True)
    split_columns.columns = remaining_columns

    # Concatenate the split columns back with the aggregated data
    combine_like_items_df = pd.concat([combine_like_items_df[group_columns + ['ItemPrice', 'ItemQuantity', 'TotalItemCost']], split_columns], axis=1)

    # Drop the concatenated column as it is no longer needed
    combine_like_items_df = combine_like_items_df.drop(columns=[concatenated_column_name])

    # Sort the DataFrame columns in the original order
    combine_like_items_df = combine_like_items_df[all_columns]
    combine_like_items_df.sort_values(['OrderPlacedDate', 'OrderPlacedTime', 'PrimaryKey'], inplace=True)

    # Combine Tips
    combine_like_items_df['RxTip'] = combine_like_items_df['RxTip'].astype(str)
    combine_like_items_df['RxTip'] = combine_like_items_df['RxTip'].replace('', 0)
    combine_like_items_df['RxTip'] = pd.to_numeric(combine_like_items_df['RxTip'], errors='coerce')

    combine_like_items_df['DriverTip'] = combine_like_items_df['DriverTip'].astype(str)
    combine_like_items_df['DriverTip'] = combine_like_items_df['DriverTip'].replace('', 0)
    combine_like_items_df['DriverTip'] = pd.to_numeric(combine_like_items_df['DriverTip'], errors='coerce')

    combine_like_items_df['RxTip'] = combine_like_items_df['RxTip'] + combine_like_items_df['DriverTip']
    combine_like_items_df = combine_like_items_df.rename(columns={'RxTip': 'Tips'})
    combine_like_items_df = combine_like_items_df.drop(columns=('DriverTip'))

    # Save to CSV
    os.chdir(r'H:\Shared drives\97 - Finance Only\20 - New Python Code\03 - Cleaned Data')
    combine_like_items_df.to_csv('D03 - Processed Item Detail Data With Combined Items.csv', index=False)
    os.chdir(r'H:\Shared drives\97 - Finance Only\20 - New Python Code\04 - Processed Data')
    combine_like_items_df.to_csv('Deliverect Item Level Detail by Order.csv', index=False)

    return combine_like_items_df

combine_like_items_df = combine_like_items()

def create_output():
    final_output = combine_like_items_df.copy()

    # Create Food, Drink and Dessert AOV
    food_aov = final_output[final_output['RevShare'] == 'Food'].groupby('PrimaryKey')['TotalItemCost'].sum().rename('FoodAOV')
    dessert_aov = final_output[final_output['RevShare'] == 'Dessert'].groupby('PrimaryKey')['TotalItemCost'].sum().rename('DessertAOV')
    drink_aov = final_output[final_output['RevShare'] == 'Drink'].groupby('PrimaryKey')['TotalItemCost'].sum().rename('DrinkAOV')

    # Merge DataFrames
    final_output = final_output.merge(food_aov, on='PrimaryKey', how='left')
    final_output = final_output.merge(dessert_aov, on='PrimaryKey', how='left')
    final_output = final_output.merge(drink_aov, on='PrimaryKey', how='left')

    # Replace Blanks with 0
    # Replace NaN with 0 and ensure the columns are of float type
    final_output['FoodAOV'] = final_output['FoodAOV'].fillna(0).astype(float)
    final_output['DessertAOV'] = final_output['DessertAOV'].fillna(0).astype(float)
    final_output['DrinkAOV'] = final_output['DrinkAOV'].fillna(0).astype(float)

    # Create Concat View of ProductPLU and ProductNames
    final_output['ProductPLUConcat'] = final_output['ProductPLU'] + ": Qty " + final_output['ItemQuantity'].astype(int).astype(str)
    final_output['ProductNameConcat'] = final_output['ProductName'] + ": Qty " + final_output['ItemQuantity'].astype(int).astype(str) + ': Price ' + final_output['TotalItemCost'].apply(lambda x: f"{x:.2f}")

    # Drop Columns that aren't needed
    final_output = final_output.drop(columns=(['Price', 'RevShare', 'DishType', 'ItemBrand']))

    # Create a summary DataFrame that contains combined strings for 'ProductPLUConcat' and 'ProductNameConcat'
    product_detail_df = final_output.groupby('PrimaryKey').agg({'ProductPLUConcat': '; '.join, 'ProductNameConcat': '; '.join}).reset_index()

    # Merge the summary information back into the original DataFrame
    final_output = final_output.merge(product_detail_df, on='PrimaryKey', suffixes=('', '_combined'))

    # Rename the '_combined' columns to the original names
    final_output['ProductPLU'] = final_output['ProductPLUConcat_combined']
    final_output['ProductName'] = final_output['ProductNameConcat_combined']

    # Drop the original 'ProductPLUConcat' and 'ProductNameConcat' columns
    final_output.drop(columns=['ItemPrice', 'ItemQuantity', 'TotalItemCost', 'ProductPLUConcat', 'ProductNameConcat', 'ProductPLUConcat_combined', 'ProductNameConcat_combined'], inplace=True)

    # Drop duplicates based on 'PrimaryKey' to keep only one record for each 'PrimaryKey'
    final_output = final_output.drop_duplicates(subset='PrimaryKey')

    # Set DataType
    string_columns = ['PrimaryKey', 'PrimaryKeyAlt', 'Location', 'LocWithBrand', 'Brand', 'OrderID', 'Channel', 'OrderStatus', 'DeliveryType', 'PaymentType', 'ProductPLU', 'ProductName']
    float_columns = ['GrossAOV', 'PromotionsOnItems', 'DeliveryFee', 'Tips', 'FoodAOV', 'DessertAOV', 'DrinkAOV']
    date_columns = ['OrderPlacedDate']
    time_columns = ['OrderPlacedTime']
    boolean_columns = ['IsTestOrder']

    # Convert string columns
    for col in string_columns:
        final_output[col] = final_output[col].astype(str)

    # Convert float columns
    for col in float_columns:
        final_output[col] = final_output[col].astype(float)

    # Convert boolean columns
    for col in boolean_columns:
        final_output[col] = final_output[col].astype(bool)

    # Order Columns for Final Output
    final_output = final_output[['PrimaryKey', 'PrimaryKeyAlt', 'Location', 'LocWithBrand', 'Brand', 'OrderID', 'OrderPlacedDate', 'OrderPlacedTime', 'Channel', 'OrderStatus', 'DeliveryType', 'GrossAOV', 'FoodAOV',
                                 'DessertAOV', 'DrinkAOV', 'PromotionsOnItems', 'DeliveryFee', 'Tips', 'IsTestOrder', 'PaymentType', 'ProductPLU', 'ProductName']]

    # Save to CSV
    os.chdir(r'H:\Shared drives\97 - Finance Only\20 - New Python Code\04 - Processed Data')
    final_output.to_csv('Deliverect Data.csv', index=False)

    return final_output

deliverect_final_output_df = create_output()