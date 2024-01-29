import pandas as pd
import numpy as np
import os
import sys

# Path to the Deliverect source folder containing order details and documents
deliverect_source_folder = r'H:\Shared drives\97 - Finance Only\01 - Orders & 3PL Documents\00 - All Restaurants\00 - Deliverect'

def load_deliverect_order_data():
    # Change the working directory to the 'Order Details' folder in the Deliverect source folder
    root_directory = deliverect_source_folder + r'\Order Details'
    os.chdir(root_directory)

    # Initialize an empty list to store individual dataframes loaded from each CSV file
    dataframe = []

    # Iterate over all files in the 'Order Details' directory
    for file_name in os.listdir(root_directory):
        # Check if the file is a CSV file and contains 'Orders' in its name
        if file_name.endswith(".csv") and 'Orders' in file_name:
            file_path = os.path.join(root_directory, file_name)
            # Load the CSV file into a dataframe, ensuring 'OrderID' is treated as a string
            df = pd.read_csv(file_path, dtype={'OrderID': str}, encoding='utf-8')
            # Append the loaded dataframe to the list
            dataframe.append(df)

    # Combine all dataframes into a single dataframe, if any CSV files were loaded
    if dataframe:
        df = pd.concat(dataframe, ignore_index=True)
    else:
        # Create an empty DataFrame if no CSV files were found in the directory
        df = pd.DataFrame()

    return df

imported_deliverect_order_data = load_deliverect_order_data()

def load_deliverect_item_level_detail_data():
    # Change the working directory to the 'Order Details' folder in the Deliverect source folder
    root_directory = deliverect_source_folder + r'\Order Level Pricing'
    os.chdir(root_directory)

    # Initialize an empty list to store individual dataframes loaded from each CSV file
    dataframe = []

    # Iterate over all files in the 'Order Details' directory
    for file_name in os.listdir(root_directory):
        # Check if the file is a CSV file and contains 'Orders' in its name
        if file_name.endswith(".csv") and 'Order Level Pricing' in file_name:
            file_path = os.path.join(root_directory, file_name)
            # Load the CSV file into a dataframe, ensuring 'OrderID' is treated as a string
            df = pd.read_csv(file_path, dtype={'OrderID': str}, encoding='utf-8', low_memory=False)
            # Append the loaded dataframe to the list
            dataframe.append(df)

    # Combine all dataframes into a single dataframe, if any CSV files were loaded
    if dataframe:
        df = pd.concat(dataframe, ignore_index=True)
    else:
        # Create an empty DataFrame if no CSV files were found in the directory
        df = pd.DataFrame()
    return df

imported_deliverect_item_level_detail_data = load_deliverect_item_level_detail_data()