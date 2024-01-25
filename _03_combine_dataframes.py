import pandas as pd
import numpy as np
import os
import sys
import datetime

# Update system path to include parent directories for module access
# This allows the script to import modules from two directories up in the folder hierarchy
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Import specific data and functions from external modules
from _02c_breakout_order_data import broken_out_order_data
from _02d_add_pricing_differences import adjusted_item_level_detail

def output_deliverect_data():
    # Initialize DataFrame for data processing
    # The 'imported_deliverect_item_level_detail_data' is assigned to 'df' for processing
    order_df = broken_out_order_data
    item_df = adjusted_item_level_detail

    # Create Output DataFrame
    output_df = order_df.copy()

    # Filter to exclude records from before 1st Jan 2023
    filter_date = datetime.date(2023, 1, 1)
    output_df = output_df.loc[output_df['OrderPlacedDate'] >= filter_date]

    # Merge the dataframes together
    output_df = pd.merge(output_df, item_df[['PrimaryKeyItem', 'ItemPrice', 'ItemQuantity', 'TotalItemCost']], on='PrimaryKeyItem', how='left')
    output_df.to_csv('Final Item Detail Master.csv', index=False)

    return order_df, item_df

output_deliverect_data()