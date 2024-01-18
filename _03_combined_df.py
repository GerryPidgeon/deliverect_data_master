import pandas as pd
import os
import sys

# Update system path to include parent directories for module access
# This allows the script to import modules from two directories up in the folder hierarchy
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Import specific data and functions from external modules
from _02a_clean_order_data import cleaned_deliverect_order_data
from _02b_clean_item_level_data import cleaned_deliverect_item_level_detail_data

def output_deliverect_data():
    # Initialize DataFrame for data processing
    # The 'imported_deliverect_item_level_detail_data' is assigned to 'df' for processing
    order_df = cleaned_deliverect_order_data
    item_df = cleaned_deliverect_item_level_detail_data
    print(item_df.columns)

    return order_df, item_df

output_deliverect_data()