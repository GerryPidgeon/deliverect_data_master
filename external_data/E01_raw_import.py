import pandas as pd
import os
import sys

# Update system path to include parent directories for module access
# This allows the script to import modules from two directories up in the folder hierarchy
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Change Directory
load_directory = r'H:\Shared drives\97 - Finance Only\20 - New Python Code\01 - Supporting Files'
os.chdir(load_directory)

def cleaned_rx_name_csv():
    df = pd.read_csv('Full Rx List, with Cleaned Names.csv', encoding='ISO-8859-1')
    return df

cleaned_rx_name_csv = cleaned_rx_name_csv()

def cleaned_item_name_csv():
    df = pd.read_csv('Cleaned Item Name List.csv', encoding='ISO-8859-1')
    return df

cleaned_item_name_csv = cleaned_item_name_csv()