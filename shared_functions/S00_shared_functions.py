import pandas as pd
import os
import sys

# Update system path to include parent directories for module access
# This allows the script to import modules from two directories up in the folder hierarchy
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Import specific data and functions from external modules
from external_data.E01_raw_import import cleaned_rx_name_csv

# Clean Locations to standard name
def clean_rx_names(df):
    # Clean Location
    cleaned_rx_name = cleaned_rx_name_csv
    df['Location'] = df['Location'].str.strip()
    df['Location'] = df['Location'].str.replace('ö', 'o').str.replace('ü', 'u')
    df = pd.merge(df, cleaned_rx_name[['Location', 'Cleaned Name']], on='Location', how='left')
    df = df.rename(columns={'Cleaned Name': 'Cleaned Location'})
    df['Location'] = df['Cleaned Location']
    df = df.drop(columns=['Cleaned Location'])
    return df

def convert_to_custom_format(scientific_notation):
    if isinstance(scientific_notation, str):
        try:
            coefficient, exponent = scientific_notation.split('E+')
            coefficient = coefficient.replace('.', '')
            new_exponent = int(exponent) - 2
            return f"{coefficient}E{new_exponent}"
        except ValueError:
            # Handle non-convertible values here (e.g., return the original value)
            return scientific_notation
    elif isinstance(scientific_notation, (int, float)):
        # Handle float or int values here (e.g., convert them to a custom format)
        # You can decide how to format them according to your requirements
        return f"CustomFormat{scientific_notation}"
    else:
        # Handle other data types (e.g., return the original value)
        return scientific_notation