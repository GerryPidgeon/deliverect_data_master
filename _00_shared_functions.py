import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime

# Rename all columns to standard notation
def column_name_cleaner(df):

    df = df.rename(columns={
        'PickupTime': 'PickupDateTimeLocal',
        'PickupTimeUTC': 'PickupDateTimeUTC',
        'CreatedTime': 'OrderPlacedDateTimeLocal',
        'CreatedTimeUTC': 'OrderPlacedDateTimeUTC',
        'ScheduledTime' : 'ScheduledDateTimeLocal',
        'ScheduledTimeUTC': 'ScheduledDateTimeUTC',
        'Location': 'Location',
        'OrderID': 'OrderID',
        'Channel': 'Channel',
        'Status': 'OrderStatus',
        'ReceiptID': 'ReceiptID',
        'Type': 'DeliveryType',
        'Voucher': 'Voucher',
        'Payment': 'PaymentType',
        'PaymentAmount': 'PaymentAmount',
        'Rebate': 'Rebate',
        'Note': 'Note',
        'ServiceCharge': 'ServiceCharge',
        'DeliveryCost': 'DeliveryCost',
        'DiscountTotal': 'PromotionsOnItems',
        'Tip': 'RxTip',
        'DriverTip': 'DriverTip',
        'SubTotal': 'GrossAOV',
        'Brands': 'Brand',
        'ChannelLink': 'ChannelLink',
        'Tax': 'Tax',
        'VAT': 'VAT',
        'FailureMessage': 'FailureMessage',
        'IsTestOrder': 'IsTestOrder',
        'LocationID': 'LocationID',
        'PosLocationID': 'PosLocationID',
        'ChannelLinkID': 'ChannelLinkID',
        'ProductPLUs': 'ProductPLU',
        'ProductNames': 'ProductName',
        'OrderTotalAmount': 'TotalOrderAmount',
        'CustomerAuthenticatedUserId': 'CustomerAuthenticatedUserId',
        'DeliveryTimeInMinutes': 'DeliveryTimeInMinutes',
        'PreparationTimeInMinutes': 'PreparationTimeInMinutes',
        'ItemPrice': 'ItemPrice',
        'ItemQuantities': 'ItemQuantity'
    })

    return df

def column_name_sorter(df):
    # List of columns in the order you want them to be
    column_order = ['PrimaryKey', 'PrimaryKeyAlt', 'Location', 'LocWithBrand', 'Brand', 'OrderID', 'OrderPlacedDate', 'OrderPlacedTime',
                    'Channel', 'OrderStatus', 'DeliveryType', 'GrossAOV', 'PromotionsOnItems', 'DeliveryFee', 'DriverTip', 'IsTestOrder',
                    'PaymentType', 'ProductPLU', 'ProductName', 'CleanedName', 'PickupTime', 'ItemPrice', 'ItemQuantity', 'TotalItemCost']

    # Select only the columns that exist in the DataFrame
    existing_columns = [col for col in column_order if col in df.columns]

    # Reorder the DataFrame using only the existing columns
    df = df[existing_columns]

    return df


# Fix Issue Where Order ID's with an E as the 3rd character comes in as a scientific number
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


# Clean Locations to standard notation
def clean_location_names(df):
   # Clean Location
   cleaned_names = pd.read_csv(r'H:\Shared drives\97 - Finance Only\10 - Cleaned Data\01 - Restaurant Names\Full Rx List, with Cleaned Names.csv')
   df['Location'] = df['Location'].str.replace('ö', 'o').str.replace('ü', 'u')
   df = pd.merge(df, cleaned_names[['Location', 'Cleaned Name']], on='Location', how='left')
   df = df.rename(columns={'Cleaned Name': 'Cleaned Location'})
   df['Location'] = df['Cleaned Location']
   df = df.drop(columns=['Cleaned Location'])
   return df

# Each DataFrame has the metedata of 'PrimaryKey', 'OrderID', 'OrderPlacedDate', 'OrderPlacedTime', 'Brand', 'Location', 'LocWithBrand', 'Channel' and 'OrderStatus'.
# This code standardises these fields, for Deliverect Data
def process_deliverect_shared_data(df):
    # Convert 'OrderID' to string and prepend each ID with a '#' symbol
    # 'astype(str)' is used to ensure 'OrderID' is treated as a string for concatenation
    df['OrderID'] = '#' + df['OrderID'].astype(str)

    # Convert and standardize the 'OrderPlacedDateTime' to UTC, then to 'Europe/Berlin' timezone
    # This ensures consistency in time representation across the dataset
    # Additionally, separate the date and time components for more granular analysis
    df['OrderPlacedDateTime'] = pd.to_datetime(df['OrderPlacedDateTimeUTC']).dt.tz_localize('UTC')
    df['OrderPlacedDateTime'] = df['OrderPlacedDateTime'].dt.tz_convert('Europe/Berlin')
    df['OrderPlacedDate'] = df['OrderPlacedDateTime'].dt.date
    df['OrderPlacedTime'] = df['OrderPlacedDateTime'].dt.time

    # Additionally, separate the date and time components for more granular analysis
    df['PickupDateDateTime'] = pd.to_datetime(df['PickupDateTimeUTC']).dt.tz_localize('UTC')
    df['PickupDateDateTime'] = df['PickupDateDateTime'].dt.tz_convert('Europe/Berlin')
    df['PickupDateDate'] = df['PickupDateDateTime'].dt.date
    df['PickupDateTime'] = df['PickupDateDateTime'].dt.time

    # Clean and standardize 'Brand' names
    # First, extract the primary brand name if multiple brands are listed (separated by a comma)
    df['Brand'] = df['Brand'].apply(lambda x: str(x).split(',')[0] if isinstance(x, str) and ',' in x else x)

    # Handle missing Brand values based on 'Location' contents
    # Assign 'Birria & the Beast' or 'Birdie Birdie' to null Brand values depending on the presence of 'beast' in 'Location'
    df['Brand'] = np.where(df['Brand'].isnull(), np.where(df['Location'].str.contains('beast', case=False), 'Birria & the Beast', 'Birdie Birdie'), df['Brand'])

    # Further standardize 'Brand' names by replacing any occurrence of 'beast' with 'Birria' and others with 'Birdie'
    df['Brand'] = np.where(df['Brand'].str.contains('beast', case=False), 'Birria', 'Birdie')

    # Clean and standardize location names using a shared function
    # df['Location'] = df['Location'].str.strip() # Clean up the Full Rx List, with Cleaned Names.csv
    df = clean_location_names(df)

    # Fix Hamburg Bergedorf and Bremen Steintor (Deliverect License was re-used)
    comparison_date = datetime.strptime('2023-09-30', '%Y-%m-%d').date()  # Convert string to datetime.date
    df['Location'] = np.where((df['Location'] == 'Bremen Steintor') & (df['OrderPlacedDate'] <= comparison_date), 'Hamburg Bergedorf',df['Location'])

    # Combine 'Location' and 'Brand' into a new column 'LocWithBrand' for better identification
    df['LocWithBrand'] = df['Location'] + ' - ' + df['Brand'].str.split(n=1).str[0]

    # Clean and standardize other columns for consistency
    df['Channel'] = df['Channel'].str.replace('TakeAway Com', 'Lieferando')
    df['OrderStatus'] = df['OrderStatus'].str.replace('_', ' ').str.title()
    df = df[df['OrderStatus'] != 'Duplicate']

    # Construct a 'PrimaryKey' for each row by concatenating 'OrderID', 'Location', and 'OrderPlacedDate'
    # This unique identifier helps in distinguishing each order entry unambiguously
    df['PrimaryKey'] = df['OrderID'] + ' - ' + df['Location'] + ' - ' + df['OrderPlacedDate'].astype(str)
    df['PrimaryKeyAlt'] = df['OrderID'] + ' - ' + df['PickupDateDate'].astype(str) + ' - ' + df['PickupDateTime'].astype(str) + ' - ' + df['GrossAOV'].astype(str)
    return df

def clean_deliverect_product_name(df):
    # Replace NaN values in 'ProductName' with an empty string
    df['ProductName'] = df['ProductName'].fillna('')
    df['ProductPLU'] = df['ProductPLU'].fillna('')

    # Correct character encoding issues in 'ProductName'
    # Encoding and then decoding with 'latin-1' helps fix any special characters or encoding errors
    df.loc[:, 'ProductPLU'] = df['ProductPLU'].apply(lambda x: x.encode('latin-1', errors='ignore').decode('utf-8', errors='ignore'))
    df.loc[:, 'ProductName'] = df['ProductName'].apply(lambda x: x.encode('latin-1', errors='ignore').decode('utf-8', errors='ignore'))

    # Replace specific abbreviations and phrases in 'ProductName' for consistency and clarity
    # For example, replace 'Stck' with 'Stack', 'Kse' with 'Cheese', and standardize variations of the word 'Hot'
    df.loc[:, 'ProductName'] = df['ProductName'].str.replace('Stck', 'Stack')
    df.loc[:, 'ProductName'] = df['ProductName'].str.replace('Kse', 'Cheese')
    df.loc[:, 'ProductName'] = df['ProductName'].str.replace('HOT HOT HOT', 'Hot')
    df.loc[:, 'ProductName'] = df['ProductName'].str.replace(' Hot Hot Hot', 'Hot')

    # Remove unnecessary commas and standardize product descriptions in 'ProductName'
    # This helps in maintaining uniformity and readability in product names
    df.loc[:, 'ProductName'] = df['ProductName'].str.replace('Mayonnaise, 17ml', 'Mayonnaise 17ml')
    df.loc[:, 'ProductName'] = df['ProductName'].str.replace('Mayo, 50ml', 'Mayo 50ml')
    df.loc[:, 'ProductName'] = df['ProductName'].str.replace('Sauce, 50ml', 'Sauce 50ml')
    df.loc[:, 'ProductName'] = df['ProductName'].str.replace('Cola 0,5l', 'Cola 0.5l')
    df.loc[:, 'ProductName'] = df['ProductName'].str.replace('Salsa, 30ml', 'Salsa 30ml')
    df.loc[:, 'ProductName'] = df['ProductName'].str.replace('Italien,', 'Italien')
    return df

def process_deliverect_remove_duplicates(df):
    # Create Custom Sort, to delete duplicates. Created a hierarchy for OrderStatus, as the first record will be retained
    df = df.drop_duplicates()
    custom_order = {'Delivered': 0, 'Auto Finalized': 1, 'In Delivery': 2, 'Ready For Pickup': 3, 'Prepared': 4, 'Preparing': 5, 'Accepted': 6, 'Deliverect Parsed': 7, 'New': 8, 'Scheduled': 9,
                    'Cancel': 10, 'Canceled': 11, 'Failed Resolved': 12, 'Failed': 13, 'Delivery Cancelled': 14, 'Manual Retry': 15, 'Failed Cancel': 16}
    df['OrderStatus'] = pd.Categorical(df['OrderStatus'], categories=custom_order.keys(), ordered=True)
    df = df.sort_values(by=['PrimaryKey', 'OrderStatus'], ascending=[True, True])
    # Keep only the first record for each unique 'PrimaryKey'
    df = df.drop_duplicates(subset='PrimaryKey', keep='first')
    df = df[df['OrderID'] != '#nan']
    df = df[df['OrderID'] != '#CustomFormatnan']
    df = df[df['OrderStatus'] != 'Duplicate']
    return df