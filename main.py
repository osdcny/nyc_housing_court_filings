import pandas as pd
from datetime import date
import os

today_date = date.today().strftime("%Y%m%d")

# Load all zip codes in NYC
nyc_zpnb_crosswalk = pd.read_csv('https://raw.githubusercontent.com/osdcny/resources/main/nyc_zpnb_crosswalk.csv')
nyc_zips = list(nyc_zpnb_crosswalk.Zip)

# Load raw data files
oca_index = pd.read_csv('https://s3.amazonaws.com/oca-data/public/oca_index.csv', parse_dates=['fileddate'], usecols=['indexnumberid', 'fileddate', 'propertytype', 'classification'])
oca_addresses = pd.read_csv('https://s3.amazonaws.com/oca-data/public/oca_addresses.csv', usecols=['indexnumberid', 'postalcode'])

# Filter by Date
oca_index = oca_index[oca_index.fileddate >= '2019-01-01']

# Create 5-digit zip code column
oca_addresses['zip'] = oca_addresses['postalcode'].str[0:5]
oca_addresses.drop(columns='postalcode', inplace=True)
oca_addresses['zip'] = oca_addresses['zip'].astype(int)

# Combine the main dataset and the address file (with zip codes)
df = oca_index.merge(oca_addresses, on='indexnumberid', how='left')

# Filter by zip codes in NYC
df = df[df.zip.isin(nyc_zips)]

# Sort by date
df = df.sort_values('fileddate')

# Print the size of the dataset
print(f'The dataset has {df.shape[0]} rows')

# Create dataset name
file_name = f'ilings_for_nyc_properties_(updated_{today_date}).csv'
file_pathname = os.path.join('datasets', file_name)

# Save the dataset
df.to_csv(file_pathname, index=False)