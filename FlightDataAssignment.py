############################################
# PREPARE A FLIGHT DATASET FOR A DASHBOARD #
############################################

# Created by Julia Huilla in September 2024

# This notebook:
# - loads the data.
# - identifies, cleans and documents inconsistencies and errors in the data.
# - adds new, relevant columns.

# The goal is to use the cleaned dataset to visualize insights in Tableau

###########

## 1 Set-up

# Import required libraries
# Assumption: required libraries are already installed
import pandas as pd
import numpy as np

###########

## 2 Load data

# Load given dataset from Databricks File System (DBFS)
# Assumption: data is already available in DBFS
df_spark = spark.read.format("delta").load("dbfs:/user/hive/warehouse/assignment")

# Convert Spark DataFrame to pandas DataFrame
df = df_spark.toPandas()

# Display the first few rows to verify
df.head(10)

# Load country and continent dataset from Databricks File System (DBFS)
# Assumption: data is already available in DBFS
# Source: Uploaded to Github Gist by stevewithington, https://gist.github.com/stevewithington/20a69c0b6d2ff846ea5d35e5fc47f26c

continent_df_spark = spark.read.format("delta").load("dbfs:/user/hive/warehouse/country_and_continent_codes_list_csv")

# Convert Spark DataFrame to pandas DataFrame
continent_df = continent_df_spark.toPandas()

# Display the first few rows to verify
continent_df.head(10)

##########

## 3 Explore and clean data

# Show a summary of the DataFrame
print(df.info())

# Missing information
# Problems: missing values, no clear documentation about the variables

# Replace "null" strings with NaN
df.replace('null', np.nan, inplace=True)

# Calculate the number of NaN values per column
na_counts = df.isna().sum()

# Display the number of NaN values per column
print("\nNumber of NaN values per column:")
print(na_counts)

# Date columns
# Problems: inconsistent formats, dtype object instead of datetime

# Print the first 10 values in 'DateDeparture'
print(df['DateDeparture'].head(10))

# Function to parse dates
def parse_dates(date_str):
    for fmt in ('%d.%m.%Y', '%Y-%m-%d'):
        try:
            return pd.to_datetime(date_str, format=fmt)
        except ValueError:
            continue
    return pd.NaT  # If none of the formats match

# Apply the function to the columns
df['DateIssued'] = df['DateIssued'].apply(parse_dates)
df['DateDeparture'] = df['DateDeparture'].apply(parse_dates)

# Print date ranges
issue_date_min = df['DateIssued'].min()
issue_date_max = df['DateIssued'].max()
dep_date_min = df['DateDeparture'].min()
dep_date_max = df['DateDeparture'].max()

print(f"Date range for 'DateIssued': from {issue_date_min} to {issue_date_max}")
print(f"Date range for 'DateDeparture': from {dep_date_min} to {dep_date_max}")

# Print the first 10 values in 'DateDeparture' to compare
print(df['DateDeparture'].head(10))

# Location columns
# Problems: missing IATA codes, one city can have multiple airports (e.g. Taipei, Portland)

# Function to check if a value is a valid IATA code (3 uppercase letters)
def is_iata_code(value):
    return isinstance(value, str) and value.isupper() and len(value) == 3

# Apply the function to filter out rows where columns have valid IATA codes
df_valid_iata = df[
    df['DestinationLeg'].apply(is_iata_code) &
    df['OriginLeg'].apply(is_iata_code) &
    df['Destination'].apply(is_iata_code) &
    df['Origin'].apply(is_iata_code)
]

# Create mapping based on both city and airport names
origin_mapping = dict(zip(zip(df_valid_iata['origin_city'], df_valid_iata['origin_ap_name']), df_valid_iata['Origin']))
destination_mapping = dict(zip(zip(df_valid_iata['destination_city'], df_valid_iata['destination_ap_name']), df_valid_iata['Destination']))
originLeg_mapping = dict(zip(zip(df_valid_iata['OriginLeg_city'], df_valid_iata['OriginLeg_ap_name']), df_valid_iata['OriginLeg']))
destinationLeg_mapping = dict(zip(zip(df_valid_iata['DestinationLeg_city'], df_valid_iata['DestinationLeg_ap_name']), df_valid_iata['DestinationLeg']))

# Combine all mappings into a comprehensive mapping
comprehensive_mapping = {**origin_mapping, **destination_mapping, **originLeg_mapping, **destinationLeg_mapping}

# Function to replace city names with IATA codes using comprehensive mapping
def replace_city_with_iata(value, airport_name, mapping):
    city_key = (value, airport_name)
    if city_key in mapping:
        return mapping[city_key]
    return value  # If not found in mapping, return the original value

# Apply the comprehensive mapping to fill in missing IATA codes
df['DestinationLeg'] = df.apply(lambda row: replace_city_with_iata(row['DestinationLeg'], row['DestinationLeg_ap_name'], comprehensive_mapping), axis=1)
df['OriginLeg'] = df.apply(lambda row: replace_city_with_iata(row['OriginLeg'], row['OriginLeg_ap_name'], comprehensive_mapping), axis=1)
df['Destination'] = df.apply(lambda row: replace_city_with_iata(row['Destination'], row['destination_ap_name'], comprehensive_mapping), axis=1)
df['Origin'] = df.apply(lambda row: replace_city_with_iata(row['Origin'], row['origin_ap_name'], comprehensive_mapping), axis=1)

# Print the first 10 values
print(df['Destination'].head(10))
print(df['Origin'].head(10))

# Filter values that are longer than 3 characters
long_destinations = df[df['Destination'].str.len() > 3]['Destination']
long_origins = df[df['Origin'].str.len() > 3]['Origin']
long_DestinationLegs = df[df['DestinationLeg'].str.len() > 3]['DestinationLeg']
long_OriginLegs = df[df['OriginLeg'].str.len() > 3]['OriginLeg']

# Print the filtered values
print(long_destinations)
print(long_origins)
print(long_DestinationLegs)
print(long_OriginLegs)

# There are 3 destinations that were not mapped because there was only one row with that destination (Porto Velho, Kochi, Santa Marta)
# Manually define the IATA codes for the missing cities
manual_replacements = {
    'Porto Velho': 'PVH',
    'Kochi': 'KCZ',
    'Santa Marta': 'SMR'
}

# Function to manually replace cities with the correct IATA codes
def manual_replace(value, replacements):
    return replacements.get(value, value)  # Replace if found, otherwise keep the original value

# Apply manual replacements to the 'Destination' column
df['Destination'] = df['Destination'].apply(lambda x: manual_replace(x, manual_replacements))

# Check if the replacements were successful
# Filter values that are longer than 3 characters
long_destinations = df[df['Destination'].str.len() > 3]['Destination']
long_origins = df[df['Origin'].str.len() > 3]['Origin']
long_DestinationLegs = df[df['DestinationLeg'].str.len() > 3]['DestinationLeg']
long_OriginLegs = df[df['OriginLeg'].str.len() > 3]['OriginLeg']

# Print the filtered values
print(long_destinations)
print(long_origins)
print(long_DestinationLegs)
print(long_OriginLegs)

###########

## 4 Feature engineering

# Add route column
# To visualize route performance a decision was made to use OriginLeg-DestinationLeg combination instead of flight numbers.  
# These are more informative for people who don't remember flight number by heart.  
# Plus there were no missing values in these columns.

# Combine OriginLeg and DestinationLeg into a new column (e.g. "ZRH-AMS")
df['Route'] = df['OriginLeg'] + '-' + df['DestinationLeg']

# Reorder columns
columns_order = ['OriginLeg', 'DestinationLeg', 'Route'] + [col for col in df.columns if col not in ['OriginLeg', 'DestinationLeg', 'Route']]
df = df[columns_order]

# Display the first few rows to verify
df.head(10)

# Add revenue categories
# To simplify filtering

# See summary statistics
df['Revenue'].describe()

# Separate non-negative values
non_negative_revenues = df[df['Revenue'] >= 0]['Revenue']

# Calculate quantiles for non-negative values
q25 = non_negative_revenues.quantile(0.25)
q50 = non_negative_revenues.quantile(0.50)
q75 = non_negative_revenues.quantile(0.75)

# Create a new column based on quantiles
def categorize_revenue(revenue):
    if revenue < 0:
        return 'Negative'
    elif revenue <= q25:
        return 'Low'
    elif revenue <= q50:
        return 'Moderate'
    elif revenue <= q75:
        return 'High'
    else:
        return 'Very high'

df['RevenueCategory'] = df['Revenue'].apply(categorize_revenue)

# View unique values and their counts
df['RevenueCategory'].value_counts()

# Print quantile values to see the ranges
print(f'25th Percentile (Q25): {q25}') # 67.47380049480626
print(f'50th Percentile (Median, Q50): {q50}') # 204.85907440373677
print(f'75th Percentile (Q75): {q75}') # 426.6709762291604

# Add continent information
# The prime focus of the dashboard should be on the North American market. Thus continent information is needed.

# Select only the relevant columns from continent_df and rename
continent_df = continent_df[['Two_Letter_Country_Code', 'Continent_Name']]
continent_df = continent_df.rename(columns={'Two_Letter_Country_Code': 'country_code', 'Continent_Name': 'continent'})

# Manually add Kosovo
kosovo_data = pd.DataFrame({
    'country_code': ['XK'],
    'continent': ['Europe']
})

# After the initial merge it was noticed that there was one missing country code ("XK" which stands for Kosovo in Europe)
# Append Kosovo data to continent_df
continent_df = pd.concat([continent_df, kosovo_data], ignore_index=True)

# Function to merge and add continent columns, handling column name conflicts
def add_continent_info(main_df, continent_df, country_col, new_continent_col):
    # Merge the DataFrame on the specified country code column with explicit suffixes to avoid conflicts
    merged_df = main_df.merge(continent_df, left_on=country_col, right_on='country_code', how='left', suffixes=('', '_continent'))
    
    # Rename the 'continent' column to the new column name
    merged_df = merged_df.rename(columns={'continent': new_continent_col})
    
    # Drop the redundant 'country_code' and any unwanted columns added by the merge
    merged_df = merged_df.drop(columns=['country_code'])
    
    return merged_df

# Add continent information for each relevant column
df = add_continent_info(df, continent_df, 'DestinationLeg_country_code', 'DestinationLeg_continent')
df = add_continent_info(df, continent_df, 'OriginLeg_country_code', 'OriginLeg_continent')
df = add_continent_info(df, continent_df, 'destination_country_code', 'destination_continent')
df = add_continent_info(df, continent_df, 'origin_country_code', 'origin_continent')

# Verify results
# Check for NaN values in the newly created continent columns
missing_DestinationLeg = df['DestinationLeg_continent'].isna().sum()
missing_OriginLeg = df['OriginLeg_continent'].isna().sum()
missing_destination = df['destination_continent'].isna().sum()
missing_origin = df['origin_continent'].isna().sum()

print(f"Missing DestinationLeg continent values: {missing_DestinationLeg}")
print(f"Missing OriginLeg continent values: {missing_OriginLeg}")
print(f"Missing destination continent values: {missing_destination}")
print(f"Missing origin continent values: {missing_origin}")

# Add week day information
# To understand the travel patterns a week day column is added.

# Add a new column for the day of the week
df['DateDepartureWeekDay'] = df['DateDeparture'].dt.day_name()

# Reorder columns to place 'DateDepartureWeekDay' after 'DateDeparture'
columns_order = ['DateDeparture', 'DateDepartureWeekDay'] + [col for col in df.columns if col not in ['DateDeparture', 'DateDepartureWeekDay']]
df = df[columns_order]

print(df.head(100))

# Add ticket sales information
# How much in advance are the tickets bought?

# Calculate the difference between DateIssued and DateDeparture (in days)
df['DifferenceDateIssuedDeparture'] = (df['DateDeparture'] - df['DateIssued']).dt.days

# See summary statistics
df['DifferenceDateIssuedDeparture'].describe()

# Filter rows where the difference is negative
negative_diff_df = df[df['DifferenceDateIssuedDeparture'] < 0]

# Filter rows where the difference is more than 400 days
large_diff_df = df[df['DifferenceDateIssuedDeparture'] > 400]

print(negative_diff_df)
print(large_diff_df)

print(df.info())

## 5 Save as csv

# Save the DataFrame to a CSV file in DBFS
csv_path = '/dbfs/FileStore/cleaned_df_new.csv'
df.to_csv(csv_path, index=False)

# Check file
display(dbutils.fs.ls("/FileStore"))
# To download the file use url like https://adb-0000000000000000.0.azuredatabricks.net/files/cleaned_df.csv

# The cleaned file can now be used to visualize the data in Tableau

## 6 Improvement ideas
# - Create more integrated flow instead of local csv files
# - Dig more into a meaning of certain variables
# - Impute missing values or handle them otherwise
# - Look for outliers and possible mistakes and/or delete rows which are suspicious (e.g. routes with 2 pax in a month)
# - Add more context information in the dahsboard (e.g. totals)
# - Improve dashboard formatting including labels and branding
