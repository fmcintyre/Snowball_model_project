from combine_primary_key_duplicates import combine_primary_key_duplicates
import pandas as pd

revenue_data = pd.read_csv('..\Alteryx datasets\Snowball example inputs v2.csv')

# primary key columns are all the columns except Month, IsRecurring, Revenue, MRR, ARR.
# This is definitely not the best way to select them but works for now
primary_key_columns = list(revenue_data.columns[:-5])

# change data types of primary key columns to strings so that we can combine them to get a string for the primary key
# for column in primary_key_columns:
#     revenue_data.loc[:, column] = revenue_data[column].astype(str)

# primary_key_label = 'Primary_Key'
date_column = 'Month'

# revenue_data[primary_key_label] = revenue_data[primary_key_columns].sum(axis=1)

revenue_data_combined = combine_primary_key_duplicates(revenue_data=revenue_data,
                                                       primary_key_columns=primary_key_columns)

# generate list with primary key columns and date column (should already have date_column in it due to the .append()
# already used inside combine_primary_key_duplicates
primary_key_date_columns = primary_key_columns

# group revenue data by primary key columns and month
primary_key_month_data = revenue_data_combined.groupby(by=primary_key_date_columns)

# check that the number of groups in the groupby is the same as the number of rows in revenue_data_combined
# which would imply that there are no duplicates in the dataset
print(revenue_data_combined.shape[0] == len(primary_key_month_data))