# import necessary packages
import pandas as pd
import datetime as dt
import time as t
from dateutil.relativedelta import relativedelta
import random as rdm
from functions import order_monthly, add_zero_months, add_n_monthly_flags

# select columns of interest for example output
columns_example_output = ['Customer_ID', 'Product_ID', 'Month', 'MRR', 'IsRecurring', 'LTM_Delta_Churn', 'LTM_Delta_Cross_Sell',
           'LTM_Delta_Downgrade', 'LTM_Delta_Downsell', 'LTM_Delta_Upsell', 'LTM_Delta_New_Customer',
           'LTM_Flag_Churn', 'LTM_Flag_Cross_Sell', 'LTM_Flag_Downgrade', 'LTM_Flag_Downsell', 'LTM_Flag_Upsell',
           'LTM_Flag_New_Customer', 'Monthly_Delta_Churn', 'Monthly_Delta_Cross_Sell', 'Monthly_Delta_Downgrade',
           'Monthly_Delta_Downsell', 'Monthly_Delta_Upsell', 'Monthly_Delta_New_Customer', 'Monthly_Flag_Churn',
           'Monthly_Flag_Cross_Sell', 'Monthly_Flag_Downgrade', 'Monthly_Flag_Downsell', 'Monthly_Flag_Upsell',
           'Monthly_Flag_New_Customer']

# read in example output
df_example_output = pd.read_csv('Snowball example output v2.csv', usecols=columns_example_output)

# select columns of interest for example input
columns_example_input = ['Customer_ID', 'Product_ID', 'Month', 'MRR', 'IsRecurring']

# read in example input
df_example_input = pd.read_csv('Snowball example inputs v2.csv', usecols=columns_example_input)

# drop all non-recurring payments from the example input and output datasets
df_example_input = df_example_input.loc[df_example_input['IsRecurring'] == 1]
df_example_output = df_example_output.loc[df_example_output['IsRecurring'] == 1]

# cast date column entries from string to datetime in case they are not already
# print(revenue_data[date_column])
# print(pd.to_datetime(revenue_data[date_column]))
df_example_input.loc[:, 'Month'] = pd.to_datetime(df_example_input['Month'])

# make all payments set for beginning of month - it feels a little unnatural to
# include this here rather than the data cleaning phase
df_example_input.loc[:, 'Month'] = df_example_input['Month'].dt.to_period('M').dt.to_timestamp()

# fix start-of-period and end-of-period dates for recurring payments before taking sample of customers
start_date = df_example_input['Month'].min() - relativedelta(months=1)
end_date = df_example_input['Month'].max() + relativedelta(months=1)

# fix number of customers to be sampled
n = 10

# choose subset of n customers
customers_first_n = list(set(df_example_input['Customer_ID']))[:n]
customers_sample = rdm.sample(list(set(df_example_input['Customer_ID'])), n)
customers_sample_manual = [int(82)]

# select only sample of the example input and output datasets
df_example_input = df_example_input.loc[df_example_input['Customer_ID'].isin(customers_sample)]
df_example_output = df_example_output.loc[df_example_output['Customer_ID'].isin(customers_sample)]
print(df_example_output.head())
