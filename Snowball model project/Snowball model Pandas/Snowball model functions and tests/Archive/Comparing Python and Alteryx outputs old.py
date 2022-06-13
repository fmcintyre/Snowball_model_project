# import necessary packages
import pandas as pd
import datetime as dt
import time as t
from dateutil.relativedelta import relativedelta
import random as rdm
from functions import order_monthly, add_zero_months, add_n_monthly_flags

# define columns names for revenue bridge flags
flag_new_customer_monthly_column = 'Monthly_Flag_New_Customer'
flag_churn_monthly_column = 'Monthly_Flag_Churn'
flag_cross_sell_monthly_column = 'Monthly_Flag_Cross_Sell'
flag_downgrade_monthly_column = 'Monthly_Flag_Downgrade'
flag_upsell_monthly_column = 'Monthly_Flag_Upsell'
flag_downsell_monthly_column = 'Monthly_Flag_Downsell'
flag_new_customer_LTM_column = 'LTM_Flag_New_Customer'
flag_churn_LTM_column = 'LTM_Flag_Churn'
flag_cross_sell_LTM_column = 'LTM_Flag_Cross_Sell'
flag_downgrade_LTM_column = 'LTM_Flag_Downgrade'
flag_upsell_LTM_column = 'LTM_Flag_Upsell'
flag_downsell_LTM_column = 'LTM_Flag_Downsell'

# define columns names for revenue bridge deltas
delta_new_customer_monthly_column = 'Monthly_Delta_New_Customer'
delta_churn_monthly_column = 'Monthly_Delta_Churn'
delta_cross_sell_monthly_column = 'Monthly_Delta_Cross_Sell'
delta_downgrade_monthly_column = 'Monthly_Delta_Downgrade'
delta_upsell_monthly_column = 'Monthly_Delta_Upsell'
delta_downsell_monthly_column = 'Monthly_Delta_Downsell'
delta_new_customer_LTM_column = 'LTM_Delta_New_Customer'
delta_churn_LTM_column = 'LTM_Delta_Churn'
delta_cross_sell_LTM_column = 'LTM_Delta_Cross_Sell'
delta_downgrade_LTM_column = 'LTM_Delta_Downgrade'
delta_upsell_LTM_column = 'LTM_Delta_Upsell'
delta_downsell_LTM_column = 'LTM_Delta_Downsell'

# select columns of interest for example output
columns_example_output = ['Customer_ID', 'Product_ID', 'Month', 'MRR', 'IsRecurring', flag_new_customer_monthly_column,
                          flag_new_customer_LTM_column, flag_churn_monthly_column, flag_churn_LTM_column,
                          flag_cross_sell_monthly_column, flag_cross_sell_LTM_column, flag_downgrade_monthly_column,
                          flag_downgrade_LTM_column, flag_upsell_monthly_column, flag_upsell_LTM_column,
                          flag_downsell_monthly_column, flag_downsell_LTM_column, delta_new_customer_monthly_column,
                          delta_new_customer_LTM_column, delta_churn_monthly_column,
                          delta_churn_LTM_column, delta_cross_sell_monthly_column, delta_cross_sell_LTM_column,
                          delta_downgrade_monthly_column, delta_downgrade_LTM_column, delta_upsell_monthly_column,
                          delta_upsell_LTM_column, delta_downsell_monthly_column, delta_downsell_LTM_column]
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
# print(df_example_input[date_column])
# print(pd.to_datetime(df_example_input[date_column]))
df_example_input.loc[:, 'Month'] = pd.to_datetime(df_example_input['Month'])

# make all payments set for beginning of month - it feels a little unnatural to
# include this here rather than the data cleaning phase
df_example_input.loc[:, 'Month'] = df_example_input['Month'].dt.to_period('M').dt.to_timestamp()

# fix start and end dates for before start and after end of data period (or first and last positive/non-zero payments)
# so that we can flag first month and last month payments as new customer and churn respectively
start_date_monthly = df_example_input['Month'].min() - relativedelta(months=1)
end_date_monthly = df_example_input['Month'].max() + relativedelta(months=1)
start_date_LTM = df_example_input['Month'].min() - relativedelta(months=12)
end_date_LTM = df_example_input['Month'].max() + relativedelta(months=12)

# fix number of customers to be sampled
n = 10

# choose subset of n customers
customers_first_n = list(set(df_example_input['Customer_ID']))[:n]
customers_sample = rdm.sample(list(set(df_example_input['Customer_ID'])), n)
customers_sample_manual = [int(2076)]

# select only sample of the example input and output datasets
df_example_input = df_example_input.loc[df_example_input['Customer_ID'].isin(customers_sample_manual)]
df_example_output = df_example_output.loc[df_example_output['Customer_ID'].isin(customers_sample_manual)]


# create new column with number of flags per row
df_example_output['num_of_flags'] = df_example_output[flag_new_customer_monthly_column] + df_example_output[
    flag_churn_monthly_column] + df_example_output[flag_cross_sell_monthly_column] + df_example_output[
    flag_downgrade_monthly_column] + df_example_output[flag_upsell_monthly_column] + df_example_output[
    flag_downsell_monthly_column] + df_example_output[flag_new_customer_LTM_column] + df_example_output[
    flag_churn_LTM_column] + df_example_output[flag_cross_sell_LTM_column] + df_example_output[
    flag_downgrade_LTM_column] + df_example_output[flag_upsell_LTM_column] + df_example_output[flag_downsell_LTM_column]

# drop rows with all flags equal to zero
df_example_output = df_example_output.loc[df_example_output['num_of_flags'] > 0]

# add all missing zero months, 12 months of zero months before first positive (non-zero?) payment for generating
# 12-monthly new customer flags, and 12 months of zero months after last positive (non-zero?) payment for generating
# 12-monthly churn flags. This will also suffice for generating monthly new customer and churn flags, so we only need
# to run add_zero_months once for the purpose of this script (we're not particularly interested in n-monthly flags
# for n > 12 at the moment.)
df_with_zeros = add_zero_months(df_example_input, start_date=start_date_LTM, end_date=end_date_LTM)

# generate LTM output using Python Snowball model script
df_python_LTM_output = add_n_monthly_flags(df_with_zeros, period=12)
df_python_output = df_python_LTM_output

# # only keep one month before data period to calculate monthly flags
# df_with_zeros_monthly = df_with_zeros.loc[(df_with_zeros['Month'] >= start_date_monthly) &
#                                           (df_with_zeros['Month'] <= end_date_monthly)]
# # generate monthly output using Python Snowball model script
# df_python_monthly_output = add_n_monthly_flags(df_with_zeros_monthly, period=1)
#
# # generate full Python output to compare with the example output
# df_python_output = pd.merge(df_python_monthly_output, df_python_LTM_output, how='left', on=['Customer_ID',
#                                                                                             'Product_ID', 'Month',
#                                                                                             'IsRecurring', 'MRR'])
# # check the sums of monthly flags and deltas in the Python output match that of the example output
# # check number of monthly new customer/churn flags match
# total_flags_new_customer_example = sum(df_example_output[flag_new_customer_monthly_column])
# total_flags_new_customer_python = sum(df_python_output[flag_new_customer_monthly_column])
# total_flags_churn_example = sum(df_example_output[flag_new_customer_monthly_column])
# total_flags_churn_python = sum(df_python_output[flag_new_customer_monthly_column])
#
# print('Number of monthly new customer flags match: ' + str(total_flags_new_customer_example == total_flags_new_customer_python))
# print('Number of monthly churn flags match: ' + str(total_flags_churn_example == total_flags_churn_python))
#
# # check total deltas of monthly new customer/churn flags match
# total_deltas_new_customer_example = sum(df_example_output[delta_new_customer_monthly_column])
# total_deltas_new_customer_python = sum(df_python_output[delta_new_customer_monthly_column])
# total_deltas_churn_example = sum(df_example_output[delta_churn_monthly_column])
# total_deltas_churn_python = sum(df_python_output[delta_churn_monthly_column])
#
# print('Sum of monthly new customer deltas match: ' + str(total_deltas_new_customer_example == total_deltas_new_customer_python))
# print('Sum of monthly churn deltas match: ' + str(total_deltas_churn_example == total_deltas_churn_python))
#
#
# # check number of monthly cross-sell/downgrade flags match
# total_flags_cross_sell_example = sum(df_example_output[flag_cross_sell_monthly_column])
# total_flags_cross_sell_python = sum(df_python_output[flag_cross_sell_monthly_column])
# total_flags_downgrade_example = sum(df_example_output[flag_downgrade_monthly_column])
# total_flags_downgrade_python = sum(df_python_output[flag_downgrade_monthly_column])
#
# print('Number of monthly cross-sell flags match: ' + str(total_flags_cross_sell_example == total_flags_cross_sell_python))
# print('Number of monthly downgrade flags match: ' + str(total_flags_downgrade_example == total_flags_downgrade_python))
#
# # check total deltas of monthly cross-sell/downgrade match
# total_deltas_cross_sell_example = sum(df_example_output[delta_cross_sell_monthly_column])
# total_deltas_cross_sell_python = sum(df_python_output[delta_cross_sell_monthly_column])
# total_deltas_downgrade_example = sum(df_example_output[delta_downgrade_monthly_column])
# total_deltas_downgrade_python = sum(df_python_output[delta_downgrade_monthly_column])
#
# print('Sum of monthly cross-sell deltas match: ' + str(total_deltas_cross_sell_example == total_deltas_cross_sell_python))
# print('Sum of monthly downgrade deltas match: ' + str(total_deltas_downgrade_example == total_deltas_downgrade_python))
#
# # check number of monthly upsell/downsell flags match
# total_flags_upsell_example = sum(df_example_output[flag_upsell_monthly_column])
# total_flags_upsell_python = sum(df_python_output[flag_upsell_monthly_column])
# total_flags_downsell_example = sum(df_example_output[flag_downsell_monthly_column])
# total_flags_downsell_python = sum(df_python_output[flag_downsell_monthly_column])
#
# print('Number of monthly upsell flags match: ' + str(total_flags_upsell_example == total_flags_upsell_python))
# print('Number of monthly downsell flags match: ' + str(total_flags_downsell_example == total_flags_downsell_python))
#
# # check total deltas of monthly upsell/downsell match
# total_deltas_upsell_example = sum(df_example_output[delta_upsell_monthly_column])
# total_deltas_upsell_python = sum(df_python_output[delta_upsell_monthly_column])
# total_deltas_downsell_example = sum(df_example_output[delta_downsell_monthly_column])
# total_deltas_downsell_python = sum(df_python_output[delta_downsell_monthly_column])
#
# print('Sum of monthly upsell deltas match: ' + str(total_deltas_upsell_example == total_deltas_upsell_python))
# print('Sum of monthly downsell deltas match: ' + str(total_deltas_downsell_example == total_deltas_downsell_python))


# do the same for LTM flags and deltas
# check number of LTM new customer/churn flags match
total_flags_new_customer_example = sum(df_example_output[flag_new_customer_LTM_column])
total_flags_new_customer_python = sum(df_python_output[flag_new_customer_LTM_column])
total_flags_churn_example = sum(df_example_output[flag_new_customer_LTM_column])
total_flags_churn_python = sum(df_python_output[flag_new_customer_LTM_column])

print('Number of LTM new customer flags match: ' + str(total_flags_new_customer_example == total_flags_new_customer_python))
print('Number of LTM churn flags match: ' + str(total_flags_churn_example == total_flags_churn_python))

# check total deltas of LTM new customer/churn flags match
total_deltas_new_customer_example = sum(df_example_output[delta_new_customer_LTM_column])
total_deltas_new_customer_python = sum(df_python_output[delta_new_customer_LTM_column])
total_deltas_churn_example = sum(df_example_output[delta_churn_LTM_column])
total_deltas_churn_python = sum(df_python_output[delta_churn_LTM_column])

print('Sum of LTM new customer deltas match: ' + str(total_deltas_new_customer_example == total_deltas_new_customer_python))
print('Sum of LTM churn deltas match: ' + str(total_deltas_churn_example == total_deltas_churn_python))


# check number of LTM cross-sell/downgrade flags match
total_flags_cross_sell_example = sum(df_example_output[flag_cross_sell_LTM_column])
total_flags_cross_sell_python = sum(df_python_output[flag_cross_sell_LTM_column])
total_flags_downgrade_example = sum(df_example_output[flag_downgrade_LTM_column])
total_flags_downgrade_python = sum(df_python_output[flag_downgrade_LTM_column])

print('Number of LTM cross-sell flags match: ' + str(total_flags_cross_sell_example == total_flags_cross_sell_python))
print('Number of LTM downgrade flags match: ' + str(total_flags_downgrade_example == total_flags_downgrade_python))

# check total deltas of LTM cross-sell/downgrade match
total_deltas_cross_sell_example = sum(df_example_output[delta_cross_sell_LTM_column])
total_deltas_cross_sell_python = sum(df_python_output[delta_cross_sell_LTM_column])
total_deltas_downgrade_example = sum(df_example_output[delta_downgrade_LTM_column])
total_deltas_downgrade_python = sum(df_python_output[delta_downgrade_LTM_column])

print('Sum of LTM cross-sell deltas match: ' + str(total_deltas_cross_sell_example == total_deltas_cross_sell_python))
print('Sum of LTM downgrade deltas match: ' + str(total_deltas_downgrade_example == total_deltas_downgrade_python))

# check number of LTM upsell/downsell flags match
total_flags_upsell_example = sum(df_example_output[flag_upsell_LTM_column])
total_flags_upsell_python = sum(df_python_output[flag_upsell_LTM_column])
total_flags_downsell_example = sum(df_example_output[flag_downsell_LTM_column])
total_flags_downsell_python = sum(df_python_output[flag_downsell_LTM_column])

print('Number of LTM upsell flags match: ' + str(total_flags_upsell_example == total_flags_upsell_python))
print('Number of LTM downsell flags match: ' + str(total_flags_downsell_example == total_flags_downsell_python))

# check total deltas of LTM upsell/downsell match
total_deltas_upsell_example = sum(df_example_output[delta_upsell_LTM_column])
total_deltas_upsell_python = sum(df_python_output[delta_upsell_LTM_column])
total_deltas_downsell_example = sum(df_example_output[delta_downsell_LTM_column])
total_deltas_downsell_python = sum(df_python_output[delta_downsell_LTM_column])

print('Sum of LTM upsell deltas match: ' + str(total_deltas_upsell_example == total_deltas_upsell_python))
print('Sum of LTM downsell deltas match: ' + str(total_deltas_downsell_example == total_deltas_downsell_python))

print('Tests completed')

# Example output strange results:
#
# customer 2076, product paye logiciel
#
# multiple downgrade records within single month
#
# churn record after downgrade record