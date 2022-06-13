import pandas as pd
import datetime as dt
from dateutil.relativedelta import relativedelta
from clean_date_columns import clean_date_columns
from combine_primary_key_duplicates import combine_primary_key_duplicates
from add_start_end_dates import add_start_end_dates
from add_zero_months import add_zero_months
from add_n_monthly_deltas import add_n_monthly_deltas
from add_n_monthly_flags import add_n_monthly_flags

revenue_data = pd.read_csv('..\Alteryx datasets\Snowball example inputs v2.csv')
example_input_data = pd.read_csv('..\Alteryx datasets\Snowball example inputs v2.csv')
example_output_data = pd.read_csv('..\Alteryx datasets\Snowball example output v2.csv')

# filter out any non-recurring payments
revenue_data = revenue_data.loc[revenue_data['IsRecurring'] == 1]

# create list of all customers
customers = list(set(revenue_data['Customer_ID']))

# fix number of customers to sample
n = 100

# restrict both input and example output datasets to first n customers
test_customers = customers[0:n]
# test_customer = [2076]
revenue_data = revenue_data.loc[revenue_data['Customer_ID'].isin(test_customers)]
example_input_data = example_input_data.loc[example_input_data['Customer_ID'].isin(test_customers)]
example_output_data = example_output_data.loc[example_output_data['Customer_ID'].isin(test_customers)]

# define parameters required for clean_date_column
date_column = 'Month'

revenue_data_clean = clean_date_columns(revenue_data=revenue_data, date_columns=[date_column])

# define extra parameters required for combine_primary_key_duplicates
customer_identifier_columns = ['Customer_ID', 'Customer_name']
product_identifier_columns = ['Product_family', 'Product_sub-family', 'Product_ID']
region_identifier_columns = ['Country', 'Region']
primary_key_columns = customer_identifier_columns + product_identifier_columns + region_identifier_columns
arr_column = 'ARR'

# make sure entries to the Customer_ID column are integers
revenue_data_clean['Customer_ID'] = revenue_data_clean['Customer_ID'].astype('int')

# set dtype entries of all primary key column to string
for column in primary_key_columns:
    revenue_data_clean[column] = revenue_data_clean[column].astype('string')

revenue_data_without_duplicates = combine_primary_key_duplicates(revenue_data=revenue_data_clean,
                                                                 date_column=date_column,
                                                                 arr_column=arr_column,
                                                                 primary_key_columns=primary_key_columns)

# define extra parameters required for add_zero_months
period = int(input('Please specify the number of months over which you would like to calculate deltas and flags: '))

# generate timestamp to save files
timestamp = str(dt.datetime.now().strftime("%Y%m%d_%H-%M-%S"))

# # save Alteryx model example output for first 100 customers as .csv file in output datasets with a timestamp
# example_output_data.to_csv('.\output datasets\Snowball output Alteryx for first ' + str(n) + ' customers ' + timestamp
#                            + '.csv')

# record n-months before data start date and n months after data end date to use when calling add_zero_months later
start_date = revenue_data_without_duplicates.loc[:, date_column].min() - relativedelta(months=period)
end_date = revenue_data_without_duplicates.loc[:, date_column].max() + relativedelta(months=period)

revenue_data_with_zeros = add_zero_months(revenue_data=revenue_data_without_duplicates, date_column=date_column,
                                          start_date=start_date, end_date=end_date, arr_column=arr_column,
                                          primary_key_columns=primary_key_columns)

# define extra parameters required for add_start_end_dates
start_date_customer_product_column = 'start_date_customer_product'
end_date_customer_product_column = 'end_date_customer_product'
start_date_customer_column = 'start_date_customer'
end_date_customer_column = 'end_date_customer'

revenue_data_with_dates = add_start_end_dates(revenue_data=revenue_data_with_zeros, date_column=date_column,
                                              arr_column=arr_column,
                                              customer_identifier_columns=customer_identifier_columns,
                                              product_identifier_columns=product_identifier_columns,
                                              start_date_customer_product_column=start_date_customer_product_column,
                                              end_date_customer_product_column=end_date_customer_product_column,
                                              start_date_customer_column=start_date_customer_column,
                                              end_date_customer_column=end_date_customer_column)

# make sure that new date columns are in the same form as date_column
start_end_date_columns = [start_date_customer_column, end_date_customer_column,
                          start_date_customer_product_column, end_date_customer_product_column]

revenue_data_with_dates = clean_date_columns(revenue_data=revenue_data_with_dates, date_columns=start_end_date_columns)

# define extra parameters required for add_n_monthly_deltas
delta_n_monthly_column = str(period) + '_monthly_ARR_delta_customer_product'

# add n monthly deltas
revenue_data_with_n_monthly_dates_and_deltas = add_n_monthly_deltas(revenue_data=revenue_data_with_dates,
                                                                    arr_column=arr_column, date_column=date_column,
                                                                    period=period, primary_key_columns=
                                                                    primary_key_columns, delta_n_monthly_column=
                                                                    delta_n_monthly_column)

# define extra parameters required for add_n_monthly_flag
flag_new_customer_n_monthly_column = str(period) + '_Monthly_Flag_New_Customer'
flag_churn_n_monthly_column = str(period) + '_Monthly_Flag_Churn'
flag_cross_sell_n_monthly_column = str(period) + '_Monthly_Flag_Cross_Sell'
flag_downgrade_n_monthly_column = str(period) + '_Monthly_Flag_Downgrade'
flag_upsell_n_monthly_column = str(period) + '_Monthly_Flag_Upsell'
flag_downsell_n_monthly_column = str(period) + '_Monthly_Flag_Downsell'

revenue_data_with_n_monthly_flags_and_deltas = add_n_monthly_flags(revenue_data_with_n_monthly_dates_and_deltas,
                                                                   date_column=date_column, period=period,
                                          start_date_customer_product_column=start_date_customer_product_column,
                                          end_date_customer_product_column=end_date_customer_product_column,
                                          start_date_customer_column=start_date_customer_column,
                                          end_date_customer_column=end_date_customer_column,
                                          delta_n_monthly_column=delta_n_monthly_column,
                                          flag_new_customer_n_monthly_column=flag_new_customer_n_monthly_column,
                                          flag_churn_n_monthly_column=flag_churn_n_monthly_column,
                                          flag_cross_sell_n_monthly_column=flag_cross_sell_n_monthly_column,
                                          flag_downgrade_n_monthly_column=flag_downgrade_n_monthly_column,
                                          flag_upsell_n_monthly_column=flag_upsell_n_monthly_column,
                                          flag_downsell_n_monthly_column=flag_downsell_n_monthly_column)

# save Python Snowball model output as a .csv file in output datasets with a timestamp
revenue_data_with_n_monthly_flags_and_deltas.to_csv('.\output datasets\Snowball output ' + str(period) +
                                                    '-monthly Python for first ' + str(n) + ' customers ' + timestamp +
                                                    '.csv')