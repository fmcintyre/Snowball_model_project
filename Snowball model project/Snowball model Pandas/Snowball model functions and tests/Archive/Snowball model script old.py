# import necessary packages
import pandas as pd
import datetime as dt
import time as t
from functions import add_zero_months_old, add_n_monthly_flags_old

# generate timestamp to save files
timestamp = str(dt.datetime.now().strftime("%Y%m%d_%H-%M-%S"))

# read in revenue data as a Panda DataFrame
df_input = pd.read_csv('..\..\Alteryx datasets\Snowball example inputs v2.csv')


# clean the data slightly
df_input_rec = df_input.loc[df_input['IsRecurring'] == 1]
df_input_rec.loc[:, 'Customer_ID'] = df_input_rec['Customer_ID'].astype(int)
df_input_rec.loc[:, 'Product_ID'] = df_input_rec['Product_ID'].astype(str)

# create list of customers
customers = list(set(df_input_rec['Customer_ID']))

# fix particular subset of customers for test case
n = int(input('How many customers should we include in our sample? '))
# run add_zero_months_old for first n customers
if n < 1:
    raise ValueError('Number of customers should be an integer greater than zero!')
elif n == 1:
    test_customer = customers[0]
    df_input_test = df_input_rec.loc[df_input_rec['Customer_ID'] == test_customer]
    df_input_test.to_csv(timestamp + '_rev_data_w_zeros_customer_' + str(test_customer) + '_input.csv')
    print('add_zero_months_old input saved')

    # initialize timer
    t0 = t.time()
    # include 'all' payments of zero
    df_with_zeros = add_zero_months_old(df_input_test, 'Month')

    # stop timer
    t1 = t.time()
    print('Runtime for add_zero_months_old:')
    print(t1 - t0)

    df_with_zeros.to_csv(timestamp + '_rev_data_w_zeros_customer_' + str(test_customer) + '_output.csv')
    print('add_zero_months_old output saved')

    # initialize timer
    t0 = t.time()
else:
    test_customers = customers[0:n]
    df_input_test = df_input_rec.loc[df_input_rec['Customer_ID'].isin(test_customers)]
    df_input_test.to_csv(timestamp + '_rev_data_w_zeros_first_' + str(n) + '_customers_input.csv')
    print('add_zero_months_old input saved')

    # initialize timer
    t0 = t.time()
    # include 'all' payments of zero
    df_with_zeros = add_zero_months_old(df_input_test, 'Month')

    # stop timer
    t1 = t.time()
    print('Runtime for add_zero_months_old:')
    print(t1 - t0)

    df_with_zeros.to_csv(timestamp + '_rev_data_w_zeros_first_' + str(n) + '_customers_output.csv')
    print('add_zero_months_old output saved')

# # prompt whether or not to run original monthly add_flags
# run_add_flags_toggle = input('Would you like to run add_flags? Please type Y or N. ')
# if run_add_flags_toggle != 'Y' and run_add_flags_toggle != 'N':
#     raise ValueError('Response should be Y or N!')
# elif run_add_flags_toggle == 'Y':
#     # run monthly snowball monthly model for first n customers
#     if n == 1:
#         # initialize timer
#         t0 = t.time()
#         # add flags and deltas using new function
#         df_with_flags = add_flags(df_with_zeros)
#
#         # stop timer
#         t1 = t.time()
#         print('Runtime for add_flags:')
#         print(t1 - t0)
#
#         df_with_flags.to_csv(timestamp + '_rev_data_w_flags_customer_' + str(customers[0]) + '_output.csv')
#         print('add_flags output saved')
#
#     elif n > 1:
#         # initialize timer
#         t0 = t.time()
#         # add flags and deltas using new function
#         df_with_flags = add_flags(df_with_zeros)
#
#         # stop timer
#         t1 = t.time()
#         print('Runtime for add_flags:')
#         print(t1 - t0)
#
#         df_with_flags.to_csv(timestamp + '_rev_data_w_flags_first_' + str(n) + '_customers_output.csv')
#         print('add_flags output saved')
#     else:
#         raise ValueError('Input should be and integer larger than zero!')
#
# else:
#     pass

# prompt whether or not to run more complicated monthly add_n_monthly_flags
run_add_n_monthly_flags_toggle = input('Would you like to run add_n_monthly_flags? Please type Y or N. ')
if run_add_n_monthly_flags_toggle != 'Y' and run_add_n_monthly_flags_toggle != 'N':
    raise ValueError('Response should be Y or N!')
# over how many months do we want to calculate our flags and deltas
period_months = int(input('Over how many months do you want to calculate flags and deltas? '))
if period_months < 1:
    raise ValueError('Number of months should be an integer greater than zero!')

if run_add_n_monthly_flags_toggle == 'Y':
    # run monthly snowball model for first n customers
    if n == 1:
        # initialize timer
        t0 = t.time()
        # add flags and deltas using new function
        df_with_flags = add_n_monthly_flags(df_with_zeros, period=period_months)

        # stop timer
        t1 = t.time()
        print('Runtime for add_n_monthly_flags:')
        print(t1 - t0)

        df_with_flags.to_csv(timestamp + '_rev_data_w_' + str(period_months) + '_monthly_flags_customer_' +
                             str(customers[0]) + '_output.csv')
        print('add_n_monthly_flags output saved')

    else:
        # initialize timer
        t0 = t.time()
        # add flags and deltas using new function
        df_with_flags = add_n_monthly_flags(df_with_zeros, period=period_months)

        # stop timer
        t1 = t.time()
        print('Runtime for add_n_monthly_flags:')
        print(t1 - t0)

        df_with_flags.to_csv(timestamp + '_rev_data_w_' + str(period_months) + '_monthly_flags_first_' + str(n) +
                             '_customers_output.csv')
        print('add_n_monthly_flags output saved')
else:
    pass
