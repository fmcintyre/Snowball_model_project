# import necessary packages
import pandas as pd
import datetime as dt
import time as t
from functions import order_monthly, add_zero_months, add_n_monthly_flags

# initialize timer
T0 = t.time()

# generate timestamp to save files
timestamp = str(dt.datetime.now().strftime("%Y%m%d_%H-%M-%S"))

# initialize timer
t0 = t.time()

# read in revenue data as a Panda DataFrame
df_input = pd.read_csv('Snowball example inputs v2.csv')

# clean the data slightly
df_input_rec = df_input.loc[df_input['IsRecurring'] == 1]
df_input_rec.loc[:, 'Customer_ID'] = df_input_rec['Customer_ID'].astype(int)
df_input_rec.loc[:, 'Product_ID'] = df_input_rec['Product_ID'].astype(str)

# create list of customers
customers = list(set(df_input_rec['Customer_ID']))
# stop time
t1 = t.time()

print('Runtime to import dataframe and compile list of customers: ' + str(t1-t0))

# generate list of total customer numbers we want to test the runtime each function for
customer_numbers = [2, 50, 100, 500, 1000]

# generate list of period lengths for testing runtime of add_n_monthly_flags
period_lengths = [1, 12]

# initiate list of runtimes
runtimes = []

# initiate test index
i = 1
# fill in runtimes
for n in customer_numbers:
    runtime_entry = dict()
    runtime_entry['Number of customers'] = n
    # run add_zero_months for first n customers
    if n < 1:
        raise ValueError('Number of customers should be an integer greater than zero!')
    elif n == 1:
        test_customer = customers[0]
        df_input_test = df_input_rec.loc[df_input_rec['Customer_ID'] == test_customer]

        # initialize timer
        t0 = t.time()
        # include 'all' payments of zero
        df_runtimes = add_zero_months(df_input_test, 'Month')

        # stop timer
        t1 = t.time()

        add_zero_months_runtime = t1 - t0

    else:
        test_customers = customers[0:n]
        df_input_test = df_input_rec.loc[df_input_rec['Customer_ID'].isin(test_customers)]
        # initialize timer
        t0 = t.time()
        # include 'all' payments of zero
        df_runtimes = add_zero_months(df_input_test, 'Month')

        # stop timer
        t1 = t.time()

        add_zero_months_runtime = t1 - t0

    runtime_entry['Runtimes for add_zero_months (seconds)'] = add_zero_months_runtime
    print('add_zero_months test ' + str(i) + ' was completed and took ' + str(add_zero_months_runtime) + ' seconds')

    for period in period_lengths:

        # run monthly snowball model for first n customers

        # initialize timer
        t0 = t.time()
        # add flags and deltas using new function
        df_with_flags = add_n_monthly_flags(df_runtimes, period=period)

        # stop timer
        t1 = t.time()
        add_n_monthly_flags_runtime = t1 - t0

        runtime_entry['Runtimes for add_' + str(period) + '_monthly_flags (seconds)'] = add_n_monthly_flags_runtime
        print('add_n_monthly_flags test ' + str(i) + ' with n = ' + str(period) +' was completed and took ' + str(add_n_monthly_flags_runtime) + ' seconds')

    runtimes.append(runtime_entry)
    i += 1

# generate dataframe of runtimes
df_runtimes = pd.DataFrame(runtimes)

df_runtimes.to_csv(timestamp + '_runtimes_data.csv')
print('Runtimes data saved.')

T1 = t.time()

print('Total runtime was ' + str(T1-T0))