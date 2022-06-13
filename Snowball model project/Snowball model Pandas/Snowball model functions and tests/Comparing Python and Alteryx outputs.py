# import necessary packages
import pandas as pd
import numpy as np
import datetime as dt

# define columns names for Alteryx revenue bridge flags
flag_new_customer_monthly_alteryx_column = 'Monthly_Flag_New_Customer'
flag_churn_monthly_alteryx_column = 'Monthly_Flag_Churn'
flag_cross_sell_monthly_alteryx_column = 'Monthly_Flag_Cross_Sell'
flag_downgrade_monthly_alteryx_column = 'Monthly_Flag_Downgrade'
flag_upsell_monthly_alteryx_column = 'Monthly_Flag_Upsell'
flag_downsell_monthly_alteryx_column = 'Monthly_Flag_Downsell'
flag_new_customer_LTM_alteryx_column = 'LTM_Flag_New_Customer'
flag_churn_LTM_alteryx_column = 'LTM_Flag_Churn'
flag_cross_sell_LTM_alteryx_column = 'LTM_Flag_Cross_Sell'
flag_downgrade_LTM_alteryx_column = 'LTM_Flag_Downgrade'
flag_upsell_LTM_alteryx_column = 'LTM_Flag_Upsell'
flag_downsell_LTM_alteryx_column = 'LTM_Flag_Downsell'

# define columns names for Python revenue bridge flags
flag_new_customer_monthly_python_column = str(1) + '_Monthly_Flag_New_Customer'
flag_churn_monthly_python_column = str(1) + '_Monthly_Flag_Churn'
flag_cross_sell_monthly_python_column = str(1) + '_Monthly_Flag_Cross_Sell'
flag_downgrade_monthly_python_column = str(1) + '_Monthly_Flag_Downgrade'
flag_upsell_monthly_python_column = str(1) + '_Monthly_Flag_Upsell'
flag_downsell_monthly_python_column = str(1) + '_Monthly_Flag_Downsell'
flag_new_customer_LTM_python_column = str(12) + '_Monthly_Flag_New_Customer'
flag_churn_LTM_python_column = str(12) + '_Monthly_Flag_Churn'
flag_cross_sell_LTM_python_column = str(12) + '_Monthly_Flag_Cross_Sell'
flag_downgrade_LTM_python_column = str(12) + '_Monthly_Flag_Downgrade'
flag_upsell_LTM_python_column = str(12) + '_Monthly_Flag_Upsell'
flag_downsell_LTM_python_column = str(12) + '_Monthly_Flag_Downsell'


# define columns names for revenue bridge deltas
delta_new_customer_monthly_alteryx_column = 'Monthly_Delta_New_Customer'
delta_churn_monthly_alteryx_column = 'Monthly_Delta_Churn'
delta_cross_sell_monthly_alteryx_column = 'Monthly_Delta_Cross_Sell'
delta_downgrade_monthly_alteryx_column = 'Monthly_Delta_Downgrade'
delta_upsell_monthly_alteryx_column = 'Monthly_Delta_Upsell'
delta_downsell_monthly_alteryx_column = 'Monthly_Delta_Downsell'
delta_new_customer_LTM_alteryx_column = 'LTM_Delta_New_Customer'
delta_churn_LTM_alteryx_column = 'LTM_Delta_Churn'
delta_cross_sell_LTM_alteryx_column = 'LTM_Delta_Cross_Sell'
delta_downgrade_LTM_alteryx_column = 'LTM_Delta_Downgrade'
delta_upsell_LTM_alteryx_column = 'LTM_Delta_Upsell'
delta_downsell_LTM_alteryx_column = 'LTM_Delta_Downsell'

delta_monthly_python_column = str(1)+'_monthly_ARR_delta_customer_product'
delta_LTM_python_column = str(12)+'_monthly_ARR_delta_customer_product'

# define columns names for Python revenue bridge deltas (these are not used in the Python output
# dataset but will be used in comparison_data!)
delta_new_customer_monthly_python_column = str(1) + '_Monthly_Delta_New_Customer'
delta_churn_monthly_python_column = str(1) + '_Monthly_Delta_Churn'
delta_cross_sell_monthly_python_column = str(1) + '_Monthly_Delta_Cross_Sell'
delta_downgrade_monthly_python_column = str(1) + '_Monthly_Delta_Downgrade'
delta_upsell_monthly_python_column = str(1) + '_Monthly_Delta_Upsell'
delta_downsell_monthly_python_column = str(1) + '_Monthly_Delta_Downsell'
delta_new_customer_LTM_python_column = str(12) + '_Monthly_Delta_New_Customer'
delta_churn_LTM_python_column = str(12) + '_Monthly_Delta_Churn'
delta_cross_sell_LTM_python_column = str(12) + '_Monthly_Delta_Cross_Sell'
delta_downgrade_LTM_python_column = str(12) + '_Monthly_Delta_Downgrade'
delta_upsell_LTM_python_column = str(12) + '_Monthly_Delta_Upsell'
delta_downsell_LTM_python_column = str(12) + '_Monthly_Delta_Downsell'

# read in input and Alteryx/Python output datasets
df_python_output_monthly = pd.read_csv('.\output datasets\Snowball output 1-monthly Python for first 100 customers '
                                         '20211105_15-02-58.csv')

df_python_output_LTM = pd.read_csv('.\output datasets\Snowball output 12-monthly Python for first 100 '
                                          'customers 20211105_15-03-48.csv')

df_example_output = pd.read_csv('.\output datasets\Snowball output Alteryx for first 100 customers 20211105_15-02-58'
                                '.csv')

df_example_input = pd.read_csv('.\output datasets\Snowball input for first 100 customers.csv')


# add new monthly and categorised delta columns to python outputs
df_python_output_monthly[delta_new_customer_monthly_python_column] = df_python_output_monthly[
                                                                           delta_monthly_python_column] * \
                                                                       df_python_output_monthly[
                                                                           flag_new_customer_monthly_python_column]

df_python_output_monthly[delta_churn_monthly_python_column] = df_python_output_monthly[
                                                                           delta_monthly_python_column] * \
                                                                       df_python_output_monthly[
                                                                           flag_churn_monthly_python_column]

df_python_output_monthly[delta_cross_sell_monthly_python_column] = df_python_output_monthly[
                                                                           delta_monthly_python_column] * \
                                                                       df_python_output_monthly[
                                                                           flag_cross_sell_monthly_python_column]

df_python_output_monthly[delta_downgrade_monthly_python_column] = df_python_output_monthly[
                                                                           delta_monthly_python_column] * \
                                                                       df_python_output_monthly[
                                                                           flag_downgrade_monthly_python_column]

df_python_output_monthly[delta_upsell_monthly_python_column] = df_python_output_monthly[
                                                                           delta_monthly_python_column] * \
                                                                       df_python_output_monthly[
                                                                           flag_upsell_monthly_python_column]

df_python_output_monthly[delta_downsell_monthly_python_column] = df_python_output_monthly[
                                                                           delta_monthly_python_column] * \
                                                                       df_python_output_monthly[
                                                                           flag_downsell_monthly_python_column]

# add new LTM and categorised delta columns to python outputs
df_python_output_LTM[delta_new_customer_LTM_python_column] = df_python_output_LTM[
                                                                           delta_LTM_python_column] * \
                                                                       df_python_output_LTM[
                                                                           flag_new_customer_LTM_python_column]

df_python_output_LTM[delta_churn_LTM_python_column] = df_python_output_LTM[
                                                                           delta_LTM_python_column] * \
                                                                       df_python_output_LTM[
                                                                           flag_churn_LTM_python_column]

df_python_output_LTM[delta_cross_sell_LTM_python_column] = df_python_output_LTM[
                                                                           delta_LTM_python_column] * \
                                                                       df_python_output_LTM[
                                                                           flag_cross_sell_LTM_python_column]

df_python_output_LTM[delta_downgrade_LTM_python_column] = df_python_output_LTM[
                                                                           delta_LTM_python_column] * \
                                                                       df_python_output_LTM[
                                                                           flag_downgrade_LTM_python_column]

df_python_output_LTM[delta_upsell_LTM_python_column] = df_python_output_LTM[
                                                                           delta_LTM_python_column] * \
                                                                       df_python_output_LTM[
                                                                           flag_upsell_LTM_python_column]

df_python_output_LTM[delta_downsell_LTM_python_column] = df_python_output_LTM[
                                                                           delta_LTM_python_column] * \
                                                                       df_python_output_LTM[
                                                                           flag_downsell_LTM_python_column]



# make sure entries to the Customer_ID column are integers, without zeros after the decimal place
df_example_input['Customer_ID'] = df_example_input['Customer_ID'].astype('int')
df_example_output['Customer_ID'] = df_example_output['Customer_ID'].astype('int')
df_python_output_monthly['Customer_ID'] = df_python_output_monthly['Customer_ID'].astype('int')
df_python_output_LTM['Customer_ID'] = df_python_output_LTM['Customer_ID'].astype('int')

# set dtype entries of Customer_ID columns to string
df_example_input['Customer_ID'] = df_example_input['Customer_ID'].astype('int')
df_example_output['Customer_ID'] = df_example_output['Customer_ID'].astype('string')
df_python_output_monthly['Customer_ID'] = df_python_output_monthly['Customer_ID'].astype('string')
df_python_output_LTM['Customer_ID'] = df_python_output_LTM['Customer_ID'].astype('string')

# generate list of customer ids
customers = list(set(df_example_input['Customer_ID']))
num_of_customers = len(customers)

# initiate empty DataFrame to record which totals match/do not match for each customer
comparison_data = pd.DataFrame(customers, columns=['Customer_ID'])

# group all datasets by Customer_ID so that we can calculate sums of flags and deltas using .agg
customer_data_alteryx = df_example_output.groupby(by='Customer_ID')
customer_data_python_monthly = df_python_output_monthly.groupby(by='Customer_ID')
customer_data_python_LTM = df_python_output_LTM.groupby(by='Customer_ID')

# generate aggregate datasets for sums of flags and deltas
customer_sums_alteryx = customer_data_alteryx.agg(flag_new_customer_monthly_alteryx_total=
                                                  (flag_new_customer_monthly_alteryx_column, 'sum'),
                                                  delta_new_customer_monthly_alteryx_total=
                                                  (delta_new_customer_monthly_alteryx_column, 'sum'),
                                                  flag_churn_monthly_alteryx_total=
                                                  (flag_churn_monthly_alteryx_column, 'sum'),
                                                  delta_churn_monthly_alteryx_total=
                                                  (delta_churn_monthly_alteryx_column, 'sum'),
                                                  flag_cross_sell_monthly_alteryx_total=
                                                  (flag_cross_sell_monthly_alteryx_column, 'sum'),
                                                  delta_cross_sell_monthly_alteryx_total=
                                                  (delta_cross_sell_monthly_alteryx_column, 'sum'), 
                                                  flag_downgrade_monthly_alteryx_total=
                                                  (flag_downgrade_monthly_alteryx_column, 'sum'),
                                                  delta_downgrade_monthly_alteryx_total=
                                                  (delta_downgrade_monthly_alteryx_column, 'sum'),
                                                  flag_upsell_monthly_alteryx_total=
                                                  (flag_upsell_monthly_alteryx_column, 'sum'),
                                                  delta_upsell_monthly_alteryx_total=
                                                  (delta_upsell_monthly_alteryx_column, 'sum'),
                                                  flag_downsell_monthly_alteryx_total=
                                                  (flag_downsell_monthly_alteryx_column, 'sum'),
                                                  delta_downsell_monthly_alteryx_total=
                                                  (delta_downsell_monthly_alteryx_column, 'sum'),
                                                  flag_new_customer_LTM_alteryx_total=
                                                  (flag_new_customer_LTM_alteryx_column, 'sum'),
                                                  delta_new_customer_LTM_alteryx_total=
                                                  (delta_new_customer_LTM_alteryx_column, 'sum'),
                                                  flag_churn_LTM_alteryx_total=
                                                  (flag_churn_LTM_alteryx_column, 'sum'),
                                                  delta_churn_LTM_alteryx_total=
                                                  (delta_churn_LTM_alteryx_column, 'sum'),
                                                  flag_cross_sell_LTM_alteryx_total=
                                                  (flag_cross_sell_LTM_alteryx_column, 'sum'),
                                                  delta_cross_sell_LTM_alteryx_total=
                                                  (delta_cross_sell_LTM_alteryx_column, 'sum'),
                                                  flag_downgrade_LTM_alteryx_total=
                                                  (flag_downgrade_LTM_alteryx_column, 'sum'),
                                                  delta_downgrade_LTM_alteryx_total=
                                                  (delta_downgrade_LTM_alteryx_column, 'sum'),
                                                  flag_upsell_LTM_alteryx_total=
                                                  (flag_upsell_LTM_alteryx_column, 'sum'),
                                                  delta_upsell_LTM_alteryx_total=
                                                  (delta_upsell_LTM_alteryx_column, 'sum'),
                                                  flag_downsell_LTM_alteryx_total=
                                                  (flag_downsell_LTM_alteryx_column, 'sum'),
                                                  delta_downsell_LTM_alteryx_total=
                                                  (delta_downsell_LTM_alteryx_column, 'sum')
                                                  ).reset_index()

customer_sums_python_monthly = customer_data_python_monthly.agg(flag_new_customer_monthly_python_total=
                                                  (flag_new_customer_monthly_python_column, 'sum'),
                                                  delta_new_customer_monthly_python_total=
                                                  (delta_new_customer_monthly_python_column, 'sum'),
                                                  flag_churn_monthly_python_total=
                                                  (flag_churn_monthly_python_column, 'sum'),
                                                  delta_churn_monthly_python_total=
                                                  (delta_churn_monthly_python_column, 'sum'),
                                                  flag_cross_sell_monthly_python_total=
                                                  (flag_cross_sell_monthly_python_column, 'sum'),
                                                  delta_cross_sell_monthly_python_total=
                                                  (delta_cross_sell_monthly_python_column, 'sum'), 
                                                  flag_downgrade_monthly_python_total=
                                                  (flag_downgrade_monthly_python_column, 'sum'),
                                                  delta_downgrade_monthly_python_total=
                                                  (delta_downgrade_monthly_python_column, 'sum'),
                                                  flag_upsell_monthly_python_total=
                                                  (flag_upsell_monthly_python_column, 'sum'),
                                                  delta_upsell_monthly_python_total=
                                                  (delta_upsell_monthly_python_column, 'sum'),
                                                  flag_downsell_monthly_python_total=
                                                  (flag_downsell_monthly_python_column, 'sum'),
                                                  delta_downsell_monthly_python_total=
                                                  (delta_downsell_monthly_python_column, 'sum')
                                                  ).reset_index()

customer_sums_python_LTM = customer_data_python_LTM.agg(flag_new_customer_LTM_python_total=
                                                  (flag_new_customer_LTM_python_column, 'sum'),
                                                  delta_new_customer_LTM_python_total=
                                                  (delta_new_customer_LTM_python_column, 'sum'),
                                                  flag_churn_LTM_python_total=
                                                  (flag_churn_LTM_python_column, 'sum'),
                                                  delta_churn_LTM_python_total=
                                                  (delta_churn_LTM_python_column, 'sum'),
                                                  flag_cross_sell_LTM_python_total=
                                                  (flag_cross_sell_LTM_python_column, 'sum'),
                                                  delta_cross_sell_LTM_python_total=
                                                  (delta_cross_sell_LTM_python_column, 'sum'), 
                                                  flag_downgrade_LTM_python_total=
                                                  (flag_downgrade_LTM_python_column, 'sum'),
                                                  delta_downgrade_LTM_python_total=
                                                  (delta_downgrade_LTM_python_column, 'sum'),
                                                  flag_upsell_LTM_python_total=
                                                  (flag_upsell_LTM_python_column, 'sum'),
                                                  delta_upsell_LTM_python_total=
                                                  (delta_upsell_LTM_python_column, 'sum'),
                                                  flag_downsell_LTM_python_total=
                                                  (flag_downsell_LTM_python_column, 'sum'),
                                                  delta_downsell_LTM_python_total=
                                                  (delta_downsell_LTM_python_column, 'sum')
                                                  ).reset_index()


# join sums of flags and deltas onto the same dataset
customer_sums = pd.merge(customer_sums_alteryx, customer_sums_python_monthly, how='left', on='Customer_ID')
customer_sums = pd.merge(customer_sums, customer_sums_python_LTM, how='left', on='Customer_ID')

# generate columns to check whether monthly sums match
customer_sums['flag_new_customer_monthly_match'] = np.where(
        (customer_sums['flag_new_customer_monthly_python_total'] == customer_sums['flag_new_customer_monthly_alteryx_total']), 1, 0)
customer_sums['delta_new_customer_monthly_match'] = np.where(
        (customer_sums['delta_new_customer_monthly_python_total'] == customer_sums['delta_new_customer_monthly_alteryx_total']), 1, 0)

customer_sums['flag_churn_monthly_match'] = np.where(
        (customer_sums['flag_churn_monthly_python_total'] == customer_sums['flag_churn_monthly_alteryx_total']), 1, 0)
customer_sums['delta_churn_monthly_match'] = np.where(
        (customer_sums['delta_churn_monthly_python_total'] == customer_sums['delta_churn_monthly_alteryx_total']), 1, 0)

customer_sums['flag_cross_sell_monthly_match'] = np.where(
        (customer_sums['flag_cross_sell_monthly_python_total'] == customer_sums['flag_cross_sell_monthly_alteryx_total']), 1, 0)
customer_sums['delta_cross_sell_monthly_match'] = np.where(
        (customer_sums['delta_cross_sell_monthly_python_total'] == customer_sums['delta_cross_sell_monthly_alteryx_total']), 1, 0)

customer_sums['flag_downgrade_monthly_match'] = np.where(
        (customer_sums['flag_downgrade_monthly_python_total'] == customer_sums['flag_downgrade_monthly_alteryx_total']), 1, 0)
customer_sums['delta_downgrade_monthly_match'] = np.where(
        (customer_sums['delta_downgrade_monthly_python_total'] == customer_sums['delta_downgrade_monthly_alteryx_total']), 1, 0)

customer_sums['flag_upsell_monthly_match'] = np.where(
        (customer_sums['flag_upsell_monthly_python_total'] == customer_sums['flag_upsell_monthly_alteryx_total']), 1, 0)
customer_sums['delta_upsell_monthly_match'] = np.where(
        (customer_sums['delta_upsell_monthly_python_total'] == customer_sums['delta_upsell_monthly_alteryx_total']), 1, 0)

customer_sums['flag_downsell_monthly_match'] = np.where(
        (customer_sums['flag_downsell_monthly_python_total'] == customer_sums['flag_downsell_monthly_alteryx_total']), 1, 0)
customer_sums['delta_downsell_monthly_match'] = np.where(
        (customer_sums['delta_downsell_monthly_python_total'] == customer_sums['delta_downsell_monthly_alteryx_total']), 1, 0)

# generate columns to check whether LTM sums match
customer_sums['flag_new_customer_LTM_match'] = np.where(
        (customer_sums['flag_new_customer_LTM_python_total'] == customer_sums['flag_new_customer_LTM_alteryx_total']), 1, 0)
customer_sums['delta_new_customer_LTM_match'] = np.where(
        (customer_sums['delta_new_customer_LTM_python_total'] == customer_sums['delta_new_customer_LTM_alteryx_total']), 1, 0)

customer_sums['flag_churn_LTM_match'] = np.where(
        (customer_sums['flag_churn_LTM_python_total'] == customer_sums['flag_churn_LTM_alteryx_total']), 1, 0)
customer_sums['delta_churn_LTM_match'] = np.where(
        (customer_sums['delta_churn_LTM_python_total'] == customer_sums['delta_churn_LTM_alteryx_total']), 1, 0)

customer_sums['flag_cross_sell_LTM_match'] = np.where(
        (customer_sums['flag_cross_sell_LTM_python_total'] == customer_sums['flag_cross_sell_LTM_alteryx_total']), 1, 0)
customer_sums['delta_cross_sell_LTM_match'] = np.where(
        (customer_sums['delta_cross_sell_LTM_python_total'] == customer_sums['delta_cross_sell_LTM_alteryx_total']), 1, 0)

customer_sums['flag_downgrade_LTM_match'] = np.where(
        (customer_sums['flag_downgrade_LTM_python_total'] == customer_sums['flag_downgrade_LTM_alteryx_total']), 1, 0)
customer_sums['delta_downgrade_LTM_match'] = np.where(
        (customer_sums['delta_downgrade_LTM_python_total'] == customer_sums['delta_downgrade_LTM_alteryx_total']), 1, 0)

customer_sums['flag_upsell_LTM_match'] = np.where(
        (customer_sums['flag_upsell_LTM_python_total'] == customer_sums['flag_upsell_LTM_alteryx_total']), 1, 0)
customer_sums['delta_upsell_LTM_match'] = np.where(
        (customer_sums['delta_upsell_LTM_python_total'] == customer_sums['delta_upsell_LTM_alteryx_total']), 1, 0)

customer_sums['flag_downsell_LTM_match'] = np.where(
        (customer_sums['flag_downsell_LTM_python_total'] == customer_sums['flag_downsell_LTM_alteryx_total']), 1, 0)
customer_sums['delta_downsell_LTM_match'] = np.where(
        (customer_sums['delta_downsell_LTM_python_total'] == customer_sums['delta_downsell_LTM_alteryx_total']), 1, 0)

# generate timestamp to save files
timestamp = str(dt.datetime.now().strftime("%Y%m%d_%H-%M-%S"))

# save comparison data as a .csv file in output datasets with a timestamp
customer_sums.to_csv('.\output datasets\comparison_data.csv')

# # initiate empty columns for sums of monthly flags
# comparison_data[flag_new_customer_monthly_python_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_new_customer_monthly_alteryx_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_churn_monthly_python_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_churn_monthly_alteryx_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_cross_sell_monthly_python_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_cross_sell_monthly_alteryx_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_downgrade_monthly_python_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_downgrade_monthly_alteryx_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_upsell_monthly_python_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_upsell_monthly_alteryx_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_downsell_monthly_python_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_downsell_monthly_alteryx_column + 'Total'] = np.zeros(num_of_customers)
#
# # initiate empty columns for sums of monthly deltas
# comparison_data[delta_new_customer_monthly_python_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[delta_new_customer_monthly_alteryx_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[delta_churn_monthly_python_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[delta_churn_monthly_alteryx_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[delta_cross_sell_monthly_python_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[delta_cross_sell_monthly_alteryx_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[delta_downgrade_monthly_python_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[delta_downgrade_monthly_alteryx_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[delta_upsell_monthly_python_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[delta_upsell_monthly_alteryx_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[delta_downsell_monthly_python_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[delta_downsell_monthly_alteryx_column + 'Total'] = np.zeros(num_of_customers)
#
# # initiate empty columns for sums of 12-monthly flags
# comparison_data[flag_new_customer_LTM_python_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_new_customer_LTM_alteryx_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_churn_LTM_python_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_churn_LTM_alteryx_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_cross_sell_LTM_python_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_cross_sell_LTM_alteryx_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_downgrade_LTM_python_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_downgrade_LTM_alteryx_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_upsell_LTM_python_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_upsell_LTM_alteryx_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_downsell_LTM_python_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_downsell_LTM_alteryx_column + 'Total'] = np.zeros(num_of_customers)
#
# # initiate empty columns for sums of 12-monthly deltas
# comparison_data[flag_new_customer_LTM_python_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_new_customer_LTM_alteryx_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_churn_LTM_python_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_churn_LTM_alteryx_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_cross_sell_LTM_python_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_cross_sell_LTM_alteryx_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_downgrade_LTM_python_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_downgrade_LTM_alteryx_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_upsell_LTM_python_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_upsell_LTM_alteryx_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_downsell_LTM_python_column + 'Total'] = np.zeros(num_of_customers)
# comparison_data[flag_downsell_LTM_alteryx_column + 'Total'] = np.zeros(num_of_customers)
#
# for row_index, customer in customers.iterrows():
#     customer[flag_new_customer_monthly_python_column + 'Total'] = df_python_output_monthly.loc[]
#
# # check the sums of monthly flags and deltas in the Python output match that of the example output
# # check number of monthly new customer/churn flags match
# total_flags_new_customer_example = sum(df_example_output[flag_new_customer_monthly_alteryx_column])
# total_flags_new_customer_python = sum(df_python_output_monthly[flag_new_customer_monthly_python_column])
# total_flags_churn_example = sum(df_example_output[flag_churn_monthly_alteryx_column])
# total_flags_churn_python = sum(df_python_output_monthly[flag_churn_monthly_python_column])
#
# print('Number of monthly new customer flags match: ' + str(total_flags_new_customer_example == total_flags_new_customer_python))
# print('Number of monthly churn flags match: ' + str(total_flags_churn_example == total_flags_churn_python))
#
# # check total deltas of monthly new customer/churn flags match
# total_deltas_new_customer_example = sum(df_example_output[delta_new_customer_monthly_column])
# total_deltas_new_customer_python = sum(df_python_output_monthly[delta_new_customer_monthly_column])
# total_deltas_churn_example = sum(df_example_output[delta_churn_monthly_column])
# total_deltas_churn_python = sum(df_python_output_monthly[delta_churn_monthly_column])
#
# print('Sum of monthly new customer deltas match: ' + str(total_deltas_new_customer_example == total_deltas_new_customer_python))
# print('Sum of monthly churn deltas match: ' + str(total_deltas_churn_example == total_deltas_churn_python))
#
#
# # check number of monthly cross-sell/downgrade flags match
# total_flags_cross_sell_example = sum(df_example_output[flag_cross_sell_monthly_alteryx_column])
# total_flags_cross_sell_python = sum(df_python_output_monthly[flag_cross_sell_monthly_python_column])
# total_flags_downgrade_example = sum(df_example_output[flag_downgrade_monthly_alteryx_column])
# total_flags_downgrade_python = sum(df_python_output_monthly[flag_downgrade_monthly_python_column])
#
# print('Number of monthly cross-sell flags match: ' + str(total_flags_cross_sell_example == total_flags_cross_sell_python))
# print('Number of monthly downgrade flags match: ' + str(total_flags_downgrade_example == total_flags_downgrade_python))
#
# # # check total deltas of monthly cross-sell/downgrade match
# # total_deltas_cross_sell_example = sum(df_example_output[delta_cross_sell_monthly_column])
# # total_deltas_cross_sell_python = sum(df_python_output_monthly[delta_cross_sell_monthly_column])
# # total_deltas_downgrade_example = sum(df_example_output[delta_downgrade_monthly_column])
# # total_deltas_downgrade_python = sum(df_python_output_monthly[delta_downgrade_monthly_column])
# #
# # print('Sum of monthly cross-sell deltas match: ' + str(total_deltas_cross_sell_example == total_deltas_cross_sell_python))
# # print('Sum of monthly downgrade deltas match: ' + str(total_deltas_downgrade_example == total_deltas_downgrade_python))
#
# # check number of monthly upsell/downsell flags match
# total_flags_upsell_example = sum(df_example_output[flag_upsell_monthly_alteryx_column])
# total_flags_upsell_python = sum(df_python_output_monthly[flag_upsell_monthly_python_column])
# total_flags_downsell_example = sum(df_example_output[flag_downsell_monthly_alteryx_column])
# total_flags_downsell_python = sum(df_python_output_monthly[flag_downsell_monthly_python_column])
#
# print('Number of monthly upsell flags match: ' + str(total_flags_upsell_example == total_flags_upsell_python))
# print('Number of monthly downsell flags match: ' + str(total_flags_downsell_example == total_flags_downsell_python))
#
# # # check total deltas of monthly upsell/downsell match
# # total_deltas_upsell_example = sum(df_example_output[delta_upsell_monthly_column])
# # total_deltas_upsell_python = sum(df_python_output_monthly[delta_upsell_monthly_column])
# # total_deltas_downsell_example = sum(df_example_output[delta_downsell_monthly_column])
# # total_deltas_downsell_python = sum(df_python_output_monthly[delta_downsell_monthly_column])
# #
# # print('Sum of monthly upsell deltas match: ' + str(total_deltas_upsell_example == total_deltas_upsell_python))
# # print('Sum of monthly downsell deltas match: ' + str(total_deltas_downsell_example == total_deltas_downsell_python))
#
#
# # do the same for LTM flags and deltas
# # check number of LTM new customer/churn flags match
# total_flags_new_customer_example = sum(df_example_output[flag_new_customer_LTM_alteryx_column])
# total_flags_new_customer_python = sum(df_python_output_LTM[flag_new_customer_LTM_python_column])
# total_flags_churn_example = sum(df_example_output[flag_churn_LTM_alteryx_column])
# total_flags_churn_python = sum(df_python_output_LTM[flag_churn_LTM_python_column])
#
# print('Number of LTM new customer flags match: ' + str(total_flags_new_customer_example == total_flags_new_customer_python))
# print('Number of LTM churn flags match: ' + str(total_flags_churn_example == total_flags_churn_python))
#
# # # check total deltas of LTM new customer/churn flags match
# # total_deltas_new_customer_example = sum(df_example_output[delta_new_customer_LTM_column])
# # total_deltas_new_customer_python = sum(df_python_output_LTM[delta_new_customer_LTM_column])
# # total_deltas_churn_example = sum(df_example_output[delta_churn_LTM_column])
# # total_deltas_churn_python = sum(df_python_output_LTM[delta_churn_LTM_column])
# #
# # print('Sum of LTM new customer deltas match: ' + str(total_deltas_new_customer_example == total_deltas_new_customer_python))
# # print('Sum of LTM churn deltas match: ' + str(total_deltas_churn_example == total_deltas_churn_python))
#
#
# # check number of LTM cross-sell/downgrade flags match
# total_flags_cross_sell_example = sum(df_example_output[flag_cross_sell_LTM_alteryx_column])
# total_flags_cross_sell_python = sum(df_python_output_LTM[flag_cross_sell_LTM_python_column])
# total_flags_downgrade_example = sum(df_example_output[flag_downgrade_LTM_alteryx_column])
# total_flags_downgrade_python = sum(df_python_output_LTM[flag_downgrade_LTM_python_column])
#
# print('Number of LTM cross-sell flags match: ' + str(total_flags_cross_sell_example == total_flags_cross_sell_python))
# print('Number of LTM downgrade flags match: ' + str(total_flags_downgrade_example == total_flags_downgrade_python))
#
# # # check total deltas of LTM cross-sell/downgrade match
# # total_deltas_cross_sell_example = sum(df_example_output[delta_cross_sell_LTM_column])
# # total_deltas_cross_sell_python = sum(df_python_output_LTM[delta_cross_sell_LTM_column])
# # total_deltas_downgrade_example = sum(df_example_output[delta_downgrade_LTM_column])
# # total_deltas_downgrade_python = sum(df_python_output_LTM[delta_downgrade_LTM_column])
# #
# # print('Sum of LTM cross-sell deltas match: ' + str(total_deltas_cross_sell_example == total_deltas_cross_sell_python))
# # print('Sum of LTM downgrade deltas match: ' + str(total_deltas_downgrade_example == total_deltas_downgrade_python))
#
# # check number of LTM upsell/downsell flags match
# total_flags_upsell_example = sum(df_example_output[flag_upsell_LTM_alteryx_column])
# total_flags_upsell_python = sum(df_python_output_LTM[flag_upsell_LTM_python_column])
# total_flags_downsell_example = sum(df_example_output[flag_downsell_LTM_alteryx_column])
# total_flags_downsell_python = sum(df_python_output_LTM[flag_downsell_LTM_python_column])
#
# print('Number of LTM upsell flags match: ' + str(total_flags_upsell_example == total_flags_upsell_python))
# print('Number of LTM downsell flags match: ' + str(total_flags_downsell_example == total_flags_downsell_python))
#
# # # check total deltas of LTM upsell/downsell match
# # total_deltas_upsell_example = sum(df_example_output[delta_upsell_LTM_column])
# # total_deltas_upsell_python = sum(df_python_output_LTM[delta_upsell_LTM_column])
# # total_deltas_downsell_example = sum(df_example_output[delta_downsell_LTM_column])
# # total_deltas_downsell_python = sum(df_python_output_LTM[delta_downsell_LTM_column])
# #
# # print('Sum of LTM upsell deltas match: ' + str(total_deltas_upsell_example == total_deltas_upsell_python))
# # print('Sum of LTM downsell deltas match: ' + str(total_deltas_downsell_example == total_deltas_downsell_python))
#
# print('Tests completed')
#
# # Example output strange results:
# #
# # customer 2076, product paye logiciel
# #
# # multiple downgrade records within single month
# #
# # churn record after downgrade record