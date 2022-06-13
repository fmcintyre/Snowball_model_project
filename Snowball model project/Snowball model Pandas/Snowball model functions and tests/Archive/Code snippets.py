# checking whether for each customer the set of product ids appearing in each month is constant
revenue_customer_data = revenue_data.groupby(by = 'Customer_ID')
for customer_index, customer in revenue_customer_data:

    revenue_data_monthly = revenue_data.loc[revenue_data['Customer_ID'] == int(customer_index)].groupby(by = 'Month')
    i = 0
    for month_index, month_content in revenue_data_monthly:
        current_month_ids = set(month_content['Product_ID'])
        if i == 0:
            pass
        else:
            flag = current_month_ids == last_month_ids
            if flag == False:
                print('So far we have added '+str(rows_added) +' rows.')
                print('Current month ids are:')
                print(current_month_ids)
                print('last month ids are:')
                print(last_month_ids)
                print(month)
                return revenue_data
        last_month_ids = current_month_ids
        i+=1


# snippet from old combine_primary_key_duplicates design
# # get list of group sizes, each group being payments for a given month with the same primary_key value
primary_key_month_group_sizes = primary_key_month_data.size()

# define subset of sizes for groups with more than one payment
primary_key_month_duplicates = primary_key_month_group_sizes[primary_key_month_group_sizes > 1]

# check whether this subset is empty or not, which will determine whether there are any two payments with the same
# primary_key value in a single month
if len(primary_key_month_duplicates) > 0:

    # if there exist two payments with the same primary_key value in a single month, produce a warning and give the
    # option to solve the issue by summing ARRs across a month and primary key basis.
    warnings.warn('In order to proceed, payments need to be uniquely identified by the primary key columns and '
                  'payment month.')
    drop_duplicates = input('Do you want to sum ARRs for duplicates, and only keep non-ARR data from first row of '
                            'records with common entries for primary key columns and payment month? Please answer '
                            'with Y on N.')
    if drop_duplicates == 'Y':

        # check whether there is more than one payment for each primary_key/month combination
        for primary_key_month_index, primary_key_month_payments in primary_key_month_data:
            # if there is month than one payment, keep only one of the records and but set the ARR value equal to
            # the sum of ARRs for that primary_key/month combination
            if primary_key_month_payments.shape[0] > 1:

                arr_sum = primary_key_month_payments[arr_column].sum(axis=0, skipna=True)

                # keep first row and drop remaining rows for this particular primary key/month combination
                index_to_keep = primary_key_month_payments.index[0]
                indexes_to_drop = primary_key_month_payments.index[1:]
                revenue_data = revenue_data.drop(index=indexes_to_drop)

                # update ARR as sum of ARRs for that primary key/month combination
                revenue_data.loc[index_to_keep, arr_column] = arr_sum

    # this function is designed only to return revenue data with no duplicates
    else:
        raise ValueError('There are multiple payments in a single month with the same primary key. Payments should '
                         'be uniquely identified by primary key and payment month.')

# saving both Python and Alteryx example output as well as input datasets with n-monthly flags and deltas
# how do you choose to save to a specific directory? I want to save this in output datasets
revenue_data_with_n_monthly_flags_and_deltas.to_csv('Snowball output ' + str(period) +'-monthly Python for first ' + str(n) + ' customers '
                                                    + timestamp + '.csv')
example_output_data.to_csv('Snowball output ' + str(period) +'-monthly Alteryx for first ' + str(n) + ' customers ' + timestamp + '.csv')

example_input_data.to_csv('Snowball input ' + str(period) + '-monthly for customer ' + str(2076) + ' ' + timestamp +
                          '.csv')
example_output_data.to_csv('Snowball output ' + str(period) + '-monthly Alteryx for customer ' + str(2076) + ' ' +
                           timestamp + '.csv')