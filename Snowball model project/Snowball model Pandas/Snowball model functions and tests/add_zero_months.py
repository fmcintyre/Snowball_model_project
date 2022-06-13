import pandas as pd
import datetime as dt

def add_zero_months(revenue_data, date_column='Month', 
                    primary_key_columns=
                    ['Customer_ID', 'Customer_name', 'Product_family', 'Product_sub-family', 'Product_ID', 'Country', 
                     'Region'], start_date=None, end_date=None, arr_column='ARR'):
    """
    Returns revenue data including payments of zero whenever a customer is not
    paying for a product which they will be or have been paying for at some point
    within the fixed time period.
    Args:
        revenue_data (pandas.DataFrame): list of recurring payments (with IsRecurring = 1). Timestamps in
                                         revenue_data[date_column] are all datetime objects and normalised to have
                                         day=1. Includes columns extra datetime columns with start_date_primary_key,
                                         end_date_primary_key, start_date_customer, end_date_customer, which similarly
                                         are normalised to have day=1 for all timestamps.
        date_column (str): title for column with payment dates.
        primary_key_columns (list of strings): titles of columns used to define primary key.
    Returns:
        revenue_data_with_zeros (pandas.DataFrame): list of payments in including any
                                                    lack of payment for every customer/
                                                    product combination as a payment of zero.
                                                    Essentially any customer/product combination appearing
                                                    in the list should have a payment for every month in
                                                    the revenue data period.
    """
    # if not given, set start-of-period and end-of-period dates using first and last payment in the revenue data
    if start_date is None:
        start_date = revenue_data[date_column].min()
    if end_date is None:
        end_date = revenue_data[date_column].max()

    # create list of months between start_data and end_date
    all_months = set(pd.date_range(start_date, end_date, freq='MS'))

    # record overall number of payments for adding rows using .loc and index labels
    record_index_max = revenue_data.index.max()

    # group data by primary key
    primary_key_data = revenue_data.groupby(by=primary_key_columns)

    # for each primary key calculate months which do not appear in the
    # dataset and include payments of zero to remedy this.
    for primary_key_index, primary_key_payments in primary_key_data:
        # create a set of all months for which there is a payment (could be zero) with that primary key
        payment_months = set(primary_key_payments[date_column])
        # create list of all months for which there is no payment with that primary key
        zero_payment_months = list(all_months - payment_months)
        if len(zero_payment_months) == 0:
            pass
        else:
            # add a NaN rows for every month in zero_payment_month
            record_index_set = list(revenue_data.index)
            new_indexes = list(range(record_index_max + 1, (record_index_max + 1) + (len(zero_payment_months))))
            record_index_set.extend(new_indexes)
            revenue_data = revenue_data.reindex(index=record_index_set)

            # input payment dates for zero payment months
            revenue_data.loc[new_indexes, date_column] = zero_payment_months

            # input remaining important column data for zero column months
            # primary key column titles should be stored in primary_key_index in the same order as
            # primary_key_columns
            for i in range(len(primary_key_columns)):
                revenue_data.loc[new_indexes, primary_key_columns[i]] = primary_key_index[i]

            # revenue_data.loc[new_indexes, start_date_customer_product_column] = list(customer_product_payments[
            #                                                                     start_date_customer_product_column])[0]
            # revenue_data.loc[new_indexes, end_date_customer_product_column] = list(customer_product_payments[
            #                                                                   end_date_customer_product_column])[0]
            # revenue_data.loc[new_indexes, start_date_customer_column] = list(customer_product_payments[
            #                                                                  start_date_customer_column])[0]
            # revenue_data.loc[new_indexes, end_date_customer_column] = list(customer_product_payments[
            #                                                                end_date_customer_column])[0]
            revenue_data.loc[new_indexes, 'IsRecurring'] = int(1)

            # set ARR to zero for new zero payment months
            revenue_data.loc[new_indexes, arr_column] = 0

            record_index_max = revenue_data.index.max()

    revenue_data_with_zeros = revenue_data

    return revenue_data_with_zeros
