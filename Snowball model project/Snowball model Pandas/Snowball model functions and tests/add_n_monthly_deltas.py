from combine_primary_key_duplicates import combine_primary_key_duplicates
from add_zero_months import add_zero_months
from dateutil.relativedelta import relativedelta
import pandas as pd
def add_n_monthly_deltas(revenue_data, arr_column = 'ARR', date_column='Month', period=12,
                         primary_key_columns=['Customer_ID', 'Customer_name', 'Product_family', 'Product_sub-family',
                                              'Product_ID', 'Country', 'Region'],
                         delta_n_monthly_column=str(12)+'_monthly_ARR_delta_customer_product'):
    """
    Returns revenue data with new columns for n-monthly deltas, and payment start/end dates (by grouping payments
    with respect to the primary_key column.)
        Args:
            revenue_data (pandas.DataFrame): list of payments (with IsRecurring = 1).
            date_column (str): title for column with payment dates.
            primary_key_columns (list of strings): titles of columns used to define primary key.

        Returns:
            revenue_data_with_dates_and_deltas (pandas.DataFrame): list of the positive payments from revenue_data_
                                                                   with_zeros with extra columns start
                                                                   and end dates with respect to customer_identifier,
                                                                   start and end dates with respect to primary_key, and
                                                                   n-monthly deltas with respect to primary_key.
    """
    # order data by month so that we can calculate deltas later using pandas .diff(periods=n)
    revenue_data = revenue_data.sort_values(by=date_column)

    # group by primary key
    primary_key_data = revenue_data.groupby(by=primary_key_columns)

    # define new column for n-monthly deltas and calculate using .diff(periods=n)
    revenue_data[delta_n_monthly_column] = primary_key_data[arr_column].diff(periods=period).fillna(0)

    revenue_data_with_n_monthly_deltas = revenue_data

    return revenue_data_with_n_monthly_deltas
