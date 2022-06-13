import warnings
import pandas as pd

def combine_primary_key_duplicates(revenue_data, primary_key_columns=['Customer_ID','Product_ID', 'Country', 'Region'],
                                   date_column='Month', arr_column='ARR'):
    """
                Returns revenue data with each payment uniquely identified by primary_key value and month - any multiple
                                                        payments with identical primary_key values in the same month
                                                        from revenue_data have been combined into single
                                                        payment records in revenue_data_combined.
                Args:
                    revenue_data (pandas.DataFrame): list of recurring payments (with IsRecurring = 1).
                    primary_key_columns (list of strings): titles of columns used to define primary key.
                    date_column (str): title of date column in data.

                Returns:
                    revenue_data_combined (pandas.DataFrame.groupby): list of recurring payments (with 'IsRecurring' = 1)
                                                              uniquely identified by primary_key and payment month.
                """

    # create list with primary key columns and date column
    primary_key_date_columns = primary_key_columns.copy()
    primary_key_date_columns.append(date_column)

    # group revenue data by primary key and month
    primary_key_month_data = revenue_data.groupby(by=primary_key_date_columns)

    # check if there are any groups with multiple records, and combine any duplicates if so
    if len(primary_key_month_data) != revenue_data.shape[0]:
        # note that by using .agg({'ARR': 'sum'}) we keep only columns in primary_key_date_columns and arr_column
        # could we do this without losing intermediate columns such as Customer_name and IsRecurring?
        revenue_data_without_duplicates = primary_key_month_data.agg({arr_column: 'sum'})
        revenue_data_without_duplicates = revenue_data_without_duplicates.reset_index()
    else:
        revenue_data_without_duplicates = revenue_data
        pass

    return revenue_data_without_duplicates