import numpy as np
from pandas.tseries.offsets import DateOffset
import pandas as pd

def add_n_monthly_flags(revenue_data_with_n_monthly_dates_and_deltas, date_column='Month', period=12,                        start_date_customer_product_column='start_date_customer_product',
                        end_date_customer_product_column='end_date_customer_product',
                        start_date_customer_column='start_date_customer',
                        end_date_customer_column='end_date_customer',
                        delta_n_monthly_column=str(12) + '_monthly_ARR_delta_primary_key',
                        flag_new_customer_n_monthly_column=str(12) + '_Monthly_Flag_New_Customer',
                        flag_churn_n_monthly_column=str(12) + '_Monthly_Flag_Churn',
                        flag_cross_sell_n_monthly_column=str(12) + '_Monthly_Flag_Cross_Sell',
                        flag_downgrade_n_monthly_column=str(12) + '_Monthly_Flag_Downgrade',
                        flag_upsell_n_monthly_column=str(12) + '_Monthly_Flag_Upsell',
                        flag_downsell_n_monthly_column=str(12) + '_Monthly_Flag_Downsell'):
    """
            Returns revenue data with new columns for n-monthly deltas, and payment start/end dates (by grouping payments
            with respect to the primary_key column.)
            Args:
                revenue_data_with_n_monthly_dates_and_deltas (pandas.DataFrame): list of payments (with IsRecurring = 1)

            Returns:
                revenue_data_with_dates_and_deltas (pandas.DataFrame): list of positive payments unique identified by primary_key
                                                            and payment month - any multiple payments
                                                            with identical primary_key values in the same month from
                                                            revenue_data_with_zeros have been combined into single
                                                            payment records in revenue_data_with_dates_and_deltas.
            """

    # Flag new customer if payment month is within n-months after first customer payment month and not after last
    # customer payment month
    revenue_data_with_n_monthly_dates_and_deltas[flag_new_customer_n_monthly_column] = np.where(
        (revenue_data_with_n_monthly_dates_and_deltas[delta_n_monthly_column] != 0) &
        ((revenue_data_with_n_monthly_dates_and_deltas[start_date_customer_column] <=
          revenue_data_with_n_monthly_dates_and_deltas[date_column]) &
         (revenue_data_with_n_monthly_dates_and_deltas[date_column] <
          revenue_data_with_n_monthly_dates_and_deltas[start_date_customer_column] + DateOffset(months=period)) &
         (revenue_data_with_n_monthly_dates_and_deltas[date_column] <=
          revenue_data_with_n_monthly_dates_and_deltas[end_date_customer_column])), 1, 0)

    # Flag churn if payment month is within n-months after month after last customer payment month
    revenue_data_with_n_monthly_dates_and_deltas[flag_churn_n_monthly_column] = np.where(
        (revenue_data_with_n_monthly_dates_and_deltas[delta_n_monthly_column] != 0) &
        ((revenue_data_with_n_monthly_dates_and_deltas[date_column] >
          revenue_data_with_n_monthly_dates_and_deltas[end_date_customer_column]) &
         (revenue_data_with_n_monthly_dates_and_deltas[date_column] <
          revenue_data_with_n_monthly_dates_and_deltas[end_date_customer_column] + DateOffset(months=period+1))), 1, 0)

    # Flag cross-sell if payment month is within n-months after first customer/product payment month, not after last
    # customer/product payment month and not already flagged as new customer
    revenue_data_with_n_monthly_dates_and_deltas[flag_cross_sell_n_monthly_column] = np.where(
        (revenue_data_with_n_monthly_dates_and_deltas[delta_n_monthly_column] != 0) &
        ((revenue_data_with_n_monthly_dates_and_deltas[date_column] >=
          revenue_data_with_n_monthly_dates_and_deltas[start_date_customer_product_column]) &
         (revenue_data_with_n_monthly_dates_and_deltas[date_column] <
          revenue_data_with_n_monthly_dates_and_deltas[start_date_customer_product_column] + pd.DateOffset(months=
                                                                                                           period))
         & (revenue_data_with_n_monthly_dates_and_deltas[date_column] <= revenue_data_with_n_monthly_dates_and_deltas[
                    end_date_customer_product_column]) &
         (revenue_data_with_n_monthly_dates_and_deltas[flag_new_customer_n_monthly_column] == 0)), 1, 0)

    # Flag downgrade if payment month is within n-months after month after last customer/product payment month and is
    # not already flagged as churn
    revenue_data_with_n_monthly_dates_and_deltas[flag_downgrade_n_monthly_column] = np.where(
        (revenue_data_with_n_monthly_dates_and_deltas[delta_n_monthly_column] != 0) &
        ((revenue_data_with_n_monthly_dates_and_deltas[date_column] >
          revenue_data_with_n_monthly_dates_and_deltas[end_date_customer_product_column]) &
         (revenue_data_with_n_monthly_dates_and_deltas[date_column] <
          revenue_data_with_n_monthly_dates_and_deltas[end_date_customer_product_column] + DateOffset(months=period+1))&
         (revenue_data_with_n_monthly_dates_and_deltas[flag_churn_n_monthly_column] == 0)), 1, 0)

    # Flag upsell if payment has a positive n-monthly delta but is not already flagged as new customer or cross-sell
    revenue_data_with_n_monthly_dates_and_deltas[flag_upsell_n_monthly_column] = np.where(
        ((revenue_data_with_n_monthly_dates_and_deltas[delta_n_monthly_column] > 0) &
         (revenue_data_with_n_monthly_dates_and_deltas[flag_new_customer_n_monthly_column] == 0) &
         (revenue_data_with_n_monthly_dates_and_deltas[flag_cross_sell_n_monthly_column] == 0)), 1, 0)

    # Flag downsell if payment has a negative n-monthly delta but is not already flagged as churn or downgrade
    revenue_data_with_n_monthly_dates_and_deltas[flag_downsell_n_monthly_column] = np.where(
        ((revenue_data_with_n_monthly_dates_and_deltas[delta_n_monthly_column] < 0) &
         (revenue_data_with_n_monthly_dates_and_deltas[flag_churn_n_monthly_column] == 0) &
         (revenue_data_with_n_monthly_dates_and_deltas[flag_downgrade_n_monthly_column] == 0)), 1, 0)

    revenue_data_with_n_monthly_flags_and_deltas = revenue_data_with_n_monthly_dates_and_deltas
    return revenue_data_with_n_monthly_flags_and_deltas