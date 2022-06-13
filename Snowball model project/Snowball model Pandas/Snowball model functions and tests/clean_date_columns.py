import pandas as pd

def clean_date_columns(revenue_data, date_columns=['Month']):
    """
            Returns revenue data with timestamps in revenue_data[date_column] set as datetime objects and normalised to
            have day=1.
                Args:
                    revenue_data (pandas.DataFrame): list of payments.
                    date_columns (str): title for column with payment dates.
                Returns:
                    revenue_data_clean (pandas.DataFrame): list of payments.
            """
    for date_column in date_columns:
        # cast entries in date_column from string to datetime in case they are not already
        revenue_data.loc[:, date_column] = pd.to_datetime(revenue_data[date_column])

        # make all payments set for beginning of month - it feels a little unnatural to
        # include this here rather than the data cleaning phase
        revenue_data.loc[:, date_column] = revenue_data[date_column].dt.to_period('M').dt.to_timestamp()

    revenue_data_clean = revenue_data

    return revenue_data_clean