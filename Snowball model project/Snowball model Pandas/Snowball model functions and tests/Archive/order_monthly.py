import pandas as pd

def order_monthly(data, date_column='Month'):
    """
    Orders dataset chronologically with respect to date strings appearing
    in the date_column column.
    Args:
        data (pandas.DataFrame): list of records including some kind of date column.
        date_column (str): title of date column in data.

    Returns:
        ordered_data (pandas.DataFrame): list of records ordered with respect
                                                             to date column.

    """
    # cast date column entries from string to datetime in case they are not already
    data.loc[:, date_column] = pd.to_datetime(data[date_column])

    # order data chronologically
    ordered_data = data.sort_values(by=date_column)

    return ordered_data