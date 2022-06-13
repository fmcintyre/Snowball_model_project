import pandas as pd


def add_start_end_dates(revenue_data, arr_column='ARR', customer_identifier_columns=['Customer_ID', 'Customer_name'],
                        product_identifier_columns=['Product_family', 'Product_sub-family', 'Product_ID'],
                        date_column='Month', start_date_customer_product_column='start_date_customer_product',
                        end_date_customer_product_column='end_date_customer_product',
                        start_date_customer_column='start_date_customer',
                        end_date_customer_column='end_date_customer'
                        ):
    """
        Returns revenue data with new columns for payment start/end dates (by grouping payments
        with respect to the customer and customer/product combinations.)
            Args:
                revenue_data (pandas.DataFrame): list of payments (with IsRecurring = 1).
                date_column (str): title for column with payment dates.
                customer_identifier_columns (str): titles for columns which uniquely identify customers.
                product_identifier_columns (str): titles for columns which uniquely identify products.
                start_date_customer_product_column (str): title for column with first positive payment date for
                                                          customer/product combinations
                end_date_customer_product_column (str): title for column with last positive payment date for
                                                          customer/product combinations
                start_date_customer_column (str): title for column with first positive payment date for
                                                          customer
                end_date_customer_column (str): title for column with last positive payment date for
                                                          customer
            Returns:
                revenue_data_with_dates (pandas.DataFrame): list of the positive payments from revenue_data_
                                                                       with_zeros with extra columns start
                                                                       and end dates with respect to customer and
                                                                       customer/product combinations.
        """

    # create list of customer/product identifiers
    customer_product_identifier_columns = customer_identifier_columns + product_identifier_columns

    # group non-zero payments by customer/product combination
    customer_product_data = revenue_data.loc[revenue_data[arr_column] != 0].groupby(by=
                                                                                    customer_product_identifier_columns)
    # aggregate start and end dates for each customer/product combination, then reset_index in order to join back onto
    # revenue_data
    customer_product_dates = customer_product_data.agg(start_date_customer_product=(date_column, 'min'),
                                                       end_date_customer_product=(date_column, 'max')).reset_index()

    # group by customer in order to further aggregate start and end dates for each customer
    customer_data = customer_product_dates.groupby(by=customer_identifier_columns)

    # aggregate start and end dates for each customer, then reset_index in order to join back onto
    # customer_product_dates before finally joining back onto revenue_data
    customer_dates = customer_data.agg(start_date_customer=(start_date_customer_product_column, 'min'),
                                       end_date_customer=(end_date_customer_product_column, 'max')).reset_index()

    # join customer start/end dates onto dataset with customer/product start/end dates
    all_dates = pd.merge(customer_product_dates, customer_dates, how='left', on=customer_identifier_columns)

    # join all start/end dates onto revenue_data
    revenue_data_with_dates = pd.merge(revenue_data, all_dates, how='left', on=customer_product_identifier_columns)

    return revenue_data_with_dates