from add_zero_months import add_zero_months
import pandas as pd
from dateutil.relativedelta import relativedelta

revenue_data = pd.read_csv(
    '..\Snowball model functions and tests\output datasets\Revenue data 10 customers example input.csv')

date_column = 'Month'

customer_identifier_columns = ['Customer_ID', 'Customer_name']
product_identifier_columns = ['Product_family', 'Product_sub-family', 'Product_ID']
customer_product_columns = customer_identifier_columns + product_identifier_columns

revenue_data.loc[:, date_column] = pd.to_datetime(revenue_data[date_column])

# start_date_LTM = df_example_input['Month'].min() - relativedelta(months=12)
# end_date_LTM = df_example_input['Month'].max() + relativedelta(months=12)

# remove zero payments
revenue_data = revenue_data.loc[revenue_data['MRR'] > 0]

# add required columns
# calculate start and end dates for each customer/product combination as first and last positive payments
revenue_data['start_date_customer_product'] = revenue_data.groupby(by=customer_product_columns
                                                                   )[date_column].transform('min')
revenue_data['end_date_customer_product'] = revenue_data.groupby(by=customer_product_columns
                                                                 )[date_column].transform('max')

# calculate start and end dates for each customer as first and last positive payments
revenue_data['start_date_customer'] = revenue_data.groupby(by=customer_identifier_columns
                                                           )['start_date_customer_product'].transform('min')
revenue_data['end_date_customer'] = revenue_data.groupby(by=customer_identifier_columns
                                                         )['end_date_customer_product'].transform('max')

revenue_data_with_zeros = add_zero_months(revenue_data=revenue_data, customer_product_columns=customer_product_columns)


print('Finished')