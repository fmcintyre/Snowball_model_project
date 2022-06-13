from combine_primary_key_duplicates import combine_primary_key_duplicates
from add_n_monthly_deltas import add_n_monthly_dates_and_deltas
import pandas as pd

revenue_data = pd.read_csv(
    '..\Snowball model functions and tests\output datasets\Revenue data 10 customers example input.csv')

primary_key = 'Primary_Key'
date_column = 'Month'
customer_identifier = 'Customer_ID'
product_identifier = 'Product_ID'

add_n_monthly_dates_and_deltas(revenue_data=revenue_data)
