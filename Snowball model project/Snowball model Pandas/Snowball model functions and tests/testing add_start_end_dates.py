from add_start_end_dates import add_start_end_dates
import pandas as pd
import datetime as dt

revenue_data_with_zeros = pd.read_csv('Snowball input for customer 2076 20211029_12-48-55.csv')

revenue_data_with_dates_test = add_start_end_dates(revenue_data_with_zeros)

# generate timestamp to save files
timestamp = str(dt.datetime.now().strftime("%Y%m%d_%H-%M-%S"))
revenue_data_with_zeros.to_csv('Snowball output with dates test for customer ' +
                                                    str(2076) + ' ' + timestamp + '.csv')
