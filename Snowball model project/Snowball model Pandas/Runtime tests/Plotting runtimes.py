import matplotlib.pyplot as plt
import pandas as pd

df_runtimes = pd.read_csv('20211015_10-39-04_runtimes_data.csv')

x = df_runtimes['Number of customers']
plt.plot(x, df_runtimes['Runtimes for add_zero_months (seconds)'], label='add_zero_months')
plt.plot(x, df_runtimes['Runtimes for add_1_monthly_flags (seconds)'], label='add_monthly_flags')
plt.plot(x, df_runtimes['Runtimes for add_12_monthly_flags (seconds)'], label='add_12_monthly_flags')

plt.xlabel('Number of customers included in sample')
plt.ylabel('Runtime in seconds')
plt.title('How runtime for Snowball model scales with number of customers')
plt.legend()
plt.show()

plt.show()
