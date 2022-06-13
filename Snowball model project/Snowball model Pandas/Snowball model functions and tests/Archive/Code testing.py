import pandas as pd
import numpy as np

df = pd.DataFrame([{'A': 13, 'B': 2, 'C': 3}, {'A': 16, 'B': 5, 'C': 6}, {'A': 16, 'B': 5, 'C': 6}])
print(df)

grouped = df.groupby(by='C')
print(len(grouped))

df_new = grouped.agg({'B':'sum'})

print(df_new)
print(type(df_new))



# index_max = df.index.max()
# index_set = list(df.index)
# new_indexes = list(range(index_max + 1, (index_max + 1) + 2))
# index_set.extend(new_indexes)
# df = df.reindex(index=index_set)
#
# new_data = pd.DataFrame(data=df[0], index=new_indexes)
#
# df.loc[new_indexes] = new_data
# print(df)
# print(df.loc[:,'A'].sum(axis=0,skipna=True))

# d = {'A': 0, 'B': 2, 'C': 3}
# print(sum(d.values()))

# df = pd.read_csv('..\Alteryx datasets\Snowball example inputs v2.csv')

# customers = set(list(df['Customer_ID']))
# num_of_customers = len(customers)
# print(num_of_customers)

# customer_data = df.groupby(by='Customer_ID')

# i = 0
# for customer_index, customer in customer_data:
#     if customer.shape[0] > 1:
#         index_to_keep = customer.index[0]
#         indexes_to_drop = customer.index[1:]
#         print(len(indexes_to_drop))
#         df = df.drop(index=indexes_to_drop)
#         print(df.shape[0])
#     i+=1
#     if i>3:
#         break

