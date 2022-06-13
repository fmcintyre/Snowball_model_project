lst_years = list(range(2010, datetime.datetime.now().year))

lst_years_col = []

for lst in range(len(df_part)):
    lst_years_col.append(lst_years)

df_part['year'] = lst_years_col

df_part = df_part.explode('year')

df_full_qty = df_full.groupby(['Part ID', 'year'], as_index=False)['Qty'].sum()

df_part = pd.merge(df_part, df_full_qty, how='left', on=['year', 'Part ID'])

df_part['Qty'] = df_part['Qty'].fillna()