# Customer Revenue Bridge

Version of the customer revenue bridge for Python

Currently, add_zero_months and add_n_monthly are quite inefficient and scale linearly with the number of customers
being sampled (see runtimes_data files for more detail.)

The function add_n_monthly_flags could be simplified (and probably made more efficient) by sticking 
to column operations which are optimized for working with pandas. In particular, one could use an offset by date 
with the MRR column to calculate n-monthly deltas (see https://stackoverflow.com/questions/58382105/groupby-and-get-value-offset-by-one-year-in-pandas)
Alternatively, on could use the inbuilt pandas method .diff(), which combined with .groupby() could calculate
n-monthly deltas directly.

The function add_zero_months could be made more efficient by setting the month column as the index using .set_index() (for each customer/product?)
, then reindexing with the missing months included using .reindex(all_months), finally using .fillna(0) to add zero payments for all the new months 
begin added. This approach would surely be a lot faster than what is already there.