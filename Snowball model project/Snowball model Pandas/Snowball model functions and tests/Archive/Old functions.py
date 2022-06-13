import pandas as pd
import numpy as np
import datetime as dt

def add_flags_old(revenue_data, customer_identifier='Customer_ID', product_identifier='Product_ID'):
    """

        Args:
            revenue_data (pandas.DataFrame): list of payments (with IsRecurring = 1) including any lack
                                             of payment for every customer/product combination as a
                                             payment of zero. Essentially any customer/product combination
                                             appearing in the list should have a payment for every month
                                             in the revenue data period.
            customer_identifier (str): title for column with a unique identifier for each customer.
            product_identifier (str): title for column with a unique identifier for each customer.

        Returns:
            revenue_data_with_flags (pandas.DataFrame): revenue data with extra columns for revenue
                                                        bridge types (one flag column for each
                                                        category new customer, churn, upgrade,
                                                        downgrade, upsell, downsell) and monthly deltas.

        """


    # order revenue data chronologically - this will be necessary to calculate revenue bridge deltas
    revenue_data = order_monthly(revenue_data, date_column='Month')

    # create ordered list of months (I think this will be a list of consecutive months if revenue_data comes
    # from applying add_zero_payments) appearing in revenue data.
    all_months = sorted(list(set(revenue_data['Month'])))

    # record total number of records
    num_records = revenue_data.shape[0]

    # create columns for revenue bridge flags
    revenue_data.loc[:, 'New_customer_flag'] = np.zeros(num_records)
    revenue_data.loc[:, 'Churn_flag'] = np.zeros(num_records)
    revenue_data.loc[:, 'Cross_sell_flag'] = np.zeros(num_records)
    revenue_data.loc[:, 'Downgrade_flag'] = np.zeros(num_records)
    revenue_data.loc[:, 'Upsell_flag'] = np.zeros(num_records)
    revenue_data.loc[:, 'Downsell_flag'] = np.zeros(num_records)
    revenue_data.loc[:, 'delta_monthly'] = np.zeros(num_records)

    # group revenue data by customer to calculate New customer/Churn data
    customer_data = revenue_data.groupby(by=customer_identifier)

    # run through lifetime of each customer to calculate New customer/Churn data as well as
    # intermittent New customer/Churn data.
    for customer_index, customer in customer_data:

        # initiate last month tracker as the first month appearing in the revenue data
        last_month = all_months[0]

        # group revenue data for each customer in the first payment month by product. We will re-do this for every
        # month in the for loop below.
        customer_product_data = revenue_data.loc[
            (revenue_data['Month'] == last_month) & (revenue_data['Customer_ID'] == customer_index)].groupby(
            by=product_identifier)

        # initiate dictionary of last month trackers for total MRRs of customer/product combinations
        MRR_customer_product_last_month = {}

        # fill initial dictionary of trackers with total MRR of customer/product combination in the first payment month
        for customer_product_index, customer_product in customer_product_data:
            MRR_customer_product_last_month[customer_product_index] = sum(
                customer_product.loc[customer_product['Month'] == last_month, 'MRR'])


        # initiate last month tracker for total MRR of customer
        MRR_customer_last_month = sum(MRR_customer_product_last_month.values())

        # loop through all months from start to end of data period to calculate revenue bridge data
        for month in all_months:

            # initiate sum for total MRR of customer in current month
            MRR_customer_current = 0

            # group revenue data for each customer in the current payment month by product
            customer_product_data = revenue_data.loc[
                (revenue_data['Month'] == month) & (revenue_data['Customer_ID'] == customer_index)].groupby(
                by=product_identifier)

            # compare total MRR of each customer/product combination from current month to last month
            for index, customer_product in customer_product_data:

                # record customer/product MRR for current month and last month
                MRR_current = sum(customer_product.loc[customer_product['Month'] == month, 'MRR'])
                MRR_last_month = MRR_customer_product_last_month[index]

                # calculate delta for each customer/product combination
                if (MRR_current == 0) and (MRR_last_month == 0):
                    delta = 0
                else:
                    delta = MRR_current - MRR_last_month

                # take note of record IDs in order to accurately update flags and deltas in revenue_data
                records = customer_product.loc[customer_product['Month'] == month]
                record_ids = records.index

                # Now we decide whether a change in revenue for each customer/product combination falls under Upsell/Downsell or
                # Cross-sell/Downgrade. At the moment we include intermittent (1-month) Cross-sell/Downgrade, which could easily
                # be edited to only care about longer periods of zero payments.
                # Of course, at this stage we are ignoring the contribution of New customer/Churn, which will be calculated
                # outside of the current customer/product for loop (and the corresponding Cross-sell/Downgrade flags will be
                # set back to zero.)

                # positive delta indicates either Cross-sell or Upsell depending on whether MRRs for previous month(s) equal zero
                if delta > 0:

                    # naively assuming this is not part of a (intermittent?) New customer contribution we update delta_monthly in revenue_data
                    # we may change this later if we decide it should be attributed to New customer
                    revenue_data.loc[record_ids, 'delta_monthly'] = delta

                    # if we want to be more stringent with the definition of intermittent Cross-sell, we can change this if condition
                    # slightly
                    if MRR_last_month == 0:
                        revenue_data.loc[record_ids, 'Cross_sell_flag'] = 1
                    else:
                        revenue_data.loc[record_ids, 'Upsell_flag'] = 1

                # negative delta indicates either Downgrade or Downsell depending on whether MRRs for current month (or following
                # few months?) equal zero
                elif delta < 0:

                    # naively assuming this is not part of a (intermittent?) Churn contribution we update delta_monthly in revenue_data
                    # we may change this later if we decide it should be attributed to New customer
                    revenue_data.loc[record_ids, 'delta_monthly'] = delta

                    # if we want to be more stringent with the definition of intermittent Downgrade, we can change this if condition
                    # slightly
                    if MRR_current == 0:
                        revenue_data.loc[record_ids, 'Downgrade_flag'] = 1
                    else:
                        revenue_data.loc[record_ids, 'Downsell_flag'] = 1

                # if there is no change in revenue we do nothing
                else:
                    pass

                # update each entry in dictionary of last month trackers for total MRRs of customer/product combinations
                MRR_customer_product_last_month[index] = MRR_current

                # add contribution of current customer/product MRR to total customer MRR for current month
                MRR_customer_current += MRR_current

            # calculate delta for each customer/product combination
            delta_customer = MRR_customer_current - MRR_customer_last_month

            # take note of record IDs in order to accurately update flags and deltas in revenue_data
            records_customer = customer.loc[customer['Month'] == month]
            record_ids_customer = records_customer.index

            # Now decide whether overall contribution for each month falls under (intermittent) New customer or Churn
            if np.abs(delta_customer) > 0:

                # if we want to be more stringent with the definition of intermittent New customer, we can change this if condition
                # slightly
                if MRR_customer_last_month == 0:
                    revenue_data.loc[record_ids_customer, 'New_customer_flag'] = 1
                    # update Cross-sell flags and deltas, which would have been wrongly recorded inside the customer/product loop
                    revenue_data.loc[record_ids_customer, 'delta_monthly'] = delta_customer
                    revenue_data.loc[record_ids_customer, 'Cross_sell_flag'] = 0

                elif MRR_customer_current == 0:
                    revenue_data.loc[record_ids_customer, 'Churn_flag'] = 1
                    # update Downgrade flags and deltas, which would have been wrongly recorded inside the customer/product loop
                    revenue_data.loc[record_ids_customer, 'delta_monthly'] = delta_customer
                    revenue_data.loc[record_ids_customer, 'Downgrade_flag'] = 0

                # if there is no (intermittent) New customer or Churn, do nothing
                else:
                    pass

            # update last month tracker for total MRR of customer
            MRR_customer_last_month = MRR_customer_current

    revenue_data_with_flags = revenue_data

    return revenue_data_with_flags


def add_flags_oldest(revenue_data, customer_identifier = 'Customer_ID', product_identifier = 'Product_ID'):
    """

    Args:
        revenue_data (pandas.DataFrame): list of payments (with IsRecurring = 1) including any
                                         lack of payment for any customer/product combination
                                         as a payment of zero.
        customer_identifier (str): title for column with a unique identifier for each customer.
        product_identifier (str): title for column with a unique identifier for each customer.

    Returns:
        revenue_data_with_flags (pandas.DataFrame): revenue data with extra columns for revenue
                                                    bridge type (one flag column for each
                                                    category new customer, churn, upgrade,
                                                    downgrade, upsell, downsell), monthly delta.

    """

    # order revenue data chronologically - this will be necessary to calculate revenue bridge deltas
    revenue_data = order_monthly(revenue_data, date_column = 'Month')

    # create ordered list of months (I think this will be a list of consecutive months if revenue_data comes
    # from applying add_zero_payments) appearing in revenue data.
    all_months = sorted(list(set(revenue_data['Month'])))

    # record total number of records
    num_records = revenue_data.shape[0]

    # create columns for monthly revenue bridge flags and deltas
    revenue_data['New_customer_flag'] = np.zeros(num_records)
    revenue_data['Churn_flag'] = np.zeros(num_records)
    revenue_data['Cross_sell_flag'] = np.zeros(num_records)
    revenue_data['Downgrade_flag'] = np.zeros(num_records)
    revenue_data['Upsell_flag'] = np.zeros(num_records)
    revenue_data['Downsell_flag'] = np.zeros(num_records)
    revenue_data['delta_monthly'] = np.zeros(num_records)

    # group revenue data by customer and product
    customer_product_data = revenue_data.groupby(by=['Customer_ID', 'Product_ID'])

    # for each customer/product combinations calculate monthly deltas and characterise payments
    # as Upsell/Downsell/Cross-sell/Downgrade. At this stage we ignore the possibility of
    # New customer/Churn.
    for customer_product_index, customer_product in customer_product_data:

        # initiate last month tracker
        last_month = all_months[0]

        # initiate tracker for total MRR of customer and product last month
        MRR_last_month = sum(customer_product[customer_product['Month'] == last_month]['MRR'])

        # for each month calculate the monthly MRR delta
        for month in all_months:

            # isolate payments for the current month
            records = customer_product[customer_product['Month'] == month]

            # extract record indexes in order to update flags and deltas later
            record_ids = records.index

            # record MRR for current month
            MRR_current = sum(customer_product[customer_product['Month'] == month]['MRR'])

            delta = MRR_current - MRR_last_month

            # decide whether revenue change falls under Cross-sell/Upsell
            if delta > 0:

                revenue_data.loc[record_ids, 'delta_monthly'] = delta

                if MRR_last_month == 0:
                    revenue_data.loc[record_ids, 'Cross_sell_flag'] = 1
                else:
                    revenue_data.loc[record_ids, 'Upsell_flag'] = 1

            # decide whether revenue change falls under Downgrade/Downsell
            elif delta < 0:

                revenue_data.loc[record_ids, 'delta_monthly'] = delta

                if MRR_current == 0:
                    revenue_data.loc[record_ids, 'Downgrade_flag'] = 1
                else:
                    revenue_data.loc[record_ids, 'Downsell_flag'] = 1

            # if there has not been a change in revenue there is no need to update the delta or
            # the flags, which should by default all be set to zero
            else:
                pass

            # redefine MRR_last_month for next iteration
            MRR_last_month = MRR_current

    # group revenue data by customer in order to extract New customer/Churn data
    customer_data = revenue_data.groupby(by='Customer_ID')

    for index, customer in customer_data:

        # initiate last month tracker
        last_month = all_months[0]

        # initiate tracker for total MRR of customer last month
        MRR_last_month = sum(customer[customer['Month'] == last_month]['MRR'])

        # for each month calculate the monthly MRR deltas
        for month in all_months:

            # isolate payments from current month
            records = customer[customer['Month'] == month]

            # similar to customer_product case
            record_ids = records.index

            # calculate the total MRR for customer this month
            MRR_current = sum(customer[customer['Month'] == month]['MRR'])

            delta = MRR_current - MRR_last_month

            # decide whether total delta for this month falls under the category on
            # New customer, and if so change all the corresponding Cross-sell flags to zero.
            if np.abs(delta) > 0:

                # New customer can only occur if the MRR for last month is zero.
                # In this case, the only flags which will already be equal to 1, will
                # be the cross-sell flags. We set these to zero, and set the New customer
                # flags to 1.
                if MRR_last_month == 0:
                    revenue_data.loc[record_ids, 'New_customer_flag'] = 1
                    revenue_data.loc[record_ids, 'delta_monthly'] = delta
                    revenue_data.loc[record_ids, 'Cross_sell_flag'] = 0

                # Similar to comment for New customers.
                elif MRR_current == 0:
                    revenue_data.loc[record_ids, 'Churn_flag'] = 1
                    revenue_data.loc[record_ids, 'delta_monthly'] = delta
                    revenue_data.loc[record_ids, 'Downgrade_flag'] = 0

                # if the MRR is non-zero for this month and last month, then neither
                # New customer nor Churn has occurred.
                else:
                    pass

            # update MRR for last month for next iteration
            MRR_last_month = MRR_current

    revenue_data_with_flags = revenue_data

    return revenue_data_with_flags