import pandas as pd
import numpy as np
import warnings
import datetime as dt


def add_n_monthly_flags_old(revenue_data, customer_identifier='Customer_ID', product_identifier='Product_ID', period=12,
                        ignore_intermittent_downgrade=True, ignore_intermittent_churn=True):
    """

        Args:
            revenue_data (pandas.DataFrame): list of payments (with IsRecurring = 1) including any lack of
                                             payment for every customer/product combination as a payment
                                             of zero. Essentially any customer/product combination appearing
                                             in the list should have a payment for every month in the revenue
                                             data period.
            customer_identifier (str): title for column with a unique identifier for each customer.
            product_identifier (str): title for column with a unique identifier for each customer.
            period (int): number of months over which we would like to calculate deltas (for example
                          period = 12 would generate yearly deltas and flags)
            ignore_intermittent_downgrade (bool): indicator to ignore intermittent downgrade, instead cross-sell and
                                                  downgrade only occur at the first and last payment for any customer/
                                                  product combination.
            ignore_intermittent_churn (bool): indicator to ignore intermittent downgrade, instead new customer and
                                                  churn only occur at the first and last payment for any customer.


        Returns:
            revenue_bridge_data (pandas.DataFrame): list of payments which cause an n-monthly change in recurring
                                                        revenue with extra columns for n-monthly revenue bridge types
                                                        (one n-monthly flag column for each category new customer, churn
                                                        , upgrade, downgrade, upsell, downsell) and n-monthly deltas.

        """

    # order revenue data chronologically - this will be necessary to calculate revenue bridge deltas
    revenue_data = order_monthly(revenue_data, date_column='Month')

    # create ordered list of months (I think this will be a list of consecutive months if revenue_data comes
    # from applying add_zero_payments) appearing in revenue data.
    all_months = sorted(list(set(revenue_data['Month'])))

    if type(period) != int:
        raise TypeError('Period parameter should be and integer!')
    elif period < 1:
        raise ValueError('Period should be integer greater than 1!')

    if len(all_months) < period + 1:
        raise TypeError('Data period is not long enough to calculate ' + str(period) + '-monthly deltas!')

    # make sure all the non-NaN MRR values are number objects
    revenue_data.loc[:, 'MRR'] = revenue_data['MRR'].astype(float)

    # record total number of records
    num_records = revenue_data.shape[0]

    if period == 1:
        flag_new_customer_column = 'Monthly_Flag_New_Customer'
        flag_churn_column = 'Monthly_Flag_Churn'
        flag_cross_sell_column = 'Monthly_Flag_Cross_Sell'
        flag_downgrade_column = 'Monthly_Flag_Downgrade'
        flag_upsell_column = 'Monthly_Flag_Upsell'
        flag_downsell_column = 'Monthly_Flag_Downsell'

        delta_new_customer_column = 'Monthly_Delta_New_Customer'
        delta_churn_column = 'Monthly_Delta_Churn'
        delta_cross_sell_column = 'Monthly_Delta_Cross_Sell'
        delta_downgrade_column = 'Monthly_Delta_Downgrade'
        delta_upsell_column = 'Monthly_Delta_Upsell'
        delta_downsell_column = 'Monthly_Delta_Downsell'
    elif period == 12:
        flag_new_customer_column = 'LTM_Flag_New_Customer'
        flag_churn_column = 'LTM_Flag_Churn'
        flag_cross_sell_column = 'LTM_Flag_Cross_Sell'
        flag_downgrade_column = 'LTM_Flag_Downgrade'
        flag_upsell_column = 'LTM_Flag_Upsell'
        flag_downsell_column = 'LTM_Flag_Downsell'

        delta_new_customer_column = 'LTM_Delta_New_Customer'
        delta_churn_column = 'LTM_Delta_Churn'
        delta_cross_sell_column = 'LTM_Delta_Cross_Sell'
        delta_downgrade_column = 'LTM_Delta_Downgrade'
        delta_upsell_column = 'LTM_Delta_Upsell'
        delta_downsell_column = 'LTM_Delta_Downsell'
    else:
        flag_new_customer_column = str(period) + '_Monthly_Flag_New_Customer'
        flag_churn_column = str(period) + '_Monthly_Flag_Churn'
        flag_cross_sell_column = str(period) + '_Monthly_Flag_Cross_Sell'
        flag_downgrade_column = str(period) + '_Monthly_Flag_Downgrade'
        flag_upsell_column = str(period) + '_Monthly_Flag_Upsell'
        flag_downsell_column = str(period) + '_Monthly_Flag_Downsell'

        delta_new_customer_column = str(period) + '_Monthly_Delta_New_Customer'
        delta_churn_column = str(period) + '_Monthly_Delta_Churn'
        delta_cross_sell_column = str(period) + '_Monthly_Delta_Cross_Sell'
        delta_downgrade_column = str(period) + '_Monthly_Delta_Downgrade'
        delta_upsell_column = str(period) + '_Monthly_Delta_Upsell'
        delta_downsell_column = str(period) + '_Monthly_Delta_Downsell'

    # create columns for revenue bridge flags
    revenue_data.loc[:, flag_new_customer_column] = np.zeros(num_records)
    revenue_data.loc[:, flag_churn_column] = np.zeros(num_records)
    revenue_data.loc[:, flag_cross_sell_column] = np.zeros(num_records)
    revenue_data.loc[:, flag_downgrade_column] = np.zeros(num_records)
    revenue_data.loc[:, flag_upsell_column] = np.zeros(num_records)
    revenue_data.loc[:, flag_downsell_column] = np.zeros(num_records)

    # create columns for n-monthly deltas
    revenue_data.loc[:, delta_new_customer_column] = np.zeros(num_records)
    revenue_data.loc[:, delta_churn_column] = np.zeros(num_records)
    revenue_data.loc[:, delta_cross_sell_column] = np.zeros(num_records)
    revenue_data.loc[:, delta_downgrade_column] = np.zeros(num_records)
    revenue_data.loc[:, delta_upsell_column] = np.zeros(num_records)
    revenue_data.loc[:, delta_downsell_column] = np.zeros(num_records)

    # group revenue data by customer to calculate New customer/Churn data
    customer_data = revenue_data.groupby(by=customer_identifier)

    if ignore_intermittent_churn:
        # run through lifetime of each customer to calculate New customer/Churn data as well as
        # intermittent New customer/Churn data.
        for customer_index, customer in customer_data:

            # record months for fist and last positive payments of customer combination (this could be altered if
            # we would like negative payments to trigger new customer flags
            first_payment_month_customer = customer.loc[customer['MRR'] > 0]['Month'].min()
            last_payment_month_customer = customer.loc[customer['MRR'] > 0]['Month'].max()

            # initiate list of last month trackers as a list of the first n months appearing in the revenue data
            last_n_months = all_months[:period]

            # initiate first month of last n month period
            last_n_month = last_n_months[0]

            # initiate dictionary of n dictionaries of last month trackers for total MRRs of customer/product
            # combinations
            MRR_customer_product_last_n_months = {}

            # initiate dictionary of last month trackers for total MRRs of customers
            MRR_customer_last_n_months = {}

            for previous_month in last_n_months:
                # group revenue data for each customer in the first payment month by product. We will re-do
                # this for every month in the for loop below.
                customer_product_data = customer.loc[customer['Month'] == previous_month].groupby(by=product_identifier)

                # for each month initiate a dictionary of last month trackers for total MRRs of customer/product
                # combinations
                MRR_customer_product_last_n_months[previous_month] = {}

                # fill each of the initial n dictionary of trackers with total MRR of customer/product combination
                # in the first payment month
                for customer_product_index, customer_product in customer_product_data:

                    # check there is only one payment per customer/product combination in each of the first n months
                    if customer_product.shape[0] > 1:
                        warnings.warn(
                            'There should be at most one payment from each customer for any product per month!' +
                            ' Customer ' + str(customer_index) + ' has made multiple payments for product ' +
                            str(customer_product_index) + ' on ' + str(previous_month) + '.')

                    # note that the we do not keep track explicitly of the customer id here, just the month
                    # and the product id. This dictionary of dictionaries is set to empty for each new customer.
                    MRR_customer_product_last_n_months[previous_month][customer_product_index] = customer_product[
                                                                                        'MRR'].sum(axis=0, skipna=True)

                # record total MRR of customer for last_month
                MRR_customer_last_month = sum(MRR_customer_product_last_n_months[previous_month].values())
                # update dictionary of last month trackers for total MRRs of customers
                MRR_customer_last_n_months[previous_month] = MRR_customer_last_month

            # loop through rest of all months starting after the final month in last_months to calculate revenue bridge
            # data. It might make sense to set start_date n-months before the first positive (or negative?) payment
            # when calling add_zero_months, so that we start this next loop around the first positive (or negative?)
            # payment date.
            for month in all_months[period:]:

                # Note that, since we are starting from n-months into the dataset, if the input data for
                # add_n_monthly_flags has a positive payment on the first month appearing in the dataset, it will not
                # try to flag anything.

                # initiate sum for total MRR of customer in current month
                MRR_customer_current = 0

                # group revenue data for each customer in the current payment month by product
                customer_product_data = customer.loc[customer['Month'] == month].groupby(by=product_identifier)

                # for each month initiate a new dictionary of last month trackers for total MRRs of customer/product
                # combinations
                MRR_customer_product_last_n_months[month] = {}

                if ignore_intermittent_downgrade:
                    # compare total MRR of each customer/product combination from current month to n-months ago
                    for customer_product_index, customer_product in customer_product_data:

                        # check there is only one payment per customer/product combination in each of the first n months
                        if customer_product.shape[0] > 1:
                            warnings.warn(
                                'There should be at most one payment from each customer for any product per month!' +
                                ' Customer ' + str(customer_index) + ' has made multiple payments for product ' +
                                str(customer_product_index) + ' on ' + str(month) + '.')

                        # record months for fist and last positive payments of customer/product combination
                        first_payment_month_customer_product = customer.loc[
                                            (customer['Product_ID'] == customer_product_index) &
                                            (customer['MRR'] > 0)]['Month'].min()
                        last_payment_month_customer_product = customer.loc[
                                            (customer['Product_ID'] == customer_product_index) &
                                            (customer['MRR'] > 0)]['Month'].max()

                        # record customer/product MRR for current month and recall from n months ago
                        MRR_customer_product_current = customer_product['MRR'].sum(axis=0,skipna=True)
                        MRR_customer_product_last_n_month = MRR_customer_product_last_n_months[last_n_month][
                            customer_product_index]

                        # calculate n-monthly delta for each customer/product combination
                        if (MRR_customer_product_current == 0) and (MRR_customer_product_last_n_month == 0):
                            delta_customer_product = 0
                        else:
                            # the ARR n-monthly delta is defined as 12 * (change in MRR over past n months)
                            delta_customer_product = 12 * (MRR_customer_product_current -
                                                               MRR_customer_product_last_n_month)

                        # take note of record IDs in order to accurately update flags and deltas in revenue_data
                        records_customer_product = customer_product.loc[customer_product['Month'] == month]
                        record_ids_customer_product = records_customer_product.index

                        # Now we decide whether a change in revenue for each customer/product combination falls under
                        # Upsell/Downsell or Cross-sell/Downgrade.

                        # positive delta indicates either Cross-sell or Upsell depending on whether MRRs current month
                        # is the first month for this customer/product combination
                        if delta_customer_product > 0:
                            # if this is the first month of this customer/product combination (with a positive payment)
                            # then flag this as cross-sell, otherwise flag as upsell
                            if month == first_payment_month_customer_product:
                                # flag this as cross-sell naively assuming it is not part of a (intermittent) new
                                # customer contribution
                                revenue_data.loc[record_ids_customer_product, flag_cross_sell_column] = 1
                                revenue_data.loc[
                                    record_ids_customer_product, delta_cross_sell_column] = delta_customer_product
                                # keep track of record ids within the customer_data groupby in case we need to unflag
                                # this as cross-sell and reflag if it turns out to be part of a new customer
                                # contribution
                                customer.loc[record_ids_customer_product, flag_cross_sell_column] = 1
                                # keep track of delta_customer_product in the new customer delta column in revenue data.
                                # This will allow us to record new customer delta accurately, breaking down the full
                                # contribution by product.
                                # If this is not a new customer month we will simply reset all the new customer deltas
                                # to zero for this customer and month, which is very straightforward.
                                revenue_data.loc[
                                    record_ids_customer_product, delta_new_customer_column] = delta_customer_product
                            else:
                                revenue_data.loc[record_ids_customer_product, flag_upsell_column] = 1
                                revenue_data.loc[record_ids_customer_product, delta_upsell_column] = delta_customer_product

                        # positive delta indicates either Cross-sell or Upsell depending on whether MRRs current month
                        # after the last month for this customer/product combination
                        elif delta_customer_product < 0:

                            # if this is after the last month of this customer/product combination (with a positive payment)
                            # then flag this as downgrade, otherwise flag as downsell
                            if month > last_payment_month_customer_product:
                                # flag this as downgrade naively assuming it is not part of a (intermittent) churn
                                # contribution
                                revenue_data.loc[record_ids_customer_product, flag_downgrade_column] = 1
                                revenue_data.loc[record_ids_customer_product, delta_downgrade_column] = delta_customer_product
                                # keep track of record ids at a customer level in case we need to unflag this as
                                # downgrade if it turns out to be part of a churn contribution
                                customer.loc[record_ids_customer_product, flag_downgrade_column] = 1
                            else:
                                revenue_data.loc[record_ids_customer_product, flag_downsell_column] = 1
                                revenue_data.loc[record_ids_customer_product, delta_downsell_column] = delta_customer_product

                        # if there is no change in revenue we do nothing
                        else:
                            pass

                        # update each entry in dictionary of last month trackers for total MRRs of customer/product combinations
                        MRR_customer_product_last_n_months[month][customer_product_index] = MRR_customer_product_current

                        # add contribution of current customer/product MRR to total customer MRR for current month
                        MRR_customer_current += MRR_customer_product_current
                else:
                    # compare total MRR of each customer/product combination from current month to n-months ago
                    for customer_product_index, customer_product in customer_product_data:

                        # check there is only one payment per customer/product combination in each of the first n months
                        if customer_product.shape[0] > 1:
                            warnings.warn(
                                'There should be at most one payment from each customer for any product per month!' +
                                ' Customer ' + str(customer_index) + ' has made multiple payments for product ' +
                                str(customer_product_index) + ' on ' + str(month) + '.')

                        # record customer/product MRR for current month and recall for n months ago
                        MRR_customer_product_current = sum(
                            customer_product.loc[customer_product['Month'] == month, 'MRR'])
                        MRR_customer_product_last_n_month = MRR_customer_product_last_n_months[last_n_month][
                            customer_product_index]

                        # calculate n-monthly delta for each customer/product combination
                        if (MRR_customer_product_current == 0) and (MRR_customer_product_last_n_month == 0):
                            delta_customer_product = 0
                        else:
                            # the ARR n-monthly delta is defined as 12 * (change in MRR over past n months)
                            delta_customer_product = 12 * (MRR_customer_product_current -
                                                               MRR_customer_product_last_n_month)

                        # take note of record IDs in order to accurately update flags and deltas in revenue_data
                        records_customer_product = customer_product.loc[customer_product['Month'] == month]
                        record_ids_customer_product = records_customer_product.index

                        # Now we decide whether a change in revenue for each customer/product combination falls under
                        # Upsell/Downsell or Cross-sell/Downgrade. At the moment we include intermittent (1-month)
                        # Cross-sell/Downgrade, which could easily be edited to only care about longer periods of zero
                        # payments.

                        # positive delta indicates either Cross-sell or Upsell depending on whether MRRs for n month's
                        # ago equal zero
                        if delta_customer_product > 0:

                            # if the MRRs for n month's ago equal zero  for this customer/product combination
                            # then flag this as cross-sell, otherwise flag as upsell
                            if MRR_customer_product_last_n_month == 0:
                                # flag this as cross-sell naively assuming it is not part of a (intermittent) new
                                # customer contribution
                                revenue_data.loc[record_ids_customer_product, flag_cross_sell_column] = 1
                                revenue_data.loc[
                                    record_ids_customer_product, delta_cross_sell_column] = delta_customer_product
                                # keep track of record ids within the customer_data groupby in case we need to unflag
                                # this as cross-sell and reflag if it turns out to be part of a new customer
                                # contribution
                                customer.loc[record_ids_customer_product, flag_cross_sell_column] = 1
                                # keep track of delta_customer_product in the new customer delta column in revenue data.
                                # This will allow us to record new customer delta accurately, breaking down the full
                                # contribution by product.
                                # If this is not a new customer month we will simply reset all the new customer deltas
                                # to zero for this customer and month, which is very straightforward.
                                revenue_data.loc[record_ids_customer_product, delta_new_customer_column] = delta_customer_product
                            else:
                                revenue_data.loc[record_ids_customer_product, flag_upsell_column] = 1
                                revenue_data.loc[
                                    record_ids_customer_product, delta_upsell_column] = delta_customer_product

                        # negative delta indicates either Downgrade or Downsell depending on whether MRRs for current month
                        # (or following few months?) equal zero
                        elif delta_customer_product < 0:

                            # if the MRRs for this month is equal zero  for this customer/product combination
                            # then flag this as downgrade, otherwise flag as downsell
                            if MRR_customer_product_current == 0:
                                # flag this as downgrade naively assuming it is not part of a (intermittent) churn
                                # contribution
                                revenue_data.loc[record_ids_customer_product, flag_downgrade_column] = 1
                                revenue_data.loc[
                                    record_ids_customer_product, delta_downgrade_column] = delta_customer_product
                                # keep track of record ids at a customer level in case we need to unflag this as
                                # downgrade if it turns out to be part of a churn contribution
                                customer.loc[record_ids_customer_product, flag_downgrade_column] = 1
                                # keep track of potential churn delta in revenue data. If this is not a churn
                                # month we will reset all the churn deltas to zero for this customer and month
                                revenue_data.loc[
                                    record_ids_customer_product, delta_churn_column] = delta_customer_product
                            else:
                                revenue_data.loc[record_ids_customer_product, flag_downsell_column] = 1
                                revenue_data.loc[
                                    record_ids_customer_product, delta_downsell_column] = delta_customer_product
                        # if there is no change in revenue we do nothing
                        else:
                            pass

                        # update each entry in dictionary of last month trackers for total MRRs of customer/product combinations
                        MRR_customer_product_last_n_months[month][customer_product_index] = MRR_customer_product_current

                        # add contribution of current customer/product MRR to total customer MRR for current month
                        MRR_customer_current += MRR_customer_product_current
                # recall total customer MRR from n months ago
                MRR_customer_last_n_month = MRR_customer_last_n_months[last_n_month]


                # calculate ARR n-monthly delta for each customer/product combination
                if MRR_customer_current == 0 and MRR_customer_last_n_month == 0:
                    delta_customer = 0
                else:
                    delta_customer = 12 * (MRR_customer_current - MRR_customer_last_n_month)

                # Now decide whether overall contribution for each month falls under New customer or Churn
                # based on whether the current month is the first or last customer payment month and there has been
                # a positive or negative change in revenue from n months ago.
                if delta_customer > 0:
                    # take note of record IDs where cross-sell was flagged in order to accurately update flags and
                    # deltas in revenue_data
                    records_cross_sell = customer.loc[(customer['Month'] == month) &
                                                      (customer[flag_cross_sell_column] == 1)]
                    record_ids_cross_sell = records_cross_sell.index
                    # if the current month is the first payment month flag new customer and update cross-sell flags to
                    # zero
                    if month == first_payment_month_customer:
                        # update new customer flags and deltas correctly
                        # Note that the corresponding new customer deltas should have already been added to
                        # revenue_data when flagging cross-sell
                        revenue_data.loc[record_ids_cross_sell, flag_new_customer_column] = 1
                        # update Cross-sell flags and deltas, which would have been wrongly recorded inside the
                        # customer/product loop
                        revenue_data.loc[record_ids_cross_sell, delta_cross_sell_column] = 0
                        revenue_data.loc[record_ids_cross_sell, flag_cross_sell_column] = 0

                    # if there is no new customer reset new customer delta columns for this month (which were edited
                    # when flagging cross-sell) to zero
                    else:
                        revenue_data.loc[record_ids_cross_sell,delta_new_customer_column] = 0



                elif delta_customer < 0:
                    # take note of record IDs where downgrade was flagged in order to accurately update flags and
                    # deltas in revenue_data
                    records_downgrade_customer = customer.loc[(customer['Month'] == month) &
                                                          (customer[flag_downgrade_column] == 1)]
                    record_ids_downgrade_customer = records_downgrade_customer.index
                    # if the current month is after the last payment month flag churn and update downgrade flags to
                    # zero
                    if month > last_payment_month_customer:

                        # update churn flags and deltas correctly
                        # Note that the corresponding new customer deltas should have already been added to
                        # revenue_data when flagging cross-sell
                        revenue_data.loc[record_ids_downgrade_customer, flag_churn_column] = 1
                        revenue_data.loc[record_ids_downgrade_customer, delta_churn_column] = delta_customer
                        # update downgrade flags and deltas, which would have been wrongly recorded as inside the
                        # customer/product loop
                        revenue_data.loc[record_ids_downgrade_customer, delta_downgrade_column] = 0
                        revenue_data.loc[record_ids_downgrade_customer, flag_downgrade_column] = 0
                    # if there is no churn reset churn delta columns for this month (which were edited
                    # when flagging downgrade) to zero
                    else:
                        revenue_data.loc[record_ids_downgrade_customer, delta_churn_column] = 0

                # if there change in revenue do nothing
                else:
                    pass

                # update last month tracker for total MRR of customer
                MRR_customer_last_n_months[month] = MRR_customer_current

                MRR_customer_product_last_n_months.pop(last_n_month)
                MRR_customer_last_n_months.pop(last_n_month)

                # check that our trackers still contain n months worth of data
                if len(MRR_customer_product_last_n_months) != period:
                    raise ValueError('Number of elements is not equal to ' + str(period) + '!')
                elif len(MRR_customer_product_last_n_months) != period:
                    raise ValueError('Number of elements is not equal to ' + str(period) + '!')
                else:
                    pass

                # update previous n months tracker
                last_n_months.pop(0)
                last_n_months.append(month)
                # update n months ago month
                last_n_month = last_n_months[0]
    else:
        for customer_index, customer in customer_data:

            # initiate list of last month trackers as list first n-month appearing in the revenue data
            last_n_months = all_months[:period]

            # initiate first month of last n month period
            last_n_month = last_n_months[0]

            # initiate dictionary of n dictionaries of last month trackers for total MRRs of customer/product combinations
            MRR_customer_product_last_n_months = {}

            # initiate dictionary of last month trackers for total MRRs of customers
            MRR_customer_last_n_months = {}

            for previous_month in last_n_months:
                # group revenue data for each customer in the first payment month by product. We will re-do this for every
                # month in the for loop below.
                customer_product_data = customer.loc[customer['Month'] == previous_month].groupby(by=product_identifier)

                # for each month initiate a dictionary of last month trackers for total MRRs of customer/product
                # combinations
                MRR_customer_product_last_n_months[previous_month] = {}

                # fill each of the initial n dictionary of trackers with total MRR of customer/product combination in the
                # first payment month
                for customer_product_index, customer_product in customer_product_data:

                    # check there is only one payment per customer/product combination in each of the first n months
                    if customer_product.shape[0] > 1:
                        warnings.warn(
                            'There should be at most one payment from each customer for any product per month!' +
                            ' Customer ' + str(customer_index) + ' has made multiple payments for product ' +
                            str(customer_product_index) + ' on ' + str(previous_month) + '.')

                    # record customer/product MRR for current month
                    MRR_customer_product_last_n_months[previous_month][customer_product_index] = sum(
                        customer_product.loc[customer_product['Month'] == previous_month, 'MRR'])

                # record total MRR of customer for last_month
                MRR_customer_last_month = sum(MRR_customer_product_last_n_months[previous_month].values())
                # update dictionary of last month trackers for total MRRs of customers
                MRR_customer_last_n_months[previous_month] = MRR_customer_last_month

            # loop through rest of all months starting after the final month in last_months to calculate revenue bridge data
            for month in all_months[period:]:

                # initiate sum for total MRR of customer in current month
                MRR_customer_current = 0

                # group revenue data for each customer in the current payment month by product
                customer_product_data = customer.loc[customer['Month'] == month].groupby(by=product_identifier)

                # for each month initiate a new dictionary of last month trackers for total MRRs of customer/product
                # combinations
                MRR_customer_product_last_n_months[month] = {}

                if ignore_intermittent_downgrade:
                    # compare total MRR of each customer/product combination from current month to n-months ago
                    for customer_product_index, customer_product in customer_product_data:

                        # record months for fist and last payments
                        first_payment_month_customer_product = customer.loc[
                                                                (customer['Product_ID'] == customer_product_index) &
                                                                (customer['MRR'] > 0)]['Month'].min()
                        last_payment_month_customer_product = customer.loc[
                                                                (customer['Product_ID'] == customer_product_index) &
                                                                (customer['MRR'] > 0)]['Month'].max()

                        # record customer/product MRR for current month and last month
                        MRR_customer_product_current = sum(
                            customer_product.loc[customer_product['Month'] == month, 'MRR'])
                        MRR_customer_product_last_n_month = MRR_customer_product_last_n_months[last_n_month][
                            customer_product_index]

                        # calculate n-monthly delta for each customer/product combination
                        if (MRR_customer_product_current == 0) and (MRR_customer_product_last_n_month == 0):
                            delta_customer_product = 0
                        else:
                            # calculater the ARR n-monthly delta as 12 * (change in ARR over past n months)
                            delta_customer_product = 12 * (MRR_customer_product_current - MRR_customer_product_last_n_month)

                        # take note of record IDs in order to accurately update flags and deltas in revenue_data
                        records_customer_product = customer_product.loc[customer_product['Month'] == month]
                        record_ids_customer_product = records_customer_product.index

                        # Now we decide whether a change in revenue for each customer/product combination falls under
                        # Upsell/Downsell or Cross-sell/Downgrade.

                        # positive delta indicates either Cross-sell or Upsell depending on whether MRRs current month
                        # is the first month for this customer/product combination
                        if delta_customer_product > 0:
                            # if this is the first month of this customer/product combination (with a positive payment)
                            # then flag this as cross-sell, otherwise flag as upsell
                            if month == first_payment_month_customer_product:
                                # flag this as cross-sell naively assuming it is not part of a (intermittent) new
                                # customer contribution
                                revenue_data.loc[record_ids_customer_product, flag_cross_sell_column] = 1
                                revenue_data.loc[
                                    record_ids_customer_product, delta_cross_sell_column] = delta_customer_product
                                # keep track of record ids at a customer level in case we need to unflag this as
                                # cross-sell if it turns out to be part of a new customer contribution
                                customer.loc[record_ids_customer_product, flag_cross_sell_column] = 1
                            else:
                                revenue_data.loc[record_ids_customer_product, flag_upsell_column] = 1
                                revenue_data.loc[
                                    record_ids_customer_product, delta_upsell_column] = delta_customer_product

                        # positive delta indicates either Cross-sell or Upsell depending on whether MRRs current month
                        # after the last month for this customer/product combination
                        elif delta_customer_product < 0:

                            # if this is after the last month of this customer/product combination (with a positive payment)
                            # then flag this as downgrade, otherwise flag as downsell
                            if month > last_payment_month_customer_product:
                                # flag this as downgrade naively assuming it is not part of a (intermittent) churn
                                # contribution
                                revenue_data.loc[record_ids_customer_product, flag_downgrade_column] = 1
                                revenue_data.loc[
                                    record_ids_customer_product, delta_downgrade_column] = delta_customer_product
                                # keep track of record ids at a customer level in case we need to unflag this as
                                # downgrade if it turns out to be part of a churn contribution
                                customer.loc[record_ids_customer_product, flag_downgrade_column] = 1
                            else:
                                revenue_data.loc[record_ids_customer_product, flag_downsell_column] = 1
                                revenue_data.loc[
                                    record_ids_customer_product, delta_downsell_column] = delta_customer_product

                        # if there is no change in revenue we do nothing
                        else:
                            pass

                        # update each entry in dictionary of last month trackers for total MRRs of customer/product combinations
                        MRR_customer_product_last_n_months[month][customer_product_index] = MRR_customer_product_current

                        # add contribution of current customer/product MRR to total customer MRR for current month
                        MRR_customer_current += MRR_customer_product_current
                else:
                    # compare total MRR of each customer/product combination from current month to n-months ago
                    for customer_product_index, customer_product in customer_product_data:

                        # record customer/product MRR for current month and last month
                        MRR_customer_product_current = sum(
                            customer_product.loc[customer_product['Month'] == month, 'MRR'])
                        MRR_customer_product_last_n_month = MRR_customer_product_last_n_months[last_n_month][
                            customer_product_index]

                        # calculate n-monthly delta for each customer/product combination
                        if (MRR_customer_product_current == 0) and (MRR_customer_product_last_n_month == 0):
                            delta_customer_product = 0
                        else:
                            # calculate the ARR n-monthly delta n * (change in MRR over past n months)
                            delta_customer_product = 12 * (MRR_customer_product_current - MRR_customer_product_last_n_month)

                        # take note of record IDs in order to accurately update flags and deltas in revenue_data
                        records_customer_product = customer_product.loc[customer_product['Month'] == month]
                        record_ids_customer_product = records_customer_product.index

                        # Now we decide whether a change in revenue for each customer/product combination falls under
                        # Upsell/Downsell or Cross-sell/Downgrade. At the moment we include intermittent (1-month)
                        # Cross-sell/Downgrade, which could easily be edited to only care about longer periods of zero
                        # payments.

                        # positive delta indicates either Cross-sell or Upsell depending on whether MRRs for n month's
                        # ago equal zero
                        if delta_customer_product > 0:

                            # if the MRRs for n month's ago equal zero  for this customer/product combination
                            # then flag this as cross-sell, otherwise flag as upsell
                            if MRR_customer_product_last_n_month == 0:
                                # flag this as cross-sell naively assuming it is not part of a (intermittent) new
                                # customer contribution
                                revenue_data.loc[record_ids_customer_product, flag_cross_sell_column] = 1
                                revenue_data.loc[
                                    record_ids_customer_product, delta_cross_sell_column] = delta_customer_product
                                # keep track of record ids at a customer level in case we need to unflag this as
                                # cross-sell if it turns out to be part of a new customer contribution
                                customer.loc[record_ids_customer_product, flag_cross_sell_column] = 1
                            else:
                                revenue_data.loc[record_ids_customer_product, flag_upsell_column] = 1
                                revenue_data.loc[
                                    record_ids_customer_product, delta_upsell_column] = delta_customer_product

                        # negative delta indicates either Downgrade or Downsell depending on whether MRRs for current month
                        # (or following few months?) equal zero
                        elif delta_customer_product < 0:

                            # if the MRRs for this month is equal zero  for this customer/product combination
                            # then flag this as downgrade, otherwise flag as downsell
                            if MRR_customer_product_current == 0:
                                # flag this as downgrade naively assuming it is not part of a (intermittent) churn
                                # contribution
                                revenue_data.loc[record_ids_customer_product, flag_downgrade_column] = 1
                                revenue_data.loc[
                                    record_ids_customer_product, delta_downgrade_column] = delta_customer_product
                                # keep track of record ids at a customer level in case we need to unflag this as
                                # downgrade if it turns out to be part of a churn contribution
                                customer.loc[record_ids_customer_product, flag_downgrade_column] = 1
                            else:
                                revenue_data.loc[record_ids_customer_product, flag_downsell_column] = 1
                                revenue_data.loc[
                                    record_ids_customer_product, delta_downsell_column] = delta_customer_product
                        # if there is no change in revenue we do nothing
                        else:
                            pass

                        # update each entry in dictionary of last month trackers for total MRRs of customer/product combinations
                        MRR_customer_product_last_n_months[month][customer_product_index] = MRR_customer_product_current

                        # add contribution of current customer/product MRR to total customer MRR for current month
                        MRR_customer_current += MRR_customer_product_current
                # recall total customer MRR from n months ago
                MRR_customer_last_n_month = MRR_customer_last_n_months[last_n_month]

                # calculate ARR n-monthly delta for each customer/product combination
                if MRR_customer_current == 0 and MRR_customer_last_n_month == 0:
                    delta_customer = 0
                else:
                    delta_customer = 12 * (MRR_customer_current - MRR_customer_last_n_month)

                # Now decide whether overall contribution for each month falls under (intermittent) New customer or Churn
                # if we want to be more stringent with the definition of intermittent New customer, we can change this
                # if condition slightly
                if MRR_customer_last_n_month == 0 and delta_customer > 0:
                    # take note of record IDs where cross-sell was flagged in order to accurately update flags and
                    # deltas in revenue_data
                    records_new_customer = customer.loc[(customer['Month'] == month) &
                                                        (customer[flag_cross_sell_column] == 1)]
                    record_ids_new_customer = records_new_customer.index

                    # update new customer flags and deltas correctly
                    revenue_data.loc[record_ids_new_customer, flag_new_customer_column] = 1
                    revenue_data.loc[record_ids_new_customer, delta_new_customer_column] = delta_customer
                    # update Cross-sell flags and deltas, which would have been wrongly recorded inside the
                    # customer/product loop
                    revenue_data.loc[record_ids_new_customer, delta_cross_sell_column] = 0
                    revenue_data.loc[record_ids_new_customer, flag_cross_sell_column] = 0

                elif MRR_customer_current == 0 and delta_customer < 0:
                    # take note of record IDs where downgrade was flagged in order to accurately update flags and
                    # deltas in revenue_data
                    records_churn_customer = customer.loc[(customer['Month'] == month) &
                                                          (customer[flag_downgrade_column] == 1)]
                    record_ids_churn_customer = records_churn_customer.index

                    # update churn flags and deltas correctly
                    revenue_data.loc[record_ids_churn_customer, flag_churn_column] = 1
                    revenue_data.loc[record_ids_churn_customer, delta_churn_column] = delta_customer

                    # update Downgrade flags and deltas, which would have been wrongly recorded inside the
                    # customer/product loop
                    revenue_data.loc[record_ids_churn_customer, delta_downgrade_column] = 0
                    revenue_data.loc[record_ids_churn_customer, flag_downgrade_column] = 0

                # if there is no (intermittent) New customer or Churn, do nothing
                else:
                    pass

                # update last month tracker for total MRR of customer
                MRR_customer_last_n_months[month] = MRR_customer_current

                MRR_customer_product_last_n_months.pop(last_n_month)
                MRR_customer_last_n_months.pop(last_n_month)

                # check that our trackers still contain n months worth of data
                if len(MRR_customer_product_last_n_months) != period:
                    raise ValueError('Number of elements is not equal to ' + str(period) + '!')
                elif len(MRR_customer_product_last_n_months) != period:
                    raise ValueError('Number of elements is not equal to ' + str(period) + '!')
                else:
                    pass

                # update previous n months tracker
                last_n_months.pop(0)
                last_n_months.append(month)
                # update n months ago month
                last_n_month = last_n_months[0]

    # create new column with number of flags per row
    revenue_data['num_of_flags'] = revenue_data[flag_new_customer_column] + revenue_data[flag_churn_column] \
                                   + revenue_data[flag_cross_sell_column] + revenue_data[flag_downgrade_column] + \
                                   revenue_data[flag_upsell_column] + revenue_data[flag_downsell_column]

    flag_totals = set(revenue_data['num_of_flags'])
    if len(flag_totals) > 2:
        for i in list(flag_totals):
            if i not in [0, 1]:
                print(revenue_data.loc[revenue_data['num_of_flags'] == i])
        raise ValueError('All payment should have at most one flag equal to 1!')

    # drop rows with all flags equal to zero
    revenue_bridge_data = revenue_data.loc[revenue_data['num_of_flags'] > 0]

    # drop number of flags column
    revenue_bridge_data = revenue_bridge_data.drop(columns=['num_of_flags'])

    return revenue_bridge_data


def add_zero_months_old(revenue_data, date_column='Month', start_date=None, end_date=None):
    """
    Returns revenue data including payments of zero whenever a customer is not
    paying for a product which they will be or have been paying for at some point
    within the fixed time period.
    Args:
        revenue_data (pandas.DataFrame): list of payments (with IsRecurring = 1) including details of
                                         customer, product, payment date, revenue.
        date_column (str): title for column with payment dates.

    Returns:
        revenue_data_with_zeros (pandas.DataFrame): list of payments in including any
                                                    lack of payment for every customer/
                                                    product combination as a payment of zero.
                                                    Essentially any customer/product combination appearing
                                                    in the list should have a payment for every month in
                                                    the revenue data period.
    """

    # cast date column entries from string to datetime in case they are not already
    # print(revenue_data[date_column])
    # print(pd.to_datetime(revenue_data[date_column]))
    revenue_data.loc[:, date_column] = pd.to_datetime(revenue_data[date_column])

    # make all payments set for beginning of month - it feels a little unnatural to
    # include this here rather than the data cleaning phase
    revenue_data.loc[:, date_column] = revenue_data[date_column].dt.to_period('M').dt.to_timestamp()

    # if not given, set start-of-period and end-of-period dates using first and last payment in the revenue data
    if start_date is None:
        start_date = revenue_data[date_column].min()
    if end_date is None:
        end_date = revenue_data[date_column].max()

    # create list of months between start_data and end_date
    all_months = set(pd.date_range(start_date, end_date, freq='MS'))

    # record overall number of payments for adding rows using .loc and index labels
    index_max = revenue_data.index.max()

    # filter out non-recurring payments from revenue data
    # revenue_data_rec = revenue_data.loc[revenue_data['IsRecurring'] == 1]

    # group data by combination of Customer_ID and Product_ID
    revenue_data_grouped = revenue_data.groupby(['Customer_ID', 'Product_ID'])

    # for each customer/product combination calculate months which do not appear in the
    # dataset and include payments of zero to remedy this.
    for index, customer_product in revenue_data_grouped:
        # create a set of all months for which the customer makes some payment (could be zero)
        # for that particular product
        payment_months = set(customer_product.Month)
        # create list of all months for which the customer makes some payment (could be zero)
        # for that particular product
        zero_payment_months = list(all_months - payment_months)

        # add a NaN rows for every month in zero_payment_month
        index_set = list(revenue_data.index)
        new_indexes = list(range(index_max + 1, (index_max + 1) + (len(zero_payment_months))))
        index_set.extend(new_indexes)
        revenue_data = revenue_data.reindex(index=index_set)

        # fill in months, customer ids and product ids for every month in zero_payment_month
        revenue_data.loc[new_indexes, 'Month'] = zero_payment_months
        revenue_data.loc[new_indexes, 'Customer_ID'] = int(index[0])
        revenue_data.loc[new_indexes, 'Product_ID'] = str(index[1])
        revenue_data.loc[new_indexes, 'MRR'] = 0
        revenue_data.loc[new_indexes, 'IsRecurring'] = 1

        index_max = revenue_data.index.max()

    revenue_data_with_zeros = revenue_data

    return revenue_data_with_zeros




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
