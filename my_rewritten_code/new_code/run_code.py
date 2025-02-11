# FILENAME OUTPUTTED/OUTPUTTED to is STARTDATE, that is, folder of 2024-05-31 (named) in Test/benchmarkingoutputs/...
# -d "2024-05-31" -c -o "2024-08-30"
#
"""
There are many ways this script can be used.

It is "safe" to be ran as many times as warranted.

SPF_BM and SPF_BM_DETAILS can be deleted at any point, provided any new forecasts are manually re-added.

When ran as main, without any arguments:
   It will do the analysis for today (using 6PM cut-off logic.)
   
   This uses DoAnalysis()
   
When ran as main, with arguments:
   It will do the analysis as if it was ran on another date.
   
   This uses DoAnalysis()

When imported, 
    It can be ran for a specific day, or a range of dates using 
    
    Use RunOnHistoricDay() and RunOnRange()

From command line or process control:

    It can be ran on a specific day, or a range of dates, historically will cause an over-write, if it's in the future, it'll be over-written when it become history.

In all cases, if dates provided are historic it will cause an over-write, since only one "asofdate" per security is allowed in the database.
"""
import json
from scipy.optimize import minimize
import os
import numpy as np
from dateutil.relativedelta import relativedelta
from equitable.db.db_functions import execute_table_query
import traceback
import pandas as pd

from benchmarking.helpers_test_2 import create_shock_tables

pd.set_option('display.width', 150)
import datetime as dt
from collections import OrderedDict
import openpyxl
import argparse
from equitable.infrastructure import sysenv, jobs, sendemail
from equitable.chronos import offsets, conversions
from equitable.db.psyw import SmartDB
from psycopg2.extras import DictCursor
import os


import sys

import objects as helpers
import model_portfolio as bench


BM_conn = SmartDB('Benchmark')
BM_cur = BM_conn.con.cursor()

Bond_conn = SmartDB('Bond')
Bond_cur = Bond_conn.con.cursor()

General_conn = SmartDB('General')
General_cur = General_conn.con.cursor()

# Generate_benchmarking_tables.py



import numpy as np
import pandas as pd

# class bond curves, all to do wiht the curves - so accumulate functions together and use same attributes - items

# so i know its the PV of CURVES
# maybe should inherit shocks but / or keep it simple for now (simplify first, that consider later)
# use inheritance for this (to hide it)
"""
class Curves:
    def __init__(self, GivenDate: pd.dt):
       self.GivenDate = GivenDate
    def get_pv(self):
"""
"""
class Curves:
    
    #>>> bond_curves = Curves()
    #>>> pv = bond_curves.get_pv()
    #>>> print(pv.type)
    #>>> print(pv.shape)
    #>>> print(pv)
        # Class attr: less common, don't necessarily do - more mem? curve_pv = None
    def __init__(self):
    # self.GivenDate = GivenDate
        self.curve_pv = None
    def get_pv(self):
        if self.curve_pv is None:
            pv = helpers.create_general_shock_table()
            self.curve_pv = pv[0]
            return self.curve_pv
        else:
            return self.curve_pv
"""


def create_summed_cashflow_tables(solution_df, given_date, asset_type='public', curMonthBs=False):
    # Adjust solution_df portfolio names to standardize column names for processing
    benchmarking_solution = solution_df.copy()
    benchmarking_solution.rename(columns={5: 6, 4: 5, 3: 4, 2: 3, 1: 2, 0: 1}, inplace=True)
    benchmarking_solution['portfolio'] = benchmarking_solution['portfolio'].replace({'Total': 'TOTAL',
                                                                                     'np': 'NONPAR',
                                                                                     'group': 'GROUP',
                                                                                     'Accum': 'ACCUM',
                                                                                     'Payout': 'PAYOUT',
                                                                                     'ul': 'UNIVERSAL',
                                                                                     'Surplus': 'SURPLUS'})

                                                                                     # Load necessary FTSE data, weights, and asset mix information
    ftse_data = helpers.get_ftse_data(given_date)
    weights, totals = helpers.create_weight_tables(ftse_data)
    ftse_data = create_indexData_table(solution_df, given_date, asset_type=asset_type)

    """ new code; can place in class"""
    bond_curves = helpers.get_bond_curves(given_date)
    shock_tables = helpers.create_shock_tables(bond_curves) # use a rating for bond classes (shock tables is general tho, can be decoupled outside and passed
    # Shock can be a class inherited by Curves or v.v.
    """end of new code"""



    # Load asset mix for the specified asset type and adjust names if needed
    df_public, df_private, df_mortgage = bench.reading_asset_mix(given_date, curMonthBS=curMonthBs)
    """
    if asset_type == 'private':
        asset_mix = df_private
    elif asset_type == 'mortgage':
        asset_mix = df_mortgage
    else:
        asset_mix = df_public
    asset_mix.rename(index={'corporateAAA_AA': 'CorporateAAA_AA', 'corporateA': 'CorporateA', 'corporateBBB': 'CorporateBBB'}, inplace=True)
    asset_mix.rename(columns={'Accum': 'ACCUM', 'group': 'GROUP', 'np': 'NONPAR', 'Payout': 'PAYOUT', 'Total': 'TOTAL',
                              'ul': 'UNIVERSAL', 'Surplus': 'SURPLUS'}, inplace=True)

    # Prepare the cashflow data for each rating
    cf = helpers.create_cf_tables(helpers.get_ftse_data(given_date))
    cfs = {}
    summed_cfs_dict = {}
    """
    if asset_type == 'private':
        asset_mix = df_private
        asset_mix.rename(
            index={'corporateAAA_AA': 'CorporateAAA_AA', 'corporateA': 'CorporateA', 'corporateBBB': 'CorporateBBB'},
            inplace=True)
    elif asset_type == 'mortgage':
        asset_mix = df_mortgage
        asset_mix.rename(index={'corporateBBB': 'CorporateBBB'}, inplace=True)
    else:
        asset_mix = df_public
        asset_mix.rename(
            index={'corporateAAA_AA': 'CorporateAAA_AA', 'corporateA': 'CorporateA', 'corporateBBB': 'CorporateBBB'},
            inplace=True)

    asset_mix.rename(columns={'Accum': 'ACCUM', 'group': 'GROUP', 'np': 'NONPAR', 'Payout': 'PAYOUT', 'Total': 'TOTAL', 'ul': 'UNIVERSAL', 'Surplus':'SURPLUS'}, inplace=True)
    cf = helpers.create_cf_tables(helpers.get_ftse_data(given_date))
    cfs = {}

    summed_cfs_dict = {}

    # date_range = pd.date_range(given_date, periods=420, freq='M')  # 420 months for 35 years
    # benchmarking_solution.rename(index={'corporateAAA_AA': 'CorporateAAA_AA', 'corporateA': 'CorporateA', 'corporateBBB': 'CorporateBBB'}) # renames but boilerplate code here

    for portfolio in ['NONPAR', 'GROUP', 'PAYOUT', 'ACCUM', 'UNIVERSAL', 'SURPLUS', 'TOTAL']:
        date_range = pd.date_range(given_date, periods=420, freq='M')  # 420 months for 35 years
        # Initialize DataFrames to hold summed cashflows and carry data for each portfolio
        summed_cfs = pd.DataFrame({'date': date_range})
        carry_table = pd.DataFrame(columns=['Federal', 'Provincial', 'CorporateAAA_AA', 'CorporateA', 'CorporateBBB'],
                                   index=['market Value', 'Average Yield'])

        """interesting: can probably replace with my code"""
        if asset_type == 'mortgage':
            portfolio_solution = benchmarking_solution.loc[benchmarking_solution['portfolio'] == portfolio].set_index(
                'rating').drop(columns='portfolio').rename(index={'corporateBBB': 'CorporateBBB'})
        else:
            portfolio_solution = benchmarking_solution.loc[benchmarking_solution['portfolio'] == portfolio].set_index(
                'rating').drop(columns='portfolio').rename(
                index={'corporateAAA_AA': 'CorporateAAA_AA', 'corporateA': 'CorporateA',
                       'corporateBBB': 'CorporateBBB'})

        """end of lol"""

        # Isolate the solution weights for this portfolio, adjusting by asset type and rating
        # TODO: I commented out this line of code:
        # portfolio_solution = benchmarking_solution.loc[benchmarking_solution['portfolio'] == portfolio].set_index('rating') # This might be the line of code that was causing the CorpBBB Error
        for rating in ['Federal', 'Provincial', 'CorporateAAA_AA', 'CorporateA', 'CorporateBBB']:
            # Skip ratings that are not applicable for this asset type
            if ((asset_type == 'mortgage') & ((portfolio == 'UNIVERSAL') or ((rating != 'Federal') & (rating != 'CorporateBBB')))) or \
               ((asset_type == 'private') & ((rating == 'Federal') or (rating == 'Provincial'))):
                carry_table.loc['market Value', rating] = 0
                carry_table.loc['Average Yield', rating] = 0
                summed_cfs[rating] = 0
                continue

            """part of new code, can fix by Shock class and retrieve for Curve etc"""
            ups = shock_tables[rating + ' - Up'] # can also be down lmao; just specific to the RATING not the up/down
            pv_bond_curve = ups.iloc[:, 0]
            """ end of new code"""
            # Dataframe
            cfs_rating_df = cf[rating].iloc[:, 3:] # Shape: (70, 70) where
            # (rows: buckets, columns: term_intervals (time))

            # Arrays or scalars (numpy)
            pv_array = cf[rating + 'PV'].values # Shape: (70,)

            # Replace pv_array with pv_actual TODO!:
            # Select the relevant portion of the cashflows DataFrame
            # cashflows_selected = cfs_rating_df = cashflows[rating].iloc[:70, 3:]

            # Perform element-wise multiplication and then sum along the rows
            # # pv_vectorized = (cashflows_selected * ups.iloc[:, 0].values).sum(axis=1) # vectorized equivalent to green code
            # # pv_vectorized = (cfs_rating_df * ups.iloc[:, 0].values).sum(axis=1)  # vectorized equivalent to green code
            # keep:
            # pv_bond_curve = ups.iloc[:, 0]
            pv_vectorized = cfs_rating_df.values @ ups.iloc[:, 0].values
            pv_array = pv_vectorized # Shape: (70,) - YUP as needed or expected!
            # Can print actual PV if wished
            # print(pv_array)

            # write to excel
            cur_date = given_date.strftime('%Y%m%d')
            path = os.path.join(sysenv.get('PORTFOLIO_ATTRIBUTION_DIR'), 'Benchmarking', 'Test', 'benchmarking_outputs',
                                'Brenda', 'pv_actuals', cur_date)
            os.makedirs(path, exist_ok=True)

            pv_array_df = pd.DataFrame(pv_array)

            # """temp comment out
            file_path = os.path.join(path, f'{rating}_pv_actuals_of_cfs_{cur_date}.xlsx')
            if not os.path.exists(file_path):
                with pd.ExcelWriter(file_path) as writer:
                    pv_array_df.to_excel(writer, sheet_name='Sheet1', index=False)  # Rows are payments, cols are buckets
            # """
            # end of write to excel

            # green code:
            #pv_array = np.sum(cf[rating].iloc[i, 3:] * ups.iloc[:, 0])  # it selects the row, nice (row, which are a bucket) - and ups.iloc[:,0] holds the PV values; of discounted curves
            #average_sensitivity.iloc[x, i + 1] = avg_sensitivity.iloc[x, i + 1] / pv

            solutions_values = portfolio_solution.loc[rating].values # Shape: (6,)
            market_value = asset_mix.loc[rating, portfolio] # Shape: ()

            # 1. Scale solutions up by market_value (PV of solutions):
            sol_scaled_mv = market_value * solutions_values # Shape: (6,), can be array of 0s
            # where (weights for 6 buckets array)

            sol_scaled_mv = np.nan_to_num(sol_scaled_mv, nan=0.0)

            # Dataframes: Cashflow calculations

            # 2. Scale cfs down by PV:
            cfs_rating_df = cfs_rating_df.div(pv_array, axis=0) # Applies pv_array on the columns of cfs_rating_df
                                                                #  i.e., across the buckets (since PV is arr of PV of *buckets*)
            # TODO: See if can replace the numpy to df
            cfs_rating_df = cfs_rating_df.replace([np.inf, -np.inf], np.nan).fillna(0) # Replace inf and NaN values with 0
            # Shape: (70, 70) where
            # (rows: buckets, columns: term_intervals (time), values: scaled by PV)

            weights_df = weights[rating].iloc[:, 3:] # Shape: (70, 6) where
            # (rows: buckets (70), columns: buckets (intervals))
            #  values: percentage of each bucket 70 in the bucket 6 intervals.

            # Remember that A @ B applies the columns of B on the rows of A to produce the column elements of C (result)

            # 3. Apply weight transformation to cfs to aggregate into 6 buckets (result: 70 time, 6 buckets, values cfs):

            # cfs_rating_df.T has shape of (time: 70, buckets: 70)
            # Aggregates the 70 buckets into 6 buckets

            # can have PV actuals (pv of cashflows) written to excel and the ftse pvs#  TODO!:


            cfs_condensed_numpy = cfs_rating_df.values.T @ weights_df.values # Shape: (time: 70, buckets: 6)
            # (time: 70, buckets: 6)

            # Cashflows in 6 buckets divided by PV
            cfs_aggregated_6_buckets = np.nan_to_num(cfs_condensed_numpy, nan=0.0) # Shape: (time: 70, buckets: 6) for (row, col)


            # 4. Print or write to excel for PV adjusted CFs aggregated in 6 buckets
            cfs_condensed_df = pd.DataFrame(cfs_aggregated_6_buckets)

            cur_date = given_date.strftime('%Y%m%d')
            path = os.path.join(sysenv.get('PORTFOLIO_ATTRIBUTION_DIR'), 'Benchmarking', 'Test', 'benchmarking_outputs',
                                'Brenda', 'benchmarking_tables', cur_date)
            os.makedirs(path, exist_ok=True)

            #"""temp comment out
            file_path = os.path.join(path, f'{rating}_CFs_divided_by_PV_w_row_time_col_6_buckets{cur_date}.xlsx')
            if not os.path.exists(file_path):
                with pd.ExcelWriter(file_path) as writer:
                    cfs_condensed_df.to_excel(writer, sheet_name='Sheet1', index=False)   # Rows are payments, cols are buckets
            #"""
            # 5. Perform a SUMPRODUCT of matrix and solutions on correct dimensions

            # matrix * array performs PRODUCT of array on buckets of matrix - applies on each row
            # then, matrix @ array performs SUMPRODUCT (it completes the sum across the cols, or of each row, step after it)
            # (so, the second performs the summation or dot product across each row)

            # cfs_aggregated_6_buckets has (time, buckets) and sol_scaled_mv has (buckets'_weights)
            # so scales the buckets for solutions :) across the cashflow times

            # Shape: (70,) yay!
            # final_CFs_rating_arr = np.nan_to_num((cfs_rating_df.values.T @ weights_df.values), nan=0.0) @ np.nan_to_num(sol_scaled_mv, nan=0.0)
            final_CFs_rating_arr = cfs_aggregated_6_buckets @ sol_scaled_mv # thanks, braodcasting, for applying array across the cols (it broadcasts array to be a col here from matrix # 2)
            # Note that portfolio_solution for NONPAR portfolio and Federal rating is 0, so that this is 0 as well - could put a condition that checks for this rather
            #  than doing all the operation? e.g., if portfolio_solution = 0, then continue (skip) with the 0 final cfs lol
            # NOTE: final_CFs final_CFs_rating_arr are in semi_annual payments across 70 terms for a rating (so an array)

            # date_range = pd.date_range(given_date, periods=420, freq='M')
            # summed_cfs = pd.DataFrame({'date': date_range})
            # summed_cfs['rating'] = 0

            # TODO: can fix to be better!
            # Generate half-year dates
            half_year_dates = pd.date_range(given_date, periods=70, freq='6M')

            # Create an indexer to populate summed_cfs['rating']
            for i, date in enumerate(half_year_dates):
                summed_cfs.loc[summed_cfs['date'] == date, rating] = final_CFs_rating_arr[i]


            """
            date_range = pd.date_range(start=given_date, periods=420, freq='M')
            # Step 5: Directly populate semi-annual cashflows into the monthly structure
            # semi_annual_totals = weighted_cf.sum(axis=1)  # Shape (70,)
            semi_annual_indices = np.arange(0, 420, 6)  # Semi-annual positions for 420 months (every 6 months)
            # Initialize the full monthly cashflows array with zeros
            monthly_cashflows = np.zeros(420)
            # Populate semi-annual positions with the calculated cashflows
            monthly_cashflows[semi_annual_indices] = semi_annual_totals
            # Assign this to the DataFrame for summed_cfs
            summed_cfs[rating] = monthly_cashflows













            # Step 1: Normalize the cashflows by their present values
            normalized_cf = cf[rating].iloc[:, 3:].div(cf[rating + 'PV'].values, axis=0)
            normalized_cf = normalized_cf.replace([np.inf, -np.inf], np.nan).fillna(0)  # Replace NaN and inf values

            # Step 2: Scale normalized cashflows by the market value
            market_value = asset_mix.loc[rating, portfolio]
            scaled_cf = normalized_cf * market_value  # Shape remains (70, 70)

            # Step 3: Expand solution_weights to match the shape of scaled_cf
            # TODO: maybe should multiply by the weights!
            solution_weights = portfolio_solution.loc[rating].values #[:-1]#.reshape(-1, 1)[:-1]  # Shape (6,)
            # shape (7, 1) without the [:-1] part
            expanded_weights = pd.DataFrame(np.zeros((70, 70)))

            interval_mapping = {
                0: range(0, 12),           # First 12 columns
                1: range(12, 22),          # Next 10 columns
                2: range(22, 32),          # Next 10 columns
                3: range(32, 42),          # Next 10 columns
                4: range(42, 55),          # Next 13 columns
                5: range(55, 70)           # Last 15 columns
            }

            # Apply solution weights to the corresponding intervals in expanded_weights
            for i, col_range in interval_mapping.items():
                expanded_weights.iloc[:, col_range] = solution_weights[i]

            # print(expanded_weights.shape) # Shape: (70, 70)
            # expanded_weights indices (count up by 1) don't match that of scaled_cf indices (count up by 0.5) so can
            #  use .values to convert to numpy
            #  if don't do this, resulting df has shape of (105, 105)

            # Step 4: Element-wise multiply the expanded weights with the scaled cashflows

            weighted_cf_np = scaled_cf.values * expanded_weights.values  # Shape (70, 70)
            # print(weighted_cf.shape)
            weighted_cf = weighted_cf_np

            # TODO: Runs fine until here

            # Step 5: Set up the final output by summing each row and aligning with semi-annual intervals
            semi_annual_totals = weighted_cf.sum(axis=1)  # Sum across columns to collapse to semi-annual totals
            # semi_annual_dates = pd.date_range(start=given_date, periods=70, freq='6M')
            # semi_annual_cashflows = pd.DataFrame({'date': semi_annual_dates, 'cashflow': semi_annual_totals}).set_index('date')
            # print(semi_annual_totals.shape) # Shape: (70,)

            # TODO: Inserted this to hope to work (replacing above comments)
            date_range = pd.date_range(start=given_date, periods=420, freq='M')
            # Step 5: Directly populate semi-annual cashflows into the monthly structure
            # semi_annual_totals = weighted_cf.sum(axis=1)  # Shape (70,)
            semi_annual_indices = np.arange(0, 420, 6)  # Semi-annual positions for 420 months (every 6 months)
            # Initialize the full monthly cashflows array with zeros
            monthly_cashflows = np.zeros(420)
            # Populate semi-annual positions with the calculated cashflows
            monthly_cashflows[semi_annual_indices] = semi_annual_totals
            # Assign this to the DataFrame for summed_cfs
            summed_cfs[rating] = monthly_cashflows

            # Convert to a 420-row format where non-semi-annual months are zero
            #monthly_cashflows = semi_annual_cashflows.reindex(date_range, fill_value=0)
            #summed_cfs[rating] = monthly_cashflows['cashflow'].reset_index(drop=True).reindex(range(420), fill_value=0)

            """
            # fill summed_cfs NAN or summed_cfs[rating] NaN with 0
            df = ftse_data.loc[ftse_data['RatingBucket'] == rating]
            if df['Benchmark ' + portfolio + ' weight'].sum() == 0:
                yield1 = 0
            else:
                yield1 = (df['Benchmark ' + portfolio + ' weight'] * df['yield']).sum() / df[
                    'Benchmark ' + portfolio + ' weight'].sum()
            carry_table.loc['market Value', rating] = market_value
            carry_table.loc['Average Yield', rating] = yield1

            """
            # Carry table calculations (optional, depending on analysis needs)
            df = ftse_data.loc[ftse_data['RatingBucket'] == rating]
            yield1 = (df['Benchmark ' + portfolio + ' weight'] * df['yield']).sum() / max(1, df['Benchmark ' + portfolio + ' weight'].sum())  # yoo this is how you weight it!
            carry_table.loc['market Value', rating] = market_value
            # Runs fine until here?
            carry_table.loc['Average Yield', rating] = yield1 # here line cannot evaluate - guess the carry table and bm codes lol to match - i can fix and und later - more consistent would be nice haha - less hardcoding in gen
            """

        # TODO: Runs fine until here

        # Format and finalize DataFrame with carry table
        summed_cfs['date'] = pd.to_datetime(summed_cfs['date']).dt.strftime('%b-%Y') # is it this???
        summed_cfs = pd.concat([carry_table, summed_cfs.set_index('date')])

        # Store results in the final output dictionary
        summed_cfs_dict[portfolio] = summed_cfs.fillna(0)

    return summed_cfs_dict



def create_summed_cashflow_tables_without_curves_WORKING_with_oldPV(solution_df, given_date, asset_type='public', curMonthBs=False):
    # Adjust solution_df portfolio names to standardize column names for processing
    benchmarking_solution = solution_df.copy()
    benchmarking_solution.rename(columns={5: 6, 4: 5, 3: 4, 2: 3, 1: 2, 0: 1}, inplace=True)
    benchmarking_solution['portfolio'] = benchmarking_solution['portfolio'].replace({'Total': 'TOTAL',
                                                                                     'np': 'NONPAR',
                                                                                     'group': 'GROUP',
                                                                                     'Accum': 'ACCUM',
                                                                                     'Payout': 'PAYOUT',
                                                                                     'ul': 'UNIVERSAL',
                                                                                     'Surplus': 'SURPLUS'})

                                                                                     # Load necessary FTSE data, weights, and asset mix information
    ftse_data = helpers.get_ftse_data(given_date)
    weights, totals = helpers.create_weight_tables(ftse_data)
    ftse_data = create_indexData_table(solution_df, given_date, asset_type=asset_type)

    # Load asset mix for the specified asset type and adjust names if needed
    df_public, df_private, df_mortgage = bench.reading_asset_mix(given_date, curMonthBS=curMonthBs)
    """
    if asset_type == 'private':
        asset_mix = df_private
    elif asset_type == 'mortgage':
        asset_mix = df_mortgage
    else:
        asset_mix = df_public
    asset_mix.rename(index={'corporateAAA_AA': 'CorporateAAA_AA', 'corporateA': 'CorporateA', 'corporateBBB': 'CorporateBBB'}, inplace=True)
    asset_mix.rename(columns={'Accum': 'ACCUM', 'group': 'GROUP', 'np': 'NONPAR', 'Payout': 'PAYOUT', 'Total': 'TOTAL',
                              'ul': 'UNIVERSAL', 'Surplus': 'SURPLUS'}, inplace=True)

    # Prepare the cashflow data for each rating
    cf = helpers.create_cf_tables(helpers.get_ftse_data(given_date))
    cfs = {}
    summed_cfs_dict = {}
    """
    if asset_type == 'private':
        asset_mix = df_private
        asset_mix.rename(
            index={'corporateAAA_AA': 'CorporateAAA_AA', 'corporateA': 'CorporateA', 'corporateBBB': 'CorporateBBB'},
            inplace=True)
    elif asset_type == 'mortgage':
        asset_mix = df_mortgage
        asset_mix.rename(index={'corporateBBB': 'CorporateBBB'}, inplace=True)
    else:
        asset_mix = df_public
        asset_mix.rename(
            index={'corporateAAA_AA': 'CorporateAAA_AA', 'corporateA': 'CorporateA', 'corporateBBB': 'CorporateBBB'},
            inplace=True)

    asset_mix.rename(columns={'Accum': 'ACCUM', 'group': 'GROUP', 'np': 'NONPAR', 'Payout': 'PAYOUT', 'Total': 'TOTAL', 'ul': 'UNIVERSAL', 'Surplus':'SURPLUS'}, inplace=True)
    cf = helpers.create_cf_tables(helpers.get_ftse_data(given_date))
    cfs = {}

    summed_cfs_dict = {}

    # date_range = pd.date_range(given_date, periods=420, freq='M')  # 420 months for 35 years
    # benchmarking_solution.rename(index={'corporateAAA_AA': 'CorporateAAA_AA', 'corporateA': 'CorporateA', 'corporateBBB': 'CorporateBBB'}) # renames but boilerplate code here

    for portfolio in ['NONPAR', 'GROUP', 'PAYOUT', 'ACCUM', 'UNIVERSAL', 'SURPLUS', 'TOTAL']:
        date_range = pd.date_range(given_date, periods=420, freq='M')  # 420 months for 35 years
        # Initialize DataFrames to hold summed cashflows and carry data for each portfolio
        summed_cfs = pd.DataFrame({'date': date_range})
        carry_table = pd.DataFrame(columns=['Federal', 'Provincial', 'CorporateAAA_AA', 'CorporateA', 'CorporateBBB'],
                                   index=['market Value', 'Average Yield'])

        """interesting: can probably replace with my code"""
        if asset_type == 'mortgage':
            portfolio_solution = benchmarking_solution.loc[benchmarking_solution['portfolio'] == portfolio].set_index(
                'rating').drop(columns='portfolio').rename(index={'corporateBBB': 'CorporateBBB'})
        else:
            portfolio_solution = benchmarking_solution.loc[benchmarking_solution['portfolio'] == portfolio].set_index(
                'rating').drop(columns='portfolio').rename(
                index={'corporateAAA_AA': 'CorporateAAA_AA', 'corporateA': 'CorporateA',
                       'corporateBBB': 'CorporateBBB'})

        """end of lol"""

        # Isolate the solution weights for this portfolio, adjusting by asset type and rating
        # TODO: I commented out this line of code:
        # portfolio_solution = benchmarking_solution.loc[benchmarking_solution['portfolio'] == portfolio].set_index('rating') # This might be the line of code that was causing the CorpBBB Error
        for rating in ['Federal', 'Provincial', 'CorporateAAA_AA', 'CorporateA', 'CorporateBBB']:
            # Skip ratings that are not applicable for this asset type
            if ((asset_type == 'mortgage') & ((portfolio == 'UNIVERSAL') or ((rating != 'Federal') & (rating != 'CorporateBBB')))) or \
               ((asset_type == 'private') & ((rating == 'Federal') or (rating == 'Provincial'))):
                carry_table.loc['market Value', rating] = 0
                carry_table.loc['Average Yield', rating] = 0
                summed_cfs[rating] = 0
                continue

            # Dataframe
            cfs_rating_df = cf[rating].iloc[:, 3:] # Shape: (70, 70) where
            # (rows: buckets, columns: term_intervals (time))

            # Arrays or scalars (numpy)
            pv_array = cf[rating + 'PV'].values # Shape: (70,)
            solutions_values = portfolio_solution.loc[rating].values # Shape: (6,)
            market_value = asset_mix.loc[rating, portfolio] # Shape: ()

            # 1. Scale solutions up by market_value (PV of solutions):
            sol_scaled_mv = market_value * solutions_values # Shape: (6,), can be array of 0s
            # where (weights for 6 buckets array)

            sol_scaled_mv = np.nan_to_num(sol_scaled_mv, nan=0.0)

            # Dataframes: Cashflow calculations

            # 2. Scale cfs down by PV:
            cfs_rating_df = cfs_rating_df.div(pv_array, axis=0) # Applies pv_array on the columns of cfs_rating_df
                                                                #  i.e., across the buckets (since PV is arr of PV of *buckets*)
            # TODO: See if can replace the numpy to df
            cfs_rating_df = cfs_rating_df.replace([np.inf, -np.inf], np.nan).fillna(0) # Replace inf and NaN values with 0
            # Shape: (70, 70) where
            # (rows: buckets, columns: term_intervals (time), values: scaled by PV)

            weights_df = weights[rating].iloc[:, 3:] # Shape: (70, 6) where
            # (rows: buckets (70), columns: buckets (intervals))
            #  values: percentage of each bucket 70 in the bucket 6 intervals.

            # Remember that A @ B applies the columns of B on the rows of A to produce the column elements of C (result)

            # 3. Apply weight transformation to cfs to aggregate into 6 buckets (result: 70 time, 6 buckets, values cfs):

            # cfs_rating_df.T has shape of (time: 70, buckets: 70)
            # Aggregates the 70 buckets into 6 buckets


            cfs_condensed_numpy = cfs_rating_df.values.T @ weights_df.values # Shape: (time: 70, buckets: 6)
            # (time: 70, buckets: 6)

            # Cashflows in 6 buckets divided by PV
            cfs_aggregated_6_buckets = np.nan_to_num(cfs_condensed_numpy, nan=0.0) # Shape: (time: 70, buckets: 6) for (row, col)


            # 4. Print or write to excel for PV adjusted CFs aggregated in 6 buckets
            cfs_condensed_df = pd.DataFrame(cfs_aggregated_6_buckets)

            cur_date = given_date.strftime('%Y%m%d')
            path = os.path.join(sysenv.get('PORTFOLIO_ATTRIBUTION_DIR'), 'Benchmarking', 'Test', 'benchmarking_outputs',
                                'Brenda', 'benchmarking_tables', cur_date)
            os.makedirs(path, exist_ok=True)

            #"""temp comment out
            file_path = os.path.join(path, f'{rating}_CFs_divided_by_PV_w_row_time_col_6_buckets{cur_date}.xlsx')
            if not os.path.exists(file_path):
                with pd.ExcelWriter(file_path) as writer:
                    cfs_condensed_df.to_excel(writer, sheet_name='Sheet1', index=False)   # Rows are payments, cols are buckets
            #"""
            # 5. Perform a SUMPRODUCT of matrix and solutions on correct dimensions

            # matrix * array performs PRODUCT of array on buckets of matrix - applies on each row
            # then, matrix @ array performs SUMPRODUCT (it completes the sum across the cols, or of each row, step after it)
            # (so, the second performs the summation or dot product across each row)

            # cfs_aggregated_6_buckets has (time, buckets) and sol_scaled_mv has (buckets'_weights)
            # so scales the buckets for solutions :) across the cashflow times

            # Shape: (70,) yay!
            # final_CFs_rating_arr = np.nan_to_num((cfs_rating_df.values.T @ weights_df.values), nan=0.0) @ np.nan_to_num(sol_scaled_mv, nan=0.0)
            final_CFs_rating_arr = cfs_aggregated_6_buckets @ sol_scaled_mv # thanks, braodcasting, for applying array across the cols (it broadcasts array to be a col here from matrix # 2)
            # Note that portfolio_solution for NONPAR portfolio and Federal rating is 0, so that this is 0 as well - could put a condition that checks for this rather
            #  than doing all the operation? e.g., if portfolio_solution = 0, then continue (skip) with the 0 final cfs lol
            # NOTE: final_CFs final_CFs_rating_arr are in semi_annual payments across 70 terms for a rating (so an array)

            # date_range = pd.date_range(given_date, periods=420, freq='M')
            # summed_cfs = pd.DataFrame({'date': date_range})
            # summed_cfs['rating'] = 0

            # TODO: can fix to be better!
            # Generate half-year dates
            half_year_dates = pd.date_range(given_date, periods=70, freq='6M')

            # Create an indexer to populate summed_cfs['rating']
            for i, date in enumerate(half_year_dates):
                summed_cfs.loc[summed_cfs['date'] == date, rating] = final_CFs_rating_arr[i]


            """
            date_range = pd.date_range(start=given_date, periods=420, freq='M')
            # Step 5: Directly populate semi-annual cashflows into the monthly structure
            # semi_annual_totals = weighted_cf.sum(axis=1)  # Shape (70,)
            semi_annual_indices = np.arange(0, 420, 6)  # Semi-annual positions for 420 months (every 6 months)
            # Initialize the full monthly cashflows array with zeros
            monthly_cashflows = np.zeros(420)
            # Populate semi-annual positions with the calculated cashflows
            monthly_cashflows[semi_annual_indices] = semi_annual_totals
            # Assign this to the DataFrame for summed_cfs
            summed_cfs[rating] = monthly_cashflows













            # Step 1: Normalize the cashflows by their present values
            normalized_cf = cf[rating].iloc[:, 3:].div(cf[rating + 'PV'].values, axis=0)
            normalized_cf = normalized_cf.replace([np.inf, -np.inf], np.nan).fillna(0)  # Replace NaN and inf values

            # Step 2: Scale normalized cashflows by the market value
            market_value = asset_mix.loc[rating, portfolio]
            scaled_cf = normalized_cf * market_value  # Shape remains (70, 70)

            # Step 3: Expand solution_weights to match the shape of scaled_cf
            # TODO: maybe should multiply by the weights!
            solution_weights = portfolio_solution.loc[rating].values #[:-1]#.reshape(-1, 1)[:-1]  # Shape (6,)
            # shape (7, 1) without the [:-1] part
            expanded_weights = pd.DataFrame(np.zeros((70, 70)))

            interval_mapping = {
                0: range(0, 12),           # First 12 columns
                1: range(12, 22),          # Next 10 columns
                2: range(22, 32),          # Next 10 columns
                3: range(32, 42),          # Next 10 columns
                4: range(42, 55),          # Next 13 columns
                5: range(55, 70)           # Last 15 columns
            }

            # Apply solution weights to the corresponding intervals in expanded_weights
            for i, col_range in interval_mapping.items():
                expanded_weights.iloc[:, col_range] = solution_weights[i]

            # print(expanded_weights.shape) # Shape: (70, 70)
            # expanded_weights indices (count up by 1) don't match that of scaled_cf indices (count up by 0.5) so can
            #  use .values to convert to numpy
            #  if don't do this, resulting df has shape of (105, 105)

            # Step 4: Element-wise multiply the expanded weights with the scaled cashflows

            weighted_cf_np = scaled_cf.values * expanded_weights.values  # Shape (70, 70)
            # print(weighted_cf.shape)
            weighted_cf = weighted_cf_np

            # TODO: Runs fine until here

            # Step 5: Set up the final output by summing each row and aligning with semi-annual intervals
            semi_annual_totals = weighted_cf.sum(axis=1)  # Sum across columns to collapse to semi-annual totals
            # semi_annual_dates = pd.date_range(start=given_date, periods=70, freq='6M')
            # semi_annual_cashflows = pd.DataFrame({'date': semi_annual_dates, 'cashflow': semi_annual_totals}).set_index('date')
            # print(semi_annual_totals.shape) # Shape: (70,)

            # TODO: Inserted this to hope to work (replacing above comments)
            date_range = pd.date_range(start=given_date, periods=420, freq='M')
            # Step 5: Directly populate semi-annual cashflows into the monthly structure
            # semi_annual_totals = weighted_cf.sum(axis=1)  # Shape (70,)
            semi_annual_indices = np.arange(0, 420, 6)  # Semi-annual positions for 420 months (every 6 months)
            # Initialize the full monthly cashflows array with zeros
            monthly_cashflows = np.zeros(420)
            # Populate semi-annual positions with the calculated cashflows
            monthly_cashflows[semi_annual_indices] = semi_annual_totals
            # Assign this to the DataFrame for summed_cfs
            summed_cfs[rating] = monthly_cashflows

            # Convert to a 420-row format where non-semi-annual months are zero
            #monthly_cashflows = semi_annual_cashflows.reindex(date_range, fill_value=0)
            #summed_cfs[rating] = monthly_cashflows['cashflow'].reset_index(drop=True).reindex(range(420), fill_value=0)

            """
            # fill summed_cfs NAN or summed_cfs[rating] NaN with 0
            df = ftse_data.loc[ftse_data['RatingBucket'] == rating]
            if df['Benchmark ' + portfolio + ' weight'].sum() == 0:
                yield1 = 0
            else:
                yield1 = (df['Benchmark ' + portfolio + ' weight'] * df['yield']).sum() / df[
                    'Benchmark ' + portfolio + ' weight'].sum()
            carry_table.loc['market Value', rating] = market_value
            carry_table.loc['Average Yield', rating] = yield1

            """
            # Carry table calculations (optional, depending on analysis needs)
            df = ftse_data.loc[ftse_data['RatingBucket'] == rating]
            yield1 = (df['Benchmark ' + portfolio + ' weight'] * df['yield']).sum() / max(1, df['Benchmark ' + portfolio + ' weight'].sum())  # yoo this is how you weight it!
            carry_table.loc['market Value', rating] = market_value
            # Runs fine until here?
            carry_table.loc['Average Yield', rating] = yield1 # here line cannot evaluate - guess the carry table and bm codes lol to match - i can fix and und later - more consistent would be nice haha - less hardcoding in gen
            """

        # TODO: Runs fine until here

        # Format and finalize DataFrame with carry table
        summed_cfs['date'] = pd.to_datetime(summed_cfs['date']).dt.strftime('%b-%Y') # is it this???
        summed_cfs = pd.concat([carry_table, summed_cfs.set_index('date')])

        # Store results in the final output dictionary
        summed_cfs_dict[portfolio] = summed_cfs.fillna(0)

    return summed_cfs_dict


# For the Custom Benchmarks
''' This function is currently used for creating the summary tables, which only contain info about the portfolio balances '''
def create_summary_table(given_date, asset_type='public', curMonthBs=False):
    df_public, df_private, df_mortgage = bench.reading_asset_mix(given_date, curMonthBS=curMonthBs)
    if asset_type == 'private':
        asset_mix = df_private
    elif asset_type == 'mortgage':
        asset_mix = df_mortgage
    else:
        asset_mix = df_public

    df = pd.DataFrame(0, index=['Portfolio Yield', 'Portfolio Duration', 'Portfolio Balance', 'quarterly expected income', 'Capital estimate'], columns=['Total', 'np', 'group', 'Accum', 'Payout', 'ul'])
    for portfolio in ['Total', 'np', 'group', 'Accum', 'Payout', 'ul']:
        df.loc['Portfolio Balance', portfolio] = sum(asset_mix[portfolio])
    df['SURPLUS'] = 0
    df.loc['Portfolio Balance', 'SURPLUS'] = df.loc['Portfolio Balance', 'Total'] - df[['np', 'group', 'Payout', 'Accum', 'ul']].sum(axis=1)['Portfolio Balance']

    df.rename(columns={'Total': 'TOTAL', 'np': 'NONPAR', 'group': 'GROUP', 'Accum': 'ACCUM', 'Payout': 'PAYOUT', 'ul': 'UNIVERSAL'}, inplace=True)
    return df


''' In this function the indexData tables are created. These are essentially the ftse constituents table but with added columns with the weights for each portfolio '''
def create_indexData_table(solution_df, given_date, asset_type='public', curMonthBs=False):
    benchmarking_solution = solution_df.copy()
    benchmarking_solution.rename(columns={5: 6, 4: 5, 3: 4, 2: 3, 1: 2, 0: 1}, inplace=True)
    benchmarking_solution['portfolio'] = benchmarking_solution['portfolio'].replace({'Total': 'TOTAL',
                                                                                     'np': 'NONPAR',
                                                                                     'group': 'GROUP',
                                                                                     'Accum': 'ACCUM',
                                                                                     'Payout': 'PAYOUT',
                                                                                     'ul': 'UNIVERSAL',
                                                                                     'Surplus': 'SURPLUS'})

    ftse_data = helpers.get_ftse_data(given_date)

    weights, totals = helpers.create_weight_tables(ftse_data)

    ''' Calculates the weight of bonds over 35 years within the ftse universe '''
    over_35y = (100 - ftse_data.loc[ftse_data['TermPt'] >= 35]['marketweight_noREITs'].sum()) / 100


    df_public, df_private, df_mortgage = bench.reading_asset_mix(given_date,curMonthBS=curMonthBs)

    if asset_type == 'private':
        asset_mix = df_private
        asset_mix.rename(index={'corporateAAA_AA': 'CorporateAAA_AA', 'corporateA': 'CorporateA', 'corporateBBB': 'CorporateBBB'}, inplace=True)

        totals = totals.drop(['Corporate', 'Provincial', 'Federal'])
    elif asset_type == 'mortgage':
        asset_mix = df_mortgage
        asset_mix.rename(index={'corporateBBB': 'CorporateBBB'}, inplace=True)

        totals = totals.drop(['Corporate', 'Provincial', 'CorporateAAA_AA', 'CorporateA'])
    else:
        asset_mix = df_public
        asset_mix.rename(index={'corporateAAA_AA': 'CorporateAAA_AA', 'corporateA': 'CorporateA', 'corporateBBB': 'CorporateBBB'}, inplace=True)

        totals = totals.drop(['Corporate'])

    asset_mix.rename(columns={'Accum': 'ACCUM', 'group': 'GROUP', 'np': 'NONPAR', 'Payout': 'PAYOUT', 'Total': 'TOTAL', 'ul': 'UNIVERSAL', 'Surplus':'SURPLUS'}, inplace=True)
    total_dollar_amount = sum(asset_mix['TOTAL'])
    print(total_dollar_amount)

    for portfolio in ['NONPAR', 'GROUP', 'PAYOUT', 'ACCUM', 'UNIVERSAL', 'TOTAL']:
        if (asset_type == 'mortgage') & (portfolio == 'UNIVERSAL'):
            ftse_data['Benchmark ' + portfolio + ' weight'] = 0
            continue
        # renaming only corpBBBs for mortgage because corpA and AAA_AAs not included
        if asset_type == 'mortgage':
            portfolio_solution = benchmarking_solution.loc[benchmarking_solution['portfolio'] == portfolio].set_index('rating').drop(columns='portfolio').rename(index={'corporateBBB': 'CorporateBBB'})
        else:
            portfolio_solution = benchmarking_solution.loc[benchmarking_solution['portfolio'] == portfolio].set_index('rating').drop(columns='portfolio').rename(index={'corporateAAA_AA': 'CorporateAAA_AA', 'corporateA': 'CorporateA', 'corporateBBB': 'CorporateBBB'})

        benchmark_weights = portfolio_solution.mul(asset_mix[portfolio], axis=0)
        benchmark_weights = benchmark_weights / asset_mix[portfolio].sum()
        benchmark_div_universe = (benchmark_weights / totals).fillna(0)
        if asset_type == 'mortgage':
            benchmark_div_universe.loc['Provincial'] = 0
            benchmark_div_universe.loc['CorporateAAA_AA'] = 0
            benchmark_div_universe.loc['CorporateA'] = 0
        elif asset_type == 'private':
            benchmark_div_universe.loc['Federal'] = 0
            benchmark_div_universe.loc['Provincial'] = 0
        ftse_data['Benchmark ' + portfolio + ' weight'] = ftse_data.apply(lambda row: 0 if row['bucket'] == 0 else benchmark_div_universe.loc[row['RatingBucket'], row['bucket']], axis=1)
        ftse_data['Benchmark ' + portfolio + ' weight'] = ftse_data['marketweight_noREITs'] * ftse_data['Benchmark ' + portfolio + ' weight'] / over_35y

    individual_portfolio_sums = asset_mix[['ACCUM', 'GROUP', 'NONPAR', 'PAYOUT', 'UNIVERSAL']].sum(axis=0)
    surplus_portfolio_balance = total_dollar_amount - sum(individual_portfolio_sums)
    ftse_data['Benchmark SURPLUS weight'] = ftse_data.apply(lambda row: ((row['Benchmark TOTAL weight'] * total_dollar_amount) - sum(row[['Benchmark ACCUM weight',
                                                                             'Benchmark GROUP weight',
                                                                             'Benchmark NONPAR weight',
                                                                             'Benchmark PAYOUT weight',
                                                                             'Benchmark UNIVERSAL weight']] * individual_portfolio_sums.values))/surplus_portfolio_balance, axis=1)
    ftse_data['Benchmark dollar investment'] = ftse_data['Benchmark TOTAL weight'] * total_dollar_amount


    return ftse_data

import parse_args as parse_args

def main_test():

    args, GivenDate, OU_Date = parse_args.get_user_info()

    ftse_data = helpers.get_ftse_data(GivenDate)

    weights, totals = helpers.create_weight_tables(ftse_data)
    #print(weights.shape)

    cashflows_granular = helpers.create_cf_tables(ftse_data)
    #print(cashflows_granular.shape)

    public_solution = bench.optimization(GivenDate, OU_Date, asset_type='public', swap=args.swap,
                                         curMonthBS=args.curMonthBS)
    # else:
    #     public_solution = bench.optimization(GivenDate, asset_type='public')

    summed_cashflows_public = create_summed_cashflow_tables(public_solution, GivenDate, asset_type='public',
                                                            curMonthBs=args.curMonthBS)





def main():

    args, GivenDate, OU_Date = parse_args.get_user_info()

    """

    parser = argparse.ArgumentParser()

    parser.add_argument("-d", "--GivenDate", type=str,
                        help="Use YYYY-MM-DD to set the Date for the calculation.")

    parser.add_argument("-o", "--OU_Date", type=str,
                        help="Use YYYY-MM-DD to use specific over_under_assetting file")

    #parser.add_argument('-c', '--create', action='store_true',
    #                    help='include this if the liabilities for the selected date have not yet been uploaded to the db')

    parser.add_argument('-s', '--swap', action='store_true',
                        help="Set to true if interest rate swap sensitivities are backed out")

    parser.add_argument('-cb', '--curMonthBS', action='store_true',
                        help='include to run economics with current month balance sheet instead of previous')

    parser.add_argument("-m", "--mortgage", action='store_true', help="Include to generate output for mortgages, or leave all 3 blank to do all 3")
    parser.add_argument("-pb", "--public", action='store_true', help="Include to generate output for publics, or leave all 3 blank to do all 3")
    parser.add_argument("-pv", "--private", action='store_true', help="Include to generate output for privates, or leave all 3 blank to do all 3")

    parser.add_argument("-j", "--jobname", type=str, default="UNSPECIFIED",
                        help="Specified Jobname")


    args = parser.parse_args()

    if args.GivenDate is None:
        GivenDate = dt.datetime.now()
    else:
        GivenDate = conversions.YYYYMMDDtoDateTime(args.GivenDate)

    if args.OU_Date is None:
        OU_Date = conversions.YYYYMMDDtoDateTime(args.GivenDate)
    else:
        OU_Date = conversions.YYYYMMDDtoDateTime(args.OU_Date)
    """
    #if args.create:
    #    if args.OU_Date:
    #        bench.reading_liabilities(OU_Date)
    #    else:
    #        bench.reading_liabilities(GivenDate)
    """new code, to use with decoupling (not sure if which prefer...for writing to excel it may depend, or as class can wrap as workflow external"""
    # Generate KRD Table for all assets (to feed to optim function and write later; keep in memory or write now)
    KRDs = bench.reading_asset_KRDs(GivenDate)
    """end of new code"""

    do_all = False
    if (args.mortgage==False) & (args.public==False) & (args.private==False):
        do_all = True

    if args.mortgage or do_all:
        mort_solution = bench.optimization(KRDs, GivenDate, OU_Date, asset_type='mortgage', curMonthBS=args.curMonthBS)
        # mort_solution = bench.optimization(GivenDate, OU_Date, asset_type='mortgage', curMonthBS=args.curMonthBS)


        summed_cashflows_mort = create_summed_cashflow_tables(mort_solution, GivenDate, asset_type='mortgage', curMonthBs=args.curMonthBS)
        summary_mort = create_summary_table(GivenDate, asset_type='mortgage', curMonthBs=args.curMonthBS)
        data_mort = create_indexData_table(mort_solution, GivenDate, asset_type='mortgage')

    if args.public or do_all:
        public_solution = bench.optimization(KRDs, GivenDate, OU_Date, asset_type='public', swap=args.swap, curMonthBS=args.curMonthBS)
            # else:
            #     public_solution = bench.optimization(GivenDate, asset_type='public')

        summed_cashflows_public = create_summed_cashflow_tables(public_solution, GivenDate, asset_type='public', curMonthBs=args.curMonthBS)

        # This was where the issue was??
        summary_public = create_summary_table(GivenDate, asset_type='public', curMonthBs=args.curMonthBS)
        data_public = create_indexData_table(public_solution, GivenDate, asset_type='public')

    if args.private or do_all:
        private_solution = bench.optimization(KRDs, GivenDate, OU_Date, asset_type='private', curMonthBS=args.curMonthBS)

        summed_cashflows_private = create_summed_cashflow_tables(private_solution, GivenDate, asset_type='private', curMonthBs=args.curMonthBS)
        summary_private = create_summary_table(GivenDate, asset_type='private', curMonthBs=args.curMonthBS)
        data_private = create_indexData_table(private_solution, GivenDate, asset_type='private')





    cur_date = GivenDate.strftime('%Y%m%d')

    folder_path = os.path.join(sysenv.get('PORTFOLIO_ATTRIBUTION_DIR'), 'Benchmarking', 'Test', 'benchmarking_outputs', 'Brenda', cur_date)

    custom_benchmarks_path = folder_path + '/Custom_benchmark_' + cur_date + '.xlsx'
    cfs_path = folder_path + '/CFs' + cur_date + '.xlsx'


    if not os.path.exists(folder_path):
        os.mkdir(folder_path)

        # can probably put these functionalities together to run as 1 pipeline...

    if not os.path.exists(custom_benchmarks_path):
        with pd.ExcelWriter(custom_benchmarks_path) as writer:
            if args.public or do_all:
                summary_public.to_excel(writer, sheet_name='summary_public')
                data_public.to_excel(writer, sheet_name='data_public', index=False)
            if args.private or do_all:
                summary_private.to_excel(writer, sheet_name='summary_private')
                data_private.to_excel(writer, sheet_name='data_private', index=False)
            
            if args.mortgage or do_all:
                summary_mort.to_excel(writer, sheet_name='summary_mort')
                data_mort.to_excel(writer, sheet_name='data_mort', index=False)
    else:
        print('custom benchmarks file for this date already exists - cant make a file with the same name')


    if not os.path.exists(cfs_path):
        with pd.ExcelWriter(cfs_path) as writer:
            for df in ['NONPAR', 'GROUP', 'PAYOUT', 'ACCUM', 'UNIVERSAL', 'TOTAL', 'SURPLUS']:

                if args.public or do_all:
                    summed_cashflows_public[df].to_excel(writer, sheet_name=('summed cfs public - ' + df), startrow=1)
                if args.private or do_all:
                    summed_cashflows_private[df].to_excel(writer, sheet_name=('summed cfs private - ' + df), startrow=1)

                if args.mortgage or do_all:
                    summed_cashflows_mort[df].to_excel(writer, sheet_name=('summed cfs mort - ' + df), startrow=1)
    else:
        print('cashflows file for this date already exists - cant make a file with the same name')

if __name__ == "__main__":
    main()

    # testing
    #import doctest
    #doctest.testmod()