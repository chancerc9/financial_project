"""
Name: cashflows_and_benchmark_tables.py

    legacy: similar to Generate_benchmarking_tables.py

Purpose:
    Generates the cashflows from solutions and custom_benchmarks file used for benchmarking processes by ALM or investment tech.

Functions:

Side effects:

"""
import os
import numpy as np
import pandas as pd
import datetime as dt

pd.set_option('display.width', 150)
from equitable.infrastructure import sysenv

import calculations as helpers
import solutions as model_portfolio


def create_summed_cashflow_tables(bond_curves: pd.DataFrame,
                                  FTSE_Universe_data: pd.DataFrame,
                                  IndexTable: pd.DataFrame,
                                  solution_df: pd.DataFrame,
                                  given_date: dt,
                                  asset_type='public',
                                  debug=False):
    """
    Creates cashflows from solutions.

    Assumptions:
cf[rating]: DataFrame with 73 columns total:
First 3 columns: [Bucket, Principal, Coupon]
Next 70 columns: Cashflow values at half-year increments from 0.5 to 35.0 years
weights[rating]: DataFrame with at least 9 columns total:
First 3 columns: [Term, Lower_Bound, Upper_Bound]
Next 6 columns: The weights for the 6 aggregated buckets
shock_tables[rating + ' - Up']: DataFrame providing discount factors or some shock adjustments with at least one column of length 70.
The function aggregates per portfolio and rating.

    Parameters:
      bond_curves: pd.DataFrame
      FTSE_Universe_data: pd.DataFrame
      IndexTable: pd.DataFrame
      solution_df: pd.DataFrame
      given_date: datetime-like, the starting date
      asset_type: str, one of ['public', 'private', 'mortgage']
      debug: bool, if True write intermediate outputs to Excel (future variable)

    Returns:
      summed_cfs_dict: dict of {portfolio: DataFrame} with aggregated cashflows and carry tables
    """

    # Data protection:
    benchmarking_solution = solution_df.copy()
    FTSE_Universe_data = FTSE_Universe_data.copy()
    ftse_data = IndexTable.copy()
    bond_curves = bond_curves.copy()

    # Rename columns and portfolios in benchmarking_solution_df
    portfolio_map = {
        'Total': 'TOTAL',
        'np': 'NONPAR',
        'group': 'GROUP',
        'Accum': 'ACCUM',
        'Payout': 'PAYOUT',
        'ul': 'UNIVERSAL',
        'Surplus': 'SURPLUS'
    }
    # rename_buckets_map = {5: 6, 4: 5, 3: 4, 2: 3, 1: 2, 0: 1}

    # benchmarking_solution.rename(columns=rename_buckets_map, inplace=True)
    # benchmarking_solution['portfolio'] = benchmarking_solution['portfolio'].replace(portfolio_map) # In place.
    # benchmarking_solution['rating'] = benchmarking_solution['rating'].str.replace(r'^([a-zA-Z])', lambda m: m.group(1).upper(), regex=True)

    # External helpers assumed:
    #   helpers.create_weight_tables(FTSE_Universe_data) -> (weights, totals)
    #   helpers.create_shock_tables(bond_curves, given_date, debug) -> shock_tables
    #   helpers.create_cf_tables(FTSE_Universe_data) -> cf (dict of rating->DataFrame)
    #   assets.reading_asset_mix(given_date) -> (df_public, df_private, df_mortgage)

    # Load necessary weights, shocks, cashflows and asset mix information
    weights, totals = helpers.create_weight_tables(FTSE_Universe_data)
    shock_tables = helpers.create_shock_tables(bond_curves, given_date, debug) # Shock can be a class inherited by Curves or vice versa.

    # Load asset mix for the specified asset type and adjust names if needed:
    df_public, df_private, df_mortgage = model_portfolio.reading_asset_mix(given_date)
    if asset_type == 'private':
        asset_mix = df_private
    elif asset_type == 'mortgage':
        asset_mix = df_mortgage
    else:
        asset_mix = df_public
    asset_mix.rename(columns=portfolio_map, inplace=True) # Future suggestion: possible suggestion to have renaming outside, as a wrapper function, if this is done.

    # Prepare the cashflow data for each rating
    cf = helpers.create_cf_tables(FTSE_Universe_data)

    # Monthly date range for 35 years
    date_range = pd.date_range(given_date, periods=420, freq='ME')      # Generate dates at the last day of every month
                                                                        # 420 months for 35 years

    # Ratings and portfolios of interest
    portfolio_tuple = ('NONPAR', 'GROUP', 'PAYOUT', 'ACCUM', 'UNIVERSAL', 'SURPLUS', 'TOTAL')
    rating_tuple = ('Federal', 'Provincial', 'CorporateAAA_AA', 'CorporateA', 'CorporateBBB')

    cur_date = given_date.strftime('%Y%m%d')

    # This function can now use the variables defined above.
    def write_debug_file(df: pd.DataFrame, file_name: str, subdir='benchmarking_tables', cur_date = given_date.strftime('%Y%m%d')):
    # if debug:
        import helpers as helper_function
        # cur_date_str = given_date.strftime('%Y%m%d')
        path = helper_function.build_and_ensure_directory(sysenv.get('PORTFOLIO_ATTRIBUTION_DIR'), 'Benchmarking', 'code_benchmarking_outputs', cur_date, 'debugging_steps', subdir)
        file_path = os.path.join(path, file_name)
        if not os.path.exists(file_path):
            with pd.ExcelWriter(file_path) as writer:
                df.to_excel(writer, sheet_name='Sheet1', index=False)


    summed_cfs_dict = {}

    for portfolio in portfolio_tuple:

        # Initialize DataFrames to hold summed cashflows and carry data for each asset, for this portfolio
        summed_cfs = pd.DataFrame({'date': date_range})
        carry_table = pd.DataFrame(columns=rating_tuple,
                                   index=['market Value', 'Average Yield'])

        # Retrieve solution data for this portfolio
        portfolio_solution = benchmarking_solution.loc[benchmarking_solution['portfolio'] == portfolio].set_index(
                'rating').drop(columns='portfolio')

        # Isolate the solution weights for this portfolio, adjusting by asset type and rating
        for rating in rating_tuple:

            # If rating not applicable for this asset type and portfolio combination, skip.

            # Code notes:
                # 'not in' is equivalent to not(A or B) which is (not A and not B) where [A, B] implies [A or B] when
                # interpreted with not in or in
                # hence 'in' is equivalent to in (A or B) for this language.

            if (
                (asset_type == 'mortgage' and ((portfolio == 'UNIVERSAL') or (rating not in ['Federal', 'CorporateBBB']))) or
                (asset_type == 'private' and (rating in ['Federal', 'Provincial']))
            ):
                carry_table.loc['market Value', rating] = 0
                carry_table.loc['Average Yield', rating] = 0
                summed_cfs[rating] = 0
                continue

            """retrieves the present value of curves, can implement Shock class and retrieve PV for Curve etc"""
            ups = shock_tables[rating + ' - Up']    # Can also be " - Down" ; just specific to the RATING not the up/down
            pv_bond_curve = ups.iloc[:, 0]          # Retrieves the present value of curves.
            """ end of possible future suggestion for clarity"""

            # Extract 70x70 CF matrix (skipping first 3 columns)
            cfs_rating_df = cf[rating].iloc[:, 3:]  # Shape: (70, 70) where
                                                    # (rows: buckets, columns: term_intervals (time))


            # Arrays or scalars (numpy): vectorized calculations where possible. #
            pv_array = cf[rating + 'PV'].values # Shape: (70,)



            # Perform element-wise multiplication and then sum along the rows to obtain an array of present value of cashflows for each bucket for this rating.
            pv_vectorized = cfs_rating_df.values @ ups.iloc[:, 0].values
                                        # Equivalent code for testing:
                                        # pv_vectorized = (cashflows_selected * ups.iloc[:, 0].values).sum(axis=1) # vectorized equivalent to green code
                                        # pv_vectorized = (cfs_rating_df * ups.iloc[:, 0].values).sum(axis=1)  # vectorized equivalent to green code
            pv_array = pv_vectorized # Shape: (70,) as needed.

            # Can print actual PV if wished
            # print(pv_array)

            # Write to excel:
            if debug:

                file_name = f'{rating}_pv_actuals_of_cfs_{cur_date}.xlsx'

                subdir = 'benchmarking_outputs'

                pv_array_df = pd.DataFrame(pv_array)
                write_debug_file(pv_array_df, file_name, subdir, cur_date)
            # end of write to excel.

            # green code (for testing - is used elsewhere in program - can change the other code if similarity exist):
                # pv_array = np.sum(cf[rating].iloc[i, 3:] * ups.iloc[:, 0])  # it selects the row, nice (row, which are a bucket) - and ups.iloc[:,0] holds the PV values; of discounted curves
                # average_sensitivity.iloc[x, i + 1] = avg_sensitivity.iloc[x, i + 1] / pv

            solutions_values = portfolio_solution.loc[rating].values # Shape: (6,)
            market_value = asset_mix.loc[rating, portfolio]          # Shape: ()

            # 1. Scale solutions up by market_value (PV of solutions):
            sol_scaled_mv = market_value * solutions_values # Shape: (6,), can be array of 0s
            # where (weights for 6 buckets array)
            sol_scaled_mv = np.nan_to_num(sol_scaled_mv, nan=0.0)



            # Dataframes: Cashflow calculations #


            # 2. Scale cfs down by PV:
            cfs_rating_df = cfs_rating_df.div(pv_array, axis=0) # Applies pv_array on the columns of cfs_rating_df
                                                                #  i.e., across the buckets (since PV is arr of PV of *buckets*)
            cfs_rating_df = cfs_rating_df.replace([np.inf, -np.inf], np.nan).fillna(0) # Replace inf and NaN values with 0
            # Shape: (70, 70) where
            # (rows: buckets, columns: term_intervals (time), values: scaled by PV)

            weights_df = weights[rating].iloc[:, 3:] # Shape: (70, 6) where
            # (rows: buckets (70), columns: buckets (intervals))
            #  values: percentage of each bucket 70 in the bucket 6 intervals.

            # Code note:
                # Remember that A @ B applies the columns of B on the rows of A to produce the column elements of C (result)



            # 3. Apply weight transformation to cfs to aggregate into 6 buckets (result: 70 time, 6 buckets, values cfs):

            # cfs_rating_df.T has shape of (time: 70, buckets: 70)
            # Aggregates the 70 buckets into 6 buckets

            # Future suggestion:
            # can have PV actuals (pv of cashflows) written to excel and the ftse pvs #

            cfs_condensed_numpy = cfs_rating_df.values.T @ weights_df.values # Shape: (time: 70, buckets: 6)
            # (time: 70, buckets: 6)

            # Cashflows in 6 buckets divided by PV
            cfs_aggregated_6_buckets = np.nan_to_num(cfs_condensed_numpy, nan=0.0) # Shape: (time: 70, buckets: 6) for (row, col)

            # Write to excel:
            if debug:

                # 4. Print or write to excel for PV adjusted CFs aggregated in 6 buckets
                debug_df = pd.DataFrame(cfs_aggregated_6_buckets)

                file_name = f'{rating}_CFs_divided_by_PV_w_row_time_col_6_buckets{cur_date}.xlsx'
                subdir = 'benchmarking_outputs'

                write_debug_file(debug_df, file_name, subdir, cur_date)
            # end of debugging.


            # 5. Perform a SUMPRODUCT of matrix and solutions on correct dimensions

            # Notes:
                # matrix * array performs PRODUCT of array on buckets of matrix - applies on each row
                # then, matrix @ array performs SUMPRODUCT (it completes the sum across the cols, or of each row, step after it)
                # (so, the second performs the summation or dot product across each row)

                # cfs_aggregated_6_buckets has (time, buckets) and sol_scaled_mv has (buckets'_weights)
                # so scales the buckets for solutions :) across the cashflow times.


            # Shape: (70,) as required
                # Equivalent, for code tests:
                # final_CFs_rating_arr = np.nan_to_num((cfs_rating_df.values.T @ weights_df.values), nan=0.0) @ np.nan_to_num(sol_scaled_mv, nan=0.0)
            final_CFs_rating_arr = cfs_aggregated_6_buckets @ sol_scaled_mv # Thanks, braodcasting, for applying array across the cols (it broadcasts array to be a col here from matrix # 2)
            # Note that portfolio_solution for NONPAR portfolio and Federal rating is 0, so that this is 0 as well - could put a condition that checks for this rather
                # than doing all the operation? e.g., if portfolio_solution = 0, then continue (skip) with the 0 final cfs
            # NOTE: final_CFs final_CFs_rating_arr are in semi_annual payments across 70 terms for a rating (so an array)


            # Generate half-year dates starting from given_date + 6 months
            start_date = given_date + pd.DateOffset(months=6)

            # Generate dates at the last day of every 6 months (half-year):
            half_year_dates = pd.date_range(start=start_date, periods=70, freq='6ME') # '6M' frequency means "every month-end", equivalent to '6ME'

            # Create an indexer to populate summed_cfs['rating'], for every 6 months
            for i, date in enumerate(half_year_dates):
                summed_cfs.loc[summed_cfs['date'] == date, rating] = final_CFs_rating_arr[i]


            # fill summed_cfs NAN or summed_cfs[rating] NaN with 0
            df = ftse_data.loc[ftse_data['RatingBucket'] == rating]
            if df['Benchmark ' + portfolio + ' weight'].sum() == 0:
                yield1 = 0
            else:
                yield1 = (df['Benchmark ' + portfolio + ' weight'] * df['yield']).sum() / df[
                    'Benchmark ' + portfolio + ' weight'].sum()
            carry_table.loc['market Value', rating] = market_value
            carry_table.loc['Average Yield', rating] = yield1


        # Format and finalize DataFrame with carry table
        summed_cfs['date'] = pd.to_datetime(summed_cfs['date']).dt.strftime('%b-%Y')
        summed_cfs = pd.concat([carry_table, summed_cfs.set_index('date')])

        # Store results in the final output dictionary
        summed_cfs_dict[portfolio] = summed_cfs.fillna(0)

    return summed_cfs_dict



def create_summary_table(given_date, asset_type='public'):
    """
    For creating the summary_portfolio sheets of
    Custom Benchmarks (custom_benchmarks.xlsx) used in performance benchmarking processes by the ALM team.

    '''This function is currently used for creating the summary tables, which only contain info about the portfolio balances.'''
    """
    df_public, df_private, df_mortgage = model_portfolio.reading_asset_mix(given_date) # TODO! Creates new one - can decouple out and make a copy (same thing as creating anew)

    if asset_type == 'private':
        asset_mix = df_private
    elif asset_type == 'mortgage':
        asset_mix = df_mortgage
    else:
        asset_mix = df_public

    asset_mix_old = asset_mix.copy()

    df = pd.DataFrame(0.0, index=['Portfolio Yield', 'Portfolio Duration', 'Portfolio Balance', 'quarterly expected income', 'Capital estimate'], columns=['Total', 'np', 'group', 'Accum', 'Payout', 'ul'])
    for portfolio in ['Total', 'np', 'group', 'Accum', 'Payout', 'ul']:
        df.loc['Portfolio Balance', portfolio] = sum(asset_mix[portfolio])
    df['SURPLUS'] = 0.0
    df.loc['Portfolio Balance', 'SURPLUS'] = df.loc['Portfolio Balance', 'Total'] - df[['np', 'group', 'Payout', 'Accum', 'ul']].sum(axis=1)['Portfolio Balance'] # Checks the correctness perhaps.

    df.rename(columns={'Total': 'TOTAL', 'np': 'NONPAR', 'group': 'GROUP', 'Accum': 'ACCUM', 'Payout': 'PAYOUT', 'ul': 'UNIVERSAL'}, inplace=True)

    bool = asset_mix_old.equals(asset_mix)

    if not bool:
        print("altered asset mix in create_summary_table - doubt this happens")

    return df


def create_indexData_table(solution_df, given_date, ftse_data: pd.DataFrame, asset_type='public'):
    """
    In this function the indexData tables are created. These are essentially the ftse constituents table
    but with added columns with the weights for each portfolio.
    """
    portfolio_map = {
        'Total': 'TOTAL',
        'np': 'NONPAR',
        'group': 'GROUP',
        'Accum': 'ACCUM',
        'Payout': 'PAYOUT',
        'ul': 'UNIVERSAL',
        'Surplus': 'SURPLUS'
    }
    # rename_buckets_map = {5: 6, 4: 5, 3: 4, 2: 3, 1: 2, 0: 1}
    #
    benchmarking_solution = solution_df.copy()
    #
    # benchmarking_solution.rename(columns=rename_buckets_map, inplace=True)
    # benchmarking_solution['portfolio'] = benchmarking_solution['portfolio'].replace(portfolio_map)


    weights, totals = helpers.create_weight_tables(ftse_data)

    df_public, df_private, df_mortgage = model_portfolio.reading_asset_mix(given_date) # TODO! Creates new one

    if asset_type == 'private':
        asset_mix = df_private
        totals = totals.drop(['Corporate', 'Provincial', 'Federal'])

    elif asset_type == 'mortgage':
        asset_mix = df_mortgage
        totals = totals.drop(['Corporate', 'Provincial', 'CorporateAAA_AA', 'CorporateA'])
    else:
        asset_mix = df_public
        totals = totals.drop(['Corporate'])

    asset_mix.rename(columns=portfolio_map, inplace=True)
    total_dollar_amount = sum(asset_mix['TOTAL'])
    print(total_dollar_amount)

    for portfolio in ['NONPAR', 'GROUP', 'PAYOUT', 'ACCUM', 'UNIVERSAL', 'TOTAL']:
        if (asset_type == 'mortgage') & (portfolio == 'UNIVERSAL'):
            ftse_data['Benchmark ' + portfolio + ' weight'] = 0
            continue

        # Reformat data as a copy benchmarking solution (.loc creates a copy):
        if asset_type == 'mortgage':
            portfolio_solution = benchmarking_solution.loc[benchmarking_solution['portfolio'] == portfolio].set_index('rating').drop(columns='portfolio') # .rename(index={'corporateBBB': 'CorporateBBB'})  # Old code: Renaming only corpBBBs for mortgage because corpA and AAA_AAs not included:
        else:
            portfolio_solution = benchmarking_solution.loc[benchmarking_solution['portfolio'] == portfolio].set_index('rating').drop(columns='portfolio') # .rename(index={'corporateAAA_AA': 'CorporateAAA_AA', 'corporateA': 'CorporateA', 'corporateBBB': 'CorporateBBB'}) => renamed earlier in the code

        benchmark_weights = portfolio_solution.mul(asset_mix[portfolio], axis=0)
        benchmark_weights = benchmark_weights / asset_mix[portfolio].sum()
        benchmark_div_universe = (benchmark_weights / totals)
        benchmark_div_universe = benchmark_div_universe.fillna(0)
        benchmark_div_universe.infer_objects(copy=False)

        if asset_type == 'mortgage':
            benchmark_div_universe.loc['Provincial'] = 0
            benchmark_div_universe.loc['CorporateAAA_AA'] = 0
            benchmark_div_universe.loc['CorporateA'] = 0
        elif asset_type == 'private':
            benchmark_div_universe.loc['Federal'] = 0
            benchmark_div_universe.loc['Provincial'] = 0
            """
            for below:
            Let 'new col' be ftse_data['Benchmark ' + portfolio + ' weight']
            
            If the bucket value is 0 (that is, the TermPt > 35.25 for FTSE data), the new column’s value is set to 0.
            
            the new column’s value is set to the corresponding value from the benchmark_div_universe DataFrame, using the RatingBucket and bucket values from the current row as keys for the lookup.
            """
        ftse_data['Benchmark ' + portfolio + ' weight'] = ftse_data.apply(lambda row: 0 if row['bucket'] == 0 else benchmark_div_universe.loc[row['RatingBucket'], row['bucket']], axis=1)
        ftse_data['Benchmark ' + portfolio + ' weight'] = ftse_data['marketweight_noREITs'] * ftse_data['Benchmark ' + portfolio + ' weight']

    individual_portfolio_sums = asset_mix[['ACCUM', 'GROUP', 'NONPAR', 'PAYOUT', 'UNIVERSAL']].sum(axis=0)
    surplus_portfolio_balance = total_dollar_amount - sum(individual_portfolio_sums)
    ftse_data['Benchmark SURPLUS weight'] = ftse_data.apply(lambda row: ((row['Benchmark TOTAL weight'] * total_dollar_amount) - sum(row[['Benchmark ACCUM weight',
                                                                             'Benchmark GROUP weight',
                                                                             'Benchmark NONPAR weight',
                                                                             'Benchmark PAYOUT weight',
                                                                             'Benchmark UNIVERSAL weight']] * individual_portfolio_sums.values))/surplus_portfolio_balance, axis=1)
    ftse_data['Benchmark dollar investment'] = ftse_data['Benchmark TOTAL weight'] * total_dollar_amount

    return ftse_data