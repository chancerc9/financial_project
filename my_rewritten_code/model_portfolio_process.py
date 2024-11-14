"""
    This provided code is a complex script that processes bond-related data, 
    calculates key rate durations (KRDs), and uploads the resulting sensitivities 
    to a database. 
"""
# Standard library imports
import argparse
import datetime as dt
import json
import os
import sys
import traceback
from collections import OrderedDict
from typing import Dict

# Third-party imports
import numpy as np
import openpyxl
import pandas as pd
from dateutil.relativedelta import relativedelta
from psycopg2.extras import DictCursor
from scipy.optimize import minimize

# Local application-specific imports
from equitable.chronos import offsets, conversions
from equitable.db.db_functions import execute_table_query
from equitable.db.psyw import SmartDB
from equitable.infrastructure import sysenv, jobs, sendemail
from equitable.utils import processtools as misc

# Adding system path for custom imports
sys.path.append(sysenv.get("ALM_DIR"))

# Required custom modules
import helpers as helpers
import file_utils as file_utils

# Configure pandas display settings
pd.set_option('display.width', 150)

# Database connections (Benchmark, Bond, and General)
BM_conn = SmartDB('Benchmark')
BM_cur = BM_conn.con.cursor()

Bond_conn = SmartDB('Bond')
Bond_cur = Bond_conn.con.cursor()

General_conn = SmartDB('General')
General_cur = General_conn.con.cursor()

# Logging directories:
MY_LOG_DIR = os.path.join(sysenv.get('PORTFOLIO_ATTRIBUTION_DIR'), 'logs', 'brenda')
os.makedirs(MY_LOG_DIR, exist_ok=True)  # Create directories if they don't exist
LOGFILE = open(os.path.join(MY_LOG_DIR, 'benchmarking_log.txt'),
               'a')  # Append to the existing logfile, or create a new one


## Start of Model Portfolio code: ##

"""
Mutates: Nothing
"""
def reading_asset_KRDs(ftse_data: pd.DataFrame, GivenDate: pd.Timestamp) -> pd.DataFrame:
    """
    Creates the Key Rate Duration (KRD) table for assets on a given date.
    (Main method to create the KRD table for assets.)

    Parameters:
    GivenDate (pd.Timestamp): The date for which to calculate the KRD table.

    What it does:
    Creates and aggregates the KRD profiles (i.e., sensitivities) and weighted-averages it into 6 buckets.

    Elaboration:
    Calculates the KRD profiles (i.e., sensitivities) and calls make_krd_table(sensitivities) to perform a weighted-averages
    for the sensitivities into 6 buckets. Final df of KRD profiles for 6 buckets is used for optimizer and produced for KRD
    profiles solutions results.


    Returns:
    pd.DataFrame: A DataFrame containing weighted sensitivities for each bond rating. For 6 buckets; used for optimizer.
    """
    # Retrieves bond curve data and FTSE bond info from our database
    bond_curves = helpers.get_bond_curves(
        GivenDate)  # Retrieve annual bond curve data (annual curve data for 35 years) - CLASSIFY so can use in multiple code and points of entry, including run_code; needs this all lol
    # ftse_data = helpers.get_ftse_data(GivenDate)  # Retrieve FTSE bond info (all required data)

    # Create weight tables, cashflow tables, shock tables, and sensitivity tables
    # Makes a weight table for the 6 buckets (to 6 buckets, from the 70 buckets cfs)
    weights, totals = helpers.create_weight_tables(ftse_data)  # Makes a weight table for each bond rating and bucket

    cf_tables = helpers.create_cf_tables(
        ftse_data)  # Makes a 30-35 year average semi-annual cashflow table for each bond rating and bucket, with principal 100
    # TODO! temp GivenDate, can make it OOP class
    shock_tables = helpers.create_shock_tables(bond_curves,
                                               GivenDate)  # Makes 30 year up and down shock tables for each bond rating and bucket
    sensitivities = helpers.create_sensitivity_tables(cf_tables,
                                                      shock_tables)  # Uses shocks and cashflows to make 30 year sensitivity tables for each bond rating and bucket

    # sensitivities = target sensitivities when still in 70 buckets - use this and weights to make final KRD tables (same thing but 6 buckets)

    # Create directories for storing the results
    cur_date = GivenDate.strftime('%Y%m%d')
    path = os.path.join(sysenv.get('PORTFOLIO_ATTRIBUTION_DIR'), 'Benchmarking', 'Test', 'benchmarking_outputs',
                        'Brenda', 'sensitivities', cur_date)
    os.makedirs(path, exist_ok=True)  # Create directories 'brenda' and 'etc' if they don't exist - Brenda

    # Save sensitivity tables as Excel files for each rating
    for rating in ['Federal', 'Provincial', 'CorporateAAA_AA', 'CorporateA', 'CorporateBBB', 'Corporate']:
        file_path = os.path.join(path, f'{rating}_sensitivities_{cur_date}.xlsx')
        if not os.path.exists(file_path):
            with pd.ExcelWriter(file_path) as writer:
                sensitivities[rating].to_excel(writer)  # 70 buckets?

    # Calculate and return the overall KRD table (6 buckets)
    df = helpers.make_krd_table(weights,
                                sensitivities)  # sensitivities are in 70 ttm buckets * 10 KRD shock intervals (terms) (TODO: !)
    df['rating'] = df['rating'].replace({
        'CorporateBBB': 'corporateBBB',
        'CorporateA': 'corporateA',
        'CorporateAAA_AA': 'corporateAAA_AA'
    })

    """
    Method for debugging:
    """

    CURR_DEBUGGING_PATH = os.path.join(sysenv.get('PORTFOLIO_ATTRIBUTION_DIR'), 'Benchmarking', 'Test',
                                       'benchmarking_outputs', 'Brenda', cur_date, 'Debugging_Steps')
    # CURR_FILE_PATH = os.path.join(CURR_DEBUGGING_PATH, 'ftse_bond_curves.xlsx')
    os.makedirs(CURR_DEBUGGING_PATH, exist_ok=True)
    file_utils.write_results_to_excel(bond_curves, CURR_DEBUGGING_PATH, cur_date,
                           'ftse_bond_curves_annual')  # unneeded, or can make into semiannual

    # write_results_to_excel(bond_curves, CURR_FILE_PATH)

    # Creates weight tables for each bond rating based on subindex percentages (for bonds):
    # CURR_FILE_PATH = os.path.join(CURR_DEBUGGING_PATH, 'bond_weights.xlsx')
    # os.makedirs(CURR_FILE_PATH, exist_ok=True)  # Create directories 'brenda' and 'etc' if they don't exist - Brenda
    # write_results_to_excel(weights, CURR_FILE_PATH)
    file_utils.write_results_to_excel(weights, CURR_DEBUGGING_PATH, cur_date,
                           'bond_weights_per_rating_for_6_buckets')  # weighting to make 70 buckets into 6 buckets
    file_utils.write_results_to_excel(totals, CURR_DEBUGGING_PATH, cur_date,
                           'total_universe_weights')  # unneeded; can use for debugging

    # shocked curves table:
    file_utils.write_results_to_excel(shock_tables, CURR_DEBUGGING_PATH, cur_date, 'shocked_bond_curves')

    # KRD table:
    # FILE_PATH = os.path.join(CURR_DEBUGGING_PATH, 'KRD_table.xlsx')
    # os.makedirs(CURR_FILE_PATH, exist_ok=True)  # Create directories 'brenda' and 'etc' if they don't exist - Brenda
    # write_results_to_excel(weights, CURR_FILE_PATH)
    file_utils.write_results_to_excel_one_sheet(df, CURR_DEBUGGING_PATH, cur_date,
                                     'final_krd_table')  # check how they format it for the writer

    # cf tables based on ftse data
    file_utils.write_results_to_excel(cf_tables, CURR_DEBUGGING_PATH, cur_date, 'cf_tables_ftse_data')

    """
    End of method for debugging
    """
    return df

"""
Mutates: Nothing
Helper that reading_asset_KRDs(ftse_data, GivenDate) relies on.
"""
# Reads in asset segments for liabilities:
def reading_asset_mix(Given_date: pd.Timestamp, curMonthBS: bool = False,
                      sheet_version: int = 1) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Reads and calculates the asset mix for liabilities based on the given date.

    Parameters:
    Given_date (pd.Timestamp): The date for which the asset mix is being calculated.
    curMonthBS (bool, optional): Whether to use the current month's balance sheet. Defaults to False.
    sheet_version (int, optional): Sheet version to use; 1 for segments, 0 for totals. Defaults to 1.

    Returns:
    Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: Returns three DataFrames:
        - df_public: Public assets.
        - df_private: Private assets.
        - df_mortgages: Mortgages.
    """
    # 1 for segments, 0 for totals
    totals = helpers.BSTotals(Given_date, sheet_version)

    weights = helpers.percents(
        Given_date)  # weights for total is same as weights for everything else, maybe that's where the problem shows - see weights in hardcoded.xlsx (OR)
    weights = weights[['ACCUM', 'PAYOUT', 'UNIVERSAL', 'NONPAR', 'GROUP', 'PARCSM', 'Total', 'Surplus', 'SEGFUNDS']]
    weights = weights.dropna(axis=1, how='all')  # Remove columns with all NaNs (* inefficient *)

    df = weights.multiply(pd.Series(totals))
    df.index.name = None

    # Adjust Corporate bonds and rename columns for clarity
    # TODO! use this for rerunning q2
    # df.loc['CorpA'] = df.loc['CorpA'] + df.loc['CorpBBB'] / 2
    # df.loc['CorpBBB'] = df.loc['CorpBBB'] / 2

    ##

    df.rename(columns={'ACCUM': 'Accum', 'PAYOUT': 'Payout', 'GROUP': 'group', 'UNIVERSAL': 'ul', 'NONPAR': 'np'},
              inplace=True)

    # Split into public, private, and mortgage tables
    df_public = df.iloc[:5]
    df_private = df.iloc[5:11].drop(columns=['SEGFUNDS'])

    df_private.rename({'PrivateAA': 'corporateAAA_AA', 'PrivateA': 'corporateA', 'PrivateBBB': 'corporateBBB',
                       'MortgagesInsured': 'Federal'}, inplace=True)
    df_public.rename({'CorpAAA_AA': 'corporateAAA_AA', 'CorpA': 'corporateA', 'CorpBBB': 'corporateBBB'},
                     inplace=True)  # Rename it better here

    df_mortgages = df_private.loc[['Federal', 'MortgagesConv']].rename({'MortgagesConv': 'corporateBBB'},
                                                                       inplace=False)  # orig: inplace=True; use inplace=FALSE for debugging purposes.
    df_private.drop(['PrivateBB_B', 'MortgagesConv', 'Federal'], inplace=True)

    # TODO: added back from old code:
    # df_private.loc['Provincial'] = 0

    return df_public, df_private, df_mortgages


"""
Functions that *may* mutate things.
"""
def optimization_worker(AssetKRDsTable: pd.DataFrame, given_date: dt, asset_type='public', curMonthBS=False,
                        sheet_version=1):  # default sheet_version is segments (1)

    KRDs = AssetKRDsTable.copy()  # maybe need to have this function do it so the benchmarks (create benchmarking tables) can run

    # or do an if-else; if given, then this; else, this (if it is none-type)
    # if you have objects, then they can recognize this; they can recognize when you are feeding it in than unknown.
    df_public, df_private, df_mortgages = reading_asset_mix(given_date, curMonthBS, sheet_version)
        # df_public, df_private, df_mortgages = reading_asset_mix(given_date)  # same meaning really as their top one

    ''' Setting Asset_mix to the correct table based on the given asset class '''
    if asset_type == 'private':
        Asset_mix = df_private
    elif asset_type == 'mortgage':
        Asset_mix = df_mortgages
    else:
        Asset_mix = df_public  # For all

    # Reads in targets sensitivities to match sols:
    ''' Separating the db values into 3 tables, one for each asset class '''
    private_sensitivity = helpers.private_sensitivities(given_date).set_index(['portfolio', 'rating'])
    mortgage_sensitivity = helpers.mortgage_sensitivities(given_date).set_index(['portfolio', 'rating'])

    ''' Setting the sensitivities to be used as targets for the optimizer, for the correct asset class'''
    if asset_type == 'private':
        net_sensitivity = helpers.private_sensitivities(given_date)
        total_sensitivity = net_sensitivity

    elif asset_type == 'mortgage':
        net_sensitivity = helpers.mortgage_sensitivities(given_date)
        total_sensitivity = net_sensitivity

    else:
        ''' For the public optimization, we subtract the private and mortgage target sensitivities from the public target and optimize for the net sensitivity '''
        net_sensitivity = helpers.public_sensitivities(given_date)

    ''' For the sensitivity targets for the public totals, we subtract the public and mortgage components of all ratings
    we sum the public sensitivities for all 5 portfolios, then subtract the sum of privates for all portfolios, including ParCSM and Surplus'''
    if asset_type == 'public':
        net_sensitivity = helpers.public_sensitivities(given_date)
    elif asset_type == 'private':
        net_sensitivity = helpers.private_sensitivities(given_date)
    elif asset_type == 'mortgage':
        net_sensitivity = helpers.mortgage_sensitivities(given_date)

    solution_df = pd.DataFrame()
    solved_dollar_sensitivities = pd.DataFrame()

    # df for targets (test output)
    krd_targets = pd.DataFrame()

    ''' This df is a table of expected returns taken from the Parallel_tilt_curve_history'''
    expected_return = helpers.get_expected_returns()

    # Optimize total first
    # and then segments
    ''' start the optimization process for each portfolio'''
    for portfolio in ['ul', 'Payout', 'Accum', 'group', 'np', 'Total']:

        ''' first get the target sensitivities from the df generated above for the current portfolio '''
        df_portfolio = net_sensitivity[net_sensitivity['portfolio'] == portfolio]

        ''' Next, go through each rating class to optimize for each. Calculate provinical last because the target 
        for total-provincial is calculated using the solution for the other ratings '''
        for rating in ['corporateBBB', 'corporateA', 'Federal', 'corporateAAA_AA', 'Provincial']:

            ''' The mortgage portfolios only include Federal and CorporateBBB, and the private doesn't include Fedearl or Provincial. Those cases are excluded from the optimization'''
            if ((asset_type == 'mortgage') & (
                    (rating == 'corporateAAA_AA') or (rating == 'corporateA') or (rating == 'Provincial'))) or (
                    (asset_type == 'private') & ((rating == 'Federal') or (rating == 'Provincial'))):
                continue

            ''' The following cases do not run through the optimizer '''
            if (asset_type == 'public'):
                if ((portfolio == 'np') or (portfolio == 'ul') or (portfolio == 'Payout')):
                    ''' CorporateBBB for Nonpar, Universal and Payout is not optimized. Buckets 3-6 are distributed according to the pre-determined weights to reduce concentration after buckets 1 and 2 are made.
                    CorporateA bonds are also not optimized for Nonpar and Universal - minimum amount is placed in buckets 1 and 2 and remaining is placed in bucket 6'''
                    if (rating == 'corporateBBB') or ((rating == 'corporateA') & (portfolio != 'Payout')):
                        ''' First get the amount to be placed in the first 2 buckets'''
                        bnds = helpers.calc_bounds(given_date, portfolio, sum(
                            Asset_mix[portfolio]))  # Looks at a single column for each segment (?)
                        new_row_df = pd.DataFrame(0, index=np.arange(1), columns=[0, 1, 2, 3, 4, 5])
                        new_row_df.iloc[0, 0] = bnds[0][0]
                        new_row_df.iloc[0, 1] = bnds[1][0]
                        if (rating == 'corporateBBB'):
                            ''' For corporateBBB, follow the weight distribution'''
                            new_row_df.iloc[0, 2:] = [val * (1 - new_row_df.iloc[0, 0:2].sum()) for val in
                                                      [0.1549, 0.2566, 0.4351, 0.1534]]

                        elif (rating == 'corporateA'):
                            ''' For corporateA, place remaining weight in bucket 6'''
                            new_row_df.iloc[0, 5] = 1 - new_row_df.iloc[0, 0:2].sum()
                        ''' Then add the row to the df'''
                        new_row_df['portfolio'] = portfolio
                        new_row_df['rating'] = rating
                        solution_df = pd.concat([new_row_df, solution_df], ignore_index=True)
                        continue

                elif (portfolio == 'Total'):
                    ''' CorporateAAA_AA and Federal in the Total portfolio are not optimized, the remaining investment allocation goes to bucket 6 for Federal, and bucket 1 for CorporateAAA_AA '''
                    if ((rating == 'corporateAAA_AA')):
                        ''' First we get the starting point already calculated by the optimizer for the 5 portfolios '''
                        total_bnds = helpers.get_bnds_for_total(Asset_mix,
                                                                solution_df)  # Change Asset_mix to Asset_mix2 so it works here (* to make totals work)

                        new_row_df = total_bnds.loc[[rating]].reset_index(drop=True)
                        new_row_df.iloc[0, 0] = 1 - sum(new_row_df.iloc[0, 1:])

                        new_row_df['portfolio'] = portfolio
                        new_row_df['rating'] = rating
                        solution_df = pd.concat([new_row_df, solution_df], ignore_index=True)
                        continue

            ''' First grab the KRDs of the assets of the corresponding rating '''  # This could have been done earlier for better logic. (Brenda Jump Here to Revise and MOVE UP for consecutive logic)
            krd = KRDs[KRDs['rating'] == rating]
            krd = krd.reset_index().drop(krd.columns[[0, 1]], axis=1).drop('index', axis=1).to_numpy()

            ''' The get the allocated investment amount for the current bond rating and portfolio'''
            investment_val = Asset_mix[portfolio].loc[rating] / 10000
            ''' If zero, add a blank row to the solution_df '''
            if investment_val == 0:
                new_row_df = pd.DataFrame(0, index=np.arange(1), columns=[0, 1, 2, 3, 4, 5])
                new_row_df['portfolio'] = portfolio
                new_row_df['rating'] = rating
                solution_df = pd.concat([new_row_df, solution_df], ignore_index=True)
                continue

            hedge_ratio = 1
            ''' Get the target sensitivities for the current rating , then use the invetment value and hedge ratio to generate the final target used in the optimization'''
            target_prep = df_portfolio[df_portfolio['rating'] == rating].drop(df_portfolio.columns[[0, 1]], axis=1)

            target = (target_prep) / investment_val
            target = target.to_numpy()[0]
            target = target.T * -1

            """ For Test Purposes (target krds without dividing by investment values)"""
            krd_target = (target_prep * hedge_ratio)
            krd_target = krd_target.to_numpy()[0]
            krd_target = krd_target.T * -1

            ''' The target sensitivities for provincial assets in the total portfolio are the remainder from the total target minus the solved sensitivities for the other bond ratings '''
            if (portfolio == 'Total') & (rating == 'Provincial'):

                ''' first calculate the solved dollar sensitivities for all the other ratings by multiplying the solved weights by the krds and the investment value '''
                for bond_rating in ['corporateBBB', 'Federal', 'corporateAAA_AA', 'corporateA']:
                    ''' Getting the KRDs '''
                    bond_krd = KRDs[KRDs['rating'] == bond_rating]
                    bond_krd = bond_krd.reset_index().drop(bond_krd.columns[[0, 1]], axis=1).drop('index',
                                                                                                  axis=1).to_numpy()

                    ''' Getting the solved weights from solution_df ''' # TODO! could have issue, solved weights is wrong, step that matters
                    weights = solution_df.loc[
                        (solution_df['portfolio'] == 'Total') & (solution_df['rating'] == bond_rating)].drop(
                        columns={'portfolio', 'rating'}).to_numpy().reshape(-1, 1)

                    ''' Getting the total amount allocated for the rating '''
                    investment = Asset_mix['Total'].loc[bond_rating] / 10000

                    ''' calculating the solved sensitivity then adding it as a column in the solved sensitivity df '''
                    solved_sensitivity = np.dot(bond_krd, weights) * investment
                    solved_dollar_sensitivities[bond_rating] = solved_sensitivity[:, 0]

                ''' The sum of all columns is subtracted from the total target sensitivities to obtain the provincial target sensitivities '''
                solved_dollar_sensitivities['Total - prov'] = solved_dollar_sensitivities.sum(axis=1)
                target_prep = df_portfolio[df_portfolio['rating'] == 'Total'].drop(df_portfolio.columns[[0, 1]], axis=1)
                target_prep = target_prep.iloc[0].reset_index(drop=True) + solved_dollar_sensitivities['Total - prov']

                ''' Applying the same procedure to the target like all previous sensitivity targets - 
                multiplying by the hedge ratio (95%) and dividing by the investment value, then transposing'''
                target = (target_prep * hedge_ratio) / investment_val
                target = target.to_numpy()
                target = target * -1

                """ For Test Purposes (target krds without dividing by investment values)"""
                krd_target = (target_prep * hedge_ratio)
                krd_target = krd_target.to_numpy()
                krd_target = krd_target.T * -1

            # for testing
            krd_targets[f"{portfolio}_{rating}"] = krd_target
            # print(krd_targets)
            # print(asset_type)

            ''' the objective of the optimizer is to minimize the difference 
            between the target sensitivities and the calculated sensitivities'''

            def objective(x):
                c = np.multiply(x, krd)
                temp = c.sum(axis=1) - target
                return np.dot(temp.T, temp)

            ''' for the total portfolio, the objective is to maximize expected return, 
            so it uses a different objective in the optimization'''

            def objective_total(x):
                c = np.multiply(x, expected_return.loc[rating].to_numpy().reshape(1, -1))
                d = -c.sum(axis=1)[0]
                return d

            ''' for corporateBBB bonds - used in the constraints'''
            corpBBBweights = [0.1627, 0.2669, 0.4079, 0.1625]
            corpBBBratios = np.divide(corpBBBweights, corpBBBweights[0])

            ''' Setting constraints for the optimizer - corporateBBB uses 
            different constrants using the ratios calculated above to reduce concentration'''
            if (rating == "corporateBBB") & (asset_type != 'mortgage'):
                cons = ({'type': 'eq', 'fun': lambda x: sum(sum(np.multiply(x, krd))) - sum(target)},
                        {'type': 'eq', 'fun': lambda x: np.sum(x) - 1},
                        {'type': 'eq', 'fun': lambda x: x[3] - corpBBBratios[1] * x[2]},
                        {'type': 'eq', 'fun': lambda x: x[4] - corpBBBratios[2] * x[2]},
                        {'type': 'eq', 'fun': lambda x: x[5] - corpBBBratios[3] * x[2]})
            else:
                cons = ({'type': 'eq', 'fun': lambda x: sum(sum(np.multiply(x, krd))) - sum(target)},
                        {'type': 'eq', 'fun': lambda x: np.sum(x) - 1})

            x0 = [1, 0, 0, 0, 0, 0]

            ''' Setting the boundaries for the optimizer this varies based on portfolio and bond rating '''
            if (asset_type == 'public') & ((portfolio == 'ul') or (portfolio == 'np')):
                ''' Universal and Nonpar are allowed to hold negative amounts for buckets one and 2, the exact amount is calculated using the IFE Estimates file'''
                bnds = helpers.calc_bounds(given_date, portfolio, sum(Asset_mix[portfolio]))
            elif (portfolio == 'Total') & (rating != 'corporateAAA_AA'):
                ''' For the Total, the bounds used are based on the solved amounts. The sum of the solved amounts for each portfolio is used as a starting point for the remainder of the total to be optimized'''
                bnds = []
                # Brenda (*begin)
                # Brenda (*end)
                total_bnds = helpers.get_bnds_for_total(Asset_mix,
                                                        solution_df)  # insert reading assetmix2 here - brenda commented out for now (temporary)
                for x in total_bnds.loc[rating]:
                    bnds.append([x, 1])
            elif (rating == "corporateAAA_AA"):
                ''' No corporateAAA_AA bonds in buckets 3 and 4 and 6, so bounds are set to zero for those buckets '''
                bnds = ((0, 1), (0, 1), (0, 0), (0, 0), (0, 1), (0, 0))
                x0 = [1, 0, 0, 0, 0, 0]
            else:
                bnds = [[0, 1], [0, 1], [0, 1], [0, 1], [0, 1], [0, 1]]

            # is the issue here?
            if portfolio == 'Total':
                ''' Uses a different x0 because [0, 0, 0, 0, 0, 1] is sometimes out of bounds and it gives an incorrect solution '''
                sumofbnds = 1 - bnds[0][0] - bnds[1][0] - bnds[2][0] - bnds[3][0] - bnds[4][0] - bnds[5][0]
                x0 = [bnds[0][0], bnds[1][0], bnds[2][0], bnds[3][0], bnds[4][0], bnds[5][0] + sumofbnds]
                # solution = minimize(objective, x0, method='SLSQP', bounds=bnds, constraints=cons)
                solution = minimize(objective_total, x0, method='SLSQP', bounds=bnds, constraints=cons)
            else:
                solution = minimize(objective, x0, method='SLSQP', bounds=bnds, constraints=cons)

            if solution.success:
                misc.log('Successful optimization for ' + rating + ' bonds in ' + portfolio, LOGFILE)

            ''' Append the solved weights to the solution_df '''
            new_row_df = pd.DataFrame(solution.x).T
            new_row_df['portfolio'] = portfolio
            new_row_df['rating'] = rating
            solution_df = pd.concat([new_row_df, solution_df], ignore_index=True)

    # Test output for krd targets
    if asset_type == 'public':
        krd_targets.to_clipboard()

    ''' Create the liability table using the results of the optimization, and add it to the end of the solution_df'''
    liabilities = helpers.liabilities_table(Asset_mix, solution_df)
    solution_df = pd.concat([solution_df, liabilities], ignore_index=True)

    ''' repeat for the surplus table, append to the end of the solution_df'''
    surplus_table = helpers.surplus_table(Asset_mix, solution_df)
    solution_df = pd.concat([solution_df, surplus_table], ignore_index=True)

    ''' Rounds the solution to 4 decimals'''
    solution_df.iloc[:, :6] = solution_df.iloc[:, :6].astype(float).round(5) # 4

    # print(solution_df)
    # print(asset_type)

    return solution_df


def optimization(KRDs: pd.DataFrame, given_date: dt, asset_type='public', curMonthBS=False):
    """
    Wrapper function for optimization; provide given KRDs or have optimization worker function call KRD function.

    Optim worker function creates a copy of KRDs for no propogation of changes.
    """

    # KRDs = KRDs.copy() # Unneeded

    # Obtain optimized solution dfs:

    # Segments:
    sol_df_seg = optimization_worker(KRDs, given_date, asset_type, curMonthBS, sheet_version=1) # segments = 1
    # Totals:
    sol_df_tot = optimization_worker(KRDs, given_date, asset_type, curMonthBS, sheet_version=0) # totals = 0

    def overwrite_total_rows(sol_df_seg, sol_df_tot):
        """
        Overwrite 'Total' portfolio rows in sol_df_seg with rows from sol_df_tot.

        Args:
        sol_df_seg: DataFrame containing segment results (public, private, mortgage).
        sol_df_tot: DataFrame containing total portfolio results.

        Returns:
        sol_df_seg: Updated DataFrame with 'Total' portfolio rows replaced by sol_df_tot rows.
        """

        # Step 1: Filter out the 'Total' rows from both sol_df_seg and sol_df_tot
        total_rows_tot = sol_df_tot[sol_df_tot['portfolio'] == 'Total']
        non_total_rows_seg = sol_df_seg[sol_df_seg['portfolio'] != 'Total']

        # Step 2: Concatenate non-'Total' rows from sol_df_seg with 'Total' rows from sol_df_tot
        updated_sol_df_seg = pd.concat([total_rows_tot, non_total_rows_seg], ignore_index=True)

        return updated_sol_df_seg

    sol_df = overwrite_total_rows(sol_df_seg, sol_df_tot)

    return sol_df


