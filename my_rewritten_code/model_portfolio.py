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
# Standard library imports
import argparse
import datetime as dt
import json
import os
import sys
import traceback
from collections import OrderedDict

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
import objects as helpers

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
os.makedirs(MY_LOG_DIR, exist_ok=True)  # Create directories 'brenda' and 'logs' if they don't exist
LOGFILE = open(os.path.join(MY_LOG_DIR, 'benchmarking_log.txt'), 'a')  # Appends to existing logfile; else creates new

# benchmarking_test


def reading_asset_KRDs(GivenDate):
    """
    Main method to create the KRD table for assets.
    """

    bond_curves = helpers.get_bond_curves(GivenDate) # gets bond curve data from our database

    ftse_data = helpers.get_ftse_data(GivenDate) # gets ftse bond info from our database

    weights, totals = helpers.create_weight_tables(ftse_data) # makes a weight table for each bond rating and bucket

    cf_tables = helpers.create_cf_tables(ftse_data) # makes a 30 year average cashflow table for each bond rating and bucket, with principal 100

    shock_tables = helpers.create_shock_tables(bond_curves) # makes 30 year up and down shock tables for each bond rating and bucket

    sensitivities = helpers.create_sensitivity_tables(cf_tables, shock_tables) # uses shocks and cashflows to make 30 year sensitivity tables for each bond rating and bucket

    cur_date = GivenDate.strftime('%Y%m%d')
    path = os.path.join(sysenv.get('PORTFOLIO_ATTRIBUTION_DIR'), 'Benchmarking', 'Test', 'benchmarking_outputs', 'Brenda', 'sensitivities', # added in 'Brenda'
                        cur_date)
    os.makedirs(path, exist_ok=True)  # Create directories 'brenda' and 'logs' if they don't exist - Brenda

    if not os.path.exists(path):
        os.mkdir(path)

    for rating in ['Federal', 'Provincial', 'CorporateAAA_AA', 'CorporateA', 'CorporateBBB', 'Corporate']:
        file_path = path + '/' + rating + '_sensitivities_' + cur_date + '.xlsx'
        if not os.path.exists(file_path):
            with pd.ExcelWriter(file_path) as writer:
                sensitivities[rating].to_excel(writer)

    df = helpers.make_krd_table(weights, sensitivities) # calculates overall krd by weighing the sensitivites against the bond weights within the ftse universe
    df['rating'] = df['rating'].replace(
        {'CorporateBBB': 'corporateBBB', 'CorporateA': 'corporateA',
         'CorporateAAA_AA': 'corporateAAA_AA'})

    return df


def reading_liabilities(cur_date, over_under=None):
    if over_under:
        string_date = over_under.strftime('%Y%m%d')

    else:
        string_date = cur_date.strftime('%Y%m%d')
    file_name = "Over_Under_Asseting_" + string_date + ".xlsm"
    # file_name = "Over_Under_Asseting_20240103.xlsm"
    if over_under:
        path_input = os.path.join(sysenv.get('OVER_UNDER_ASSETING_DIR'), over_under.strftime('%Y'), "past_files",
                                  file_name)
    else:
        path_input = os.path.join(sysenv.get('OVER_UNDER_ASSETING_DIR'), cur_date.strftime('%Y'), "past_files", file_name)
    # path_input = os.path.join(sysenv.get('OVER_UNDER_ASSETING_DIR'), "2024", "past_files", file_name)
    workbook = openpyxl.load_workbook(path_input, read_only=True, data_only=True)
    sheet = workbook.sheetnames[12]
    sheet = 'CR01 QE'
    ws = workbook[sheet]


    df = pd.DataFrame(ws.values, columns=next(ws.values)[0:])

    df2 = df.iloc[53:128, :15]



    # This section calculates a reduction to be applied to the federal bonds, with the info taken from the over_under_asseting file
    federal_reduction = df.iloc[35:45, 1:6]
    federal_reduction.columns = federal_reduction.iloc[0]
    federal_reduction = federal_reduction.drop(federal_reduction.index[0])
    federal_reduction.loc['NFI weights'] = 1 - federal_reduction.iloc[-1]
    federal_reduction = federal_reduction.iloc[[0, -1]]
    federal_reduction.loc['Canada / (Canada + NFI)'] = federal_reduction.iloc[0] / (federal_reduction.iloc[0] + federal_reduction.iloc[1])
    federal_reduction = federal_reduction.rename(columns={'Payout FPC': 'Payout',
                                                          'Group': 'group',
                                                          'UL': 'ul',
                                                          'NP': 'np'})




    df2 = df2.rename(columns={'Liability': 'Payout',
                              'Assets': 'Payout Assets',
                              'Assets / Liabilities': 'ul',
                              'Unnamed: 3': 'Universal Assets',
                              'Unnamed: 4': 'Accum',
                              'Unnamed: 5': 'Accum Assets',
                              'Unnamed: 6': 'group',
                              'Liability.1': 'Group Assets',
                              'Assets.1': 'np',
                              'Assets / Liabilities.1': 'Nonpar Assets',
                              'Unnamed: 10': 'Surplus Assets',
                              'BEAR FLATTENING': 'Parcsm Assets',
                              'Unnamed: 12': 'Total Liability',
                              'Unnamed: 13': 'Total Assets'})
    df2.columns = ['index', 'Payout', 'Payout Assets', 'ul', 'Universal Assets', 'Accum', 'Accum Assets', 'group', 'Group Assets', 'np', 'Nonpar Assets', 'Surplus Assets', 'Parcsm Assets', 'Total Liability', 'Total Assets']
    df2.set_index(df2.columns[0], inplace=True)
    df2 = df2[['Payout', 'ul', 'Accum', 'group', 'np']]

    df2 = df2.reset_index()
    federal_ratio = df2.iloc[63, 1:]

    df2 = df2[df2['index'].str.contains('DV01', na=False)]

    df2 = df2.reset_index(drop=True)

    ratings = ['PROVINCIAL' if i < 10 else 'CorpAAA_AA' if i < 20 else 'CorpA' if i < 30 else 'CorpBBB' if i < 40 else 'FEDERAL' for i in range(50)]
    df2.insert(0, 'Rating', ratings)

    rating_dict = {}
    for rating in ['PROVINCIAL', 'CorpAAA_AA', 'CorpA', 'CorpBBB', 'FEDERAL']:
        df = df2.loc[df2['Rating'] == rating].iloc[:, 1:].set_index('index')
        df.index = df.index.str[5:]
        df.index.name = None
        df.loc['70Y'] = 0

        if rating == 'CorpBBB':
            df = df / 1.4
        rating_dict[rating] = df#.to_dict()
    rating_dict['Total'] = rating_dict['FEDERAL'].div(federal_ratio, axis=1)
    new_fed = rating_dict['Total'] - rating_dict['PROVINCIAL'] - rating_dict['CorpAAA_AA'] - rating_dict['CorpA'] - rating_dict['CorpBBB']
    rating_dict['FEDERAL'] = new_fed.mul(federal_reduction.iloc[2], axis=1) # The federal reduction is applied here



    year = cur_date.year
    quarter = ((cur_date.month - 1) // 3) + 1
    if quarter == 1:
        prev_quarter = 4
        year -= 1
    else:
        prev_quarter = quarter - 1
    year_quarter = str(year) + 'Q' + str(prev_quarter)

    # in this seciton of the code the private sensitivities are obtained from the appropriate Risk measure file
    # year_quarter = '2023Q4'

    private_sensitivity_dict = {}
    mortgage_sensitivity_dict = {}
    for portfolio in ['Payout', 'group', 'Accum', 'ul', 'np', 'Surplus', 'ParCSM']:

        file_name = "Risk measure " + year_quarter + " - " + portfolio + ".xlsm"
        ''' to account for the new naming convention in the risk measures folders'''
        if year <= 2023:
            path_input = os.path.join(sysenv.get('RISK_MANAGEMENT_DIR'), "Risk Measures", year_quarter, file_name)
        else:
            path_input = os.path.join(sysenv.get('RISK_MANAGEMENT_DIR'), "Risk Measures", str(year), ('Q' + str(prev_quarter)), file_name)
        workbook = openpyxl.load_workbook(path_input, data_only=True)
        sheet = 'KRD sensitivities'  # workbook.sheetnames[0]
        ws = workbook[sheet]
        data = ws.values
        columns = next(data)[0:]
        df = pd.DataFrame(data, columns=columns).iloc[:, 1:15]


        df.columns = df.iloc[0]
        df = df.drop(df.index[0])

        df['Year'] = df['Base'].shift(1)
        df['Type'] = df['Base']
        df = df.drop(df.index[0]).iloc[11:, 8:]

    # mortgage Insured -> Federal, MortgagesConv -> CorpBBB
        df = df[df['Type'] == 'Current assets'].reset_index().drop('index', axis=1)
        df[['Year', 'direction']] = df['Year'].str.split('_', n=1, expand=True)
        df = df.drop(['Type'], axis=1).set_index('Year').drop('direction', axis=1)
        df['PROVINCIAL'] = 0.0
        df_up = df.iloc[:11, :]
        df_down = df.iloc[11:, :]

        private_sensitivities = (df_up - df_down) / (2 * 0.0001)
        mortgage_sensitivities = private_sensitivities.loc[:, "MortgagesInsured": 'MortgagesConv']
        mortgage_sensitivities = mortgage_sensitivities.rename(columns={'MortgagesInsured': 'FEDERAL', 'MortgagesConv': 'CorpBBB'})
        private_sensitivities['PrivateBBB'] = private_sensitivities['PrivateBBB'] # + private_sensitivities['MortgagesConv']
        private_sensitivities = private_sensitivities.drop(['MortgagesConv', 'MortgagesInsured'], axis=1)
        private_sensitivities = private_sensitivities.rename(
            columns={'PrivateBBB': 'CorpBBB', 'PrivateA': 'CorpA', 'PrivateAA': 'CorpAAA_AA', 'PrivateBB_B': 'CorpBB_B'})
                    # 'MortgagesInsured': 'FEDERAL'})

        private_sensitivity_dict[portfolio] = private_sensitivities / 10000.0
        mortgage_sensitivity_dict[portfolio] = mortgage_sensitivities / 10000.0



    # Here the sensitivities are uploaded to our database to avoid repeating the previous section since extracting from excel takes a long time
    Create_comand = """
                   CREATE TABLE IF NOT EXISTS target_sensitivity
                    (portfolio character varying(100), asset_type character varying(100), date date, rating character varying(100), "1Y" double precision,
                    "2Y" double precision, "3Y" double precision, "5Y" double precision, "7Y" double precision, "10Y" double precision,
                    "15Y" double precision, "20Y" double precision, "25Y" double precision, "30Y" double precision,
                    "70Y" double precision,
                    CONSTRAINT target_sensitivity_pk  PRIMARY KEY(portfolio, asset_type, date, rating) )
                    """
    execute_table_query(Create_comand, 'Benchmark')
    # BM_cur.execute(Create_comand)
    # BM_conn.con.commit()

    total_sensitivity_dict = {}
    for portfolio in ['Payout', 'group', 'Accum', 'ul', 'np', 'Surplus', 'ParCSM']:
        rating_sensitivity_dict = {}
        for rating in ['FEDERAL', 'CorpBBB', 'CorpA', 'PROVINCIAL', 'CorpAAA_AA', 'CorpBB_B', 'Total']:
            if (portfolio != 'Surplus') & (portfolio != 'ParCSM') & (rating != 'CorpBB_B'):
                rating_sensitivity = rating_dict[rating][portfolio]
            if (rating != 'FEDERAL') & (rating != 'Total'):
                private_sensitivity = private_sensitivity_dict[portfolio][rating]#.to_dict()
            if (rating == 'CorpBBB') or (rating == 'FEDERAL'):
                mortgage_sensitivity = mortgage_sensitivity_dict[portfolio][rating]



            Delete_Comand = """
                            DELETE FROM target_sensitivity WHERE date = '{}' AND portfolio = '{}' AND rating = '{}' """.format(
                cur_date, portfolio, rating)
            execute_table_query(Delete_Comand, 'Benchmark')
            # BM_cur.execute(Delete_Comand)
            # BM_conn.con.commit()

            if (portfolio != 'Surplus') & (portfolio != 'ParCSM') & (rating != 'CorpBB_B'):
                # insert public sensitivity
                INSERT_query = """
                                  INSERT INTO target_sensitivity (portfolio, asset_type, date,  rating, "1Y", "2Y", "3Y", "5Y", "7Y", "10Y", "15Y", "20Y", "25Y", "30Y", "70Y") VALUES ( %s,%s, %s, %s, %s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s)
                                  """
                # BM_cur.execute(INSERT_query, (portfolio, 'public', cur_date, rating) + tuple(rating_sensitivity.values))
                # BM_conn.con.commit()
                execute_table_query(INSERT_query, 'Benchmark', parameters=(portfolio, 'public', cur_date, rating) + tuple(rating_sensitivity.values))

            if (rating != 'FEDERAL') & (rating != 'Total') & (rating != 'PROVINCIAL'):
                # insert private sensitivity
                INSERT_query = """
                                              INSERT INTO target_sensitivity (portfolio, asset_type, date, rating, "1Y", "2Y", "3Y", "5Y", "7Y", "10Y", "15Y", "20Y", "25Y", "30Y", "70Y") VALUES ( %s,%s, %s, %s, %s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s)
                                              """
                # BM_cur.execute(INSERT_query, (portfolio, 'private', cur_date, rating) + tuple(private_sensitivity.values))
                # BM_conn.con.commit()
                execute_table_query(INSERT_query, 'Benchmark', parameters=(portfolio, 'private', cur_date, rating) + tuple(private_sensitivity.values))

            if (rating == 'CorpBBB') or (rating == 'FEDERAL'):
                #insert mortgage sensitivity
                INSERT_query = """
                                                              INSERT INTO target_sensitivity (portfolio, asset_type, date, rating, "1Y", "2Y", "3Y", "5Y", "7Y", "10Y", "15Y", "20Y", "25Y", "30Y", "70Y") VALUES ( %s,%s, %s, %s, %s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s)
                                                              """
                # BM_cur.execute(INSERT_query,
                #                (portfolio, 'mortgage', cur_date, rating) + tuple(mortgage_sensitivity.values))
                # BM_conn.con.commit()
                execute_table_query(INSERT_query, 'Benchmark', parameters=(portfolio, 'mortgage', cur_date, rating) + tuple(mortgage_sensitivity.values))


            print("uploaded data for '{}' + '{}'".format(portfolio, rating))

            if (portfolio != 'Surplus') & (portfolio != 'ParCSM'):
                rating_sensitivity_dict[rating] = rating_sensitivity

        total_sensitivity_dict[portfolio] = rating_sensitivity_dict

# Reads in asset segments for liabilities:

def reading_asset_mix(Given_date, curMonthBS=False, sheet_version=1):
    if sheet_version == 1:  # 1 for segments, 0 for totals
        totals = helpers.BSTotals(Given_date, 1)  # totals 1 = segments
    # the way they do this is so that they can use the self-made SBSTotals balance sheet replaccement (allows for easier swapping)
    else:
    # getting the hardcoded weights
        totals = helpers.BSTotals(Given_date, 0)  # Brenda # totals 0 = total

    weights = helpers.percents(Given_date) # weights for total is same as weights for everything else, maybe that's where the problem shows - see weights in hardcoded.xlsx (OR)
    weights = weights[['ACCUM', 'PAYOUT', 'UNIVERSAL', 'NONPAR', 'GROUP', 'PARCSM', 'Total', 'Surplus', 'SEGFUNDS']]
    weights = weights.dropna(axis=1, how='all')  # inefficient
    df = weights.multiply(pd.Series(totals))
    df.index.name = None



    # splits the table into public and privates
    df.loc['CorpA'] = df.loc['CorpA'] + df.loc['CorpBBB'] / 2
    df.loc['CorpBBB'] = df.loc['CorpBBB'] / 2
    df.rename(columns={'ACCUM': 'Accum', 'PAYOUT': 'Payout', 'GROUP': 'group', 'UNIVERSAL': 'ul', 'NONPAR': 'np'}, inplace=True)
    df_public = df[:5:]
    df_private = df[5:11]
    df_private = df_private.drop(columns=['SEGFUNDS'])

    df_private.rename({'PrivateAA': 'corporateAAA_AA', 'PrivateA': 'corporateA',
                       'PrivateBBB': 'corporateBBB', 'MortgagesInsured': 'Federal'}, inplace=True)
    df_public = df_public.rename({'CorpAAA_AA': 'corporateAAA_AA', 'CorpA': 'corporateA',
                       'CorpBBB': 'corporateBBB'})


    df_mortgages = df_private.loc[['Federal', 'MortgagesConv']]
    df_mortgages.rename({'MortgagesConv': 'corporateBBB'}, inplace=True)

    df_private.drop(['PrivateBB_B', 'MortgagesConv', 'Federal'], inplace=True)
    df_private.loc['Provincial'] = 0
    df_private.drop(['Provincial'], inplace=True)

    return df_public, df_private, df_mortgages

# Brenda (*begin)
def optimization_comments(given_date, over_under, asset_type='public', swap=False, curMonthBS=False):
    KRDs = reading_asset_KRDs(given_date)

    # Reading asset mix
    if curMonthBS:
        df_public, df_private, df_mortgages = reading_asset_mix(given_date, curMonthBS)
    else:
        df_public, df_private, df_mortgages = reading_asset_mix(given_date)

    # Setting asset mix based on asset type
    if asset_type == 'private':
        Asset_mix = df_private
    elif asset_type == 'mortgage':
        Asset_mix = df_mortgages
    else:
        Asset_mix = df_public

    # Get target sensitivities from the database
    get_target_sensitivities_query = """ SELECT * FROM target_sensitivity WHERE date= '{}' """.format(over_under.date())
    get_col_names = '''SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'target_sensitivity';'''
    col_names = [name[0] for name in execute_table_query(get_col_names, 'Benchmark', fetch=True)]
    df = pd.DataFrame(execute_table_query(get_target_sensitivities_query, 'Benchmark', fetch=True), columns=col_names)
    df['rating'] = df['rating'].replace({
        'FEDERAL': 'Federal',
        'CorpBBB': 'corporateBBB',
        'PROVINCIAL': 'Provincial',
        'CorpA': 'corporateA',
        'CorpAAA_AA': 'corporateAAA_AA',
        'CorpBB_B': 'corporateBB_B'
    })
    df = df.drop('70Y', axis=1)

    # Separate DB values into tables for different asset classes
    private_sensitivity = helpers.private_sensitivities().set_index(['portfolio', 'rating'])
    mortgage_sensitivity = helpers.mortgage_sensitivities().set_index(['portfolio', 'rating'])

    # Set the net sensitivity based on asset type
    if asset_type == 'private':
        net_sensitivity = helpers.private_sensitivities()
    elif asset_type == 'mortgage':
        net_sensitivity = helpers.mortgage_sensitivities()
    else:
        net_sensitivity = helpers.public_sensitivities()

    # Create a solution dataframe
    solution_df = pd.DataFrame()

    # Loop through each portfolio
    for portfolio in ['ul', 'Payout', 'Accum', 'group', 'np', 'Total']:
        df_portfolio = net_sensitivity[net_sensitivity['portfolio'] == portfolio]

        # Adjust weights for Total portfolio (swap weights to 1.0)
        if portfolio == 'Total' and swap:
            Asset_mix['Total'] = Asset_mix['Total'].apply(lambda x: 1.0)

        # Loop through each rating class
        for rating in ['corporateBBB', 'corporateA', 'Federal', 'corporateAAA_AA', 'Provincial']:
            if ((asset_type == 'mortgage') and rating in ['corporateAAA_AA', 'corporateA', 'Provincial']) or \
                    ((asset_type == 'private') and rating in ['Federal', 'Provincial']):
                continue

            # Skip certain portfolios for public optimization
            if asset_type == 'public' and portfolio in ['np', 'ul', 'Payout']:
                if rating in ['corporateBBB', 'corporateA']:
                    continue

            # Obtain the KRDs for the corresponding rating
            krd = KRDs[KRDs['rating'] == rating].reset_index(drop=True).drop(columns=['index'])
            investment_val = Asset_mix[portfolio].loc[rating] / 10000

            # Skip optimization if investment value is zero
            if investment_val == 0:
                continue

            hedge_ratio = 1
            ''' Get the target sensitivities for the current rating , then use the invetment value and hedge ratio to generate the final target used in the optimization'''
            target_prep = df_portfolio[df_portfolio['rating'] == rating].drop(df_portfolio.columns[[0, 1]], axis=1)

            target = (target_prep) / investment_val
            target = target.to_numpy()[0]
            target = target.T * -1

            # Define the objective function and constraints
            def objective(x):
                c = np.multiply(x, krd.to_numpy())
                temp = c.sum(axis=1) - target
                return np.dot(temp.T, temp)

            # Optimizer setup
            cons = ({'type': 'eq', 'fun': lambda x: np.sum(x) - 1})
            bnds = [(0, 1)] * 6
            solution = minimize(objective, [1, 0, 0, 0, 0, 0], method='SLSQP', bounds=bnds, constraints=cons)

            # Store solution
            if solution.success:
                new_row_df = pd.DataFrame(solution.x).T
                new_row_df['portfolio'] = portfolio
                new_row_df['rating'] = rating
                solution_df = pd.concat([solution_df, new_row_df], ignore_index=True)

    # Finalize and return solution dataframe
    solution_df.iloc[:, :6] = solution_df.iloc[:, :6].astype(float).round(4)
    return solution_df

def optimization_c(given_date, over_under, asset_type='public', swap=False, curMonthBS=False):  # default sheet_version is segments (1)
    KRDs = reading_asset_KRDs(given_date)

    # Reading asset mix
    if curMonthBS:
        df_public, df_private, df_mortgages = reading_asset_mix(given_date, curMonthBS) #, sheet_version) # top
    else:
        df_public, df_private, df_mortgages = reading_asset_mix(given_date) #, False, sheet_version)
        # df_public, df_private, df_mortgages = reading_asset_mix(given_date)  # same meaning really as their top one

    # Setting asset mix based on asset type
    if asset_type == 'private':
        Asset_mix = df_private
    elif asset_type == 'mortgage':
        Asset_mix = df_mortgages
    else:
        Asset_mix = df_public # For all


    #''' Getting target sensitivities for all asset classes from the database '''
    #get_target_sensitivities_query = """
    #                SELECT *
    #                FROM target_sensitivity
    #                WHERE date= '{}'
    #                """.format(over_under.date())
    #get_col_names = '''SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'target_sensitivity';'''
    #col_names = [name[0] for name in execute_table_query(get_col_names, 'Benchmark', fetch=True)]
    #df = pd.DataFrame(execute_table_query(get_target_sensitivities_query, 'Benchmark', fetch=True), columns=col_names)



    #df['rating'] = df['rating'].replace(
     #   {'FEDERAL': 'Federal', 'CorpBBB': 'corporateBBB', 'PROVINCIAL': 'Provincial', 'CorpA': 'corporateA',
    #     'CorpAAA_AA': 'corporateAAA_AA', 'CorpBB_B': 'corporateBB_B'})
    #df = df.drop('70Y', axis=1)



    ''' Separating the db values into 3 tables, one for each asset class '''
    private_sensitivity = helpers.private_sensitivities().set_index(['portfolio', 'rating'])
    mortgage_sensitivity = helpers.mortgage_sensitivities().set_index(['portfolio', 'rating'])


    ''' Setting the sensitivities to be used as targets for the optimizer, for the correct asset class'''
    if asset_type == 'private':
        net_sensitivity = helpers.private_sensitivities()
        total_sensitivity = net_sensitivity

    elif asset_type == 'mortgage':
        net_sensitivity = helpers.mortgage_sensitivities()
        total_sensitivity = net_sensitivity

    else:
        ''' For the public optimization, we subtract the private and mortgage target sensitivities from the public target and optimize for the net sensitivity '''
        net_sensitivity = helpers.public_sensitivities()




    ''' For the sensitivity targets for the public totals, we subtract the public and mortgage components of all ratings
    we sum the public sensitivities for all 5 portfolios, then subtract the sum of privates for all portfolios, including ParCSM and Surplus'''
    if asset_type == 'public':
        net_sensitivity = helpers.public_sensitivities()
    elif asset_type == 'private':
        net_sensitivity = helpers.private_sensitivities()
    elif asset_type == 'mortgage':
        net_sensitivity = helpers.mortgage_sensitivities()



    solution_df = pd.DataFrame()
    solved_dollar_sensitivities = pd.DataFrame()

    # df for targets (test output)
    krd_targets = pd.DataFrame()

    ''' This df is a table of expected returns taken from the Parallel_tilt_curve_history'''
    expected_return = helpers.get_expected_returns()

# Brenda
    # KRDs = reading_asset_KRDs(given_date)
    if curMonthBS:
        df_public2, df_private2, df_mortgages2 = reading_asset_mix(given_date,
                                                                   curMonthBS, 0)  # , sheet_version) # top
    else:
        df_public2, df_private2, df_mortgages2 = reading_asset_mix(given_date, False, 0)  # , False, sheet_version)
        # df_public, df_private, df_mortgages = reading_asset_mix(given_date)  # same meaning really as their top one

    # reading_liabilities(given_date)

    ''' Setting Asset_mix to the correct table based on the given asset class '''
    if asset_type == 'private':
        Asset_mix2 = df_private2
    elif asset_type == 'mortgage':
        Asset_mix2 = df_mortgages2
    else:
        Asset_mix2 = df_public2


# Brenda


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
            if ((asset_type == 'mortgage') & ((rating == 'corporateAAA_AA') or (rating == 'corporateA') or (rating == 'Provincial'))) or ((asset_type == 'private') & ((rating == 'Federal') or (rating == 'Provincial'))):
                continue

            ''' The following cases do not run through the optimizer '''
            if (asset_type == 'public'):
                if ((portfolio == 'np') or (portfolio == 'ul') or (portfolio == 'Payout')):
                    ''' CorporateBBB for Nonpar, Universal and Payout is not optimized. Buckets 3-6 are distributed according to the pre-determined weights to reduce concentration after buckets 1 and 2 are made.
                    CorporateA bonds are also not optimized for Nonpar and Universal - minimum amount is placed in buckets 1 and 2 and remaining is placed in bucket 6'''
                    if (rating == 'corporateBBB') or ((rating == 'corporateA') & (portfolio != 'Payout')):
                        ''' First get the amount to be placed in the first 2 buckets'''
                        bnds = helpers.calc_bounds(given_date, portfolio, sum(Asset_mix[portfolio])) # Looks at a single column for each segment (?)
                        new_row_df = pd.DataFrame(0, index=np.arange(1), columns=[0, 1, 2, 3, 4, 5])
                        new_row_df.iloc[0, 0] = bnds[0][0]
                        new_row_df.iloc[0, 1] = bnds[1][0]
                        if (rating == 'corporateBBB'):
                            ''' For corporateBBB, follow the weight distribution'''
                            new_row_df.iloc[0, 2:] = [val * (1 - new_row_df.iloc[0, 0:2].sum()) for val in [0.1549, 0.2566, 0.4351, 0.1534]]

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
                        total_bnds = helpers.get_bnds_for_total(Asset_mix2, solution_df) # Change Asset_mix to Asset_mix2 so it works here (* to make totals work)
                        # old: Asset_mix - Brenda
                        new_row_df = total_bnds.loc[[rating]].reset_index(drop=True)
                        new_row_df.iloc[0, 0] = 1 - sum(new_row_df.iloc[0, 1:])

                        new_row_df['portfolio'] = portfolio
                        new_row_df['rating'] = rating
                        solution_df = pd.concat([new_row_df, solution_df], ignore_index=True)
                        continue


            ''' First grab the KRDs of the assets of the corresponding rating '''
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
            target = target.T*-1

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
                    bond_krd = bond_krd.reset_index().drop(bond_krd.columns[[0, 1]], axis=1).drop('index', axis=1).to_numpy()

                    ''' Getting the solved weights from solution_df '''
                    weights = solution_df.loc[(solution_df['portfolio'] == 'Total') & (solution_df['rating'] == bond_rating)].drop(columns={'portfolio', 'rating'}).to_numpy().reshape(-1, 1)

                    ''' Getting the total amount allocated for the rating '''
                    investment = Asset_mix['Total'].loc[bond_rating]/10000

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
                cons = ({'type': 'eq', 'fun': lambda x: sum(sum(np.multiply(x, krd))    ) - sum(target)},
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

                total_bnds = helpers.get_bnds_for_total(Asset_mix2, solution_df) # insert reading assetmix2 here

                # Brenda (*end)
                # total_bnds = helpers.get_bnds_for_total(Asset_mix, solution_df) # insert reading assetmix2 here - brenda commented out for now (temporary)
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
    solution_df.iloc[:, :6] = solution_df.iloc[:, :6].astype(float).round(4)

    # print(solution_df)
    # print(asset_type)

    return solution_df

def optimization_b(given_date, over_under, asset_type='public', swap=False, curMonthBS=False):  # default sheet_version is segments (1)

#def optimization(given_date, over_under, asset_type='public', swap=False, curMonthBS=False, sheet_version=1):  # default sheet_version is segments (1)

    KRDs = reading_asset_KRDs(given_date)
    if curMonthBS:
        df_public, df_private, df_mortgages = reading_asset_mix(given_date, curMonthBS) #, sheet_version) # top
    else:
        df_public, df_private, df_mortgages = reading_asset_mix(given_date) #, False, sheet_version)
        # df_public, df_private, df_mortgages = reading_asset_mix(given_date)  # same meaning really as their top one

    # reading_liabilities(given_date)


    ''' Setting Asset_mix to the correct table based on the given asset class '''
    if asset_type == 'private':
        Asset_mix = df_private
    elif asset_type == 'mortgage':
        Asset_mix = df_mortgages
    else:
        Asset_mix = df_public # For all


    ''' Getting target sensitivities for all asset classes from the database '''
    #get_target_sensitivities_query = """
    #                SELECT *
    #                FROM target_sensitivity
    #                WHERE date= '{}'
    #                """.format(over_under.date())
    #get_col_names = '''SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'target_sensitivity';'''
    #col_names = [name[0] for name in execute_table_query(get_col_names, 'Benchmark', fetch=True)]
    #df = pd.DataFrame(execute_table_query(get_target_sensitivities_query, 'Benchmark', fetch=True), columns=col_names)



    #df['rating'] = df['rating'].replace(
    #    {'FEDERAL': 'Federal', 'CorpBBB': 'corporateBBB', 'PROVINCIAL': 'Provincial', 'CorpA': 'corporateA',
    #     'CorpAAA_AA': 'corporateAAA_AA', 'CorpBB_B': 'corporateBB_B'})
    #df = df.drop('70Y', axis=1)



    ''' Separating the db values into 3 tables, one for each asset class '''
    private_sensitivity = helpers.private_sensitivities().set_index(['portfolio', 'rating'])
    mortgage_sensitivity = helpers.mortgage_sensitivities().set_index(['portfolio', 'rating'])


    ''' Setting the sensitivities to be used as targets for the optimizer, for the correct asset class'''
    if asset_type == 'private':
        net_sensitivity = helpers.private_sensitivities()
        total_sensitivity = net_sensitivity

    elif asset_type == 'mortgage':
        net_sensitivity = helpers.mortgage_sensitivities()
        total_sensitivity = net_sensitivity

    else:
        ''' For the public optimization, we subtract the private and mortgage target sensitivities from the public target and optimize for the net sensitivity '''
        net_sensitivity = helpers.public_sensitivities()




    ''' For the sensitivity targets for the public totals, we subtract the public and mortgage components of all ratings
    we sum the public sensitivities for all 5 portfolios, then subtract the sum of privates for all portfolios, including ParCSM and Surplus'''
    if asset_type == 'public':
        net_sensitivity = helpers.public_sensitivities()
    elif asset_type == 'private':
        net_sensitivity = helpers.private_sensitivities()
    elif asset_type == 'mortgage':
        net_sensitivity = helpers.mortgage_sensitivities()



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
            if ((asset_type == 'mortgage') & ((rating == 'corporateAAA_AA') or (rating == 'corporateA') or (rating == 'Provincial'))) or ((asset_type == 'private') & ((rating == 'Federal') or (rating == 'Provincial'))):
                continue

            ''' The following cases do not run through the optimizer '''
            if (asset_type == 'public'):
                if ((portfolio == 'np') or (portfolio == 'ul') or (portfolio == 'Payout')):
                    ''' CorporateBBB for Nonpar, Universal and Payout is not optimized. Buckets 3-6 are distributed according to the pre-determined weights to reduce concentration after buckets 1 and 2 are made.
                    CorporateA bonds are also not optimized for Nonpar and Universal - minimum amount is placed in buckets 1 and 2 and remaining is placed in bucket 6'''
                    if (rating == 'corporateBBB') or ((rating == 'corporateA') & (portfolio != 'Payout')):
                        ''' First get the amount to be placed in the first 2 buckets'''
                        bnds = helpers.calc_bounds(given_date, portfolio, sum(Asset_mix[portfolio])) # Looks at a single column for each segment (?)
                        new_row_df = pd.DataFrame(0, index=np.arange(1), columns=[0, 1, 2, 3, 4, 5])
                        new_row_df.iloc[0, 0] = bnds[0][0]
                        new_row_df.iloc[0, 1] = bnds[1][0]
                        if (rating == 'corporateBBB'):
                            ''' For corporateBBB, follow the weight distribution'''
                            new_row_df.iloc[0, 2:] = [val * (1 - new_row_df.iloc[0, 0:2].sum()) for val in [0.1549, 0.2566, 0.4351, 0.1534]]

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
                        total_bnds = helpers.get_bnds_for_total(Asset_mix, solution_df) # Change Asset_mix to Asset_mix2 so it works here (* to make totals work)

                        new_row_df = total_bnds.loc[[rating]].reset_index(drop=True)
                        new_row_df.iloc[0, 0] = 1 - sum(new_row_df.iloc[0, 1:])

                        new_row_df['portfolio'] = portfolio
                        new_row_df['rating'] = rating
                        solution_df = pd.concat([new_row_df, solution_df], ignore_index=True)
                        continue


            ''' First grab the KRDs of the assets of the corresponding rating '''
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
            target = target.T*-1

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
                    bond_krd = bond_krd.reset_index().drop(bond_krd.columns[[0, 1]], axis=1).drop('index', axis=1).to_numpy()

                    ''' Getting the solved weights from solution_df '''
                    weights = solution_df.loc[(solution_df['portfolio'] == 'Total') & (solution_df['rating'] == bond_rating)].drop(columns={'portfolio', 'rating'}).to_numpy().reshape(-1, 1)

                    ''' Getting the total amount allocated for the rating '''
                    investment = Asset_mix['Total'].loc[bond_rating]/10000

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
                cons = ({'type': 'eq', 'fun': lambda x: sum(sum(np.multiply(x, krd))    ) - sum(target)},
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

                # KRDs = reading_asset_KRDs(given_date)
                if curMonthBS:
                    df_public2, df_private2, df_mortgages2 = reading_asset_mix(given_date,
                                                                            curMonthBS, 0)  # , sheet_version) # top
                else:
                    df_public2, df_private2, df_mortgages2 = reading_asset_mix(given_date, False, 0)  # , False, sheet_version)
                    # df_public, df_private, df_mortgages = reading_asset_mix(given_date)  # same meaning really as their top one

                # reading_liabilities(given_date)

                ''' Setting Asset_mix to the correct table based on the given asset class '''
                if asset_type == 'private':
                    Asset_mix2 = df_private2
                elif asset_type == 'mortgage':
                    Asset_mix2 = df_mortgages2
                else:
                    Asset_mix2 = df_public2

                total_bnds = helpers.get_bnds_for_total(Asset_mix2, solution_df) # insert reading assetmix2 here

                # Brenda (*end)
                # total_bnds = helpers.get_bnds_for_total(Asset_mix, solution_df) # insert reading assetmix2 here - brenda commented out for now (temporary)
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
    solution_df.iloc[:, :6] = solution_df.iloc[:, :6].astype(float).round(4)

    # print(solution_df)
    # print(asset_type)

    return solution_df

def optimization_worker(given_date, over_under, asset_type='public', swap=False, curMonthBS=False, sheet_version=1):  # default sheet_version is segments (1)

    KRDs = reading_asset_KRDs(given_date)

    if curMonthBS:
        df_public, df_private, df_mortgages = reading_asset_mix(given_date, curMonthBS, sheet_version) # top
    else:
        df_public, df_private, df_mortgages = reading_asset_mix(given_date, False, sheet_version)
        # df_public, df_private, df_mortgages = reading_asset_mix(given_date)  # same meaning really as their top one

    ''' Setting Asset_mix to the correct table based on the given asset class '''
    if asset_type == 'private':
        Asset_mix = df_private
    elif asset_type == 'mortgage':
        Asset_mix = df_mortgages
    else:
        Asset_mix = df_public # For all

    #''' Getting target sensitivities for all asset classes from the database '''
    #get_target_sensitivities_query = """
    #                SELECT *
    #                FROM target_sensitivity
    #                WHERE date= '{}'
    #                """.format(over_under.date())
    #get_col_names = '''SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'target_sensitivity';'''
    #col_names = [name[0] for name in execute_table_query(get_col_names, 'Benchmark', fetch=True)]
    #df = pd.DataFrame(execute_table_query(get_target_sensitivities_query, 'Benchmark', fetch=True), columns=col_names)



    #df['rating'] = df['rating'].replace(
    #    {'FEDERAL': 'Federal', 'CorpBBB': 'corporateBBB', 'PROVINCIAL': 'Provincial', 'CorpA': 'corporateA',
    #     'CorpAAA_AA': 'corporateAAA_AA', 'CorpBB_B': 'corporateBB_B'})
    #df = df.drop('70Y', axis=1)



    ''' Separating the db values into 3 tables, one for each asset class '''
    private_sensitivity = helpers.private_sensitivities().set_index(['portfolio', 'rating'])
    mortgage_sensitivity = helpers.mortgage_sensitivities().set_index(['portfolio', 'rating'])


    ''' Setting the sensitivities to be used as targets for the optimizer, for the correct asset class'''
    if asset_type == 'private':
        net_sensitivity = helpers.private_sensitivities()
        total_sensitivity = net_sensitivity

    elif asset_type == 'mortgage':
        net_sensitivity = helpers.mortgage_sensitivities()
        total_sensitivity = net_sensitivity

    else:
        ''' For the public optimization, we subtract the private and mortgage target sensitivities from the public target and optimize for the net sensitivity '''
        net_sensitivity = helpers.public_sensitivities()




    ''' For the sensitivity targets for the public totals, we subtract the public and mortgage components of all ratings
    we sum the public sensitivities for all 5 portfolios, then subtract the sum of privates for all portfolios, including ParCSM and Surplus'''
    if asset_type == 'public':
        net_sensitivity = helpers.public_sensitivities()
    elif asset_type == 'private':
        net_sensitivity = helpers.private_sensitivities()
    elif asset_type == 'mortgage':
        net_sensitivity = helpers.mortgage_sensitivities()



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
            if ((asset_type == 'mortgage') & ((rating == 'corporateAAA_AA') or (rating == 'corporateA') or (rating == 'Provincial'))) or ((asset_type == 'private') & ((rating == 'Federal') or (rating == 'Provincial'))):
                continue

            ''' The following cases do not run through the optimizer '''
            if (asset_type == 'public'):
                if ((portfolio == 'np') or (portfolio == 'ul') or (portfolio == 'Payout')):
                    ''' CorporateBBB for Nonpar, Universal and Payout is not optimized. Buckets 3-6 are distributed according to the pre-determined weights to reduce concentration after buckets 1 and 2 are made.
                    CorporateA bonds are also not optimized for Nonpar and Universal - minimum amount is placed in buckets 1 and 2 and remaining is placed in bucket 6'''
                    if (rating == 'corporateBBB') or ((rating == 'corporateA') & (portfolio != 'Payout')):
                        ''' First get the amount to be placed in the first 2 buckets'''
                        bnds = helpers.calc_bounds(given_date, portfolio, sum(Asset_mix[portfolio])) # Looks at a single column for each segment (?)
                        new_row_df = pd.DataFrame(0, index=np.arange(1), columns=[0, 1, 2, 3, 4, 5])
                        new_row_df.iloc[0, 0] = bnds[0][0]
                        new_row_df.iloc[0, 1] = bnds[1][0]
                        if (rating == 'corporateBBB'):
                            ''' For corporateBBB, follow the weight distribution'''
                            new_row_df.iloc[0, 2:] = [val * (1 - new_row_df.iloc[0, 0:2].sum()) for val in [0.1549, 0.2566, 0.4351, 0.1534]]

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
                        total_bnds = helpers.get_bnds_for_total(Asset_mix, solution_df) # Change Asset_mix to Asset_mix2 so it works here (* to make totals work)

                        new_row_df = total_bnds.loc[[rating]].reset_index(drop=True)
                        new_row_df.iloc[0, 0] = 1 - sum(new_row_df.iloc[0, 1:])

                        new_row_df['portfolio'] = portfolio
                        new_row_df['rating'] = rating
                        solution_df = pd.concat([new_row_df, solution_df], ignore_index=True)
                        continue


            ''' First grab the KRDs of the assets of the corresponding rating '''
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
            target = target.T*-1

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
                    bond_krd = bond_krd.reset_index().drop(bond_krd.columns[[0, 1]], axis=1).drop('index', axis=1).to_numpy()

                    ''' Getting the solved weights from solution_df '''
                    weights = solution_df.loc[(solution_df['portfolio'] == 'Total') & (solution_df['rating'] == bond_rating)].drop(columns={'portfolio', 'rating'}).to_numpy().reshape(-1, 1)

                    ''' Getting the total amount allocated for the rating '''
                    investment = Asset_mix['Total'].loc[bond_rating]/10000

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
                cons = ({'type': 'eq', 'fun': lambda x: sum(sum(np.multiply(x, krd))    ) - sum(target)},
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
                total_bnds = helpers.get_bnds_for_total(Asset_mix, solution_df) # insert reading assetmix2 here - brenda commented out for now (temporary)
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
    solution_df.iloc[:, :6] = solution_df.iloc[:, :6].astype(float).round(4)

    # print(solution_df)
    # print(asset_type)

    return solution_df

def optimization(given_date, over_under, asset_type='public', swap=False, curMonthBS=False):
    sheet_version = 1 # segments
    sol_df_seg = optimization_worker(given_date, over_under, asset_type, swap, curMonthBS, sheet_version)
    sheet_version = 0 # totals
    sol_df_tot = optimization_worker(given_date, over_under, asset_type, swap, curMonthBS, sheet_version)

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


# Brenda (*end of test)
def optimization_orig(given_date, over_under, asset_type='public', swap=False, curMonthBS=False):  # default sheet_version is segments (1)

#def optimization(given_date, over_under, asset_type='public', swap=False, curMonthBS=False, sheet_version=1):  # default sheet_version is segments (1)

    KRDs = reading_asset_KRDs(given_date)
    if curMonthBS:
        df_public, df_private, df_mortgages = reading_asset_mix(given_date, curMonthBS) #, sheet_version) # top
    else:
        df_public, df_private, df_mortgages = reading_asset_mix(given_date) #, False, sheet_version)
        # df_public, df_private, df_mortgages = reading_asset_mix(given_date)  # same meaning really as their top one

    # reading_liabilities(given_date)


    ''' Setting Asset_mix to the correct table based on the given asset class '''
    if asset_type == 'private':
        Asset_mix = df_private
    elif asset_type == 'mortgage':
        Asset_mix = df_mortgages
    else:
        Asset_mix = df_public # For all


    ''' Getting target sensitivities for all asset classes from the database '''
    get_target_sensitivities_query = """
                    SELECT *
                    FROM target_sensitivity
                    WHERE date= '{}' 
                    """.format(over_under.date())
    get_col_names = '''SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'target_sensitivity';'''
    col_names = [name[0] for name in execute_table_query(get_col_names, 'Benchmark', fetch=True)]
    df = pd.DataFrame(execute_table_query(get_target_sensitivities_query, 'Benchmark', fetch=True), columns=col_names)



    df['rating'] = df['rating'].replace(
        {'FEDERAL': 'Federal', 'CorpBBB': 'corporateBBB', 'PROVINCIAL': 'Provincial', 'CorpA': 'corporateA',
         'CorpAAA_AA': 'corporateAAA_AA', 'CorpBB_B': 'corporateBB_B'})
    df = df.drop('70Y', axis=1)



    ''' Separating the db values into 3 tables, one for each asset class '''
    private_sensitivity = helpers.private_sensitivities().set_index(['portfolio', 'rating'])
    mortgage_sensitivity = helpers.mortgage_sensitivities().set_index(['portfolio', 'rating'])


    ''' Setting the sensitivities to be used as targets for the optimizer, for the correct asset class'''
    if asset_type == 'private':
        net_sensitivity = helpers.private_sensitivities()
        total_sensitivity = net_sensitivity

    elif asset_type == 'mortgage':
        net_sensitivity = helpers.mortgage_sensitivities()
        total_sensitivity = net_sensitivity

    else:
        ''' For the public optimization, we subtract the private and mortgage target sensitivities from the public target and optimize for the net sensitivity '''
        net_sensitivity = helpers.public_sensitivities()




    ''' For the sensitivity targets for the public totals, we subtract the public and mortgage components of all ratings
    we sum the public sensitivities for all 5 portfolios, then subtract the sum of privates for all portfolios, including ParCSM and Surplus'''
    if asset_type == 'public':
        net_sensitivity = helpers.public_sensitivities()
    elif asset_type == 'private':
        net_sensitivity = helpers.private_sensitivities()
    elif asset_type == 'mortgage':
        net_sensitivity = helpers.mortgage_sensitivities()



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
            if ((asset_type == 'mortgage') & ((rating == 'corporateAAA_AA') or (rating == 'corporateA') or (rating == 'Provincial'))) or ((asset_type == 'private') & ((rating == 'Federal') or (rating == 'Provincial'))):
                continue

            ''' The following cases do not run through the optimizer '''
            if (asset_type == 'public'):
                if ((portfolio == 'np') or (portfolio == 'ul') or (portfolio == 'Payout')):
                    ''' CorporateBBB for Nonpar, Universal and Payout is not optimized. Buckets 3-6 are distributed according to the pre-determined weights to reduce concentration after buckets 1 and 2 are made.
                    CorporateA bonds are also not optimized for Nonpar and Universal - minimum amount is placed in buckets 1 and 2 and remaining is placed in bucket 6'''
                    if (rating == 'corporateBBB') or ((rating == 'corporateA') & (portfolio != 'Payout')):
                        ''' First get the amount to be placed in the first 2 buckets'''
                        bnds = helpers.calc_bounds(given_date, portfolio, sum(Asset_mix[portfolio])) # Looks at a single column for each segment (?)
                        new_row_df = pd.DataFrame(0, index=np.arange(1), columns=[0, 1, 2, 3, 4, 5])
                        new_row_df.iloc[0, 0] = bnds[0][0]
                        new_row_df.iloc[0, 1] = bnds[1][0]
                        if (rating == 'corporateBBB'):
                            ''' For corporateBBB, follow the weight distribution'''
                            new_row_df.iloc[0, 2:] = [val * (1 - new_row_df.iloc[0, 0:2].sum()) for val in [0.1549, 0.2566, 0.4351, 0.1534]]

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
                        total_bnds = helpers.get_bnds_for_total(Asset_mix, solution_df) # Change Asset_mix to Asset_mix2 so it works here (* to make totals work)

                        new_row_df = total_bnds.loc[[rating]].reset_index(drop=True)
                        new_row_df.iloc[0, 0] = 1 - sum(new_row_df.iloc[0, 1:])

                        new_row_df['portfolio'] = portfolio
                        new_row_df['rating'] = rating
                        solution_df = pd.concat([new_row_df, solution_df], ignore_index=True)
                        continue


            ''' First grab the KRDs of the assets of the corresponding rating '''
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
            target = target.T*-1

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
                    bond_krd = bond_krd.reset_index().drop(bond_krd.columns[[0, 1]], axis=1).drop('index', axis=1).to_numpy()

                    ''' Getting the solved weights from solution_df '''
                    weights = solution_df.loc[(solution_df['portfolio'] == 'Total') & (solution_df['rating'] == bond_rating)].drop(columns={'portfolio', 'rating'}).to_numpy().reshape(-1, 1)

                    ''' Getting the total amount allocated for the rating '''
                    investment = Asset_mix['Total'].loc[bond_rating]/10000

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
                cons = ({'type': 'eq', 'fun': lambda x: sum(sum(np.multiply(x, krd))    ) - sum(target)},
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

                # KRDs = reading_asset_KRDs(given_date)
                if curMonthBS:
                    df_public2, df_private2, df_mortgages2 = reading_asset_mix(given_date,
                                                                            curMonthBS, 0)  # , sheet_version) # top
                else:
                    df_public2, df_private2, df_mortgages2 = reading_asset_mix(given_date, False, 0)  # , False, sheet_version)
                    # df_public, df_private, df_mortgages = reading_asset_mix(given_date)  # same meaning really as their top one

                # reading_liabilities(given_date)

                ''' Setting Asset_mix to the correct table based on the given asset class '''
                if asset_type == 'private':
                    Asset_mix2 = df_private2
                elif asset_type == 'mortgage':
                    Asset_mix2 = df_mortgages2
                else:
                    Asset_mix2 = df_public2

                total_bnds = helpers.get_bnds_for_total(Asset_mix2, solution_df) # insert reading assetmix2 here

                # Brenda (*end)
                # total_bnds = helpers.get_bnds_for_total(Asset_mix, solution_df) # insert reading assetmix2 here - brenda commented out for now (temporary)
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
    solution_df.iloc[:, :6] = solution_df.iloc[:, :6].astype(float).round(4)

    # print(solution_df)
    # print(asset_type)

    return solution_df

def get_user_info():
    """
    Gets command-line info from User.
    """
    # Gets Parser:
    parser = argparse.ArgumentParser(description="Portfolio Optimization Tool")
    # Uses current date for both if not given:
    parser.add_argument("-d", "--GivenDate", type=str,
                        help="Use YYYY-MM-DD to set the Date for the calculation.")
    parser.add_argument("-o", "--OU_Date", type=str,
                        help="Use YYYY-MM-DD to use specific over_under_assetting file")
    parser.add_argument('-c', '--create', action='store_true', help='include this if the liabilities for the selected date have not yet been uploaded to the db')

    # Optional for tests (benchmarking.py):
    parser.add_argument("-m", "--mortgage", action='store_true', help="Include to generate output for mortgages, or leave all 3 blank to do all 3")
    parser.add_argument("-pb", "--public", action='store_true', help="Include to generate output for publics, or leave all 3 blank to do all 3")
    parser.add_argument("-pv", "--private", action='store_true', help="Include to generate output for privates, or leave all 3 blank to do all 3")
    # For benchmarking tables generator
    # parser.add_argument('-s', '--swap', action='store_true',
    #                    help="Set to true if interest rate swap sensitivities are backed out")
    # parser.add_argument('-cb', '--curMonthBS', action='store_true',
    #                    help='include to run economics with current month balance sheet instead of previous')

    # Add debugging version to produce all tables:

    #

    # Assign args, GivenDate, OU_Date:
    args = parser.parse_args()

    if args.GivenDate is None:
        GivenDate = dt.datetime.now() # Can use these as default arguments if desired
    else:
        GivenDate = conversions.YYYYMMDDtoDateTime(args.GivenDate)

    if args.OU_Date is None:
        OU_Date = conversions.YYYYMMDDtoDateTime(args.GivenDate)
    else:
        OU_Date = conversions.YYYYMMDDtoDateTime(args.OU_Date)

    # Return function:
    return args, GivenDate, OU_Date

def main(): # model_portfolio.py new version

    try:
        args, GivenDate, OU_Date = get_user_info()

        # ftse_data_cache = {}

        misc.log('Starting run of: ' + str(GivenDate), LOGFILE)

        #if args.create:
        #    misc.log('reading liabilities option selected', LOGFILE)
        #    reading_liabilities(OU_Date)

        do_all = False
        if (args.mortgage == False) & (args.public == False) & (args.private == False):
            do_all = True

        if args.mortgage or do_all:
            misc.log('Optimizing mortgages', LOGFILE)
            mort_solution = optimization(GivenDate, OU_Date, asset_type='mortgage')

        if args.public or do_all:
            misc.log('Optimizing publics', LOGFILE)
            public_solution = optimization(GivenDate, OU_Date, asset_type='public')

        if args.private or do_all:
            misc.log('Optimizing privates', LOGFILE)
            private_solution = optimization(GivenDate, OU_Date, asset_type='private')

        cur_date = GivenDate.strftime('%Y%m%d')
        # path = os.path.join(sysenv.get('PORTFOLIO_ATTRIBUTION_DIR'), 'Benchmarking', 'Test', 'benchmarking_outputs', cur_date) - old (normal)

        folder_path = os.path.join(sysenv.get('PORTFOLIO_ATTRIBUTION_DIR'), 'Benchmarking', 'Test',
                                   'benchmarking_outputs',
                                   'Brenda', cur_date)  # added folder 'Brenda' - Brenda
        os.makedirs(folder_path, exist_ok=True) # (*) delete and replace with Normal (below)

        test_ftse_cashflows_path = folder_path + '/FTSE_Cashflows_' + cur_date + '.xlsx'  # Brenda (temp solution)

        # for benchmarking only:
        # custom_benchmarks_path = folder_path + '/Custom_benchmark_' + cur_date + '.xlsx'
        # cfs_path = folder_path + '/CFs' + cur_date + '.xlsx'

        # Replace (*)
        # if not os.path.exists(path): # - old (Normal)
        #     os.mkdir(path)

        # Creates folder for FTSE Cashflows
        # lol ill fix this code

        ftse_data = helpers.get_ftse_data(GivenDate)  # gets ftse bond info from our database

        cf_dict = helpers.create_cf_tables(ftse_data)

        # Writes to cashflows:
        FCF_PATH = os.path.join(sysenv.get('PORTFOLIO_ATTRIBUTION_DIR'), 'Benchmarking', 'Test',
                                   'benchmarking_outputs',
                                   'Brenda')
        FTSE_CASHFLOWS_DIR = os.path.join(FCF_PATH, 'FTSE_Cashflows', cur_date)
        os.makedirs(FTSE_CASHFLOWS_DIR, exist_ok=True)  # Create directories 'brenda' and 'logs' if they don't exist

        for rating in ['Federal', 'Provincial', 'CorporateAAA_AA', 'CorporateA', 'CorporateBBB', 'Corporate']:
            file_path = FTSE_CASHFLOWS_DIR + '/' + rating + '_FTSE_bucketed_cashflows_' + cur_date + '.xlsx'
            if not os.path.exists(file_path):
                with pd.ExcelWriter(file_path) as writer:
                    cf_dict[rating].to_excel(writer)
        print("Successfully written to cashflows")

        # Creates solutions folder:
        file_path = folder_path + '/solutions' + cur_date + '.xlsx' # folder_path used to be var path - old (Normal)
        if not os.path.exists(file_path):
            with pd.ExcelWriter(file_path) as writer:
                if args.public or do_all:
                    public_solution.to_excel(writer, sheet_name='public_solution')
                if args.private or do_all:
                    private_solution.to_excel(writer, sheet_name='private_solution')
                if args.mortgage or do_all:
                    mort_solution.to_excel(writer, sheet_name='mortgage_solution')
                reading_asset_KRDs(GivenDate).to_excel(writer, sheet_name='asset KRDs')
                reading_asset_mix(GivenDate)[0].to_excel(writer, sheet_name='public asset mix')
                reading_asset_mix(GivenDate)[1].to_excel(writer, sheet_name='private asset mix')
                reading_asset_mix(GivenDate)[2].to_excel(writer, sheet_name='mort asset mix')

        else:
            print("file already exists - delete file before running")

        print("Success")

    except:
        misc.log("Failed " + misc.errtxt(), LOGFILE)

if __name__ == "__main__":
    main()



