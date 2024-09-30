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

''' This function creates the summed cashflow tables using the solution from benchmarking.py '''
def create_summed_cashflow_tables(solution_df, given_date, asset_type='public',curMonthBs=False):
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

    ftse_data = create_indexData_table(solution_df, given_date, asset_type=asset_type)

    df_public, df_private, df_mortgage = bench.reading_asset_mix(given_date, curMonthBS=curMonthBs)

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

    for portfolio in ['NONPAR', 'GROUP', 'PAYOUT', 'ACCUM', 'UNIVERSAL', 'SURPLUS', 'TOTAL']:
        date_range = pd.date_range(given_date, periods=70, freq='6M') # (given_date, periods=420, freq='M') - old (brenda change)
        summed_cfs = pd.DataFrame({'date': date_range})

        carry_table = pd.DataFrame(columns=['Federal', 'Provincial', 'CorporateAAA_AA', 'CorporateA', 'CorporateBBB'],
                                   # index=['Carry income', 'Carry yield'])
                                   index=['market Value', 'Average Yield'])

        # renaming only corpBBBs for mortgage because corpA and AAA_AAs not included
        if asset_type == 'mortgage':
            portfolio_solution = benchmarking_solution.loc[benchmarking_solution['portfolio'] == portfolio].set_index(
                'rating').drop(columns='portfolio').rename(index={'corporateBBB': 'CorporateBBB'})
        else:
            portfolio_solution = benchmarking_solution.loc[benchmarking_solution['portfolio'] == portfolio].set_index(
                'rating').drop(columns='portfolio').rename(
                index={'corporateAAA_AA': 'CorporateAAA_AA', 'corporateA': 'CorporateA',
                       'corporateBBB': 'CorporateBBB'})



        for rating in ['Federal', 'Provincial', 'CorporateAAA_AA', 'CorporateA', 'CorporateBBB']:
            if ((asset_type == 'mortgage') & ((portfolio == 'UNIVERSAL') or ((rating != 'Federal') & (rating != 'CorporateBBB')))) or (
                    (asset_type == 'private') & ((rating == 'Federal') or (rating == 'Provincial'))):

                carry_table.loc['market Value', rating] = 0
                carry_table.loc['Average Yield', rating] = 0
                summed_cfs[rating] = 0

                continue
            df = weights[rating].iloc[:, 3:]
            df = portfolio_solution.loc[rating].values * df
            df = df.fillna(0)

            pv = cf[rating + 'PV']
            market_value = asset_mix.loc[rating, portfolio]
            bucketed_cashflow_totals = (df * market_value).sum(axis=1)
            bucket_aligned = pd.Series(bucketed_cashflow_totals.values, index=pv.index)
            #pv_aligned = pv.reindex(bucketed_cashflow_totals.index)
            df = bucket_aligned / pv
            cfs[rating] = cf[rating].iloc[:, 3:].mul(df, axis=0)
            summed_cfs[rating] = ((cfs[rating].sum() / 6).repeat(6)).reset_index(drop=True).reindex(range(70), # (given_date, periods=70, freq='6M'), with old was reindex(range(420)
                                                                                                      fill_value=0)

            df = ftse_data.loc[ftse_data['RatingBucket'] == rating]
            if df['Benchmark ' + portfolio + ' weight'].sum() == 0:
                yield1 = 0
            else:
                yield1 = (df['Benchmark ' + portfolio + ' weight'] * df['yield']).sum() / df[
                    'Benchmark ' + portfolio + ' weight'].sum()
            carry_table.loc['market Value', rating] = market_value
            carry_table.loc['Average Yield', rating] = yield1

        summed_cfs['date'] = pd.to_datetime(summed_cfs['date']).dt.strftime('%b-%Y')
        summed_cfs = pd.concat([carry_table, summed_cfs.set_index('date')])

        summed_cfs_dict[portfolio] = summed_cfs.fillna(0)


    # totals_cfs = summed_cfs_dict['NONPAR']
    # for portfolio in ['GROUP', 'PAYOUT', 'ACCUM', 'UNIVERSAL', 'SURPLUS']:
    #     totals_cfs = totals_cfs.add(summed_cfs_dict[portfolio])
    # summed_cfs_dict['TOTAL'] = totals_cfs
    return summed_cfs_dict

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




if __name__ == "__main__":

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

    #if args.create:
    #    if args.OU_Date:
    #        bench.reading_liabilities(OU_Date)
    #    else:
    #        bench.reading_liabilities(GivenDate)

    do_all = False
    if (args.mortgage==False) & (args.public==False) & (args.private==False):
        do_all = True

    if args.mortgage or do_all:
        mort_solution = bench.optimization(GivenDate, OU_Date, asset_type='mortgage', curMonthBS=args.curMonthBS)

        summed_cashflows_mort = create_summed_cashflow_tables(mort_solution, GivenDate, asset_type='mortgage', curMonthBs=args.curMonthBS)
        summary_mort = create_summary_table(GivenDate, asset_type='mortgage', curMonthBs=args.curMonthBS)
        data_mort = create_indexData_table(mort_solution, GivenDate, asset_type='mortgage')

    if args.public or do_all:
        public_solution = bench.optimization(GivenDate, OU_Date, asset_type='public', swap=args.swap, curMonthBS=args.curMonthBS)
            # else:
            #     public_solution = bench.optimization(GivenDate, asset_type='public')

        summed_cashflows_public = create_summed_cashflow_tables(public_solution, GivenDate, asset_type='public', curMonthBs=args.curMonthBS)
        summary_public = create_summary_table(GivenDate, asset_type='public', curMonthBs=args.curMonthBS)
        data_public = create_indexData_table(public_solution, GivenDate, asset_type='public')

    if args.private or do_all:
        private_solution = bench.optimization(GivenDate, OU_Date, asset_type='private', curMonthBS=args.curMonthBS)

        summed_cashflows_private = create_summed_cashflow_tables(private_solution, GivenDate, asset_type='private', curMonthBs=args.curMonthBS)
        summary_private = create_summary_table(GivenDate, asset_type='private', curMonthBs=args.curMonthBS)
        data_private = create_indexData_table(private_solution, GivenDate, asset_type='private')





    cur_date = GivenDate.strftime('%Y%m%d')

    folder_path = os.path.join(sysenv.get('PORTFOLIO_ATTRIBUTION_DIR'), 'Benchmarking', 'Test', 'benchmarking_outputs', 'Brenda', cur_date)

    custom_benchmarks_path = folder_path + '/Custom_benchmark_' + cur_date + '.xlsx'
    cfs_path = folder_path + '/CFs' + cur_date + '.xlsx'


    if not os.path.exists(folder_path):
        os.mkdir(folder_path)

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

