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
LOGFILE = open(os.path.join(MY_LOG_DIR, 'benchmarking_log.txt'), 'a')  # Append to the existing logfile, or create a new one



import model_portfolio_process as model_portfolio
import cashflows_and_benchmark_tables as cashflows_and_benchmark_tables

""" Changelog Brenda (09-30-24):
    I've now modified it to generate KRDs and hold them to pass down in memory rather than generate new KRDs during 
    each sector's back-to-back optimization.

    Considering, optimization and writing KRD solutions to excel file happen back-to-back hence most of stack memory
    holds it concurrently anyways.

    They are the same KRD profiles tables to write to excel in the very end, and used for every type of liability optimization. 
    Since, we have
        asset KRD profiles + list(liability segments)
    for optimization.
"""

def main():  # model_portfolio.py new version
    """
    Main function to orchestrate the portfolio optimization process.
    It gathers user input, runs the appropriate optimizations, and saves the results.
    """
    try:
        # Retrieve user input
        args, GivenDate = model_portfolio.get_user_info()

        misc.log(f'Starting run of: {GivenDate}', LOGFILE)

        # Determine if all outputs need to be optimized
        do_all = not (args.mortgage or args.public or args.private)

        # Str form of GivenDate
        cur_date: str = GivenDate.strftime('%Y%m%d')

        # Define Results (output) Directory:
        OUTPUT_DIR_PATH: str = os.path.join(sysenv.get('PORTFOLIO_ATTRIBUTION_DIR'), 'Benchmarking', 'Test',
                                            'benchmarking_outputs',
                                            'Brenda', cur_date)  # Test path - Brenda
        os.makedirs(OUTPUT_DIR_PATH, exist_ok=True)
        """Original filepath:
        path = os.path.join(sysenv.get('PORTFOLIO_ATTRIBUTION_DIR'), 'Benchmarking', 'Test', 'benchmarking_outputs', cur_date) - old (normal)
        """


        # Begin process:

        # Gets info
        KRDs = model_portfolio.reading_asset_KRDs(GivenDate)    # Generate KRD Table for all assets (to feed to optim function and write later; keep in memory or write now)
        ftse_data = helpers.get_ftse_data(GivenDate)            # Gets ftse bond info from our database

        # Output items:
        model_portfolio.write_results_to_excel_one_sheet(ftse_data,OUTPUT_DIR_PATH,cur_date,'ftse_data') # for ftse data


        # Optimize for mortgages, publics, and privates based on user input
        if args.mortgage or do_all:
            misc.log('Optimizing mortgages', LOGFILE)

            # Model portfolio:
            mort_solution = model_portfolio.optimization(KRDs, GivenDate, asset_type='mortgage') # make ftse data a parent class that is inherited by any data manipulating function classes below - any without using it can be a method(s)

            # Benchmarking code:
            # mort_solution = model_portfolio.optimization(KRDs, GivenDate, asset_type='mortgage', curMonthBS=args.curMonthBS)

        if args.public or do_all:
            misc.log('Optimizing publics', LOGFILE)

            # Model portfolio:
            public_solution = model_portfolio.optimization(KRDs, GivenDate, asset_type='public')

            # Benchmarking code: , curMonthBS=args.curMonthBS)

        if args.private or do_all:
            misc.log('Optimizing privates', LOGFILE)

            # Model portfolio:
            private_solution = model_portfolio.optimization(KRDs, GivenDate, asset_type='private')

            # Benchmarking code: , curMonthBS=args.curMonthBS)


        # Creates solutions folder:
        solutions_path = OUTPUT_DIR_PATH + '/solutions' + cur_date + '.xlsx' # folder_path used to be var path - old (Normal)

        if not os.path.exists(solutions_path):
            with pd.ExcelWriter(solutions_path) as writer:
                if args.public or do_all:
                    public_solution.to_excel(writer, sheet_name='public_solution')
                if args.private or do_all:
                    private_solution.to_excel(writer, sheet_name='private_solution')
                if args.mortgage or do_all:
                    mort_solution.to_excel(writer, sheet_name='mortgage_solution')
                model_portfolio.reading_asset_KRDs(GivenDate).to_excel(writer, sheet_name='asset KRDs')
                model_portfolio.reading_asset_mix(GivenDate)[0].to_excel(writer, sheet_name='public asset mix')
                model_portfolio.reading_asset_mix(GivenDate)[1].to_excel(writer, sheet_name='private asset mix')
                model_portfolio.reading_asset_mix(GivenDate)[2].to_excel(writer, sheet_name='mort asset mix')

        else:
            print("solutions file already exists - can't make a file with the same name'")


        print("Successfully ran: solutions")



        # Run and output benchmarks:
        if args.mortgage or do_all:

            summed_cashflows_mort = cashflows_and_benchmark_tables.create_summed_cashflow_tables(ftse_data, mort_solution, GivenDate, asset_type='mortgage',
                                                                  curMonthBs=args.curMonthBS)
            summary_mort = cashflows_and_benchmark_tables.create_summary_table(GivenDate, asset_type='mortgage', curMonthBs=args.curMonthBS)
            data_mort = cashflows_and_benchmark_tables.create_indexData_table(mort_solution, GivenDate, ftse_data, asset_type='mortgage')

        if args.public or do_all:

            summed_cashflows_public = cashflows_and_benchmark_tables.create_summed_cashflow_tables(ftse_data, public_solution, GivenDate, asset_type='public',
                                                                    curMonthBs=args.curMonthBS)

            summary_public = cashflows_and_benchmark_tables.create_summary_table(GivenDate, asset_type='public', curMonthBs=args.curMonthBS)
            data_public = cashflows_and_benchmark_tables.create_indexData_table(public_solution, GivenDate, ftse_data, asset_type='public')

        if args.private or do_all:

            summed_cashflows_private = cashflows_and_benchmark_tables.create_summed_cashflow_tables(ftse_data, private_solution, GivenDate, asset_type='private',
                                                                     curMonthBs=args.curMonthBS)
            summary_private = cashflows_and_benchmark_tables.create_summary_table(GivenDate, asset_type='private', curMonthBs=args.curMonthBS)
            data_private = cashflows_and_benchmark_tables.create_indexData_table(private_solution, GivenDate, ftse_data, asset_type='private')



        custom_benchmarks_path = OUTPUT_DIR_PATH + '/Custom_benchmark_' + cur_date + '.xlsx'
        cfs_path = OUTPUT_DIR_PATH + '/CFs' + cur_date + '.xlsx'

        # Can probably check if_exist before running / creating Custom_benchmarks tables to just output some:
        # Solutions will ALWAYS run since needed for even 1 table
        # cashflows should always run IMO since part of both debugging and mandatory

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
                        summed_cashflows_public[df].to_excel(writer, sheet_name=('summed cfs public - ' + df),
                                                             startrow=1)
                    if args.private or do_all:
                        summed_cashflows_private[df].to_excel(writer, sheet_name=('summed cfs private - ' + df),
                                                              startrow=1)

                    if args.mortgage or do_all:
                        summed_cashflows_mort[df].to_excel(writer, sheet_name=('summed cfs mort - ' + df), startrow=1)
        else:
            print('cashflows file for this date already exists - cant make a file with the same name')

        print('Successfully ran: Cashflows from solutions.')

        print('Success.')


    except:
        misc.log("Failed " + misc.errtxt(), LOGFILE)



if __name__ == "__main__":
    main()




