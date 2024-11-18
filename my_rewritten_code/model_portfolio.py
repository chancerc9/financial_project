"""
Name: model_portfolio_generate.py

Purpose:

Functions:

Side effects:

"""

"""
    This provided code is a complex script that processes bond-related data, 
    calculates key rate durations (KRDs), and uploads the resulting sensitivities 
    to a database. 
    
    
    Original description (a portion of):
        There are many ways this script can be used.

        It is "safe" to be ran as many times as warranted.

        When ran as main, without any arguments:
            It will do the analysis for today (using 6PM cut-off logic.)
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
import helpers
import model_portfolio_process as model_portfolio
import cashflows_and_benchmark_tables
import cli

# Configure pandas display settings
pd.set_option('display.width', 150)

# Database connections (Benchmark, Bond, and General)
BM_conn = SmartDB('Benchmark')
BM_cur = BM_conn.con.cursor()

Bond_conn = SmartDB('Bond')
Bond_cur = Bond_conn.con.cursor()

General_conn = SmartDB('General')
General_cur = General_conn.con.cursor()

import encapsulated_objects as datahandler
import file_utils

def main():  # model_portfolio.py new version
    """
    Main function to orchestrate the portfolio optimization process.
    It gathers user input, runs the appropriate optimizations, and saves the results.
    """

    # Logging directories:
    MY_LOG_DIR = os.path.join(sysenv.get('PORTFOLIO_ATTRIBUTION_DIR'), 'logs', 'brenda')
    os.makedirs(MY_LOG_DIR, exist_ok=True)  # Create directories if they don't exist
    LOGFILE = open(os.path.join(MY_LOG_DIR, 'benchmarking_log.txt'),
                   'a')  # Append to the existing logfile, or create a new one

    # Main code:
    try:
        # Retrieve user input
        args, GivenDate = cli.get_user_info()

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

        bond_curves = datahandler.get_bond_curves(
                GivenDate)  # Retrieve annual bond curve data (annual curve data for 35 years)


        # Begin process:

        # 1. Define FTSE Universe:

        # i) Initialize FTSEDataHandler for GivenDate:
        ftse_handler = datahandler.FTSEDataHandler(GivenDate)

        # ii) Retrieve a copy of the FTSE bond data DataFrame
        ftse_data = ftse_handler.data

        # Display the data to see its structure and contents - can write to excel file here.
        print(ftse_data.head())  # Prints the first few rows of the DataFrame


        # 2. Get cashflows

        # 3. Get KRDs from cashflows - Gets info using copy of ftse data
        KRDs = model_portfolio.reading_asset_KRDs(bond_curves, ftse_handler.data, GivenDate)    # Generate KRD Table for all assets (to feed to optim function and write later; keep in memory or write now)

        print(f"KRDs.head() initially {KRDs.head()}")
        print(f"KRDs.tail() initially {KRDs.tail()}")

        # Test code to keep; verified: KRDs does not change, which is good and means useable
        import copy
        copied = copy.deepcopy(KRDs)

        # fast initially tho may take a bit or few of mem

        # ftse_data = helpers.get_ftse_data(GivenDate)            # Gets ftse bond info from our database

        CURR_DEBUGGING_PATH = os.path.join(OUTPUT_DIR_PATH, 'Debugging_Steps')
        # CURR_FILE_PATH = os.path.join(CURR_DEBUGGING_PATH, 'ftse_bond_curves.xlsx')
        os.makedirs(CURR_DEBUGGING_PATH, exist_ok=True)

        # Output items:
        file_utils.write_results_to_excel_one_sheet(ftse_data,CURR_DEBUGGING_PATH,cur_date,'ftse_data') # for ftse data


        # Optimization function uses .copy()

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
            private_solution = model_portfolio.optimization(KRDs, GivenDate, asset_type='private') # supposed to optimize the privates last ?

            # Benchmarking code: , curMonthBS=args.curMonthBS)

        print(f"KRDs.head() after running other functions: {KRDs.head()}")
        print(f"KRDs.tail() after running other functions: {KRDs.tail()}")

        if copied.equals(KRDs):
            print("No changes were made to KRDs data")
        else:
            print("The data has been modified")

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
                # model_portfolio.reading_asset_KRDs(bond_curves, ftse_handler.data, GivenDate).to_excel(writer, sheet_name='asset KRDs')
                KRDs.to_excel(writer, sheet_name='asset KRDs') # see if works lol
                model_portfolio.reading_asset_mix(GivenDate)[0].to_excel(writer, sheet_name='public asset mix')
                model_portfolio.reading_asset_mix(GivenDate)[1].to_excel(writer, sheet_name='private asset mix')
                model_portfolio.reading_asset_mix(GivenDate)[2].to_excel(writer, sheet_name='mort asset mix')

        else:
            print("solutions file already exists - can't make a file with the same name'")


        print("Successfully ran: solutions")


        ftse_data = ftse_handler.data

        # Run and output benchmarks:
        if args.mortgage or do_all:

            summary_mort = cashflows_and_benchmark_tables.create_summary_table(GivenDate, asset_type='mortgage', curMonthBs=args.curMonthBS)
            data_mort = cashflows_and_benchmark_tables.create_indexData_table(mort_solution, GivenDate, ftse_handler.data, asset_type='mortgage')

            # Data is protected here:
            summed_cashflows_mort = cashflows_and_benchmark_tables.create_summed_cashflow_tables(bond_curves, ftse_data, data_mort, mort_solution, GivenDate, asset_type='mortgage',
                                                                  curMonthBs=args.curMonthBS)


        if args.public or do_all:

            summary_public = cashflows_and_benchmark_tables.create_summary_table(GivenDate, asset_type='public', curMonthBs=args.curMonthBS)
            data_public = cashflows_and_benchmark_tables.create_indexData_table(public_solution, GivenDate, ftse_handler.data, asset_type='public')

            # Data is protected here:
            summed_cashflows_public = cashflows_and_benchmark_tables.create_summed_cashflow_tables(bond_curves, ftse_data, data_public, public_solution, GivenDate, asset_type='public',
                                                                    curMonthBs=args.curMonthBS)
        if args.private or do_all:

            summary_private = cashflows_and_benchmark_tables.create_summary_table(GivenDate, asset_type='private', curMonthBs=args.curMonthBS)
            data_private = cashflows_and_benchmark_tables.create_indexData_table(private_solution, GivenDate, ftse_handler.data, asset_type='private')

            # Data is protected here:
            summed_cashflows_private = cashflows_and_benchmark_tables.create_summed_cashflow_tables(bond_curves, ftse_data, data_private, private_solution, GivenDate, asset_type='private',
                                                                     curMonthBs=args.curMonthBS)

        custom_benchmarks_path = OUTPUT_DIR_PATH + '/Custom_benchmark_' + cur_date + '.xlsx'
        cfs_path = OUTPUT_DIR_PATH + '/CFs' + cur_date + '.xlsx'

        # Can probably check if_exist before running / creating Custom_benchmarks tables to just output some:
        # Solutions will ALWAYS run since needed for even 1 table
        # cashflows should always run IMO since part of both debugging and mandatory

        import psutil
        process = psutil.Process()
        memory_info = process.memory_info()
        print(f"Mem usage: {memory_info.rss} bytes")

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



