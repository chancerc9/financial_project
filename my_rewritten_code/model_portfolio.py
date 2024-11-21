"""
Name: model_portfolio_generate.py

Purpose:

Functions:

Side effects:

"""
from flask.logging import has_level_handler

"""
    This provided code is a complex script that processes bond-related data, 
    calculates key rate durations (KRDs), brings in sensitivities for liabilities and optimizes them with assets, and
    calculates cashflows from solution.

    It is "safe" to be ran as many times as warranted.

"""

"""
Definitions

ASSET CLASS: mortgage, public, private (split the items into ratings or UL, PAR, SURPLUS, etc)

--- separate ---

SEGMENTS: UL, PAR, SURPLUS
- a.k.a. 'portfolio'
These are broader categories for assets, each of which has a different allocated asset mix.

RATINGS:
- a.k.a. asset types, asset mix

To determine the asset types per segment, multiply the segment balance by the asset mix percentage that is unique to each
segment. This percentage matrix is called the "Asset Mix.xlsx" file, where the segment balances and total asset balance is 
called the "SBS Totals.xlsx" file. # This holds the liabilities, asset, and totals balance for all segments.

A: Assets
1. Calculate cashflows from FTSE universe.
2. Calculate KRDs from cashflows from FTSE universe.

B:
1. Bring in liability sensitivities through balance sheet and asset percentage matrix. The liabilities for our holdings. (FALSE)
1. Bring in liability sensitivities through "Targets by Asset Class.xlsx" as the targets to hedge asset krds and liabilities to.

OPTIMIZATION:
The calculated KRDs (simple) and brought-in KRDs from "Targets by Asset Class.xlsx" (to match) are matched during this 
process.

We essentially match the asset KRDs to liability KRDs for liabilities hedging, and perform an optimization function to 
maximize returns for Totals.
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

# Logging directories:
MY_LOG_DIR = os.path.join(sysenv.get('PORTFOLIO_ATTRIBUTION_DIR'), 'logs', 'brenda')
os.makedirs(MY_LOG_DIR, exist_ok=True)  # Create directories if they don't exist
LOGFILE = open(os.path.join(MY_LOG_DIR, 'benchmarking_log.txt'),
               'a')  # Append to the existing logfile, or create a new one


# Generalized function for optimization
def process_asset_type(asset_type, KRDs, GivenDate):
    misc.log(f"Optimizing {asset_type}", LOGFILE)
    solution = model_portfolio.optimization(KRDs, GivenDate, asset_type=asset_type)
    print(f"Successfully optimized: {asset_type}")
    return solution


# Generalized function for writing solutions.xlsx to Excel
def write_solutions_to_excel(solutions, solutions_path, KRDs, GivenDate):
    if not os.path.exists(solutions_path):
        with pd.ExcelWriter(solutions_path) as writer:
            # Write solutions dynamically based on available data
            for asset_type, solution in solutions.items():
                solution.to_excel(writer, sheet_name=f"{asset_type}_solution")
            # Write shared data
            KRDs.to_excel(writer, sheet_name="asset KRDs")
            mixes = model_portfolio.reading_asset_mix(GivenDate)
            mixes[0].to_excel(writer, sheet_name="public asset mix")
            mixes[1].to_excel(writer, sheet_name="private asset mix")
            mixes[2].to_excel(writer, sheet_name="mort asset mix")
        print("Successfully output solutions.xlsx file")
    else:
        print("solutions.xlsx file already exists - can't make a file with the same name")


def main():  # model_portfolio.py new version
    """
    Main function to orchestrate the portfolio optimization process.
    It gathers user input, runs the appropriate optimizations, and saves the results.
    """

    # Retrieve user input
    args, GivenDate = cli.get_user_info()

    # Str form of GivenDate
    cur_date: str = GivenDate.strftime('%Y%m%d')

    # Define Results (output) Directory:
    OUTPUT_DIR_PATH: str = os.path.join(sysenv.get('PORTFOLIO_ATTRIBUTION_DIR'), 'Benchmarking', 'Test',
                                        'benchmarking_outputs',
                                        'Brenda', cur_date)  # Test path - Brenda
    os.makedirs(OUTPUT_DIR_PATH, exist_ok=True)

    CURR_DEBUGGING_PATH = os.path.join(OUTPUT_DIR_PATH, 'Debugging_Steps')
    # CURR_FILE_PATH = os.path.join(CURR_DEBUGGING_PATH, 'ftse_bond_curves.xlsx')
    os.makedirs(CURR_DEBUGGING_PATH, exist_ok=True)

    # Creates folders for output items:
    solutions_path = OUTPUT_DIR_PATH + '/solutions' + cur_date + '.xlsx'  # folder_path used to be var path - old (Normal)

    custom_benchmarks_path = OUTPUT_DIR_PATH + '/Custom_benchmark_' + cur_date + '.xlsx'

    cfs_path = OUTPUT_DIR_PATH + '/CFs' + cur_date + '.xlsx'

    # Main code:
    try:
        misc.log(f'Starting run of: {GivenDate}', LOGFILE)

        # Begin process:

        bond_curves = datahandler.get_bond_curves(
                GivenDate)  # Retrieve annual bond curve data (annual curve data for 35 years)


        # 1. Define FTSE Universe:

        # i) Initialize FTSEDataHandler for GivenDate:
        ftse_handler = datahandler.FTSEDataHandler(GivenDate)

        # ii) Retrieve a copy of the FTSE bond data DataFrame
        ftse_data = ftse_handler.data

        # Display the data to see its structure and contents - can write to excel file here.
        # print(ftse_data.head())  # Prints the first few rows of the DataFrame

        # 2. Get cashflows

        # 3. Get KRDs from cashflows - Gets info using copy of ftse data
        KRDs = model_portfolio.reading_asset_KRDs(bond_curves, ftse_handler.data, GivenDate)    # Generate KRD Table for all assets (to feed to optim function and write later; keep in memory or write now)

        # TODO! Tests:
        # print(f"KRDs.head() initially {KRDs.head()}")
        # print(f"KRDs.tail() initially {KRDs.tail()}")

        # Test code to keep; verified: KRDs does not change, which is good and means useable
        # import copy
        # copied = copy.deepcopy(KRDs)

        # fast initially tho may take a bit or few of mem

        # ftse_data = helpers.get_ftse_data(GivenDate)            # Gets ftse bond info from our database


        # Output items:
        file_utils.write_results_to_excel_one_sheet(ftse_data,CURR_DEBUGGING_PATH,cur_date,'ftse_data') # for ftse data

        # Define the mask for conditionals
        mask = {
            "mortgage": args.mortgage,
            "public": args.public,
            "private": args.private,
        }

        # Determine if all outputs need to be optimized
        if not (args.mortgage or args.public or args.private):
            mask = {key: True for key in mask}

        """
        mask2 = {
            "custom_benchmarks": args.benchmarks,
            "cfs": args.cfs,
    
        }
        
        if not (args.benchmarks,args.cfs):
            mask2 = {key: True for key in mask2}
        """

        # Main logic
        print("Optimizing solutions")

        # Dictionary to hold results dynamically
        solutions = {}

        summary = {}
        data = {}

        summed_cashflows = {}

        # Process only the specified conditions
        for asset_type, condition in mask.items():
            if condition:
                solutions[asset_type] = process_asset_type(asset_type, KRDs, GivenDate)
                """
                misc.log(f"Optimizing {asset_type}", LOGFILE)
                solutions[asset_type] = model_portfolio.optimization(KRDs, GivenDate, asset_type)
                print(f"Successfully optimized: {asset_type}")
                """
        # Write solutions to Excel
        write_solutions_to_excel(solutions, solutions_path, KRDs, GivenDate)
        print("Successfully ran: solutions")


        print("Creating Custom_benchmark.xlsx")

        for asset_type, condition in mask.items():
            if condition:
                summary[asset_type] = cashflows_and_benchmark_tables.create_summary_table(GivenDate, asset_type,
                                                                               curMonthBs=args.curMonthBS)
                data[asset_type] = cashflows_and_benchmark_tables.create_indexData_table(solutions[asset_type], GivenDate,
                                                                                  ftse_handler.data,
                                                                                  asset_type)
        # Map asset types to their respective data
        dict_data = {
            "public": {"summary": summary.get("public"), "data": data.get("public")},
            "private": {"summary": summary.get("private"), "data": data.get("private")},
            "mortgage": {"summary": summary.get("mortgage"), "data": data.get("mortgage")},
        }
        # Write summaries and data to Excel
        if not os.path.exists(custom_benchmarks_path):
            with pd.ExcelWriter(custom_benchmarks_path) as writer:
                for asset_type, content in dict_data.items():
                    if mask[asset_type]:
                        # Write summary and data for the asset type
                        content["summary"].to_excel(writer, sheet_name=f"summary_{asset_type}")
                        content["data"].to_excel(writer, sheet_name=f"data_{asset_type}", index=False)
            print("Successfully created custom benchmarks file.")
        else:
            print("Custom benchmarks file for this date already exists - can't make a file with the same name.")


        print("Successfully ran: custom benchmarks")


        print("Creating cashflows from solutions: CFs.xlsx")

        for asset_type, condition in mask.items():
            if condition:
                summed_cashflows[asset_type] = cashflows_and_benchmark_tables.create_summed_cashflow_tables(bond_curves, ftse_data, # Data is protected here:
                                                                                                 data[asset_type],
                                                                                                 solutions[asset_type],
                                                                                                 GivenDate,
                                                                                                 asset_type,
                                                                                                 curMonthBs=args.curMonthBS)

        SEGMENTS = ('NONPAR', 'GROUP', 'PAYOUT', 'ACCUM', 'UNIVERSAL', 'TOTAL', 'SURPLUS')

        if not os.path.exists(cfs_path):
            with pd.ExcelWriter(cfs_path) as writer:
                for segment in SEGMENTS:
                    for asset_type, content in summed_cashflows.items():
                        content[segment].to_excel(writer, sheet_name=(f"summed cfs {asset_type} - {segment}"), startrow=1)
            print("Successfully output CFs.xlsx file")

        else:
            print('cashflows file for this date already exists - cant make a file with the same name')

        print('Successfully ran: Cashflows from solutions.')

        print('Success.')



        import psutil
        process = psutil.Process()
        memory_info = process.memory_info()
        print(f"Mem usage: {memory_info.rss} bytes")


    except:
        misc.log("Failed " + misc.errtxt(), LOGFILE)
        # sendemail.error('Model Portfolio Generation Failed', misc.errtxt())
        # # jobs.jobStatusUpdate(args.jobname, 'ERROR') - dont use




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


                # print("Successfully optimized: matched asset-liability sensitivities and optimized returns")


        # Rewrite the conditionals for more logical coherence and sense in isolating conditionals, like sets - can have boilerplate code

        # Optimization function uses .copy()
        # supposed to optimize the privates last ?

            #print(f"KRDs.head() after running other functions: {KRDs.head()}")
            #print(f"KRDs.tail() after running other functions: {KRDs.tail()}")

            # boilerplate code:


"""
if copied.equals(KRDs):
    print("No changes were made to KRDs data")
else:
    print("The data has been modified")
"""


            # actually, have do_all be an else case lol




        # Can probably check if_exist before running / creating Custom_benchmarks tables to just output some:
        # Solutions will ALWAYS run since needed for even 1 table
        # cashflows should always run IMO since part of both debugging and mandatory

