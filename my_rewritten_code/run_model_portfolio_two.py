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
import calculations
import solutions as model_portfolio
import cashflows_and_benchmark_tables
import cli
import datahandler as datahandler
import file_utils

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




def read_specific_solutions(excel_file_path: str) -> dict:
    """
    Reads the 'public_solution', 'mortgage_solution', and 'private_solution' sheets
    from the provided Excel workbook. Returns a dictionary mapping sheet names to DataFrames.
    Parameters
    ----------
    excel_file_path : str
        The full path to the Excel file containing the required sheets.
    Returns
    -------
    dict
        A dictionary with keys 'public_solution', 'mortgage_solution', 'private_solution',
        and values as the corresponding Pandas DataFrames.
    Raises
    ------
    FileNotFoundError
        If the specified Excel file does not exist or is unreadable.
    ValueError
        If one or more of the required sheets are missing from the workbook.
    Examples
    --------
    >>> dfs = read_specific_solutions("path/to/data_solutions.xlsx")
    >>> public_df = dfs["public_solution"]
    >>> mortgage_df = dfs["mortgage_solution"]
    >>> private_df = dfs["private_solution"]
    """
    required_sheets = ["public_solution", "mortgage_solution", "private_solution"]
    # First, we verify that the file can be opened and the required sheets exist.
    try:
        excel_obj = pd.ExcelFile(excel_file_path)
    except FileNotFoundError:
        raise FileNotFoundError(f"Could not find or open the Excel file at: {excel_file_path}")
    except Exception as e:
        raise ValueError(f"An error occurred while reading the Excel file: {e}")
    missing_sheets = [sheet for sheet in required_sheets if sheet not in excel_obj.sheet_names]
    if missing_sheets:
        raise ValueError(
            f"The following required sheets are missing from {excel_file_path}: {missing_sheets}"
        )
    # Read the three sheets into a dictionary of DataFrames.
    solutions = {}
    for sheet in required_sheets:
        df = pd.read_excel(excel_file_path, sheet_name=sheet, index_col=0)
        asset_type_name = sheet.replace("_solution", "")
        solutions[asset_type_name] = df
    return solutions


def main():  # model_portfolio.py new version
    """
    Main function to orchestrate the portfolio optimization process.
    It gathers user input, runs the appropriate optimizations, and saves the results.

    Creates model portfolio, benchmarking file, and cashflows from solutions.
    """

    # ----- A: Set Program Configurations: -------
    # Retrieve user input for program:
    args, GivenDate = cli.get_user_info()
    # GivenDate in string form:
    cur_date: str = GivenDate.strftime('%Y%m%d')

    # Define Results Directory (output):
    OUTPUT_DIR_PATH: str = os.path.join(sysenv.get('PORTFOLIO_ATTRIBUTION_DIR'), 'Benchmarking', 'Test',
                                        'benchmarking_outputs',
                                        'Brenda', cur_date)  # Test path - Brenda
    os.makedirs(OUTPUT_DIR_PATH, exist_ok=True)

    # Define Debugging_Steps Directory (debugging outputs):
    CURR_DEBUGGING_PATH = os.path.join(OUTPUT_DIR_PATH, 'Debugging_Steps')
    os.makedirs(CURR_DEBUGGING_PATH, exist_ok=True)

    # Define file-name paths for output items:
    # existing_solutions_file_name = 'solutions' + cur_date + '.xlsx'
    existing_solutions_file_name = 'solutions' + cur_date + ' Modified' + '.xlsx'

    solutions_path = datahandler.set_input_path(GivenDate, existing_solutions_file_name)
    # solutions_path = OUTPUT_DIR_PATH + '/solutions' + cur_date + '.xlsx'
    custom_benchmarks_path = OUTPUT_DIR_PATH + '/Custom_benchmark_' + cur_date + '.xlsx'
    cfs_path = OUTPUT_DIR_PATH + '/CFs' + cur_date + '.xlsx'

    # ----- B: Run Model Portfolio (Main code + logging): ------
    # Example usage:
    excel_path = solutions_path

    try:
        dataframes_dict = read_specific_solutions(excel_path)
        # At this point, dataframes_dict contains the three DataFrames keyed by their sheet names.
        # You may proceed with further analysis or transformations here.
    except Exception as err:
        # Handle the error as appropriate for your environment—logging, re-raising, etc.
        print(f"Error encountered: {err}")


    try:
        # Start logging:
        misc.log(f'Starting run of: {GivenDate}', LOGFILE)

        # Log 'Step 1':
        misc.log('Begin Process: Read-in data from database and set user input', LOGFILE)

        # 1.1) Retrieve semiannual bond curve data across 35 years.
        bond_curves = datahandler.get_bond_curves(
            GivenDate)  # old code: annual_bond_curves = datahandler.get_bond_curves_query(GivenDate)  # Query to retrieve annual bond curve data (annual curve data for 35 years)
        #           original_KRDs = model_portfolio.reading_asset_KRDs_with_annual_bond_curves(annual_bond_curves, ftse_handler.data, GivenDate)    # Generate KRD Table for all assets (to feed to optim function and write later; keep in memory or write now)

        # 1.2) Retrieve FTSE Universe data from database query:
        ftse_handler = datahandler.FTSEDataHandler(GivenDate)  # Initialize FTSEDataHandler for GivenDate.

        ftse_data = ftse_handler.data  # Retrieve a copy of the FTSE bond data DataFrame.

        # Output items:
        file_utils.write_results_to_excel_one_sheet(ftse_data, CURR_DEBUGGING_PATH, cur_date, 'ftse_data')

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
        user_configs = {
            "custom_benchmarks": args.benchmarks,
            "cfs": args.cfs,

        }

        if not (args.benchmarks,args.cfs):
            user_configs = {key: True for key in mask2}
        """

        # ---- Main logic for Model Portfolio: -----

        misc.log('Step 2: Create 70 bucket cashflows and calculate asset KRDs from excel inputs', LOGFILE)

        # 2.1. Create 70 bucket cashflows from a copy of FTSE data and create 6 bucket KRDs from cashflows:
        misc.log(
            'Step 3: Read in liability KRDs from excel inputs; optimize asset KRDs and liability KRDs; output to a solutions.xlsx file',
            LOGFILE)

        misc.log('Create dictionaries to hold results', LOGFILE)

        # For solutions. Calculates asset KRDs, brings in liability KRDs, runs optimization function in Python (output: solutions.xlsx). Model Portfolio.
        solutions = dataframes_dict
        # For ALM team Custom benchmarks. Generates quarterly Model Portfolio tables file to use for benchmarking code (output: Custom_benchmarks.xlsx).
        summary = {}
        data = {}
        # Cashflows from solutions (output: CFs.xlsx).
        summed_cashflows = {}

        # print("Optimizing solutions")
        misc.log('Reading solutions', LOGFILE)

        misc.log("Creating Custom_benchmark.xlsx", LOGFILE)
        # print("Creating Custom_benchmark.xlsx")

        for asset_type, condition in mask.items():
            if condition:
                summary[asset_type] = cashflows_and_benchmark_tables.create_summary_table(GivenDate, asset_type)
                data[asset_type] = cashflows_and_benchmark_tables.create_indexData_table(solutions[asset_type],
                                                                                         GivenDate,
                                                                                         ftse_handler.data,
                                                                                         asset_type)
        # print("Successfully ran: Creating tables for Custom benchmarks")
        misc.log("Successfully ran: Creating tables for Custom benchmarks", LOGFILE)

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

        # print("Creating cashflows from solutions: CFs.xlsx")
        misc.log("Creating cashflows from solutions: CFs.xlsx", LOGFILE)

        for asset_type, condition in mask.items():
            if condition:
                summed_cashflows[asset_type] = cashflows_and_benchmark_tables.create_summed_cashflow_tables(bond_curves,
                                                                                                            ftse_data,
                                                                                                            # Data is protected here:
                                                                                                            data[
                                                                                                                asset_type],
                                                                                                            solutions[
                                                                                                                asset_type],
                                                                                                            GivenDate,
                                                                                                            asset_type)

        # print('Successfully ran: Cashflows from solutions.')
        misc.log('Successfully ran: Cashflows from solutions.', LOGFILE)

        SEGMENTS = ('NONPAR', 'GROUP', 'PAYOUT', 'ACCUM', 'UNIVERSAL', 'TOTAL', 'SURPLUS')

        if not os.path.exists(cfs_path):
            with pd.ExcelWriter(cfs_path) as writer:
                for segment in SEGMENTS:
                    for asset_type, content in summed_cashflows.items():
                        content[segment].to_excel(writer, sheet_name=(f"summed cfs {asset_type} - {segment}"),
                                                  startrow=1)
            print("Successfully output CFs.xlsx file")

        else:
            print('Cashflows file for this date already exists - cant make a file with the same name')

        # print('Success.')
        misc.log('Success: Completion of program.', LOGFILE)

        import psutil
        process = psutil.Process()
        memory_info = process.memory_info()
        print(f"Mem usage: {memory_info.rss} bytes")


    except:
        misc.log("Failed " + misc.errtxt(), LOGFILE)
        # sendemail.error('Model Portfolio Generation Failed', misc.errtxt())
        # # jobs.jobStatusUpdate(args.jobname, 'ERROR')



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

