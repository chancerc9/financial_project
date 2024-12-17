"""
Name: model_portfolio_generate.py

Purpose:

Functions:

Side effects:

"""
from Web_flask.web_flask.utility.template_filters import datetime

"""
    This provided code is a complex script that processes bond-related data, 
    calculates key rate durations (KRDs), brings in sensitivities for liabilities and optimizes them with assets, and
    calculates cashflows from solution.

    It is "safe" to be ran as many times as warranted.
"""

# Standard library imports
import os
import sys

# Third-party imports
import pandas as pd
# Local application-specific imports
from equitable.db.psyw import SmartDB
from equitable.infrastructure import sysenv
from equitable.utils import processtools as misc

# Adding system path for custom imports
sys.path.append(sysenv.get("ALM_DIR"))

# Required custom modules
import solutions as model_portfolio
import cashflows_and_benchmark_tables
import cli
import datahandler as datahandler
import file_utils
import helpers

# Configure pandas display settings
pd.set_option('display.width', 150)

# Database connections (Benchmark, Bond, and General)
#BM_conn = SmartDB('Benchmark')
#BM_cur = BM_conn.con.cursor()

#Bond_conn = SmartDB('Bond')
#Bond_cur = Bond_conn.con.cursor()

#General_conn = SmartDB('General')
#General_cur = General_conn.con.cursor()

# Logging directories:
MY_LOG_DIR = os.path.join(sysenv.get('PORTFOLIO_ATTRIBUTION_DIR'), 'logs', 'brenda')
os.makedirs(MY_LOG_DIR, exist_ok=True)  # Create directories if they don't exist
LOGFILE = open(os.path.join(MY_LOG_DIR, 'benchmarking_log.txt'),
               'a')  # Append to the existing logfile, or create a new one

import os
from typing import Union

import os
from typing import Union


def build_and_ensure_directory(*path_segments: Union[str, bytes]) -> str:
    """
    Constructs a directory path from the given segments and ensures that the directory exists.
    If the directory does not exist, it will be created. If it already exists, no error is raised.
    Parameters:
    path_segments (str or bytes): The parts that form the directory path. This can be passed as
                                  multiple string arguments or a tuple of strings.
    Returns:
    str: The full, absolute path of the directory.
    Example:
        build_and_ensure_directory('some', 'nested', 'directory')
        # Ensures 'some/nested/directory' exists and returns that path.
    """
    directory_path = os.path.join(*path_segments)
    os.makedirs(directory_path, exist_ok=True)
    return directory_path


def main():  # model_portfolio.py new version
    """
    Functionality:
    - gathers user input
    - creates variables,
    - produces output
    - (modifies and runs the appropriate optimizations, and saves the results.)

    This function orchestrates the portfolio optimization process and saves results to Excel files;
    it creates model portfolio solutions file, benchmarking file, and cashflows from solutions.
    """

    # ----- A: Set Program Configurations: -------
    # Retrieve user input for program:
    args, GivenDate = cli.get_user_info()   # retrieves user input for the program
    # GivenDate in string form:
    cur_date: str = GivenDate.strftime('%Y%m%d')    # current date
    timestamp: str = datetime.now().strftime("%Y%m%d-%H%M%S")   # time as of now


    # Ensure input-output directories exist:
    base_dir: str = sysenv.get('PORTFOLIO_ATTRIBUTION_DIR')

    # Output directory for solutions and debugging files:
    output_directory: str = helpers.build_and_ensure_directory(base_dir, 'Benchmarking', 'code_benchmarking_outputs', cur_date)
    print(f"Directory created or ensured:{output_directory}")
    
    # Timestamped solutions directory:
    SOLUTIONS_DIR: str = helpers.build_and_ensure_directory(output_directory, timestamp)
    
    # Output directory for debugging files:
    DEBUGGING_DIRECTORY: str = helpers.build_and_ensure_directory(output_directory, 'debugging_steps')
    
    # Specific output file paths for 3 solutions files:
    # Define file-name paths for output items:
    solutions_path = SOLUTIONS_DIR + '/solutions' + cur_date + '.xlsx'
    custom_benchmarks_path = SOLUTIONS_DIR + '/Custom_benchmark_' + cur_date + '.xlsx'
    cfs_path = SOLUTIONS_DIR + '/CFs' + cur_date + '.xlsx'

    
    # Define Results Directory (output):
    output_directory: str = os.path.join(sysenv.get('PORTFOLIO_ATTRIBUTION_DIR'), 'Benchmarking', 'Test',
                                        'benchmarking_outputs',
                                        'Brenda', cur_date)
    os.makedirs(output_directory, exist_ok=True)

    # Define Debugging_Steps Directory (debugging outputs):
    DEBUGGING_DIRECTORY = os.path.join(output_directory, 'Debugging_Steps')
    os.makedirs(DEBUGGING_DIRECTORY, exist_ok=True)


    # ----- B: Run Model Portfolio (Main code + logging): ------
    try:
        # Start logging:
        misc.log(f'Starting run of: {GivenDate}', LOGFILE)

        # Log 'Step 1':
        # misc.log('Begin Process: Read-in data from database and set user input', LOGFILE)
        print('Begin Process: Read-in data from database and set user input')

        # 1.1) Retrieve semiannual bond curve data across 35 years.
        bond_curves = datahandler.get_bond_curves(
            GivenDate)  # old code: annual_bond_curves = datahandler.get_bond_curves_query(GivenDate)  # Query to retrieve annual bond curve data (annual curve data for 35 years)
        #           original_KRDs = model_portfolio.reading_asset_KRDs_with_annual_bond_curves(annual_bond_curves, ftse_handler.data, GivenDate)    # Generate KRD Table for all assets (to feed to optim function and write later; keep in memory or write now)

        # 1.2) Retrieve FTSE Universe data from database query:
        ftse_handler = datahandler.FTSEDataHandler(GivenDate)  # Initialize FTSEDataHandler for GivenDate.

        ftse_data = ftse_handler.data  # Retrieve a copy of the FTSE bond data DataFrame.

        # Output items:
        file_utils.write_results_to_excel_one_sheet(ftse_data, DEBUGGING_DIRECTORY, cur_date, 'ftse_data')

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

        # misc.log('Step 2: Create 70 bucket cashflows and calculate asset KRDs from excel inputs', LOGFILE)

        # 2.1. Create 70 bucket cashflows from a copy of FTSE data and create 6 bucket KRDs from cashflows:
        KRDs = model_portfolio.reading_asset_KRDs(bond_curves, ftse_handler.data,
                                                  GivenDate)  # sensitivities variable is 70 bucket KRDs

        # misc.log('Step 3: Read in liability KRDs from excel inputs; optimize asset KRDs and liability KRDs; output to a solutions.xlsx file', LOGFILE)

        # misc.log('Create dictionaries to hold results', LOGFILE)

        # For solutions. Calculates asset KRDs, brings in liability KRDs, runs optimization function in Python (output: solutions.xlsx). Model Portfolio.
        solutions = {}
        # For ALM team Custom benchmarks. Generates quarterly Model Portfolio tables file to use for benchmarking code (output: Custom_benchmarks.xlsx).
        summary = {}
        data = {}
        # Cashflows from solutions (output: CFs.xlsx).
        summed_cashflows = {}

        # print("Optimizing solutions")
        # misc.log('Optimizing solutions', LOGFILE)

        # Process only the specified conditions
        for asset_type, condition in mask.items():
            if condition:
                solutions[asset_type] = helpers.process_asset_type(asset_type, KRDs, GivenDate)
                """
                misc.log(f"Optimizing {asset_type}", LOGFILE)
                solutions[asset_type] = model_portfolio.optimization(KRDs, GivenDate, asset_type)
                print(f"Successfully optimized: {asset_type}")
                """
        print("Successfully ran: solutions")
        # misc.log("Successfully ran: solutions", LOGFILE)

        # Write solutions to Excel
        helpers.write_solutions_to_excel(solutions, solutions_path, KRDs, GivenDate)

        # misc.log("Creating Custom_benchmark.xlsx", LOGFILE)
        print("Creating Custom_benchmark.xlsx")

        for asset_type, condition in mask.items():
            if condition:
                summary[asset_type] = cashflows_and_benchmark_tables.create_summary_table(GivenDate, asset_type)
                data[asset_type] = cashflows_and_benchmark_tables.create_indexData_table(solutions[asset_type],
                                                                                         GivenDate,
                                                                                         ftse_handler.data,
                                                                                         asset_type)
        print("Successfully ran: Creating tables for Custom benchmarks")
        # misc.log("Successfully ran: Creating tables for Custom benchmarks", LOGFILE)

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

        print("Creating cashflows from solutions: CFs.xlsx")
        # misc.log("Creating cashflows from solutions: CFs.xlsx", LOGFILE)

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

        print('Successfully ran: Cashflows from solutions.')
        # misc.log('Successfully ran: Cashflows from solutions.', LOGFILE)

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
