"""
Name: model_portfolio_generate.py

Purpose:

Functions:

Side effects:

"""

"""
    This provided code is a complex script that processes bond-related data, 
    calculates key rate durations (KRDs), brings in sensitivities for liabilities and optimizes them with assets, and
    calculates cashflows from solution.

    It is "safe" to be ran as many times as warranted.
"""

import datetime
# Standard library imports
import os
import sys

# Third-party imports
import pandas as pd
# Local application-specific imports
from equitable.infrastructure import sysenv
from equitable.utils import processtools as misc

# Adding system path for custom imports
sys.path.append(sysenv.get("ALM_DIR"))

# Required custom modules
import solutions as model_portfolio
import cashflows_and_benchmark_tables
import cli
import datahandler
import file_utils
import helpers

# Configure pandas display settings
pd.set_option('display.width', 150)


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
    timestamp: str = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")   # time as of now


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


    # Logging directory and LOGFILE (for basic logs):
    #MY_LOG_DIR = os.path.join(sysenv.get('PORTFOLIO_ATTRIBUTION_DIR'), 'logs', 'model_portfolio')
    # os.makedirs(MY_LOG_DIR, exist_ok=True)  # Create directories if they don't exist
    # LOGFILE = open(os.path.join(MY_LOG_DIR, 'benchmarking_log.txt'),
    #               'a')  # Append to the existing logfile, or create a new one

    MY_LOG_DIR: str = helpers.build_and_ensure_directory(base_dir, 'logs', 'model_portfolio')
    LOGFILE = open(os.path.join(MY_LOG_DIR, 'model_portfolio_code_log.txt'),
                   'a')  # Append to the existing logfile, or create a new one

    # ----- B: Run Model Portfolio (Main code + logging): ------
    try:
        # Start logging:
        misc.log(f'Starting run of: {GivenDate}', LOGFILE)

        # Log 'Step 1':
        # misc.log('Begin Process: Read-in data from database and set user input', LOGFILE)
        print('Begin Process: Read-in data from database and set user input')

        # 1.1) Retrieve semiannual bond curve data across 35 years.
        bond_curves = datahandler.get_bond_curves(GivenDate)

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
                                                  GivenDate, args.debug)  # sensitivities variable is 70 bucket KRDs

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

        if args.read_in_solutions_file:
            print("Reading in solutions file:")

            # Solutions file name to feed into the code:
            # existing_solutions_file_name = 'solutions' + cur_date + '.xlsx'
            existing_solutions_file_name = 'solutions' + cur_date + ' SC.xlsx'
            # existing_solutions_file_name = 'solutions' + cur_date + ' Modified' + '.xlsx'
            # existing_solutions_file_name = 'solutions' + cur_date + ' Modified v2.xlsx'

            # Get solutions file path:
            solutions_path = datahandler.set_input_path(GivenDate, existing_solutions_file_name)

            excel_path = solutions_path

            try:
                dataframes_dict = helpers.read_specific_solutions(excel_path)
                # At this point, dataframes_dict contains the three DataFrames keyed by their sheet names.
                # You may proceed with further analysis or transformations here.
            except Exception as err:
                # Handle the error as appropriate for your environment—logging, re-raising, etc.
                print(f"Error encountered: {err}")

            solutions = dataframes_dict

        else:
            # Process only the specified conditions
            for asset_type, condition in mask.items():
                if condition:
                    solutions[asset_type] = helpers.process_asset_type(asset_type, KRDs, GivenDate, LOGFILE)
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
                                                                                                            asset_type,
                                                                                                            args.debug)

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


