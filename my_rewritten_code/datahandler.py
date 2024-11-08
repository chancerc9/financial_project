# TODO: only used in run_code for now but can be used elsewhere

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


def parse_args(): # -> Tuple[argparse.Namespace, pd.Timestamp, pd.Timestamp]:
    """
    Retrieves command-line arguments and converts them to usable date objects.

    Returns:
    Tuple[argparse.Namespace, pd.Timestamp, pd.Timestamp]:
        - args: Parsed command-line arguments.
        - GivenDate: The date for the optimization (current date if not provided).
        - OU_Date: The over/under date for liabilities (defaults to GivenDate if not provided).
    """
    parser = (argparse.ArgumentParser(description="The Model Portfolio takes our assets and liabilities,"
                                                  "calculates asset sensitivities and performs an asset-liability"
                                                  "matching optimization in Python (simple optimization), and the"
                                                  "solutions from such is used to both generate hypothetical"
                                                  "CFs and run benchmarks (Custom_benchmarking.xlsx) for the next quarter."))

    # Step 1: Determine which files are outputted.
    #   - Custom_benchmarking.xlsx is generated for benchmarking
    #   - CFs.xlsx is cashflows file generated from solutions
    #   - solutions.xlsx file is solutions file containing percentage allocations for our asset mix from optimization
    #   - using asset KRD profiles (accounts for interest rate fluctuation by modelling 1bp shocks every several years), liabilities to hedge liabilities risk of our assets  # TODO! can fix for more concision
    """debugging"""
    #parser.add_argument("-bench", "--create_benchmarking_tables", action='store_true', help="Include to generate output for benchmarking [Custom_benchmarking.xlsx], or leave both blank to do both alongside regular solutions file.")
    #parser.add_argument("-cf", "--create_solution_cashflows", action='store_true', help="Include to generate output for solution cashflows [CFs.xlsx], or leave both blank to do both alongside regular solutions file.")

    # Step 2: Consider if debugging steps are ran.
    #parser.add_argument("-nodebug", "--no_debugger_outputs", action='store_true', help="Include to not generate debugging steps [/Debugging Steps/CFs.xlsx], or leave blank to opt in by default.")
    """debugging"""
    # Part 1: (description="Portfolio Optimization Tool")

    # Required arguments: date for which quarter is ran:
    parser.add_argument("-d", "--GivenDate", type=str, help="Use YYYY-MM-DD to set the Date for the calculation.")
    parser.add_argument("-o", "--OU_Date", type=str, help="Use YYYY-MM-DD to use specific over_under_assetting file") # This variable is never needed, read, or used for any function - ask Mitchell Waters if needed for any function.
    # parser.add_argument('-c', '--create', action='store_true',
    #                     help='Include this if the liabilities for the selected date have not yet been uploaded to the db')


    # Optional for specific outputs (mortgages, publics, privates)
    parser.add_argument("-m", "--mortgage", action='store_true',
                        help="Include to generate output for mortgages, or leave all 3 blank to do all 3")
    parser.add_argument("-pb", "--public", action='store_true',
                        help="Include to generate output for publics, or leave all 3 blank to do all 3")
    parser.add_argument("-pv", "--private", action='store_true',
                        help="Include to generate output for privates, or leave all 3 blank to do all 3")

    # Part 2: (description="Benchmark Creation Tool")

    # Used for Custom_benchmarks, potentially. Not necessary for solutions files, and never are the following required.
    parser.add_argument('-s', '--swap', action='store_true',
                        help="Set to true if interest rate swap sensitivities are backed out")
    parser.add_argument('-cb', '--curMonthBS', action='store_true',
                        help='include to run economics with current month balance sheet instead of previous')
    parser.add_argument("-j", "--jobname", type=str, default="UNSPECIFIED",
                        help="Specified Jobname")

    # Parse arguments:
    args = parser.parse_args()

    # Date conversion and defaults to GivenDate:
    if args.GivenDate is None:
        GivenDate = dt.datetime.now()
    else:
        GivenDate = conversions.YYYYMMDDtoDateTime(args.GivenDate)

    if args.OU_Date is None:
        OU_Date = conversions.YYYYMMDDtoDateTime(args.GivenDate)
    else:
        OU_Date = conversions.YYYYMMDDtoDateTime(args.OU_Date)

    return args, GivenDate, OU_Date

