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


def get_user_info():
    parser = argparse.ArgumentParser(description="Benchmark Creation Tool")  # ask what this does

    parser.add_argument("-d", "--GivenDate", type=str,
                        help="Use YYYY-MM-DD to set the Date for the calculation.")

    parser.add_argument("-o", "--OU_Date", type=str,
                        help="Use YYYY-MM-DD to use specific over_under_assetting file")

    # parser.add_argument('-c', '--create', action='store_true',
    #                    help='include this if the liabilities for the selected date have not yet been uploaded to the db')

    parser.add_argument('-s', '--swap', action='store_true', # we removed all the swaps anyways
                        help="Set to true if interest rate swap sensitivities are backed out") # we don't need this

    parser.add_argument('-cb', '--curMonthBS', action='store_true',
                        help='include to run economics with current month balance sheet instead of previous') # we give it balance sheet

    parser.add_argument("-m", "--mortgage", action='store_true',
                        help="Include to generate output for mortgages, or leave all 3 blank to do all 3")
    parser.add_argument("-pb", "--public", action='store_true',
                        help="Include to generate output for publics, or leave all 3 blank to do all 3")
    parser.add_argument("-pv", "--private", action='store_true',
                        help="Include to generate output for privates, or leave all 3 blank to do all 3")

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

    return args, GivenDate, OU_Date

