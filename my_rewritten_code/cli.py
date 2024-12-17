"""
Name: cli.py

Purpose:
    User interaction and program configuration.

Functions:
    def get_user_info() -> tuple[argparse.Namespace, pd.Timestamp, pd.Timestamp]

Side effects:
    Configures program state based on external input.
    No interactions with persistent storage.
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


"""
Side effects: None.

User interaction and program configuration.

Setting IO functions:
"""

def get_user_info() -> tuple[argparse.Namespace, pd.Timestamp, pd.Timestamp]:
    """
    Retrieves command-line arguments from user and instructs code's date (Year, Quarter) and type of output desired
    ('mortgage', 'public', 'private', or all).

    Returns:
    Tuple[argparse.Namespace, pd.Timestamp, pd.Timestamp]:
        - args: User's command-line arguments.
        - GivenDate: The date for the optimization (current date if not provided).
    """
    parser = argparse.ArgumentParser(description="Model Portfolio")
    parser.add_argument("-d", "--GivenDate", type=str, help="Use YYYY-MM-DD to set the Date for the calculation.")

    # for debugging, if desired
    parser.add_argument('-debug', '--debug', action='store_true',
                        help='debugging files')

    # Use previous solutions file, as desired.
    parser.add_argument('-use_solutions', '--read_in_solutions_file', action='store_true',
                        help='use previous solutions file')

    # Optional for specific outputs (mortgages, publics, privates)
    parser.add_argument("-m", "--mortgage", action='store_true',
                        help="Include to generate output for mortgages, or leave all 3 blank to do all 3")
    parser.add_argument("-pb", "--public", action='store_true',
                        help="Include to generate output for publics, or leave all 3 blank to do all 3")
    parser.add_argument("-pv", "--private", action='store_true',
                        help="Include to generate output for privates, or leave all 3 blank to do all 3")

    # Parse arguments
    args = parser.parse_args()

    # Convert GivenDate or use current date
    if args.GivenDate is None:
        GivenDate = dt.datetime.now()
    else:
        GivenDate = conversions.YYYYMMDDtoDateTime(args.GivenDate)

    return args, GivenDate


