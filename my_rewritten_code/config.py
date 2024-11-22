"""
Name: config.py

Purpose:
    Program configurations and constants.
    Do not change without knowing what you are doing.

    Readily change the path of outputs or logging file as preferred.

Functions:

Side effects:
    Amends program constants that program functions depend on.
    Changes output directory, debugging outputs directory, and logging directory.
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

# Logging directories


