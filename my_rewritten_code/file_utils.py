"""
Name: data_io.py or file_utils.py

Purpose:


Functions:


Side effects:

"""

"""
# class or decorator to_excel -> so functions can be more descriptive without needing excel in front of every one of



# can have PATH be a parameter for this
# and put it in various (decoupled) functions in main
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
ExcelWriters
"""

# all items in one sheet, need to write this
def write_results_to_excel_one_sheet(item_to_excel: Dict[str, pd.DataFrame], base_dir: str, cur_date: str,
                                     excel_filename: str):
    """
    Writes the optimization results (solutions) to an Excel file with each DataFrame in a separate sheet.

    Parameters:
    solutions (Dict[str, pd.DataFrame]): A dictionary of DataFrame solutions.
    base_dir (str): The base directory where the Excel file will be stored.
    cur_date (str): Current date as a string for including in the file name.
    excel_filename (str): The name of the overall Excel file to save. The file stem name.
    """
    # Construct the full directory path
    folder_path = os.path.join(base_dir, excel_filename, cur_date)
    # Ensure the directory exists (create it if necessary)
    os.makedirs(folder_path, exist_ok=True)

    file_path = os.path.join(folder_path, f'{excel_filename}_{cur_date}.xlsx')

    if not os.path.exists(file_path):
        with pd.ExcelWriter(file_path) as writer:
            item_to_excel.to_excel(writer, sheet_name=excel_filename)
    else:
        print(f'{excel_filename} for this quarter already exists - cant make a file with the same name')

    print("Successfully written to debugging steps")


# One sheet per book; multiple books per rating.
def write_results_to_excel_by_rating_doesnt_work_yet(item_to_excel: Dict[str, pd.DataFrame], base_dir: str,
                                                     cur_date: str, excel_filename: str):
    """
    Writes the optimization results (solutions) to an Excel file with each DataFrame in a separate sheet.

    Parameters:
    solutions (Dict[str, pd.DataFrame]): A dictionary of DataFrame solutions.
    base_dir (str): The base directory where the Excel file will be stored.
    cur_date (str): Current date as a string for including in the file name.
    excel_filename (str): The name of the overall Excel file to save. The file stem name.
    """
    # Construct the full directory path
    output_dir = os.path.join(base_dir, excel_filename, cur_date)
    # Ensure the directory exists (create it if necessary)
    os.makedirs(output_dir, exist_ok=True)

    for rating in ['Federal', 'Provincial', 'CorporateAAA_AA', 'CorporateA', 'CorporateBBB', 'Corporate']:
        # Construct the full file path for the overall Excel file
        file_path = os.path.join(output_dir, f'{rating}_{excel_filename}_{cur_date}.xlsx')
        if not os.path.exists(file_path):
            with pd.ExcelWriter(file_path) as writer:
                item_to_excel[rating].to_excel(writer)
        else:
            print(
                'debugging steps files for this date already exists - delete for new version - cant make a file with the same name')

    print("Successfully written to debugging steps")


# One item per sheet; multiple sheets per book
def write_results_to_excel(item_to_excel: Dict[str, pd.DataFrame], base_dir: str, cur_date: str, excel_filename: str):
    """
    Writes the optimization results (solutions) to an Excel file with each DataFrame in a separate sheet.

    Parameters:
    solutions (Dict[str, pd.DataFrame]): A dictionary of DataFrame solutions.
    base_dir (str): The base directory where the Excel file will be stored.
    cur_date (str): Current date as a string for including in the file name.
    excel_filename (str): The name of the overall Excel file to save. The file stem name.
    """
    # Construct the full directory path
    output_dir = os.path.join(base_dir, cur_date)
    # Ensure the directory exists (create it if necessary)
    os.makedirs(output_dir, exist_ok=True)

    # Construct the full file path for the overall Excel file
    file_path = os.path.join(output_dir, f'{excel_filename}_{cur_date}.xlsx')

    if not os.path.exists(file_path):
        # Write all solutions to separate sheets within the same Excel file
        with pd.ExcelWriter(file_path) as writer:
            for rating, df in item_to_excel.items():
                sheet_name = f'{rating}_solution'
                df.to_excel(writer, sheet_name=sheet_name)  # take the rating
    else:
        print(
            'debugging steps files for this date already exists - delete for new version - cant make a file with the same name')

    print(f"Successfully written all solutions to {file_path}")

