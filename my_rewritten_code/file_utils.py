"""
Name: file_utils.py

Purpose:
    Writes output to Excel files.
    Does not mutate program data.

Functions:

Side effects:
    Writes output if prior filepath does not exist.
    Creates folder(s) if prior filepath does not exist.
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

# Required custom modules
#from config import DEBUGGING_PATH

#GLOBAL_PATH = DEBUGGING_PATH

"""
ExcelWriters
"""
"""
# Define a decorator to handle writing with a dynamic or default path.
def excel_writer(path=None):
    def decorator_write_to_excel(func):
        def wrapper(*args, **kwargs):
            # Default to global path if no path is provided
            file_path = path or GLOBAL_PATH
            result = func(*args, file_path=file_path, **kwargs)
            # Additional functionality here (e.g., logging, error handling)
            return result
        return wrapper
    return decorator_write_to_excel
# Use case with decorator
@excel_writer(path='specified_path.xlsx')
def write_data_to_excel(data, file_path):
    data.to_excel(file_path)  # Simplified example
    
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
    folder_path = os.path.join(base_dir, excel_filename) # , cur_date)
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

# Same folder:

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
                sheet_name = f'{rating}'
                df.to_excel(writer, sheet_name=sheet_name)  # take the rating
    else:
        print(f'{excel_filename} for this quarter already exists - delete for new version - cant make a file with the same name')

    print("Successfully written to debugging steps") # {excel_filename} to {file_path}")


# all items in one sheet, need to write this
def write_results_to_excel_one_sheet_low_level_or_no_separate_folder(item_to_excel: Dict[str, pd.DataFrame], base_dir: str,
                                                        cur_date: str,
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
    folder_path = os.path.join(base_dir, cur_date)  #
    # Ensure the directory exists (create it if necessary)
    os.makedirs(folder_path, exist_ok=True)

    file_path = os.path.join(folder_path, f'{excel_filename}_{cur_date}.xlsx')

    if not os.path.exists(file_path):
        with pd.ExcelWriter(file_path) as writer:
            item_to_excel.to_excel(writer, sheet_name=excel_filename)
    else:
        print(f'{excel_filename} for this quarter already exists - cant make a file with the same name')

    print("Successfully written to debugging steps")
