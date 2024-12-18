"""
Name: file_utils.py (output utils)

Purpose:
    Writes output to Excel files.
    Does not mutate program data.

Functions:

Side effects:
    Writes output if prior filepath does not exist.
    Creates folder(s) if prior filepath does not exist.
"""

"""
# Future implementations: class or decorator to_excel -> so functions can be more descriptive without needing excel in 
front of every one of the names.

# can have PATH be a parameter for this
# and put it in various (decoupled) functions in main
"""


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

import os
from pathlib import Path
from typing import Dict
import pandas as pd


class ExcelOutputWriter:
    def __init__(self, base_dir: str, cur_date: str):
        self.base_dir = Path(base_dir)
        self.cur_date = cur_date
    def _ensure_directory_exists(self, directory: Path) -> None:
        directory.mkdir(parents=True, exist_ok=True)
    def write_single_sheet_in_named_dir(
        self,
        df: pd.DataFrame,
        excel_filename: str
    ) -> None:
        """
        Writes a single DataFrame to an Excel file in a directory named after excel_filename.
        Resulting file: {base_dir}/{excel_filename}/{excel_filename}_{cur_date}.xlsx
        """
        folder_path = self.base_dir / excel_filename
        self._ensure_directory_exists(folder_path)
        file_path = folder_path / f"{excel_filename}_{self.cur_date}.xlsx"
        if not file_path.exists():
            with pd.ExcelWriter(file_path) as writer:
                df.to_excel(writer, sheet_name=excel_filename)
            print("Successfully written to debugging steps")
        else:
            print(f"{excel_filename} for this quarter already exists - cant make a file with the same name")
    def write_multiple_sheets_in_dated_dir(
        self,
        items: Dict[str, pd.DataFrame],
        excel_filename: str
    ) -> None:
        """
        Writes multiple DataFrames to a single Excel file, each DataFrame in its own sheet.
        Resulting file: {base_dir}/{cur_date}/{excel_filename}_{cur_date}.xlsx
        """
        output_dir = self.base_dir / self.cur_date
        self._ensure_directory_exists(output_dir)
        file_path = output_dir / f"{excel_filename}_{self.cur_date}.xlsx"
        if not file_path.exists():
            with pd.ExcelWriter(file_path) as writer:
                for sheet_name, df in items.items():
                    df.to_excel(writer, sheet_name=str(sheet_name))
            print("Successfully written to debugging steps")
        else:
            print(f"{excel_filename} for this quarter already exists - delete for new version - cant make a file with the same name")
    def write_single_sheet_in_dated_dir(
        self,
        df: pd.DataFrame,
        excel_filename: str
    ) -> None:
        """
        Writes a single DataFrame to an Excel file in a directory named after the current date.
        Resulting file: {base_dir}/{cur_date}/{excel_filename}_{cur_date}.xlsx
        """
        folder_path = self.base_dir / self.cur_date
        self._ensure_directory_exists(folder_path)
        file_path = folder_path / f"{excel_filename}_{self.cur_date}.xlsx"
        if not file_path.exists():
            with pd.ExcelWriter(file_path) as writer:
                df.to_excel(writer, sheet_name=excel_filename)
            print("Successfully written to debugging steps")
        else:
            print(f"{excel_filename} for this quarter already exists - cant make a file with the same name")



"""Old"""

# All items in one sheet, need to write this:
def write_results_to_excel_one_sheet(item_to_excel: Dict[str, pd.DataFrame], base_dir: str, cur_date: str,
                                     excel_filename: str):
    """
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


# One item per sheet; multiple sheets per book:
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
