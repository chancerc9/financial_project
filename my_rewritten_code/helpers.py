# Standard library imports
import sys
import os
from typing import Union

# Third-party imports
import pandas as pd

# Local application-specific imports
from equitable.infrastructure import sysenv
from equitable.utils import processtools as misc

# Adding system path for custom imports
sys.path.append(sysenv.get("ALM_DIR"))

# Required custom modules
#import file_utils
import solutions as model_portfolio

# Pandas configuration
pd.set_option('display.width', 150)

# Add to system path
sys.path.append(sysenv.get("ALM_DIR"))



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

# For Model Portfolio code:


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


# Generalized function for optimization
def process_asset_type(asset_type, KRDs, GivenDate, LOGFILE):
    misc.log(f"Optimizing {asset_type}", LOGFILE)
    solution = model_portfolio.optimization(KRDs, GivenDate, LOGFILE, asset_type=asset_type)
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

