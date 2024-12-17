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
#import file_utils
import solutions as model_portfolio

# Pandas configuration
pd.set_option('display.width', 150)

# Add to system path
sys.path.append(sysenv.get("ALM_DIR"))


# Logging directories:
MY_LOG_DIR = os.path.join(sysenv.get('PORTFOLIO_ATTRIBUTION_DIR'), 'logs', 'brenda')
os.makedirs(MY_LOG_DIR, exist_ok=True)  # Create directories if they don't exist
LOGFILE = open(os.path.join(MY_LOG_DIR, 'benchmarking_log.txt'),
               'a')  # Append to the existing logfile, or create a new one

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

# For Model Portfolio code:

# Generalized function for optimization
def process_asset_type(asset_type, KRDs, GivenDate):
    misc.log(f"Optimizing {asset_type}", LOGFILE)
    solution = model_portfolio.optimization(KRDs, GivenDate, asset_type=asset_type)
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

