"""
Name: datahandler.py or encapsulated_objects.py

Purpose:
    MODIFIES DATA
    Allows for data protection via classes
    Provides data protection

Functions:

Side effects:

"""
# Standard library imports
import argparse
import datetime
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

# --- Helper Functions ---
import pandas as pd
import numpy as np
from datetime import datetime


class FTSEDataHandler:
    """
    A class to retrieve, process, and provide controlled access to bond data
    from the FTSE universe.

    The `FTSEDataHandler` class allows users to retrieve FTSE bond data for a
    specified date, process it, and access it in a read-only format through
    a `@property`. Data mutations are internally managed, ensuring that the
    data remains immutable to external code. Users can refresh the data or
    update the date through specific methods.

    Side effects:
        Modifies __data
        Performs query on SQL database - does not modify database.

    Attributes:
    ----------
    given_date : datetime
        The date for which bond data is retrieved.
    data : pd.DataFrame
        A read-only DataFrame containing processed FTSE bond data with additional
        calculated columns.

    Methods:
    -------
    refresh_data():
        Reloads bond data for the current date (i.e., refreshes data without need to change the current date).

    update_date(new_date):
        Updates the date for retrieving bond data and refreshes the data.
    """
    def __init__(self, given_date: datetime):
        """
        Initializes the FTSEDataHandler with a specified date.

        Parameters:
        ----------
        given_date : datetime
            The date for which FTSE bond data will be retrieved and processed.
        N
        otes:
        ------
        The initial data load occurs upon instantiation by calling `_load_data`.
        """
        self._given_date = given_date
        self._data = None
        self._load_data()

    @property
    def data(self) -> pd.DataFrame:
        """
        Accesses the processed FTSE bond data as a read-only DataFrame.

        Returns:
        -------
        pd.DataFrame
            A copy of the internally stored FTSE bond data DataFrame,
            preventing external modification.

        Notes:
        ------
        The returned DataFrame is a copy of the internal `_data` attribute
        to ensure immutability. Any changes to the returned DataFrame do not
        affect the original data stored within the class.
        """
        return self._data.copy()

    def _load_data(self):
        """
        Retrieves and processes FTSE bond data for the specified date.
        This method executes an SQL query to fetch bond data, processes it to
        calculate additional columns, and stores the processed data in `_data`.

        Notes:
        ------
        This method is called internally by `__init__` and `refresh_data` to
        handle data loading. It applies transformations to add calculated
        columns such as 'marketweight_noREITs', 'Sector', 'RatingBucket',
        'mvai', 'TermPt', and 'bucket' for analysis.
        """
        get_bond_info_query = f"""
            SELECT date, cusip, term, issuername,
            annualcouponrate, maturitydate, yield,
            accruedinterest, modduration, rating,
            industrysector, industrygroup, industrysubgroup,
            marketweight, price
            FROM ftse_universe_constituents
            WHERE date= '{self._given_date.date()}'
        """

        # Execute SQL query and create DataFrame:
        df = pd.DataFrame(execute_table_query(get_bond_info_query, 'Bond', fetch=True))
        df.columns = ['date', 'cusip', 'term', 'issuername', 'annualcouponrate', 'maturitydate', 'yield',
                      'accruedinterest', 'modduration', 'rating', 'industrysector', 'industrygroup',
                      'industrysubgroup', 'marketweight', 'price']

        # --- Process data columns           ---
        #     Effects: modifies _data values ---

        # a) Calculate the market weight excluding real estate (REITs):
        total_marketweight = df['marketweight'].sum()
        real_estate_weight = df[df['industrygroup'] == "Real Estate"]['marketweight'].sum()
        df['marketweight_noREITs'] = df.apply(                  # Variable name == ['market_weight_noREITs']
            lambda row: 0 if row['industrygroup'] == "Real Estate"
            else row['marketweight'] / (total_marketweight - real_estate_weight) * 100,
            axis=1)

        # b) Add classification columns for sector (e.g. a bond name) and rating
        df['Sector'] = df.apply(
            lambda row: row['industrygroup'] if row['industrysector'] == 'Government' else row['industrysector'],
            axis=1)
        df.drop(df[df['Sector'] == 'Municipal'].index, inplace=True)  # Drop municipal bonds
        df['SectorM'] = df['Sector']
        df['Rating_c'] = df.apply(lambda row: "AAA_AA" if row['rating'] in ['AA', 'AAA'] else row['rating'], axis=1)
        df['RatingBucket'] = df.apply(
            lambda row: row['SectorM'] + row['Rating_c'] if row['SectorM'] == 'Corporate' else row['SectorM'], axis=1)

        # Note: MVAI is accrued interest + price (TODO! can place into a calculations sheet if desired)
        df['mvai'] = df['accruedinterest'] + df['price']

        # c) Calculate term points based on maturity date (TODO! Can isolate float64 365.25 into global var SET_YEAR_LEN)
        df['TermPt'] = df.apply(lambda row: round((row['maturitydate'] - self._given_date.date()).days / 365.25, 2),
                                axis=1) # TODO! As this is hardcoded, worthwhile putting the calculations on another page - for how we process the ftse data

        # d) Bucket the bonds into six term buckets (conditions => maintainability - *Brenda*) # TODO! remove name
        conditions = [
            (df['TermPt'] < 5.75),
            (df['TermPt'] < 10.75),
            (df['TermPt'] < 15.75),
            (df['TermPt'] < 20.75),
            (df['TermPt'] < 27.75),
            (df['TermPt'] < 35.25)      # Datahandler?
        ]
        choices = [1, 2, 3, 4, 5, 6]  # TODO! Remove comment: # only non-mutating functions methods are for bond terms and curves, not even for ftse data as it manipulates it (!!)
        # For TermPt >= 35.25, bucket = 0
        df['bucket'] = np.select(conditions, choices, default=0)  # TODO! Remove comment: # np.select() for vectorization (*Brenda* - these comments are removable)
        self._data = df

    def refresh_data(self):
        """
        Reloads FTSE bond data for the current date.
        Notes:
        ------
        This method re-fetches and processes the data based on the current
        `_given_date`, allowing users to refresh the data without modifying
        the specified date.
        """
        self._load_data()

    def update_date(self, new_date: datetime):
        """
        Updates the date and refreshes FTSE bond data.
        Parameters:
        ----------
        new_date : datetime
            The new date for which FTSE bond data should be retrieved.
        Notes:
        ------
        This method sets a new date and automatically triggers data
        re-processing by calling `refresh_data`.
        """
        self._given_date = new_date
        self.refresh_data()









"""Queries, not encapsulated:
"""
# ---- Non encapsulated objects -----

# Queries or things that effectively cause no mutation on external or parameter objects: includes get_bond_curves, create_general_shock_tables() can be placed in one file
def get_bond_curves(GivenDate: datetime) -> pd.DataFrame:
    """
    Retrieves and processes bond curves from the database for a given date. Bond curves remain annual curves as current.

    Parameters:
    GivenDate (datetime): The date for which bond curves are retrieved.

    Returns:
    pd.DataFrame: A DataFrame containing processed bond curves with selected bond types.
    """
    # SQL query to retrieve bond curve data from the database for the given date
    get_bond_curves_query = f"""
                    SELECT *
                    FROM bondcurves_byterm
                    WHERE date= '{GivenDate.date()}' 
                    """

    # Query to retrieve column names from the bond curves table
    get_column_names_query = '''SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'bondcurves_byterm';'''
    col_names = [name[0] for name in execute_table_query(get_column_names_query, 'General', fetch=True)]

    # Execute the query and create a DataFrame with the result
    df = pd.DataFrame(execute_table_query(get_bond_curves_query, 'General', fetch=True), columns=col_names)

    # Clean the DataFrame by removing unnecessary columns and transposing it
    df.set_index('name', inplace=True)
    df.drop(['date', 'curvetype'], axis=1, inplace=True)
    df = df.transpose()
    df.reset_index(inplace=True)

    # Select and rename specific bond categories for further analysis
    df = df[['CANADA', 'Provincial', 'AAA & AA', 'A', 'BBB', 'Corporate']]
    df.rename(columns={'CANADA': 'Federal', 'AAA & AA': "CorporateAAA_AA", 'A': 'CorporateA', 'BBB': 'CorporateBBB'},
              inplace=True)

    # Shift and divide by 100 to normalize the rates
    df = df.shift()[1:]
    df = df / 100

    return df  # Returns df: a Dataframe of bond curves for all years, per annum (IIRC)






"""
Old functions.
Work, may be less safe to use.
"""
def get_ftse_data(givenDate: datetime) -> pd.DataFrame:
    """
    Retrieves bond data from the FTSE universe for a given date and processes it.

    Parameters:
    givenDate (datetime): The date for which bond data is retrieved.

    Returns:
    pd.DataFrame: A DataFrame containing processed FTSE bond data with additional calculated columns.
    """
    # SQL query to retrieve bond information from the FTSE universe
    get_bond_info_query = f"""
                    SELECT date, cusip, term, issuername, 
                    annualcouponrate, maturitydate, yield, 
                    accruedinterest, modduration, rating, 
                    industrysector, industrygroup, industrysubgroup, 
                    marketweight, price
                    FROM ftse_universe_constituents
                    WHERE date= '{givenDate.date()}'
                    """

    # Execute the query and create a DataFrame with the result
    df = pd.DataFrame(execute_table_query(get_bond_info_query, 'Bond', fetch=True))
    df.columns = ['date', 'cusip', 'term', 'issuername', 'annualcouponrate', 'maturitydate', 'yield',
                  'accruedinterest', 'modduration', 'rating', 'industrysector', 'industrygroup',
                  'industrysubgroup', 'marketweight', 'price']

    # Calculate the market weight excluding real estate (REITs)
    total_marketweight = df['marketweight'].sum()
    real_estate_weight = df[df['industrygroup'] == "Real Estate"]['marketweight'].sum()
    df['marketweight_noREITs'] = df.apply(lambda row: 0 if row['industrygroup'] == "Real Estate"
    else row['marketweight'] / (total_marketweight - real_estate_weight) * 100,
                                          axis=1)  # TODO! This manipulates it for market_weight_noRET too

    # Add classification columns for sector (e.g. a bond name) and rating
    df['Sector'] = df.apply(
        lambda row: row['industrygroup'] if row['industrysector'] == 'Government' else row['industrysector'], axis=1)
    df.drop(df[df['Sector'] == 'Municipal'].index, inplace=True)  # Drop municipal bonds
    df['SectorM'] = df['Sector']
    df['Rating_c'] = df.apply(lambda row: "AAA_AA" if row['rating'] in ['AA', 'AAA'] else row['rating'], axis=1)
    df['RatingBucket'] = df.apply(
        lambda row: row['SectorM'] + row['Rating_c'] if row['SectorM'] == 'Corporate' else row['SectorM'], axis=1)
    df['mvai'] = df['accruedinterest'] + df[
        'price']  # TODO! This is what MVAI is; accrued interest + price (should have a calculations sheet) - does FTSE normally have an mvai, which we don't query and use substitute ours for instead here?

    # Calculate term points based on maturity date
    df['TermPt'] = df.apply(lambda row: round((row['maturitydate'] - givenDate.date()).days / 365.25, 2),
                            axis=1)  # TODO! This is hardcoded, worthwhile putting the calculations on another page - for how we process the ftse data

    # Bucket the bonds into six term buckets (conditions => maintainability - *Brenda*)
    conditions = [
        (df['TermPt'] < 5.75),
        (df['TermPt'] < 10.75),
        (df['TermPt'] < 15.75),
        (df['TermPt'] < 20.75),
        (df['TermPt'] < 27.75),
        (df['TermPt'] < 35.25)  # datahandler
    ]
    choices = [1, 2, 3, 4, 5,
               6]  # only methods are for bond terms and curves, not even for ftse data as it manipulates it (!!)
    df['bucket'] = np.select(conditions, choices,
                             default=0)  # np.select() for vectorization (*Brenda* - these comments are removable)

    return df

    """
    code takes it, puts it into 70 buckets, figure out the coupons and the weights, and puts it back down to 
    6 buckts (*a) , to find assets to invest, and to match up to our sensitivities
    """


