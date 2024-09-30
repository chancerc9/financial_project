
import json
from scipy.optimize import minimize
import os
import numpy as np
from dateutil.relativedelta import relativedelta
from equitable.db.db_functions import execute_table_query
import traceback
import pandas as pd
pd.set_option('display.width', 150)
import datetime as dt
from collections import OrderedDict
import openpyxl
import argparse
from equitable.infrastructure import sysenv, jobs, sendemail
from equitable.chronos import offsets, conversions
from equitable.db.psyw import SmartDB
from psycopg2.extras import DictCursor
from scipy import interpolate

# Adds system path for required modules:
import sys
sys.path.append(sysenv.get("ALM_DIR"))  # Adds system path for required modules (above)

# Establish database connections:
BM_conn = SmartDB('Benchmark')
BM_cur = BM_conn.con.cursor()

Bond_conn = SmartDB('Bond')
Bond_cur = Bond_conn.con.cursor()

General_conn = SmartDB('General')
General_cur = General_conn.con.cursor()

# for the (?), double-check through terminology

### CODE ###

#class GetData:


#class CreateShocks:


# 1. Get bond curves and process (add half-year intervals, choose) - replaces get_bond_curves() and - functionality

## USED FUNCTION ##

def get_bond_curves(GivenDate):
    """
    Function to calculate bond curves from the database, based on a given date
    """

    # Returns bond curves from pgadmin as a dataframe:
    get_bond_curves_query = """
                    SELECT *
                    FROM bondcurves_byterm
                    WHERE date= '{}' 
                    """.format(GivenDate.date())
    get_column_names_query = '''SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'bondcurves_byterm';'''
    col_names = [name[0] for name in execute_table_query(get_column_names_query, 'General', fetch=True)]
    df = pd.DataFrame(execute_table_query(get_bond_curves_query, 'General', fetch=True), columns=col_names)

    # Cleaning up df to only have the curves we want; for all available years of data
    df.set_index('name', inplace=True)
    df.drop(['date', 'curvetype'], axis=1, inplace=True)
    df = df.transpose()
    df.reset_index(inplace=True)
    df = df[['CANADA', 'Provincial', 'AAA & AA', 'A', 'BBB', 'Corporate']]
    df.rename(columns={'CANADA': 'Federal', 'AAA & AA': "CorporateAAA_AA", 'A': 'CorporateA', 'BBB': 'CorporateBBB'}, inplace=True)
    df = df.shift()[1:]
    df = df/100

    return df

## USED FUNCTION ##
def get_ftse_data(givenDate):
    """
    Function to get bond data from the FTSE universe
    """

    # Gets data from pgadmin and returns it as a dataframe
    get_bond_info_query = """
                    SELECT date, cusip, term, issuername, 
                    annualcouponrate, maturitydate, yield, 
                    accruedinterest, modduration, rating, 
                    industrysector, industrygroup, industrysubgroup, 
                    marketweight, price
                    FROM ftse_universe_constituents
                    WHERE date= '{}'
                    """.format(givenDate.date())
    df = pd.DataFrame(execute_table_query(get_bond_info_query, 'Bond', fetch=True))
    df.columns=['date', 'cusip', 'term', 'issuername', 'annualcouponrate', 'maturitydate', 'yield', 'accruedinterest', 'modduration', 'rating', 'industrysector', 'industrygroup', 'industrysubgroup', 'marketweight', 'price']

    # Calculate the marketweight excluding REITs (real estate)
    total_marketweight = df['marketweight'].sum()
    real_estate = df.loc[df['industrygroup'] == "Real Estate"]['marketweight'].sum()
    df['marketweight_noREITs'] = df.apply(lambda row: 0 if row['industrygroup'] == "Real Estate" else row['marketweight']/(total_marketweight - real_estate)*100, axis=1)

    # Additional classification columns for sector and rating, ex: making a column that explicitly states the bond name and rating
    df['Sector'] = df.apply(lambda row: row['industrygroup'] if row['industrysector'] == 'Government' else row['industrysector'], axis=1)
    munis = df[df['Sector'] == 'Municipal'].index   ##
    df.drop(munis , inplace=True)                   ## Dropped municipals - Change #1 ##
    df['SectorM'] = df.apply(lambda row: row['Sector'], axis=1)
    df['Rating_c'] = df.apply(lambda row: ("AAA_AA" if (row['rating'] == 'AA' or row['rating'] == 'AAA') else row['rating']), axis=1)
    df['RatingBucket'] = df.apply(lambda row: row['SectorM'] + row['Rating_c'] if row['SectorM'] == 'Corporate' else row['SectorM'], axis=1)
    df['mvai'] = df['accruedinterest'] + df['price']

    # Term points in years based on maturity date
    df['TermPt'] = df.apply(lambda row: round((row['maturitydate'] - givenDate.date()).days/365.25, 2), axis=1)

    # Bucketing the bonds into 6 term buckets
    df['bucket'] = df.apply(lambda row: 1 if row['TermPt'] < 5.75 else
    (2 if row['TermPt'] < 10.75 else # 6 bucketing tables as normal #
     (3 if row['TermPt'] < 15.75 else
      (4 if row['TermPt'] < 20.75 else
       (5 if row['TermPt'] < 27.75 else
        (6 if row['TermPt'] < 35.25 else 0))))), axis=1)

    return df

# code takes it, puts it into 70 buckets, figure out the coupons and the weights, and puts it back down to 6 buckts (*a) , to find assets to invest, and to match up to our sensitivities

## USED FUNCTION ##
def create_bucketing_table():
    """
    Function to create bucketing table based on term intervals

    i.e., making the table for bucket ranges for each term; ex: 0-1.5 for 1 yrs, 1.5-2.5 for 2 yrs, 2.5-4 for 3 yrs, etc.
    """
    d = {'Term': list(np.linspace(start=0.5, stop=35, num=70))} ## Previous 10 bucketing tables became 70 - Change #2
    df = pd.DataFrame(data=d)
    df['Lower_Bound'] = (df['Term'] + df['Term'].shift(1))/2
    df['Upper_Bound'] = df['Lower_Bound'].shift(-1)
    df.iloc[0, 1] = 0
    df.iloc[69, 2] = 100
    return df

## USED FUNCTION ##
def create_weight_tables(ftse_data):
    """
    Function to create weight tables for each rating based on subindex percentages
    """
    buckets = [1, 5.75, 10.75, 15.75, 20.75, 27.75, 35.25] # These 6 buckets here (*a) - where (*a) means new thingg
    weight_dict = {}

    total_universe_weights = pd.DataFrame(
        index=['Federal', 'Provincial', 'CorporateAAA_AA', 'CorporateA', 'CorporateBBB', 'Corporate'],
        columns=list(range(1, 7)))

    for rating in ['Federal', 'Provincial', 'CorporateAAA_AA', 'CorporateA', 'CorporateBBB', 'Corporate']:
        column_to_look_in = "RatingBucket"
        if rating == 'Corporate':
            column_to_look_in = "Sector"

        df = create_bucketing_table()
        for x in range(6):
            lower_b = buckets[x]
            upper_b = buckets[x+1]
            column_name = str(lower_b) + " - " + str(upper_b)

            # Sum market weights for securities within each bucket
            df[column_name] = df.apply(lambda row: ftse_data.loc[
                (ftse_data[column_to_look_in] == rating) &
                (ftse_data['TermPt'] < upper_b) &
                (ftse_data['TermPt'] >= lower_b) &
                (ftse_data['TermPt'] < row['Upper_Bound']) &
                (ftse_data['TermPt'] > row['Lower_Bound']-0.0001)
            ]['marketweight_noREITs'].sum(), axis=1)

            total_universe_weights.loc[rating, x + 1] = df[column_name].sum()
            # Dividing by the sum of the column to get the weight as a percentage of the subindex
            # i.e., to normalize weights by sum of the column
            df[column_name] = df[column_name]/df[column_name].sum()

        weight_dict[rating] = df

    return weight_dict, total_universe_weights

## USED FUNCTION ##
def create_general_shock_table():
    """
    Creates a general shock table as a dataframe to use for calculating shocks for each security type
    """
    shock_size = 0.0001
    buckets = [0, 1, 2, 3, 5, 7, 10, 15, 20, 25, 30, 100]
    df = pd.DataFrame(columns=buckets, index=list(np.linspace(start=0.5, stop=35, num=70))) # (*a) changed bucket size
    df[0] = df.index
    for i in range(1,11):
        df[buckets[i]] = df.apply(lambda row: ((1 - (row[0] - buckets[i])/(buckets[i+1] - buckets[i]))*shock_size) if (row[0] <= buckets[i+1]) & (row[0] >= buckets[i])
        else (((row[0] - buckets[i-1])/(buckets[i] - buckets[i-1])*shock_size) if (row[0] <= buckets[i]) & (row[0] >= buckets[i-1]) else 0), axis=1)
    df = df.drop(100, axis=1)
    return df


## USED FUNCTION ##
# Interpolating for half-years - side-eff 1

# Applies the shocks to the bond curves for each rating and store results in shocks_dict - side eff 2 (main eff...major purpose)
def create_shock_tables(curves): # CURVES are BOND CURVES LOL
    """
    Function to create up and down shock tables for bond curves
    """
    # makes a dictionary containing tables for up shocks and down shocks for each rating
    shocks_dict = {}
    up_shocks = create_general_shock_table()
    down_shocks = create_general_shock_table()
    down_shocks = -down_shocks
    down_shocks[0] = -down_shocks[0]

    #### Interpolates BOND CURVES for half years
    ## (*begin) Interpolates half-year curves (avg of 1, 2y rate for 11/12yr curve
        # 1+1/2 is average of 1y and 2y rate

    # Interpolating bond curves for half-year intervals
    curves_mod = pd.DataFrame(
        columns=['Federal', 'Provincial', 'CorporateAAA_AA', 'CorporateA', 'CorporateBBB', 'Corporate'],
        index=list(np.linspace(start=0.5, stop=35, num=70)))

    for rating in ['Federal', 'Provincial', 'CorporateAAA_AA', 'CorporateA', 'CorporateBBB', 'Corporate']:
        for i in range(1, 35):
            curves_mod.loc[i, rating] = curves.loc[i, rating]
        for yr in np.linspace(start=1.5, stop=34.5, num=34):
            curves_mod.loc[yr, rating] = (curves.loc[yr + .5, rating] + curves.loc[yr - .5, rating]) / 2

    curves_mod.loc[0.5] = curves_mod.loc[1]
    curves_mod.fillna(method='ffill', inplace=True) ## (*end)

    ####

    # Apply up and down shocks to bond curves
    for rating in ['Federal', 'Provincial', 'CorporateAAA_AA', 'CorporateA', 'CorporateBBB', 'Corporate']:
        for direction in ['Up', 'Down']:
            table_name = rating + ' - ' + direction
            if direction == 'Up':
                df = up_shocks
            else:
                df = down_shocks

            # Add shocks to bond curves
            df = df.add(curves_mod[rating], axis=0)
            df[0] = curves_mod[rating]

            # Apply power function to simulate the bond curve transformation after shocks
            df += 1
            df['Powers'] = df.index
            df = df.pow(df['Powers'], axis=0)
            df = 1 / df
            df = df.drop('Powers', axis=1)

            shocks_dict[table_name] = df

    return shocks_dict

## USED FUNCTIONs ##

# (*begin) Takes each year and looks at rating and FTSE universe (half-year would be from .25 to .75; up quarter year and down quarter year for half year, and so on for every year
def calc_avg_coupon(year, rating, ftse_data):
    # calculates the average coupon for all ratings based on ftse_data formatted like the output of get_ftse_data
    column_to_look_in = "RatingBucket"
    if rating == 'Corporate':
        column_to_look_in = "Sector"

    lower_bound = year - 0.25
    upper_bound = year + 0.25

    df = ftse_data.loc[(ftse_data[column_to_look_in] == rating) & (ftse_data['TermPt'] < upper_bound) & (ftse_data['TermPt'] > (lower_bound - 0.001)) & (ftse_data['marketweight_noREITs'] > 0)]

    if df.empty:
        avg_coupon = 0
    else:
        avg_coupon = ((df['marketweight_noREITs'] * df['annualcouponrate'] / df['mvai']).sum() / (df['marketweight_noREITs'] / df['mvai']).sum())/2

    return avg_coupon

def calc_pv(year, rating, ftse_data):
    column_to_look_in = "RatingBucket"
    if rating == 'Corporate':
        column_to_look_in = "Sector"

    lower_bound = year - 0.25
    upper_bound = year + 0.25

    df = ftse_data.loc[(ftse_data[column_to_look_in] == rating) & (ftse_data['TermPt'] < upper_bound) & (
                ftse_data['TermPt'] > (lower_bound - 0.001)) & (ftse_data['marketweight_noREITs'] > 0)]

    if df.empty:
        pv = 0
    else:
        pv = (df['marketweight_noREITs'] * df['mvai']).sum() / df['marketweight_noREITs'].sum()

    return pv
## USED FUNCTIONs ##

# (*end)

## USED FUNCTION ##

def create_cf_tables(ftse_data):
    # uses the average coupon rate to calculate annual cashflows for each rating type
    cf_dict = {}
    years = list(np.linspace(start=0.5, stop=35, num=70)) ## changing buckets from 10 to 70
    buckets = list(np.linspace(start=0.5, stop=35, num=70))
    df = pd.DataFrame(columns=years, index=buckets)
    df.insert(0, 'Bucket', buckets)
    df.insert(1, 'Principal', 100)

    for rating in ['Federal', 'Provincial', 'CorporateAAA_AA', 'CorporateA', 'CorporateBBB', 'Corporate']:

        df = pd.DataFrame(columns=years,index=buckets)
        df.insert(0, 'Bucket', buckets)
        df.insert(1, 'Principal', 100)

        df['PV'] = df.apply(lambda row: calc_pv(row['Bucket'], rating, ftse_data), axis=1)
        df['Coupon'] = df.apply(lambda row: calc_avg_coupon(row['Bucket'], rating, ftse_data), axis=1)

        coupons = df.pop(df.columns[-1])
        df.insert(2, 'Coupon', coupons)

        for col in np.linspace(start=0.5, stop=35, num=70): # change buckets to 70
            df[col] = df.apply(lambda row: row['Coupon'] if row['Bucket'] > col else ((row['Coupon'] + row['Principal']) if row['Bucket'] == col else 0), axis=1)

        cf_dict[rating] = df.iloc[:, :73]
        cf_dict[rating + 'PV'] = df.iloc[:, 73]

    return cf_dict

# you can data transform it first
# THEN do the code
# do it fast then it'll be well

## USED FUNCTION ##

def create_sensitivity_tables(cashflows, shocks): # CASHFLOWS table (cashflow sensitivities, applied on cashflows via shocks
    """
    Function to calculate cashflow sensitivities based on shocks applied to bond curves
    """
    sensitivities_dict = {}

    # Key Rate Duration (KRD) buckets based on predefined time intervals
    buckets_krd = [0, 1, 2, 3, 5, 7, 10, 15, 20, 25, 30]

    for rating in ['Federal', 'Provincial', 'CorporateAAA_AA', 'CorporateA', 'CorporateBBB', 'Corporate']:
        cfs = cashflows[rating]  # Retrieve cashflows for the current rating
        ups = shocks[rating + ' - Up']  # Retrieve up shock table for the current rating
        downs = shocks[rating + ' - Down']  # Retrieve down shock table for the current rating

        ## sumproduct for each, changed to get the sensitivities
        # cahsflows for the square (70*70) table, and it fits into the 10*70 sensitivities that it matches up to - sum to each one, cahsflow*shocks.

        # Create empty DataFrames for storing up and down shock sensitivities
        df_up = pd.DataFrame(columns=buckets_krd[1:], index=range(71))
        df_up.insert(0, 'Bucket', list(np.linspace(start=0, stop=35, num=71)))

        df_down = pd.DataFrame(columns=buckets_krd[1:], index=range(71))
        df_down.insert(0, 'Bucket', list(np.linspace(start=0, stop=35, num=71)))

        # Calculate sensitivities by summing the product of cashflows and shocks for up and down tables
        for x in range(1, 11):
            for i in range(70):
                df_up.iloc[i, x] = np.sum(cfs.iloc[i, 3:] * ups.iloc[:, x])  # Multiply cashflows by up shocks
                df_down.iloc[i, x] = np.sum(cfs.iloc[i, 3:] * downs.iloc[:, x])  # Multiply cashflows by down shocks

        up_shock_sensitivities = df_up
        down_shock_sensitivities = df_down

        # Calculate the average sensitivity (difference between down and up shocks divided by 2)
        average_sensitivity = (down_shock_sensitivities - up_shock_sensitivities) / 2 * 10000

        # Add bucket information and transpose the result for better readability
        average_sensitivity['Bucket'] = list(np.linspace(start=.5, stop=35.5, num=71))
        average_sensitivity = average_sensitivity.transpose()
        average_sensitivity = average_sensitivity.drop(70, axis=1)
        average_sensitivity = average_sensitivity.iloc[1:]

        # Insert bucket names for KRD
        average_sensitivity.insert(0, 'Bucket', [1, 2, 3, 5, 7, 10, 15, 20, 25, 30])

        # Normalize the sensitivities by the cashflow present values (PV)
        avg_sensitivity = average_sensitivity
        for x in range(10):
            for i in range(70):
                average_sensitivity.iloc[x, i + 1] = avg_sensitivity.iloc[x, i + 1] / cashflows[rating + 'PV'].iloc[i]

        sensitivities_dict[rating] = average_sensitivity

    return sensitivities_dict

"""    # Create empty DataFrames for storing up and down shock sensitivities
        df_up = pd.DataFrame(columns=buckets_krd[1:], index=range(71))
        df_up.insert(0, 'Bucket', list(np.linspace(start=0, stop=35, num=71)))

        df_down = pd.DataFrame(columns=buckets_krd[1:], index=range(71))
        df_down.insert(0, 'Bucket', list(np.linspace(start=0, stop=35, num=71)))

        # Old:
        #'''
        for x in range(1,11):
            for i in range(70):

                df_up.iloc[i, x] = np.sum(cfs.iloc[i, 3:] * ups.iloc[:, x])  # cashflows table, sum prod
                df_down.iloc[i, x] = np.sum(cfs.iloc[i, 3:] * downs.iloc[:, x])
        #'''
        # New (Brenda):
        #df_up = np.sum(cfs.iloc[:, 3:].values * ups.values[:, 1:], axis=1)
        #df_down = np.sum(cfs.iloc[:, 3:].values * downs.values[:, 1:], axis=1)
        # New (Brenda)

        up_shock_sensitivities = df_up
        down_shock_sensitivities = df_down
        average_sensitivity = (down_shock_sensitivities - up_shock_sensitivities)/2 * 10000

        #average_sensitivity = (average_sensitivity.divide(ups.iloc[:,0]*100) * 10000).iloc[:,1:]
        average_sensitivity['Bucket'] = list(np.linspace(start=.5, stop=35.5, num=71))

        average_sensitivity = average_sensitivity.transpose()

        average_sensitivity = average_sensitivity.drop(70, axis=1)

        average_sensitivity = average_sensitivity.iloc[1:]

        average_sensitivity.insert(0, 'Bucket', [1, 2, 3, 5, 7, 10, 15, 20, 25, 30])

        avg_sensitivity = average_sensitivity

        for x in range(10):
            for i in range(70):
                average_sensitivity.iloc[x, i + 1] = avg_sensitivity.iloc[x, i + 1] / cashflows[rating + 'PV'].iloc[i]

        sensitivities_dict[rating] = average_sensitivity

    return sensitivities_dict"""
## (*end)

def make_krd_table(weights, sensitivities):
    # makes the final KRD table based on sensitivities and market weight and puts it all together in one dataframe
    KRDs = {}
    cols = ['rating', 'term', 'bucket1', 'bucket2', 'bucket3', 'bucket4', 'bucket5', 'bucket6']
    buckets = [1, 2, 3, 5, 7, 10, 15, 20, 25, 30]

    for rating in ['Federal', 'Provincial', 'CorporateAAA_AA', 'CorporateA', 'CorporateBBB', 'Corporate']:
        df = pd.DataFrame(columns=cols, index=range(10))
        df['term'] = buckets
        df['rating'] = rating
        for x in range(2, 8):
            df.iloc[:, x] = df.apply(lambda row: (sensitivities[rating].loc[sensitivities[rating]['Bucket'] == row['term']].iloc[:, 1:].values[0] * weights[rating].iloc[:, (x+1)]).sum(), axis=1)
        KRDs[rating] = df
    df = pd.concat([KRDs['Federal'], KRDs['Provincial'], KRDs['CorporateAAA_AA'], KRDs['CorporateA'], KRDs['CorporateBBB'], KRDs['Corporate']], ignore_index=True)

    df.fillna(0, inplace=True)
    return df



def getBSPath(date):
    bsFileName = 'SEGMENTED BALANCE SHEET-{year}.xlsx'
    # bsPathRoot = '\\\\estorage.equitable.int\\pcshared\\Financial Reporting\\Segmented Balance Sheets\\'
    bsPathRoot = os.path.join(sysenv.get('PORTFOLIO_ATTRIBUTION_DIR'), 'Benchmarking')

    current_year_path = bsPathRoot + '\\' + bsFileName.format(year=date.strftime('%Y'))
    prev_year = date.year - 1
    prev_year_path = bsPathRoot + '\\' + bsFileName.format(year=str(prev_year))

    if os.path.exists(current_year_path):
        path = current_year_path
    # elif os.path.exists(prev_year_path):
    #     path = prev_year_path
    else:
        path = bsPathRoot + '\\Prior Years\\' + date.strftime('%Y') + '\\' + bsFileName.format(year=date.strftime('%Y'))

    if os.path.exists(path):
        return path
    else:
        raise Exception('Path not found: {0}'.format(path))



def get_expected_returns():
    file_name = "Parallel_tilt_curve_history.xlsx"
    path_input = os.path.join(sysenv.get('PORTFOLIO_ATTRIBUTION_DIR'), "Benchmarking", file_name)
    workbook = openpyxl.load_workbook(path_input, data_only=True, read_only=True)

    expected_returns = pd.DataFrame()

    ratings = ['Federal', 'Provincial', 'corporateAAA_AA', 'corporateA', 'corporateBBB']
    for sheet in ['analysis_quarterly_RF', 'analysis_quarterly_prov', 'analysis_quarterly_AA', 'analysis_quarterly_A', 'analysis_quarterly_BBB']:
        rownum = 27 if (sheet == 'analysis_quarterly_RF') else 22
        ws = workbook[sheet]
        data = ws.values
        columns = next(data)[0:]
        df = pd.DataFrame(data, columns=columns)
        returns = df.loc[rownum:rownum, 'term1': 'term30']
        expected_returns = pd.concat([expected_returns, returns], ignore_index=True)
    expected_returns['ratings'] = ratings
    expected_returns.set_index('ratings', inplace=True)

    term_assumptions = [2, 7, 12, 17, 23, 29]
    return_assumptions = pd.DataFrame(columns=[0, 1, 2, 3, 4, 5])

    x = [1, 2, 3, 4, 5, 7, 10, 20, 30]
    for rating in ratings:
        y = expected_returns.loc[rating].to_numpy()

        temp = interpolate.splrep(x, y, s=0)
        xnew = np.arange(1, 31)
        ynew = interpolate.splev(xnew, temp, der=0)

        return_assumptions.loc[rating] = ynew[term_assumptions]

    return return_assumptions/100

## New fUNCTION to read in stuff (no need from balance sheet) - check it easier from the balance sheet, easier
# helper function to read in from balance sheet (execel) not from the comapny blcsht its

# so have a function for reading in data
# for modifiability
# yu don't know how much i know the code - which is a lot
# i was doing it durin

# before it was reading in assets from balance sheet, but now its reading in data from excel sheet

# ReadInData
# this class also process it

# i forgot, bond class or calc omg
# recall and reconnect
#check the idea again
# thibk again for what makes sense///what was it again
# what did i think and mention


def BSTotals(given_date, sheet_version): # ask to say output (1 for segments, 0 for totals)

    year = given_date.strftime("%Y")
    quarter = ((given_date.month - 1) // 3) + 1
    year_quarter = year + "Q" + str(quarter)

    file_name = "SBS Totals.xlsx"
    path_input = os.path.join(sysenv.get('PORTFOLIO_ATTRIBUTION_DIR'), "Benchmarking", file_name)
    workbook = openpyxl.load_workbook(path_input, data_only = True)
    # ws = workbook['2024Q1']

    if sheet_version == 1: # 1 for segments, 0 for totals (better to name if_total, and have bool) - (*li)
        ws = workbook[year_quarter]
    else:
        ws = workbook[year_quarter + ' (Total)']  # 0 for totals - (*li)

    data = ws.values
    columns = next(data)[0:]
    df = pd.DataFrame(data, columns=columns)
    totals = {
        'ACCUM': 0,
        'PAYOUT': 0,
        'UNIVERSAL': 0,
        'NONPAR': 0,
        'GROUP': 0,
        'PARCSM': 0,
        'SEGFUNDS': 0,
        'Surplus': 0,
        'Total': 0
    }

    totals['ACCUM'] = df.loc[2,'ACCUM']
    totals['PAYOUT'] = df.loc[2,'PAYOUT']
    totals['UNIVERSAL'] = df.loc[2,'UNIVERSAL']
    totals['NONPAR'] = df.loc[2,'NONPAR']
    totals['GROUP'] = df.loc[2,'GROUP']
    totals['PARCSM'] = df.loc[2,'PARCSM']
    totals['SEGFUNDS'] = df.loc[2,'SEGFUNDS']
    totals['Surplus'] = df.loc[2, 'Surplus']
    totals['Total'] = df.loc[2, 'Total']

    return totals


def percents(given_date, curMonthBS=False):

    year = given_date.strftime("%Y")
    quarter = ((given_date.month - 1) // 3) + 1
    if curMonthBS and quarter < 4:
        quarter += 1
    year_quarter = year + "Q" + str(quarter)

    file_name = "Mix_hardcoded.xlsx"
    path_input = os.path.join(sysenv.get('PORTFOLIO_ATTRIBUTION_DIR'), "Benchmarking", file_name)
    workbook = openpyxl.load_workbook(path_input)
    # ws = workbook['2024Q1']
    ws = workbook[year_quarter]
    data = ws.values
    columns = next(data)[0:]
    df = pd.DataFrame(data, columns=columns)
    df = df.set_index('rating')
    print(year_quarter)
    df['Surplus'] = df['SEGFUNDS'] = 0

    df = df.loc[['Public Federal',
                 'Public Provincial',
                 'Public Corporate - AA',
                 'Public Corporate - A',
                 'Public Corporate - BBB',
                 'MortgagesInsured',
                 'MortgagesConv',
                 'PrivateAA',
                 'PrivateA',
                 'PrivateBBB',
                 'PrivateBB_B']]

    df.rename({'Public Federal': 'Federal',
               'Public Provincial': 'Provincial',
               'Public Corporate - AA': 'CorpAAA_AA',
               'Public Corporate - A': 'CorpA',
               'Public Corporate - BBB': 'CorpBBB'}, inplace=True)
    return df

def solution_dollar_amounts(Asset_mix, solution_df):
    weights = Asset_mix[['Accum', 'group', 'ul', 'Payout', 'np']].stack()
    weights = weights.sort_index()
    weights2 = weights.reset_index(drop=True)
    sols = solution_df[(solution_df['portfolio'] != 'Liability') & (solution_df['portfolio'] != 'Total')].set_index(['rating', 'portfolio'])
    sols = sols.sort_index()
    sols2 = sols.reset_index(drop=True)
    w = sols2.mul(weights2, axis=0)
    w['rating'] = sols.reset_index()['rating']
    w['portfolio'] = sols.reset_index()['portfolio']
    w = w.set_index(['portfolio', 'rating'])
    w_grouped = w.groupby('rating')
    for index, row in w_grouped:
        total_values = row.sum()
        total_values['rating'] = index
        total_values['portfolio'] = 'Total'
        total_values = pd.DataFrame(total_values).T.set_index(['portfolio', 'rating'])
        w = pd.concat([w, total_values])
    w = w.reset_index()
    return w

''' This function takes in the asset mix and the solved solution up to this point to calculate how much of the total allocation has been allocated in each portfolio. Those weights are used as bounds for the total optimization'''
def get_bnds_for_total(Asset_mix, solution_df):
    sol = solution_dollar_amounts(Asset_mix, solution_df)
    total = sol.loc[sol['portfolio'] == 'Total'].drop(columns=['portfolio']).set_index('rating')
    dollars = Asset_mix['Total']
    bounds = total.div(dollars, axis=0)
    bounds = bounds.where(bounds > 0, 0)
    return bounds

def liabilities_table(Asset_mix, solution_df):
    sol = solution_dollar_amounts(Asset_mix, solution_df)
    total = sol.loc[sol['portfolio'] == 'Total'].drop(columns=['portfolio']).set_index('rating')
    dollars = total.sum(axis=1)
    liabilities = total.div(dollars, axis=0)
    liabilities['rating'] = liabilities.index
    liabilities['portfolio'] = 'Liability'
    liabilities = liabilities.reset_index(drop=True)
    return liabilities

def surplus_table(Asset_mix, solution_df):
    npt_weights = Asset_mix['Total']
    npt_sol = solution_df[(solution_df['portfolio'] == 'Total')].drop(columns=['portfolio']).set_index('rating')

    optimization_sol = npt_sol.mul(npt_weights, axis=0)


    sol = solution_dollar_amounts(Asset_mix, solution_df)
    total = sol.loc[sol['portfolio'] == 'Total'].drop(columns=['portfolio']).set_index('rating')

    total = optimization_sol - total
    dollars = total.sum(axis=1)
    surplus = total.div(dollars, axis=0)
    surplus['rating'] = surplus.index
    surplus['portfolio'] = 'Surplus'
    surplus = surplus.reset_index(drop=True)
    return surplus

def calc_bounds(given_date, portfolio, total):
    if ((portfolio != 'ul') & (portfolio != 'np')):
        return [[0, 1],  [0, 1], [0, 1], [0, 1], [0, 1], [0, 1]]

    year = given_date.strftime('%Y')
    year_folder = given_date.strftime('%Y')

    quarter = ((given_date.month - 1) // 3) + 1
    prev_quarter = quarter - 1
    if prev_quarter == 0:
        prev_quarter = 4
        year = str(given_date.year - 1)
    quarter = str(quarter)
    prev_quarter = str(prev_quarter)

    if (given_date.year == 2024) & (quarter == '1'):
        file_name = portfolio + ' IFE Estimate Q1 2024.xlsx'
    else:
        file_name = f"{portfolio} IFE Estimate Q{quarter} {year}.xlsx"
        # file_name = portfolio + ' IFE Estimate Q' + prev_quarter + ' ' + year + ' to Q' + quarter + '.xlsx'



    path_input = os.path.join(sysenv.get('LOB_MANAGEMENT_DIR'), "Investment Income Explanation", year_folder, 'IFE estimates', ('Q'+quarter), file_name)
    try:
        workbook = openpyxl.load_workbook(path_input, data_only=True, read_only=True)
    except FileNotFoundError:
        file_name = portfolio + ' IFE Estimate Q' + prev_quarter + ' ' + year + ' to Q' + quarter + '.xlsx'
        path_input = os.path.join(sysenv.get('LOB_MANAGEMENT_DIR'), "Investment Income Explanation", year_folder,
                                  'IFE estimates', ('Q' + quarter), file_name)
        workbook = openpyxl.load_workbook(path_input, data_only=True, read_only=True)

    ws = workbook['CF']
    data = ws.values
    columns = next(data)[0:]
    df = pd.DataFrame(data, columns=columns)

    cf_pvs = df.iloc[1:7, 34].tolist()
    bnds = []

    # if the cf pv is negative, allow short positions in those buckets, otherwise (0, 6)
    for x in cf_pvs:
        if x >= 0:
            bnds.append([0, 6])
        else:
            bnds.append([x/total, 6])
    return bnds

''' given a df with a multi-index, portfolio and rating, this function will sum all rows with the same rating, and append the sum to a new row with portfolio 'Total' '''
def get_totals_for_rating(df, reset_index=False):
    print(df)
    df_copy = df.copy()
    df_grouped = df_copy.groupby('rating')
    for index, row in df_grouped:
        total_values = row.sum()
        total_values['rating'] = index
        total_values['portfolio'] = 'Total'
        total_values = pd.DataFrame(total_values).T.set_index(['portfolio', 'rating'])
        df_copy = pd.concat([df_copy, total_values])

    if reset_index:
        return df_copy.reset_index()

    return df_copy

def public_sensitivities():
    file_name = "Targets By Asset Class.xlsx"
    path_input = os.path.join(sysenv.get('PORTFOLIO_ATTRIBUTION_DIR'), "Benchmarking", file_name)
    sheet = 'public'
    workbook = openpyxl.load_workbook(path_input, data_only=True)
    ws = workbook[sheet]
    data = ws.values
    columns = next(data)[0:]
    df = pd.DataFrame(data, columns=columns)

    return df

def private_sensitivities():
    file_name = "Targets By Asset Class.xlsx"
    path_input = os.path.join(sysenv.get('PORTFOLIO_ATTRIBUTION_DIR'), "Benchmarking", file_name)
    sheet = 'private'
    workbook = openpyxl.load_workbook(path_input, data_only=True)
    ws = workbook[sheet]
    data = ws.values
    columns = next(data)[0:]
    df = pd.DataFrame(data, columns=columns)

    return df

def mortgage_sensitivities():
    file_name = "Targets By Asset Class.xlsx"
    path_input = os.path.join(sysenv.get('PORTFOLIO_ATTRIBUTION_DIR'), "Benchmarking", file_name)
    sheet = 'mortgage'
    workbook = openpyxl.load_workbook(path_input, data_only=True)
    ws = workbook[sheet]
    data = ws.values
    columns = next(data)[0:]
    df = pd.DataFrame(data, columns=columns)

    return df