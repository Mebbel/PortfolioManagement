#----------------------------------------------------------------------------------
#
# Download and preprocess data for ETFs from broker's websites
#
#----------------------------------------------------------------------------------


import numpy as np
import pandas as pd
import os
import requests




def getData_ETF(fund_company, url_positions):
    """Gets Data from the web for each Fondsgesellschaft 
    and tackles the different data formats 
    from csv to pdfs to HTML webscrape
    
    :param fund_company: Name of broker
    :param url_positions: Link to the data at the broker's website
    
    """

    # General variables ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    file_path = "./Downloads/"

    # File type ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # TODO: replace with case or dict
    if ((fund_company == "iShares") | (fund_company == "L&G")):
        file_name = "temp.csv"
        
    elif fund_company == "XTrackers":
        file_name = "temp.xlsx"
        
    elif ((fund_company == "HSBC") | (fund_company == "Lyxor")):
        file_name = "temp.xls"

    else:
        print("Fund company undefined: " + fund_company)
        

    # Download and save the file ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    resp = requests.get(url_positions)
    with open(file_path + file_name, 'wb') as output:
        output.write(resp.content)

    return([file_path, file_name])



def readData_ETF(fund_company, file_path, file_name, isin):
    """Reads the specified file and harmonizes the data
    over the provided formats of different brokers.
    Attempts to preserve as much information as possible.
    
    :param fund_company: Name of broker
    :param file_path: Path to temporary file of last downloaded data
    :param file_name: Name of temporary file. Different endings for different brokers.
    :param isin: ISIN Number of current ETF product
    
    """

    # Prepare dictionaries to collection information
    fund_requests = {}

    # iShares ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    if fund_company == "iShares":

        # Read data from files~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        fund_requests["FILE_EXISTS"] = os.path.exists(file_path + file_name)

        data = pd.read_csv(file_path + file_name, skiprows = 2)

        fund_requests["N_ENTRIES"] = data.shape[0]

        # data['DATE_REQUEST'] = timestamp
        data['ISIN_FUND'] = isin

        data.rename(
            columns = {
                "Emittententicker" : "TICKER",
                "Name" : "NAME",
                "Gewichtung (%)": "WEIGHT",
                "Kurs": "PRICE",
                "Sektor": "SECTOR",
                "Börse": "EXCHANGE",
                "Standort": "COUNTRY",
                "Marktwährung": "CCY"
            }, inplace = True
        )

        # Select only relevant columns
        # PL 21.04.2021 -> removed , 'PRICE'
        # PL 04.03.2022 -> conditional columns as Equity Funds do not have ISIN, but Debt funds do.
        cols = ['ISIN_FUND', 'ISIN', 'TICKER', 'NAME', 'WEIGHT', 'SECTOR', 'EXCHANGE', 'COUNTRY', 'CCY']
        data = data[[i for i in cols if i in data.columns]] 

        # Adjust data types

        # numeric columns must be in English decimal notation
        data.fillna('', inplace=True)
        data['WEIGHT'] = [x.replace(',', '.') for x in data['WEIGHT']]
        data['WEIGHT'] = pd.to_numeric(data['WEIGHT'])

    # XTrackers ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    elif fund_company == "XTrackers":

        fund_requests["FILE_EXISTS"] = os.path.exists(file_path + file_name)

        data = pd.read_excel(file_path + file_name, skiprows = 3)

        fund_requests["N_ENTRIES"] = data.shape[0]

        # data['DATE_REQUEST'] = timestamp
        data['ISIN_FUND'] = isin

        data.rename(
            columns = {
                "Name" : "NAME",
                "Weighting": "WEIGHT",
                "Industry Classification": "SECTOR",
                "Exchange": "EXCHANGE",
                "Country": "COUNTRY",
                "Currency": "CCY"
            }, inplace = True
        )

        # Select only relevant columns
        data = data[['ISIN_FUND', 'ISIN', 'NAME', 'WEIGHT', 'SECTOR', 'EXCHANGE', 'COUNTRY', 'CCY']]

        data['WEIGHT'] = pd.to_numeric(data['WEIGHT'])

    # HSBC ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    elif fund_company == "HSBC":

        fund_requests["FILE_EXISTS"] = os.path.exists(file_path + file_name)

        data = pd.read_excel(file_path + file_name, skiprows = 10)

        fund_requests["N_ENTRIES"] = data.shape[0]

        # data['DATE_REQUEST'] = timestamp
        data['ISIN_FUND'] = isin

        data.rename(
            columns = {
                "SecurityName" : "NAME",
                "Weighting": "WEIGHT",
                "Country": "COUNTRY",
                "LocalCurrencyCode": "CCY"
            }, inplace = True
        )

        # Select only relevant columns
        data = data[['ISIN_FUND', 'ISIN', 'NAME', 'WEIGHT', 'COUNTRY', 'CCY']]

        data['WEIGHT'] = pd.to_numeric(data['WEIGHT'])

    # HSBC ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    elif fund_company == "Lyxor":

        fund_requests["FILE_EXISTS"] = os.path.exists(file_path + file_name)

        try:
            data = pd.read_excel(file_path + file_name, skiprows = 13)
        except ValueError:
            print("Error - problem with .xls file")

        fund_requests["N_ENTRIES"] = data.shape[0]

        # data['DATE_REQUEST'] = timestamp
        data['ISIN_FUND'] = isin

        data.rename(
            columns = {
                "Instrument Name" : "NAME",
                "% weight": "WEIGHT",
                "Sector": "SECTOR",
                "Country": "COUNTRY",
                "Spot underlying": "PRICE",
                "Underlying Currency": "CCY"
            }, inplace = True
        )

        # Select only relevant columns
        data = data[['ISIN_FUND', 'ISIN', 'NAME', 'WEIGHT', 'SECTOR', 'COUNTRY', 'CCY']] # PL 21.04.2021 -> removed , 'PRICE'

        data['WEIGHT'] = pd.to_numeric(data['WEIGHT'])

    # L&G ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    elif fund_company == "L&G":

        fund_requests["FILE_EXISTS"] = os.path.exists(file_path + file_name)

        try:
            data = pd.read_csv(file_path + file_name, skiprows = 16)
        except ValueError:
            print("Error with file")

        fund_requests["N_ENTRIES"] = data.shape[0]

        # data['DATE_REQUEST'] = timestamp
        data['ISIN_FUND'] = isin

        data.rename(
            columns = {
                "COMPONENTS" : "NAME",
                "Weight": "WEIGHT"
            }, inplace = True
        )

        # Select only relevant columns
        data = data[['ISIN_FUND', 'ISIN', 'NAME', 'WEIGHT']]

        data['WEIGHT'] = pd.to_numeric(data['WEIGHT'])


    # Undefined ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    else:
        print("Fund company undefined: " + fund_company)

    # Return ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    return([fund_requests, data])


def cleanData_ETF(data):
    """Preprocesses the security data by removing empty rows,
    0-weight securities and aggregates information by ISIN.
    
    :param data: Harmonized dataframe as returned from readData_ETF
    
    """

    # Check for columns with reference index 
    cols_ref = ['DATE_REQUEST', 'ISIN_FUND', 'ISIN', 'TICKER', 'NAME', 'WEIGHT', 'PRICE', 'SECTOR', 'EXCHANGE', 'COUNTRY', 'CCY']

    # Error when there are columns that are not in the list
    if any(~data.columns.isin(cols_ref)):
        print(f"Error: not recognized column(s) {data.columns[~data.columns.isin(cols_ref)].to_list()}")

    # Check if all key columns are available
    cols_ref_required = ['DATE_REQUEST', 'ISIN_FUND', 'ISIN', 'WEIGHT']

    if ~len(set(cols_ref_required).intersection(data.columns.to_list())) == len(cols_ref_required):
        raise Exception("Error: not all necessary columns available")
        sys.exit("Error")

    # Drop rows without Weight
    data = data.dropna(subset = ['WEIGHT'])

    # Aggregate information by ISIN_Fund, ISIN -> sum(weight)

    # Aggregate columns depending on which ones exist in data!!!
    cols_agg = ['TICKER', 'NAME', 'SECTOR', 'EXCHANGE', 'COUNTRY', 'CCY']
    dict_agg = {'WEIGHT': 'sum'}
    dict_agg.update({v:'first' for v in cols_agg if v in data.columns})

    data = (
        data
            .groupby(['ISIN_FUND', 'ISIN'])
            .agg(dict_agg)
    )

    data.reset_index(inplace = True)

    # Replace TICKER / NAME / SECTOR -> NA WHERE ISIN == "-"
    if any(x == "-" for x in data.ISIN):
        data.loc[data['ISIN'] == "-", ['TICKER', 'NAME', 'SECTOR', 'COUNTRY', 'CCY']] = np.NaN

    return data