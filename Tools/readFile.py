#----------------------------------------------------------------------------------
#
# Read selected sheets form control file and return in required data format
#
#----------------------------------------------------------------------------------

# PL 07.11.2021 -> Extended by all possible read_* functions

import pandas as pd

def readControl(file, sheets):
    """Reads the specified list of sheets from the control file
    and returns in the required data format
    
    :param file: Path to the control file
    :param sheets: List of sheets to read and combine
    
    """

    # Read Excel file
    control_xlsx = pd.ExcelFile(file)
    control = pd.read_excel(control_xlsx, sheet_name = sheets, skiprows = 1)
    control_xlsx.close()

    # Define list of columns to keep from control
    control_cols = ['ISIN', 'Name', 'URL_Positions']

    # Subset control data to selected columns
    control = {k:v[control_cols] for k, v in control.items()}

    # Combine into single dataframe with dict keys as column names
    control = pd.concat(control, keys = control.keys(), names = ["fund_company", "row_index"])
    control.reset_index(inplace = True)
    control.drop(columns = ['row_index'], inplace = True)

    # Remove empty rows
    control.dropna(subset = ['URL_Positions'], inplace = True)
    control.reset_index(inplace = True, drop = True)

    return control


def readPortfolio(file):
    """Reads and transform the raw portfolio file.
    
    :param file: Path to the portfolio file
    
    """

    pf = pd.read_csv(file, sep = "\t")

    # Select relevant columns
    pf = pf.iloc[:,0:6]

    # Drop empty rows
    pf.dropna(subset = ["ISIN"], inplace = True)

    # Transform numeric columns to numeric type
    cols = ['N', 'Cost']
    pf['Cost'] = pf['Cost'].str.replace(",", "")
    pf['N'] = pf['N'].str.replace(",", "")
    pf[cols] = pf[cols].apply(pd.to_numeric, errors = 'coerce', axis = 1)

    # Drop not-invested securities, i.e. Cost & N = 0 
    # -> There can always be 0 costs securities, i.e. stock options
    pf = pf[pf.N != 0]

    return pf


def readMapping(file, sep, cols):
    """Reads and transform the raw mapping file.
    Uses specfieid separator (could be tsv or csv with ; or ,)
    And keeps relevant columns
    
    :param file: Path to the portfolio file
    :param sep: separator of the file
    :param cols: columns to keep
    
    """

    # Read file
    data = pd.read_csv(file, sep = sep)

    # Keep relevant columns
    data = data[cols]

    return data














