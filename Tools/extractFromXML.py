#----------------------------------------------------------------------------------
#
# Read XML files, extract and prepare as dataframe
#
#----------------------------------------------------------------------------------

# Fix corrupt excel files

# IT has BOM characters before the XML delcaration
# https://stackoverflow.com/questions/65895726/whats-wrong-with-ishares-sp-500-etfs-excel-file
# Replace & with &amp;

# And, maybe first decode the file
# https://stackoverflow.com/questions/18664712/split-function-add-xef-xbb-xbf-n-to-my-list
#, From UTF-8-sign to Unicode ?

# Explanation of b'\xef\xbb\xbf\xef\xbb\xbf<?'
# https://stackoverflow.com/questions/50130605/python-2-7-csv-file-read-write-xef-xbb-xbf-code


# Encode file in utf-8 from utf-8-BOM
# https://stackoverflow.com/questions/8898294/convert-utf-8-with-bom-to-utf-8-with-no-bom-in-python

# XML to Excel
# https://stackoverflow.com/questions/64431307/converting-multisheet-xml-to-excel-with-python

# XML to pandas
# https://github.com/kanishkan91/ConvertXMLtoDataframepy/blob/master/ConvertXMLtoDataframe.py

# Parse XML
# https://stackoverflow.com/questions/61548942/attempting-to-parse-an-xls-xml-file-using-python

# !!! MY EXACT PROBLEM !!!  # 
# https://stackoverflow.com/questions/42181901/xml-to-xlsx-in-python


# https://stackoverflow.com/questions/36387312/how-to-read-excel-xml-file-in-python
# Thanks for the solution! Not OP but this is the route I took to read an xml with an xls name, then rebuilt it with openpyxl into xlsx. 

# https://docs.python.org/2/library/xml.etree.elementtree.html

# Guide for XML navigation
# https://realpython.com/python-xml-parser/

import pandas as pd
import xml.etree.ElementTree as ET

# Reference: https://stackoverflow.com/questions/54107550/reading-a-spreadsheet-like-xml-with-elementtree
def extractFromXML_read_file(filename, ws_config, ns):

    '''
    Expects an XML file with worksheets and tables.
    Which is the case, when fund providers deliver data from excel in native xml format.

    :param filename: name of the xlm file disguised in other fileformat
    :param ws_config: configuration of the worksheets: dictionary with {name: {row_start: x}}
    :param ns: namespace of xml

    '''

    # parser = ET.XMLParser(encoding = "utf-8")

    tree = ET.parse(filename) #, parser = parser)
    root = tree.getroot()

    # Create empty dict to collect extracted tables
    dict_tables = {}

    for ws_i, ws in enumerate(root.findall('ss:Worksheet', ns)):
        ws_name = [i for i in ws.attrib.values()][0]
        
        if ws_name in ws_config.keys():
            # print(ws_name)

            # Create empty dictionary to store information
            temp = []

            # Extract config from ws_config
            row_start = ws_config[ws_name]['row_start']

            for table_i, table in enumerate(ws.findall('ss:Table', ns)):

                for row_i, row in enumerate(table.findall('ss:Row', ns)):
                    if row_i >= row_start:
                        # Create new row
                        temp.append([])

                        for c_i, c in enumerate(row.findall('ss:Cell', ns)):
                            # Enter values in current row
                            data = c.find('ss:Data', ns)
                            temp[row_i - row_start].append(data.text)
            
            # Add collected data to dict
            dict_tables[ws_name] = temp

    return dict_tables


def extractFromXML_trans_df(data):

    # Extract column names
    colnames = data[0]

    return pd.DataFrame(data[1:], columns = colnames)

