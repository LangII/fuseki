


####################################################################################################
"""
*** DaysToDeliverReport_DL.py ***
Query data from '____', modify, and save to csv.  This script works in conjunction with
'UPSTracking_DL.py' and 'USPSTracking_DL.py'.  Which are used to fill '____' with shipment
arrival data.
The current primary modification is to add an additional column to the saved table.  This new column
takes the values from '____' and '____', to derive the total number of days
each shipment took to deliver.

by David Lang
last modified 2019-08-05
"""
####################################################################################################



                                                     ###############################################
                                                     #####   \/   IMPORTS & SYSTEM MODS   \/   #####
                                                     ###############################################



import sys
sys.path.insert(0, '/____/____')

import pandas
from collections import OrderedDict
from datetime import datetime, timedelta
from ____ import ____



                                                       #############################################
                                                       #####   \/   GLOBALS & CONSTANTS   \/   #####
                                                       #############################################



# DEBUG ...  Give value to 'TEST_SIZE' to limit number of entries processed by given value.  If
# value is 0 then no testing is performed.
TEST_SIZE = 0

# GLOBALS ...  For runtime output and MySQL query.
start = datetime.now()
today = start.strftime('%Y-%m-%d')
# Assign value to 'designated_days_ago' to say how many days into the past the query will pull from.
designated_days_ago = 10
designated_date = (start - timedelta(days=designated_days_ago)).strftime('%Y-%m-%d')

# CONSTANTS ...  'get____Data()'
COMPANY_ID = '____'
SHIP_METHOD = 'UPS MI Dom'
START_DATE = designated_date
END_DATE = today
DELIVERED_MESSAGES = ('Package delivered by post office', 'Delivered',)

# CONSTANTS ...  'get____Data()', 'convertToPandas()', and 'saveToCsv()'
TBLPACKAGESHIPMENT_HEADERS = (
    '____', '____', '____', '____', '____', '____', '____',
    '____', '____', '____', '____'
)
TBLARRIVAL_HEADERS = ('____', '____',)
ADD_ON_HEADERS = ('DaysToDeliver',)
HEADERS = TBLPACKAGESHIPMENT_HEADERS + TBLARRIVAL_HEADERS + ADD_ON_HEADERS
CSV_FILE_NAME = 'DaysToDeliver.csv'

# GLOBALS ...  MySQL connection objects.
conn = ____.connect()
cur = conn.cursor()



                                                                 ###################################
                                                                 #####   \/   FUNCTIONS   \/   #####
                                                                 ###################################



def get____Data():
    """
    output: Tuple of tuples containing values from designated columns in
            'TBLPACKAGESHIPMENT_HEADERS' and 'TBLARRIVAL_HEADERS'.
    """
    sql = """
        SELECT {} FROM ____ AS p LEFT JOIN ____ AS a
            ON p.____ = a.____
        WHERE p.____ = 'completed'
            AND p.CompanyID = %s
            AND p.____ = %s
            AND p.____ > %s
            AND p.____ < %s
            AND a.____ IN{}
        {}
    """

    # Build column selection for query, from designated headers in CONSTANTS.
    p_columns = ''.join('p.{}, '.format(i) for i in TBLPACKAGESHIPMENT_HEADERS)
    a_columns = ''.join('a.{}, '.format(i) for i in TBLARRIVAL_HEADERS)
    select_columns = p_columns + a_columns[:-2]

    # if/else used for testing, by inserting 'LIMIT' keyword into sql or not.
    if not TEST_SIZE:  sql = sql.format(select_columns, DELIVERED_MESSAGES, '')
    else:  sql = sql.format(select_columns, DELIVERED_MESSAGES, 'LIMIT {}'.format(TEST_SIZE))

    insert = [COMPANY_ID, SHIP_METHOD, START_DATE, END_DATE]
    cur.execute(sql, insert)
    _____data_ = cur.fetchall()

    return _____data_



def modifyData(______data):
    """
    input:  ______data =    The original queried data from 'get____Data()' as a tuple of tuples
                            needing to be modified.
    output: A modified version of '______data'.  A list of lists (table).  The modification made
            to the table is an additional column is added.  The value assigned to the column is the
            number of days to deliver that package.  This is derived by subtracting '____'
            from '____'.
            An additional minor modification to the table is adding a "'" to the '____'
            value.  This is for easy reading in excel.
    """
    # Retrieval of header index through soft coded GLOBALS.  This way headers can be rearranged
    # without affecting compiling.
    mts_index = HEADERS.index('____')
    cd_index = HEADERS.index('____')
    tn_index = HEADERS.index('____')

    # Have to convert from tuple-of-tuples to list-of-lists for mutations.
    modified_ = []
    for shipment in ______data:
        mod_shipment = list(shipment)
        # Add the values of '____' and '____' then append to list.
        mod_shipment.append((shipment[mts_index] - shipment[cd_index]).days)
        # Perform minor modification to '____' value.
        mod_shipment[tn_index] = shipment[tn_index] + "'"

        modified_.append(mod_shipment)

    return modified_



def convertToPandas(_modified):
    """
    input:  _modified = List of lists from 'modifyData()'.  Table's values all collected to be
                        converted to DataFrame then saved to csv.
    output: Pandas DataFrame object to be saved to csv.
    """
    # Use a zipped OrderedDict to maintain header order.
    dict_table = [ OrderedDict(zip(HEADERS, row)) for row in _modified ]
    converted_ = pandas.DataFrame(dict_table)

    return converted_



def modifyFrame(_converted):
    """
    input:  _converted = Table of completed values and has been converted to DataFrame.
    output: Pandas DataFrame with some additional modification.  Modifications include order of
            headers and renaming of headers.
    """
    reordered_ = pandas.DataFrame()

    # Reordering of headers.  Headers can be literally cut/pasted into desired order for save file.
    reordered_['____'] = _converted['____']
    reordered_['DaysToDeliver'] = _converted['DaysToDeliver']
    reordered_['____'] = _converted['____']
    reordered_['____'] = _converted['____']
    reordered_['____'] = _converted['____']
    reordered_['____'] = _converted['____']
    reordered_['____'] = _converted['____']
    reordered_['____'] = _converted['____']
    reordered_['____'] = _converted['____']
    reordered_['____'] = _converted['____']
    reordered_['____'] = _converted['____']
    reordered_['____'] = _converted['____']
    reordered_['____'] = _converted['____']
    reordered_['____'] = _converted['____']

    # Renaming of headers.  Additional keys and values can be added to 'columns' dict.  Keys are
    # old header name, values are new header name.
    reordered_ = reordered_.rename(columns={
        '____': 'DeliveredDate',
    })

    return reordered_



def saveToCsv(_final):
    """
    Take '_final' input and save it at 'CSV_FILE_NAME' location.
    """
    _final.to_csv(CSV_FILE_NAME, index=False)



                                                                      ##############################
                                                                      #####   \/   MAIN   \/   #####
                                                                      ##############################



print("\n>>> getting data from ____ table...\n")
data = get____Data()
print(">>> shipments found =", len(data), "\n")

print(">>> modifying data...\n")
modified = modifyData(data)

print(">>> converting to pandas frame...\n")
converted = convertToPandas(modified)

print(">>> modifying frame...\n")
final = modifyFrame(converted)

print(">>> saving to csv...\n")
saveToCsv(final)

end = datetime.now()
print(">>> done ... runtime =", end - start, "\n")
