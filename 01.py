
########################################################################################################################
"""

 *** __________Report.py ***

 -  Updates database for ___ __________ tracking information based on tracking number requests from the ___ _______.
    Then creates csv file of tracking information for updated ___ tracking numbers, saves file to hard drive and emails
    it to _____, ________, and ________.  This process is done on all tracking numbers with this month's time stamp and
    then again for all tracking numbers with last month's time stamp.

 -  last updated = 2019-05-15 by DL

"""
########################################################################################################################





import sys
sys.path.insert(0, '/______/______')

import requests
import json
import time
import operator
import pandas

from datetime import datetime

from Required import Connections, Mail





testing = False # <-- leave alone
################                                                                                                        ---  \/  INPUT DEBUG  \/
# testing = True # <-- COMMENT-IN to put script into DEBUG mode ... (limit process iterations and no database updates)
################                                                                                                        ---  /\  INPUT DEBUG  /\

# Script currently only developed for this company.
company_id = '___'
# Unique identifiers per company.
______________________ = '________'
client_id = '_____'

# Make containers global for overall tracking of what has been processed accurately, and what can not be processed.
_____data, exceptions = {}, {}

conn = Connections.connect()
cur = conn.cursor()





def getTimeStamp(month='this_month'):
    """
    input:  month = Determines whether the output will be a time stamp of the current month or the previous month.
                    Default is this month.  Only other acceptable argument is 'last_month', else function will
                    return "unacceptable argument".
    output: Return 'stamp', a time stamp in str format of year and month only ('YYYY-MM').
    """
    now = datetime.now()
    y, m = now.strftime('%Y'), now.strftime('%m')

    # Gives default return of time stamp for this month.
    if month == 'this_month':  stamp = y + '-' + m

    # Gives alternative return of time stamp for last month.
    elif month == 'last_month':
        # Check to see if current month is January.  If not January, subtract formatted month by 1.  If month is
        # January, subtract formatted year by 1 and auto set month to December.
        if m != '01':  stamp = y + '-' + "{:0>2d}".format(int(m) - 1)
        else:  stamp = str(int(y) - 1) + '-12'

    # Error statement, incase unacceptable parameter is accidentally keyed.
    else:  return "error:  unacceptable parameter '" + month + "', will accept 'this_month' or 'last_month'"

    return stamp





def get____Data(time_stamp):
    """
    input:  time_stamp = Str of given time stamp in ('YYYY-MM') format.
    output: Return '_____data' as queried data from '___________________' as dict.  Dict keys are strings of
            '______________' and values are strings of '_________________'.
    """
    sql =   """
            SELECT p._________________, p.______________ FROM ___________________ AS p

            LEFT JOIN __________ AS a ON p._________________ = a._________________

        	  WHERE   p._________ = {} AND
                    p.______ = 'completed' AND
                    p.______________ LIKE '%{}%' AND
                    p._____________ LIKE '___%' AND
                    (a._______ NOT LIKE '%DELIVERED%' OR a._______ IS NULL)
            """
    # Format sql with input variables then query database.
    sql = sql.format(company_id, time_stamp)
    cur.execute(sql)
    raw_data = cur.fetchall()

    # Convert data type from list-of-tuples to dict, and make sure keys and values are in string format.
    _____data = { y: str(x) for (x, y) in raw_data }

    ##################################### ... primary debug variable ... #######################                        ---  \/  INPUT DEBUG  \/
    if testing:
        number_of_entries_to_process = 100 # <-- adjust to change amount of all data to process
        temp = {}
        for index, each in enumerate(_____data):
            temp[each] = _____data[each]
            if index > number_of_entries_to_process:  break
        _____data = temp
    ############################################################################################                        ---  /\  INPUT DEBUG  /\

    # Compiler notes.
    print("  >>>  ____ database accessed ... retrieved", len(_____data), "items\n")

    return _____data





def getKey():
    """
    Return 'key', a string that is the current live access key.
    """

    # Build variables for get request.
    headers = {'Content-Type': 'application/json'}
    params = {'username': '_____________.track', 'password': ______________________}
    getKey_url = 'https://___._____________.com/__/____/____________/'

    # Perform request, filtr, and return live key.
    get = requests.get(getKey_url, headers=headers, params=params, timeout=5).json()
    key = get['data']['access_token']

    return key





def get___Data(batch):
    """
    input:  batch =     Dictionary of entries from '_____data' dictionary.  ___ website only allows 10 entries to be
                        submitted at a time.
    output: Return '____data' as list of dictionaries compiled from ___ website.  Data collected from website includes
            'time_stamp', '_______', 'tracking_num', and 'shipping_id'.  Each dictionary entry (of collected data)
            corresponds to each entry from the 'batch' parameter.
    """
    # Initiate while-loop conditional variable.
    get___Data_attempts = 0
    # while-loop used to track and limit number of attempts made to connect to ___ website.
    while get___Data_attempts < 10:
        # Bool variable used to determine direction after while loop (whether connection was success or not).
        success = False

        try:
            # Get 'tracking' keys from parameter dict 'batch'.
            tracking = list(dict.keys(batch))
            # Build output parameters for retrieving ___ data.
            parameters = {'access_token': getKey(), 'client_id': client_id, 'number': tracking}
            get___Data_url = 'https://___._____________.com/__/_________/_____'
            # Request data from ___ website.
            response = requests.get(get___Data_url, params=parameters, timeout=5)
            # Another embedded error catch.  Most common '_______code' returned is 400:  Bad client request.
            if response._______code != 200:
                print("  >>>  exception caught ... _______code 400 (bad client request) ...")
                return ('exception', '_______code 400 (bad client request)')

            # If connection is successful break from loop and proceed with script.
            success = True
            break

        # Series of known reoccuring exceptions.
        except requests.exceptions.ConnectionError:
            print("  >>>  ___ connection error (requests.exceptions.ConnectionError) ...")
        except requests.exceptions.ReadTimeout:
            print("  >>>  ___ connection error (requests.exceptions.ReadTimeout) ...")
        except requests.exceptions.RequestsException:
            print("  >>>  ___ connection error (requests.exceptions.RequestsException) ...")
        # A catch all in case there are any more irregular exceptions (no others have yet to be seen).
        except:
            print("  >>>  ___ connection error (other) ...")

        # Some final procedures before looping again.  Along with an update to 'get___Data_attempts'.
        get___Data_attempts += 1
        print("  >>>  attempting to reconnect ...\n")
        time.sleep(3)

    # Bad connection catch.  If there are repeated ___ connection errors, then the while-loop breaks without triggering
    # 'success = True', thereby getting caught here and exiting program.
    if not success:  print("  >>>  having repeated issues with ___ connection ... try again later  :("), exit()

    # Convert 'response' to readable dict through json.  Sometimes ___ returns from request an "Expecting ',' delimiter"
    # error.  If exception is caught return "exception" to collect 'tracking-nums' from bad batch.
    try:  response = response.json()
    except json.decoder.JSONDecodeError:
        print("  >>>  exception caught ... json.decoder.JSONDecodeError ...")
        return ('exception', 'json.decoder.JSONDecodeError')

    # Start the data filter, only need contents from '_________'.
    raw_data = response['data']['_________']

    # Initiate return variable '___-data' as list of dictionaries of filtered data.  Then start for-loop to handle each
    # entry of 'raw_data' at a time.
    ____data = []
    for each in raw_data:
        # Initiate data collection variable to be appended to return variable '____data'.
        each_____data = {}

        # ...  Note:  Refering to "['events'][0]" when declaring 'time_stamp' and '_______' because the ['events'] list
        # is in chronological order and we always want the data from the most recent event.

        # Get 'time_stamp' from 'date' and 'time' entries in 'raw_data'.
        each_____data['time_stamp'] = each['events'][0]['date'] + ' ' + each['events'][0]['time']

        # Get '_______' from 'raw_data'.
        each_____data['_______'] = each['events'][0]['description']

        # Get 'tracking_num' from 'raw_data'.  Have to test 3 different locations for ___'s storage location of
        # 'tracking_number'.
        if str(each['mail']['_________']) in tracking:
            each_____data['tracking_num'] = str(each['mail']['_________'])
        elif str(each['mail']['__________________________']) in tracking:
            each_____data['tracking_num'] = str(each['mail']['__________________________'])
        elif str(each['mail']['____________________']) in tracking:
            each_____data['tracking_num'] = str(each['mail']['____________________'])

        # Get 'shipping_id' from 'raw_data'.
        each_____data['shipping_id'] = batch[each_____data['tracking_num']]

        ____data.append(each_____data)

    return ____data





def updateDatabase(____data, single=False):
    """
    input:  ____data =  List of dicts as batch data pulled from ___ website.  Batch is part of the '_____data' whole to
                        be processed.
            single =    Boolean determining style of compiler notes.
    output: Update ____ database table '__________' with data from parameter '____data'.
    """

    # ... Building sql query.

    # Initial query determining what columns to update on what table and what to do with duplicate entries.
    sql =   """
            INSERT INTO __________ (_________________, ______________, _______Timestamp, _______, _________)
                VALUES {}

            ON DUPLICATE KEY UPDATE
                _______ = VALUES(_______),
                LastChecked = NOW(),
                _______Timestamp = VALUES(_______Timestamp)
            """
    # Initiate variable used to build values to be inserted into table.
    insert = ""

    # Compiling notes.
    note_list = []
    for index, each in enumerate(____data):
        # Pack data from each '____data' entry to be formatted into the 'insert' string.
        a, b, c, d, e = each['shipping_id'], each['tracking_num'], each['time_stamp'], each['_______'], company_id
        # Compiling notes.
        note_list.append(b.ljust(22))
        # if/else statements used to insert comma at the end of each entry except for the last entry.
        if index != len(____data) - 1:  insert += "('{}', '{}', '{}', '{}', '{}'),".format(a, b, c, d, e)
        else: insert += "('{}', '{}', '{}', '{}', '{}')".format(a, b, c, d, e)

    sql = sql.format(insert)

    ##############################################################                                                      ---  \/  AUTO DEBUG  \/
    if not testing:
        cur.execute(sql)    # <-- final database update statements
        conn.commit()       # <-- final database update statements
        print("  >>>  '__________' updated for ...")
    else:  print("  >>>  <> TESTING <>")
    ##############################################################                                                      ---  /\  AUTO DEBUG  /\

    # Compiling notes.
    if not single:
        note_list += [ "" for each in range(10 - len(note_list)) ]
        print("  >>>  batch size:    ", len(____data))
        print("  >>>  each processed:", note_list[0], note_list[1])
        print("  >>>                 ", note_list[2], note_list[3])
        print("  >>>                 ", note_list[4], note_list[5])
        print("  >>>                 ", note_list[6], note_list[7])
        print("  >>>                 ", note_list[8], note_list[9], "\n")
    else:  print("  >>>  single processed:", note_list[0], "\n")





def saveToCsvAndEmail(time_stamp):
    """
    input:  time_stamp = Str of given time stamp in ('YYYY-MM') format.
    output: Save data from updated ____ database tables '___________________' and '__________' to csv.
    """
    # Compiling notes.
    print("\n  >>>  *** SAVING TO FILE ***\n\n")

    # Build 'headers' for csv.
    sql =   """
            describe ___________________
            """
    cur.execute(sql)
    headers = [ each[0] for each in cur.fetchall() ]
    headers += [ each for each in ('_______', '_______Timestamp', 'LastChecked') ]

    # Build 'raw_data' for csv.
    sql =   """
            SELECT p.*, a._______, a._______Timestamp, a.LastChecked
                FROM ___________________ AS p

            LEFT JOIN __________ AS a ON p._________________ = a._________________

            WHERE   p.______ = 'completed'
                AND p._____________ LIKE '___%'
                AND p._________________ > 1100000
                AND p._________ = {}
                AND ______________ LIKE '%{}%'
            """
    sql = sql.format(company_id, time_stamp)
    cur.execute(sql)
    raw_data = cur.fetchall()

    # Convert 'headers' and 'raw_data' into list of dicts for each entry selected from the sql query.
    processing = [ dict(zip(headers,each)) for each in raw_data ]

    #################################################################################                                   ---  \/  INPUT DEBUG  \/
    number_of_rows_to_file = 10 # <-- adjust int to change number of rows in csv file
    if testing:  processing = processing[:number_of_rows_to_file]
    #################################################################################                                   ---  /\  INPUT DEBUG  /\

    # More data conversion...  Convert all null _______s to empty strings.
    for each in processing:
        if each['_______'] == None:  each['_______'] = ''
    # Sort data by '_______'.
    processing.sort(key=operator.itemgetter('_______'))
    # More data conversion...  Use 'pandas' to convert data into csv format.
    processing = pandas.DataFrame(processing)

    # Create final data container.
    processed = pandas.DataFrame()

    # Note ... Using pandas' DataFraming, reformat data from 'processing' to 'processed' in designated column order.

    processed['_________________'] = processing['_________________']
    # Build string for hyperlink.
    processed['PackageShipmentLink'] =  "=HYPERLINK(" + "\"http://_________.____.com/__________________________.___" + \
                                        "__________________=" + processing['_________________'].apply(str) + \
                                        "&______=" + company_id +"\"" + ",\"" + \
                                        processing['_________________'].apply(str) + "\")"
    processed['_________________'] = processing['_________________']
    processed['ShipDate'] = processing['______________']
    processed['_______Timestamp'] = processing['_______Timestamp']
    processed['LastChecked'] = processing['LastChecked']
    # Pull from 2 'processing' columns to format to single 'processed' column.
    processed['Days to Ship'] = processing['______________'] - processing['_________________']
    # Pull from 2 'processing' columns to format to single 'processed' column.
    processed['Days To Deliver'] = processing['_______Timestamp'] - processing['______________']
    processed['_______'] = processing['_______']
    processed['Attention'] = processing['Attention']
    processed['ShippingNumber'] = processing['ShippingNumber']
    processed['RequestedBy'] = processing['_____________________']
    # Build string for hyperlink.
    processed['TrackLink'] =    "=HYPERLINK(\"https://________._____________.com/_______________=" + \
                                processing['______________'] + "\",\"" + processing['______________'] + "\")"
    processed['ShippingMethod'] = processing['ShippingMethod']
    processed['_____________'] = processing['_____________']
    processed['Address1'] = processing['Address1']
    processed['Address2'] = processing['Address2']
    processed['City'] = processing['City']
    processed['State'] = processing['State']
    processed['ZipCode'] = processing['ZipCode']
    processed['Country'] = processing['Country']

    file_name = '___-___-' + time_stamp + '.csv'
    ####################################################################################                                ---  \/  AUTO DEBUG  \/
    if not testing:
        processed.to_csv(file_name, index=False)
        Mail.SendFile  (['_____@____.com', '________@____.com', '________@____.com'],       # to
                        '_____@____.com',                                                   # from
                        "(automated) ... This week's report for {}.".format(time_stamp),    # _______ body
                        "(automated) ... ___ report",                                       # subject
                        '/______/______/_______________/_______/___/' + file_name,          # file
                        file_name)                                                          # name assigned to file
        print("  >>>  *** EMAILED FILE ***\n\n")
    else:  processed.to_csv('___-test.csv', index=False)
    ####################################################################################                                ---  /\  AUTO DEBUG  /\

    print("  >>>  *** SAVED TO FILE ***\n\n")





def batchProcessing(_____data):
    """
    input:  _____data = A dict containing all shipping-numbers and tracking-numbers to be processed.
    output:           - The function's primary job is to perform batch looping on '_____data', updating the database and
                        'exceptions' with each batch.
                      - The function also returns '_____data', but the dict only contains entries that still need to be
                        processed.
    """
    # Need to make 'exceptions' global to track whole data exceptions.
    global exceptions
    # Variable used to track how many attempts are made to process '_____data'.
    while_attempts = 0
    no_movement = False
    previous_to_process = 0

    # Instead of 'while True', this condition to the while-loop is meant also as a catch incase all data is processed
    # with no exceptions (happens sometimes in testing).
    while len(_____data) > 0:

        while_attempts += 1
        processed = []

        # while-loop controls ...  Tests if a loop has passed with no updates on the data.  If so, break loop to start
        # 'singleProcessing()'.
        if len(_____data) == previous_to_process:
            print("\n  >>>  *** NO MOVEMENT ON BATCH PROCESSING, STARTING SINGLE PROCESSING ***\n\n")
            break
        # Variable defined to track if while-loop processing has no progress.
        previous_to_process = len(_____data)

        # Initiate batch variables.
        pivot = size = 10
        batch = {}
        batch_num = 0
        for index, each in enumerate(_____data):
            # Loading the batch.
            batch[each] = _____data[each]
            # Batch controls ...  At indexing of 'pivot' or end of '_____data', process current batch.
            if index == pivot - 1 or index == len(_____data) - 1:
                batch_num += 1

                # Compiling notes.
                print("  >>>  still to process (per attempt):", len(_____data))
                print("  >>>  attempt#:", while_attempts, "... batch#:", batch_num, "...")

                ##################################################################
                ###   \/   FUNCTION CALLS AND DATA PROCESSING PER BATCH   \/   ###
                ##                                                              ##

                ____data = get___Data(batch)

                # Check to see if 'get___Data()' request threw an exception.  If so, collect batch tracking numbers
                # in 'exceptions' and move to next batch.  If not, continue to process data.
                if ____data[0] != "exception":
                    updateDatabase(____data)
                    # Get list of processed 'shipping_id's from '____data' to track what entries from '_____data' have
                    # been processed.
                    processed += [ d['tracking_num'] for d in ____data ]
                else:
                    print("  >>>  processing exception batch ...", ____data[1],"...\n")
                    exceptions = { b : ____data[1] for b in batch }

                ##                                                              ##
                ###   /\   FUNCTION CALLS AND DATA PROCESSING PER BATCH   /\   ###
                ##################################################################

                # Batch controls ...  Reset 'batch' and adjust 'pivot' by increment of 'size'.
                batch = {}
                pivot += size

        #######################################################################################                         ---  \/  INPUT DEBUG  \/
        #     number_of_batches_to_process = 10 # <-- adjust to limit number of batches processed
        #     if testing and index == number_of_batches_to_process * 10:  break
        # number_of_whileloop_passes = 1 # <-- adjust to limit number of while-loop iterations
        # if testing and while_attempts > number_of_whileloop_passes:  break
        #######################################################################################                         ---  /\  INPUT DEBUG  /\

        # Remove 'processed' from '_____data' to not waste time on repeats.  Catch 'KeyError's (still not sure why
        # they're occurring).
        for p in processed:
            try:  del _____data[p]
            except KeyError:
                print("  >>>  exception caught ... KeyError ...\n")
                exceptions[each] = "KeyError"

    # Return what is left in '_____data' still to be processed for 'singleProcessing()'.
    return _____data





def singleProcessing(_____data):
    """
    input:  _____data = Left over ____ data from 'batchProcessing()', now to be processed individually.
    output: Return '_____data', dict of ____ data not processing.
    """
    global exceptions
    processed = []

    for index, each in enumerate(_____data):
        single = {}
        single[each] = _____data[each]

        # Compiler notes.
        print("  >>>  processing", index + 1, "of", len(_____data), "singles ...")

        ###################################################################
        ###   \/   FUNCTION CALLS AND DATA PROCESSING PER SINGLE   \/   ###
        ##                                                               ##

        ____data = get___Data(single)

        # Check to see if 'get___Data()' request threw an exception.  If so, collect batch tracking numbers
        # in 'exceptions' and move to next batch.  If not, continue to process data.
        if ____data[0] != "exception":
            updateDatabase(____data, single=True)
            # Get list of processed 'shipping_id's from '____data' to track what entries from '_____data' have
            # been processed.
            processed.append(____data[0]['tracking_num'])
        else:
            print("  >>>  processing single exception ...", ____data[1],"...\n")
            exceptions[each] = ____data[1]

        ##                                                               ##
        ###   /\   FUNCTION CALLS AND DATA PROCESSING PER SINGLE   /\   ###
        ###################################################################

    for each in processed:
        try:  del _____data[each]
        except KeyError:
            print("  >>>  exception caught ... KeyError ...\n")
            exceptions[each] = "KeyError"

    return _____data





            ########################################
            #####   \/   FUNCTION CALLS   \/   #####
            ########################################





def functionCalls(time_stamp):
    """
    This generic function is created to perform functions of script twice:  Once under 'time_stamp' for 'this_month' and
    once under 'time_stamp' for 'last_month'.
    """
    _____data = get____Data(time_stamp)
    _____data = batchProcessing(_____data) # Note ...  See batchProcessing() for looping function calls.
    # Make sure there are entries in '_____data' to process before calling 'singleProcessing()'.
    if _____data:  _____data = singleProcessing(_____data)
    #####################                                                                                               ---  \/  INPUT DEBUG  \/
    saveToCsvAndEmail(time_stamp) # <-- COMMENT-OUT WHILE TESTING TO NOT PERFORM FILE TRANSFERS ... (makes run-time faster)
    #####################                                                                                               ---  /\  INPUT DEBUG  /\
    # Compiler notes ... Output error _______s.
    if not exceptions:  print("  >>>  *** all entries processed, no exceptions found ***\n")
    else:  print("  >>>  exceptions found:", len(exceptions), "\n")





try:
    print("")
    start_time = datetime.now() # tracking runtime

    # Perform script functions on 'this_month'.
    print("  >>>  *** STARTING FUNCTION CALLS ON THIS MONTH ***\n")
    time_stamp = getTimeStamp('this_month')
    functionCalls(time_stamp)

    ###########################################                                                                         ---  \/  AUTO DEBUG  \/
    # Perform script functions on 'last_month'.
    print("  >>>  *** STARTING FUNCTION CALLS ON LAST MONTH ***\n")
    if not testing:
        time_stamp = getTimeStamp('last_month')
        functionCalls(time_stamp)
    ###########################################                                                                         ---  /\  AUTO DEBUG  /\

    end_time = datetime.now() # tracking runtime
    print("  >>>  *** DONE ***\n\n  >>>  runtime =", end_time - start_time, "\n")

except Exception as ex:
    print("  >>>  Exception caught ...")
    print("  >>> ", ex)
    Mail.Send________   (['_____@____.com'], '_____@____.com', str(ex), "ERROR - __________Report.py")
