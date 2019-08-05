


####################################################################################################
"""
*** USPSTracking_DL.py ***
Takes order data from ____ table ____, to use to call UPS API to obtain tracking
data.  The tracking data is filtered to get the latest tracking message and message time stamp.
Then the order data from ____ and the filtered data from the UPS API is used to
update the ____ table ____.  The primary purpose is to obtain package delivery time stamps.
With this, statistics regarding days-to-deliver can be tracked.

by David Lang
last modified 2019-07-26
"""
####################################################################################################



                                                     ###############################################
                                                     #####   \/   IMPORTS & SYSTEM MODS   \/   #####
                                                     ###############################################



import sys
sys.path.insert(0, '/____/____')

import time
import requests
import xmltodict
import json
from datetime import datetime
from ____ import ____



                                                       #############################################
                                                       #####   \/   GLOBALS & CONSTANTS   \/   #####
                                                       #############################################



# DEBUG ...  Give value to 'TEST_SIZE' to limit number of entries processed by given value.  If
# value is 0 then no testing is performed.
TEST_SIZE = 0

# GLOBALS ...  For runtime output and sql queries.
start = datetime.now()
today = start.strftime('%Y-%m-%d')

# CONSTANTS...
COMPANY_ID = '____'

# CONSTANTS ...  'get____Data()'
SHIP_METHOD = 'USPS Priority (Endicia)'
START_DATE = '2019-04-01'
END_DATE = '2019-05-10'
DELIVERED_MESSAGES = (
    'We attempted to deliver',
    'Your item has been delivered',
    'Your item was delivered',
    'Your item was picked up',
    'Your item was refused',
    'Your item was returned',
)

# CONSTANTS ...  'getUspsData()'
USER_ID = '____'
REQUEST_DELAY = 0.4

# GLOBALS ...  MySQL connection objects.
conn = ____.connect()
cur = conn.cursor()



                                                                 ###################################
                                                                 #####   \/   FUNCTIONS   \/   #####
                                                                 ###################################



def get____Data():
    """
    output: Tuple of tuples containing '____' and '____' of orders designated
            by CONSTANTS.
    """
    sql = """
        SELECT p.____, p.____ FROM ____ AS p
        LEFT JOIN ____ AS a ON p.____ = a.____
            WHERE p.____ = %s
            AND p.____ = 'completed'
            AND p.____ = %s
            AND p.____ > %s
            AND p.____ < %s
            AND (a.____ NOT REGEXP %s OR a.____ IS NULL)
        {}
    """
    # if/else used for testing, by inserting 'LIMIT' keyword into sql or not.
    if not TEST_SIZE:  sql = sql.format('')
    else:  sql = sql.format('LIMIT {}'.format(TEST_SIZE))

    # MySQL 'REGEXP' has unique syntax.  Last value of 'insert' uses 'join' to put
    # 'DELIVERED_MESSAGES' into syntax readable by 'REGEXP'.
    insert = [COMPANY_ID, SHIP_METHOD, START_DATE, END_DATE, '|'.join(DELIVERED_MESSAGES)]

    cur.execute(sql, insert)
    _____data_ = cur.fetchall()

    return _____data_



def process____Data(______data):
    """
    input:  ______data = Tuple of orders containing '____' and '____'.
    output: List of dicts.  The dicts are reformatted orders from 'get____Data()'.
    """
    processing_ = []
    for (x, y) in ______data:
        order = {'package_shipment_id': str(x), 'company_id': COMPANY_ID, 'tracking_number': y}
        processing_.append(order)

    return processing_



def getUspsData(_tracking_number):
    """
    input:
    output:
    """
    url = 'http://production.shippingapis.com/ShippingAPI.dll'
    xml = """
        <? xml version="1.0" encoding="UTF-8" ?>
        <TrackRequest USERID="{}">
            <TrackID ID="{}"></TrackID>
        </TrackRequest>
    """.format(USER_ID, _tracking_number)
    parameters = {'API': 'TrackV2', 'XML': xml}

    # Attempt to connect with USPS API.  With a 3 second timeout window, if 5 attempts are made
    # resulting in timeouts or connection errors then the program exits.
    attempts = 0
    time.sleep(REQUEST_DELAY)
    try:
        response = requests.get(url, params=parameters, timeout=3).text
    except (requests.exceptions.ConnectTimeout, ConnectionError):
        attempts += 1
        if attempts == 5:
            exit(">>> too many timeouts, something's wrong, exiting program...")
        print("\n>>> connection error, trying again...\n")
        time.sleep(3)
        response = requests.get(url, params=parameters, timeout=3).text

    # Turn USPS API 'response' into json object.
    usps_data_ = json.loads(json.dumps(xmltodict.parse(response)))

    return usps_data_



def removeNonAscii(string, replace=''):
    """
    input:  string = String to have non-ascii characters removed.
            replace = String to replace into 'string' where non-ascii characters are found.
    output: String of 'string', where non-ascii characters have been replaced by 'replace'.
    """
    if not all(ord(char) < 128 for char in string):
        return ''.join( char if ord(char) < 128 else replace for char in string )
    else:
        return string



def getTimeStamp(_message):
    """
    input:  _message = String from USPS API describing most recent shipment activity.
    output: Return datetime object derived from a variety of slice formats from the input
            '_message'.  The USPS API outputs many various formats for their shipment activity
            messages.  This function sorts through '_message' to find the correct values for the
            datetime object.
    """
    # The primary time stamp indicator from '_message' is the ':' from hours/minutes.  So, we first
    # find the colon as a starting point.
    colon = _message.find(':')

    # Sometimes '_message' does not have time.  If so, return empty string.
    if colon == -1:  return ''

    # Get number of digit places for hours.
    if _message[colon - 2] == '1':  hour_digits = 2
    else:  hour_digits = 1

    # Some '_message' formats have time-before-date, some have date-before-time.  These if/else
    # conditions test for which of these formats '_message' is in.
    if _message[colon:].find('on') == 7:
        # If time-before-date, then we quickly get 'time_string' with 'begin' value starting
        # immediately before hours, and 'end' value is found 6 digits beyond ','.
        begin = colon - hour_digits
        end = colon + _message[colon:].find(',') + 6
        time_string = _message[begin:end]

        return datetime.strptime(time_string, '%I:%M %p on %B %d, %Y')

    else:
        # There are 2 possible variations to the date-before-time format.  One is preceded with 'of'
        # and parsed with ',', the other is preceded with 'on' and parsed with 'at'.  These if/else
        # conditions determine which this '_message' is using 'cut_point' as reference.  Then
        # assigns values to 'cut_more' and 'look_for' for use in slicing out 'time_string'
        # from '_message'.
        cut_point = colon - hour_digits - 2
        if _message[cut_point] == ',':
            cut_more = 0
            look_for = 'of'
        else:
            cut_more = 2
            look_for = 'on'
        time_string = _message[:cut_point - cut_more] + _message[cut_point + 1:]
        # Have to reset value of 'colon' due to setup slicing.
        colon = time_string.find(':')
        begin = time_string[:colon].rfind(look_for) + 3
        end = colon + 6
        time_string = time_string[begin:end]

        return datetime.strptime(time_string, '%B %d, %Y %I:%M %p')



def loopThroughData(_data):
    """
    input:  _data = List of dicts from ____ data.  To be used to pull further data from USPS API.
    output: List of dicts of shipment data to be updated to ____ table.
    """
    to_update_ = []
    for index, shipment in enumerate(_data):

        print(">>> Processing", index + 1, "of", len(_data), "...")
        print(">>> ____ =", shipment['package_shipment_id'])
        print(">>> ____ =", shipment['tracking_number'])

        usps_data = getUspsData(shipment['tracking_number'])

        # try/except to catch bad USPS responses (typically error replies).
        try:
            message = usps_data['TrackResponse']['TrackInfo']['TrackSummary']
        except KeyError:
            print(">>> bad response, ignoring shipment...\n")
            continue

        # try/except uses 'removeNonAscii()' to clean string 'message'.
        try:
            print(">>> looking for non-ascii...", message)
        except UnicodeEncodeError:
            message = removeNonAscii(message)
            print(">>> non-ascii found...", message)

        print(">>> filtering time stamp from message...")
        time_stamp = getTimeStamp(message)

        print(">>> formatting data for table update...\n")
        shipment['message'] = message
        shipment['message_time_stamp'] = time_stamp
        shipment['last_checked'] = start
        to_update_.append(shipment)

    return to_update_



def update____Data(_to_update):
    """
    input:  _to_update = List of dicts containing orders to be updated to ____ table.
    output: (no return) Updates ____ table '____' with orders from '_to_update'.
    """
    sql = """
        INSERT INTO ____ (
            ____, ____, ____, ____, ____
        ) VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            ____ = VALUES(____),
            ____ = NOW(),
            ____ = VALUES(____)
    """
    # Build 'inserts' with list of order data from '_to_update' to be each inserted into 'sql' for
    # ____ table updating.
    inserts = []
    for order in _to_update:
        insert = (
            order['package_shipment_id'],
            order['tracking_number'],
            order['message_time_stamp'],
            order['message'],
            order['company_id']
        )
        inserts.append(insert)

    cur.executemany(sql, inserts)



                                                                      ##############################
                                                                      #####   \/   MAIN   \/   #####
                                                                      ##############################



print("\n>>> getting data from ____ to be processed...\n")
_____data = get____Data()

print(">>> orders to be processed =", len(_____data), "\n")

print(">>> processing data from ____...\n")
processing = process____Data(_____data)

print(">>> looping through processed ____ data...\n")
to_update = loopThroughData(processing)

if TEST_SIZE:
    for shipment in to_update:  print(shipment)
    exit(">>> EXITING TEST")

print(">>> updating ____ table...\n")
update____Data(to_update)

end = datetime.now()
print(">>> done ... runtime =", end - start)
