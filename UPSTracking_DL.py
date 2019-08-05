


####################################################################################################
"""
*** UPSTracking_DL.py ***
Takes order data from ____ table ____, to use to call UPS API to obtain tracking
data.  The tracking data is filtered to get the latest tracking message and message time stamp.
Then the order data from ____ and the filtered data from the UPS API is used to
update the ____ table ____.  The primary purpose is to obtain package delivery time stamps.
With this, statistics regarding days-to-deliver can be tracked.

by David Lang
last modified 2019-07-24
"""
####################################################################################################



                                                     ###############################################
                                                     #####   \/   IMPORTS & SYSTEM MODS   \/   #####
                                                     ###############################################



import sys
sys.path.insert(0, '/____/____')

# Patch for issues with 'urllib.request.Request'.
import ssl
try:  _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:  pass
else:  ssl._create_default_https_context = _create_unverified_https_context

import xmltodict
import json
import time
from datetime import datetime
from urllib.request import Request, urlopen
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
SHIP_METHOD = 'UPS MI'
START_DATE = '2019-04-01'
END_DATE = '2019-04-10'
DELIVERED_MESSAGES = (
    'Package delivered by post office',
    'Delivered'
)

# CONSTANTS ...  'getUpsData()'
ACCESS_LICENSE_NUMBER = '____'
USER_ID = '____'
PASSWORD = '____'
UPS_ONLINETOOLS_URL = 'https://onlinetools.ups.com/ups.app/xml/Track'
UPS_REQUEST_HEADERS = {'Content-Type': 'application/x-www-form-urlencoded'}
MAIL_INNOVATION_TAG = '<IncludeMailInnovationIndicator/>'
CALL_DELAY = 0.2

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
    a, b, c, d, e = [COMPANY_ID, SHIP_METHOD, START_DATE, END_DATE, DELIVERED_MESSAGES]
    sql = """
        SELECT p.____, p.____ FROM ____ AS p
        LEFT JOIN ____ AS a ON p.____ = a.____
            WHERE p.____ = '{}'
            AND p.____ = 'completed'
            AND p.____ LIKE '%{}%'
            AND p.____ > '{}'
            AND p.____ < '{}'
            AND (a.____ NOT IN{} OR a.____ IS NULL)
        {}
    """
    # if/else used for testing, by inserting 'LIMIT' keyword into sql or not.
    if not TEST_SIZE:  sql = sql.format(a, b, c, d, e, '')
    else:  sql = sql.format(a, b, c, d, e, 'LIMIT {}'.format(TEST_SIZE))

    cur.execute(sql)
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



def getUpsData(_tracking_number):
    """
    input:  _tracking_number = String of single tracking number.
    output: Json object containing UPS API return data.
    """
    # 'MAIL_INNOVATION_TAG' is an xml tag needed to designate that '____' is for an
    # envelope package type.
    a, b, c, d, e = [
        ACCESS_LICENSE_NUMBER, USER_ID, PASSWORD, MAIL_INNOVATION_TAG, _tracking_number
    ]
    xml = """
        <AccessRequest xml:lang="en-US">
            <AccessLicenseNumber>{}</AccessLicenseNumber>
            <UserId>{}</UserId>
            <Password>{}</Password>
        </AccessRequest>
        <?xml version="1.0"?>
        <TrackRequest xml:lang="en-US">
            <Request>
                <TransactionReference>
                    <CustomerContext>Get tracking ____</CustomerContext>
                </TransactionReference>
                <XpciVersion>1.0</XpciVersion>
                <RequestAction>Track</RequestAction>
                <RequestOption>activity</RequestOption>
            </Request>
            {}
            <____>{}</____>
        </TrackRequest>
    """
    # Take CONSTANTS and insert them into 'xml' then convert to byte type.
    xml = xml.format(a, b, c, d, e).encode('utf-8')
    time.sleep(CALL_DELAY)
    # Make 'Request' of UPS API, with formatted 'xml' and CONSTANTS parameters.
    response = Request(url=UPS_ONLINETOOLS_URL, data=xml, headers=UPS_REQUEST_HEADERS)
    ups_data_ = urlopen(response).read()
    # Convert response to 'json' format from 'xml'.
    ups_data_ = json.loads(json.dumps(xmltodict.parse(ups_data_)))

    return ups_data_



def removeNonAscii(string, replace=''):
    """
    input:  string = String needing to have non-ascii characters removed from.
            replace = String to be replaced into 'string' where non-ascii characters are found.
    output: String of 'string', where non-ascii characters have been replaced by 'replace'.
    """
    if not all(ord(char) < 128 for char in string):
        return ''.join( char if ord(char) < 128 else replace for char in string )
    else:
        return string



def update____Data(_to_update):
    """
    input:  _to_update = List of dicts containing orders to be updated to ____ table.
    output: (no return) Updates ____ table '____' with orders from '_to_update'.
    """
    sql = """
        INSERT INTO ____ (
            ____, ____, ________, ____, ____
        ) VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            ____ = VALUES(____),
            ____ = NOW(),
            ________ = VALUES(________)
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



def loopThroughData(_data):
    """
    input:  _data = List of dicts from ____ data.  To be used to pull further data from UPS API.
    output: List of dicts of order data to be updated to ____ table.
    """
    to_update_ = []
    for index, order in enumerate(_data):

        print(">>> Processing", index + 1, "of", len(_data), "...")
        print(">>> ____ =", order['package_shipment_id'])
        print(">>> ____ =", order['tracking_number'])

        # Get json from UPS API for 'tracking_number'.
        ups_data = getUpsData(order['tracking_number'])

        # Perform first try/except on 'ups_data'.  Catches 'KeyError' and 'TypeError' to indicate
        # no returned data from UPS API.
        try:
            activity = ups_data['TrackResponse']['Shipment']['Package']['Activity']
            description = activity['____']['____Type']['Description']
            date = activity['Date']
        except (KeyError, TypeError):
            print(">>> BAD ORDER...\n")
            continue

        # Second try/excepts to remove non-ascii characters from API response data.
        try:
            print(">>> looking for non-ascii...", description)
        except UnicodeEncodeError:
            description = removeNonAscii(description)
            print(">>> non-ascii found...", description)
        try:
            print(">>> looking for non-ascii...", date, "\n")
        except UnicodeEncodeError:
            date = removeNonAscii(date)
            print(">>> non-ascii found...", date, "\n")

        # Take collected values from UPS API, add them to the individual order, then append order to
        # 'to_update_' for return output.
        order['message'] = description
        order['message_time_stamp'] = date
        order['last_checked'] = start
        to_update_.append(order)

    return to_update_



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

print(">>> updating ____ table...\n")
update____Data(to_update)

end = datetime.now()
print(">>> done ... runtime =", end - start)
