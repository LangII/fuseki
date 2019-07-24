


####################################################################################################
"""
*** UPSTracking_DL.py ***
Takes order data from d___ table tbl________________, to use to call UPS API to obtain tracking
data.  The tracking data is filtered to get the latest tracking message and message time stamp.
Then the order data from tbl________________ and the filtered data from the UPS API is used to
update the d___ table tbl_______.  The primary purpose is to obtain package delivery time stamps.
With this, statistics regarding days-to-deliver can be tracked.

by David Lang
last modified 2019-07-24
"""
####################################################################################################



                                                     ###############################################
                                                     #####   \/   IMPORTS & SYSTEM MODS   \/   #####
                                                     ###############################################



import sys
sys.path.insert(0, '/t_____/p_____')

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
from R_______ import C__________



                                                       #############################################
                                                       #####   \/   GLOBALS & CONSTANTS   \/   #####
                                                       #############################################



start = datetime.now()
today = start.strftime('%Y-%m-%d')

# Give value to 'TEST_SIZE' to limit number of orders to process.
TEST_SIZE = 0

# CONSTANTS...
COMPANY_ID = '____'

# 'getD___Data()' CONSTANTS...
SHIP_METHOD = 'UPS MI'
START_DATE = '2019-04-20'
END_DATE = '2019-05-01'
DELIVERED_MESSAGES = (
    'Package delivered by post office',
    'Delivered'
)

# 'getUpsData()' CONSTANTS...
ACCESS_LICENSE_NUMBER = ''
USER_ID = ''
PASSWORD = ''
UPS_ONLINETOOLS_URL = 'https://onlinetools.ups.com/ups.app/xml/Track'
UPS_REQUEST_HEADERS = {'Content-Type': 'application/x-www-form-urlencoded'}
MAIL_INNOVATION_TAG = '<IncludeMailInnovationIndicator/>'
CALL_DELAY = .2

conn = C__________.connect()
cur = conn.cursor()



                                                                 ###################################
                                                                 #####   \/   FUNCTIONS   \/   #####
                                                                 ###################################



def getD___Data():
    """
    output: Tuple of tuples containing 'P________________' and 'T_____________' of orders designated
            by CONSTANTS.
    """
    a, b, c, d, e = [COMPANY_ID, SHIP_METHOD, START_DATE, END_DATE, DELIVERED_MESSAGES]
    sql = """
        SELECT p.P________________, p.T_____________ FROM tbl________________ AS p
        LEFT JOIN tbl_______ AS a ON p.P________________ = a.P________________
            WHERE p.C________ = '{}'
            AND p.status = 'completed'
            AND p.S____________ LIKE '%{}%'
            AND p.C_____________ > '{}'
            AND p.C_____________ < '{}'
            AND (a.M______ NOT IN{} OR a.M______ IS NULL)
        {}
    """
    # if/else used for testing, by inserting 'LIMIT' keyword into sql or not.
    if not TEST_SIZE:  sql = sql.format(a, b, c, d, e, '')
    else:  sql = sql.format(a, b, c, d, e, 'LIMIT {}'.format(TEST_SIZE))

    cur.execute(sql)
    d____data_ = cur.fetchall()

    return d____data_



def processD___Data(_d____data):
    """
    input:  _d____data = Tuple of orders containing 'P________________' and 'T_____________'.
    output: List of dicts.  The dicts are reformatted orders from 'getD___Data()'.
    """
    processing_ = []
    for (x, y) in _d____data:
        order = {'package_shipment_id': str(x), 'company_id': COMPANY_ID, 'tracking_number': y}
        processing_.append(order)

    return processing_



def getUpsData(_tracking_number):
    """
    input:  _tracking_number = String of single tracking number.
    output: Json object containing UPS API return data.
    """
    # 'MAIL_INNOVATION_TAG' is an xml tag needed to designate that 'T_____________' is for an
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
                    <CustomerContext>Get tracking status</CustomerContext>
                </TransactionReference>
                <XpciVersion>1.0</XpciVersion>
                <RequestAction>Track</RequestAction>
                <RequestOption>activity</RequestOption>
            </Request>
            {}
            <TrackingNumber>{}</TrackingNumber>
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
    input:  string = String to have non-ascii characters removed.
            replace = String to replace into 'string' where non-ascii characters are found.
    output: String of 'string', where non-ascii characters have been replaced by 'replace'.
    """
    if not all(ord(char) < 128 for char in string):
        return ''.join( char if ord(char) < 128 else replace for char in string )
    else:
        return string



def updateD___Data(_to_update):
    """
    input:  _to_update = List of dicts containing orders to be updated to d___ table.
    output: (no return) Updates d___ table 'tbl_______' with orders from '_to_update'.
    """
    sql = """
        INSERT INTO tbl_______ (
            P________________, T_____________, M______Timestamp, M______, C________
        ) VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            M______ = VALUES(M______),
            LastChecked = NOW(),
            M______Timestamp = VALUES(M______Timestamp)
    """
    # Build 'inserts' with list of order data from '_to_update' to be each inserted into 'sql' for
    # d___ table updating.
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
    input:  _data = List of dicts from d___ data.  To be used to pull further data from UPS API.
    output: List of dicts of order data to be updated to d___ table.
    """
    to_update_ = []
    for index, order in enumerate(_data):

        print(">>> Processing", index + 1, "of", len(_data), "...")
        print(">>> P________________ =", order['package_shipment_id'])
        print(">>> T_____________ =", order['tracking_number'])

        # Get json from UPS API of data for order 'tracking_number'.
        ups_data = getUpsData(order['tracking_number'])

        # Perform first try/except on 'ups_data'.  Catches 'KeyError' and 'TypeError' to indicate
        # no returned data from UPS API.
        try:
            activity = ups_data['TrackResponse']['Shipment']['Package']['Activity']
            description = activity['Status']['StatusType']['Description']
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



print("\n>>> getting data from d___ to be processed...\n")
d____data = getD___Data()

print(">>> orders to be processed =", len(d____data), "\n")

print(">>> processing data from d___...\n")
processing = processD___Data(d____data)

print(">>> looping through processed d___ data...\n")
to_update = loopThroughData(processing)

print(">>> updating d___ table...\n")
updateD___Data(to_update)

end = datetime.now()
print(">>> done ... runtime =", end - start)
