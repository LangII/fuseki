
"""

TrackingReportRequest.py

- 2019-12-06 by David Lang
    - Update tblArrival with tracking status for ____ shipments with shipped method of
    UPS International Expedited.  Then generate report and send email of updated tracking status.

"""

####################################################################################################
                                                                                 ###   IMPORTS   ###
                                                                                 ###################
from datetime import datetime, timedelta
from Required import Connections, Tracking, Mail
from collections import OrderedDict
import pandas

####################################################################################################
                                                                                 ###   GLOBALS   ###
                                                                                 ###################
begin = datetime.now()

conn = Connections.connect()
cur = conn.cursor()

####################################################################################################
                                                                               ###   CONSTANTS   ###
                                                                               #####################
COMPANY_ID      = 1772
SHIPPED_METHOD  = 'UPS International Expedited'
DAYS_AGO        = 90
CSV_NAME        = '____IntlTrackingReport.csv'

TBL_PS_COLS = [
    'PackageShipmentID', 'Attention', 'Address1', 'City', 'State', 'ZipCode', 'Country',
    'CompletionDate', 'ShippedMethod'
]
TBL_A_COLS = [
    'TrackingNumber', 'Message', 'LastChecked', 'MessageTimestamp', 'Delivered'
]

DAYS_AGO_DATE = (begin - timedelta(days=DAYS_AGO)).strftime('%Y-%m-%d')

####################################################################################################
                                                                                    ###   MAIN   ###
                                                                                    ################
def main():

    print("\n\n\n")

    packages_to_update = getPackagesToUpdate()
    packages_to_update = filterMultiTrackingNums(packages_to_update)
    print(">>> retrieved", len(packages_to_update), "packages to update\n")

    print(">>> starting loop through packages to update table ...\n")
    mainLoop(packages_to_update)

    packages_for_report = getPackagesForReport()
    print(">>> retrieved", len(packages_for_report), "packages for report\n")

    print(">>> converting packages for report to dataframe ...\n")
    df = convertPackagesToDf(packages_for_report)
    print(df, "\n")

    print(">>> saving dataframe to csv ...\n")
    df.to_csv(CSV_NAME, index=False)

    print(">>> emailing csv ...\n")
    emailCsv()

    end = datetime.now()
    exit(">>> DONE ... runtime = " + str(end - begin) + "\n\n\n")



def mainLoop(_packages_to_update):

    for i, (package_shipment_id, tracking_number) in enumerate(_packages_to_update):

        print(">>>", i + 1, "of", len(_packages_to_update))
        print(">>> PackageShipmentID =", package_shipment_id, "/ TrackingNumber =", tracking_number)

        if tracking_number == '':
            print(">>>     - BAD TRACKING NUMBER ... moving on\n")
            continue

        vitals = Tracking.getSingleUpsVitals(tracking_number)
        updateTableArrival(package_shipment_id, tracking_number, vitals)
        print(">>>     - tblArrival updated\n")

####################################################################################################
                                                                               ###   FUNCTIONS   ###
                                                                               #####################

def getPackagesToUpdate():
    """
    input:  constants = COMPANY_ID, SHIPPED_METHOD, DAYS_AGO_DATE
    output: Return 'select_', a list-of-lists from database with conditions from constants.
    """

    query = """
        SELECT ps.PackageShipmentID, ps.TrackingNumber
            FROM tblPackageShipments AS ps LEFT JOIN tblArrival AS a
                ON ps.PackageShipmentID = a.PackageShipmentID
            WHERE ps.CompanyID = %s
                AND ps.ShippedMethod = %s
                AND ps.CompletionDate > %s
                AND (a.Delivered != 'Y' OR a.Delivered IS NULL)
    """
    cur.execute(query, [COMPANY_ID, SHIPPED_METHOD, DAYS_AGO_DATE])
    select_ = [ list(i) for i in cur.fetchall() ]

    return select_



def filterMultiTrackingNums(_packages):
    """
    Quick filtering for packages with multiple tracking numbers.  Then, if found, multiple tracking
    number packages are separated into individual packages with individual tracking numbers.
    """

    def splitTrackingNums(_pack):
        """ Subroutine...  Separation of packages with multiple tracking numbers. """
        multi = [ i.strip() for i in _pack[1].split(';') ]
        splits_ = [ [_pack[0], m] for m in multi ]
        return splits_

    packages_ = []
    for pack in _packages:
        if ';' in pack[1]:
            for split in splitTrackingNums(pack):  packages_.append(split)
        else:
            packages_.append(pack)

    return packages_



def updateTableArrival(_package_shipment_id, _tracking_number, _vitals):
    """
    input:  constants = COMPANY_ID
            _package_shipment_id = Package shipment ID of package to update.
            _tracking_number = Tracking number of package to update.
            _vitals['delivered'] = Bool of delivery confirmation.
            _vitals['message'] = Description of most recent package event.
            _vitals['time_stamp'] = Time stamp of most recent package event.
    output: Update tblArrival with input values.
    """

    query = """
        INSERT INTO tblArrival (
            PackageShipmentID, TrackingNumber, MessageTimestamp, Message, CompanyID, Delivered
        )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                Message = VALUES(Message),
                LastChecked = NOW(),
                MessageTimestamp = VALUES(MessageTimestamp),
                Delivered = VALUES(Delivered)
    """
    insert = [
        _package_shipment_id,   _tracking_number,
        _vitals['time_stamp'],  _vitals['message'],
        COMPANY_ID,             'Y' if _vitals['delivered'] else 'N'
    ]
    cur.execute(query, insert)
    conn.commit()



def getPackagesForReport():
    """
    input:  constants = TBL_PS_COLS, TBL_A_COLS, COMPANY_ID, SHIPPED_METHOD, DAYS_AGO_DATE
    output: Return 'select_', a list-of-lists from database with conditions from constants.
    """

    query = """
        SELECT {}, {}
            FROM tblPackageShipments AS ps LEFT JOIN tblArrival AS a
                ON ps.PackageShipmentID = a.PackageShipmentID
            WHERE ps.CompanyID = %s
                AND ps.ShippedMethod = %s
                AND ps.CompletionDate > %s
    """
    tbl_ps_cols = ', '.join([ 'ps.' + col for col in TBL_PS_COLS ])
    tbl_a_cols = ', '.join([ 'a.' + col for col in TBL_A_COLS ])
    query = query.format(tbl_ps_cols, tbl_a_cols)

    cur.execute(query, [COMPANY_ID, SHIPPED_METHOD, DAYS_AGO_DATE])
    select_ = [ list(i) for i in cur.fetchall() ]

    return select_



def convertPackagesToDf(_packages):
    """
    input:  constants = TBL_PS_COLS, TBL_A_COLS
            _packages = List-of-tuples from 'getPackagesForReport()'.
    output: Return sorted dataframe object, of data from '_packages'.
    """

    converted_ = [ OrderedDict(zip(TBL_PS_COLS + TBL_A_COLS, row)) for row in _packages ]
    converted_ = pandas.DataFrame(converted_)

    # Sort dataframe before returning.
    converted_.sort_values(by=['Delivered', 'CompletionDate'], inplace=True)
    converted_ = converted_.reset_index(drop=True)

    return converted_



def emailCsv():
    """
    input:  constants = CSV_NAME
    output: Send email with conditions from local variables and attachment of created csv file
            'CSV_NAME'.
    """

    email_to = ['____', '____']
    email_from = '____'
    email_message = """
        Attached is your Daily Tracking Report for your package shipments with
        shipping method of UPS International Expedited.

        Have a nice day!  :)
    """
    email_subject = 'CDC - Daily Tracking Report'
    file_name = 'UPS_International_Expedited.csv'

    Mail.SendFile(email_to, email_from, email_message, email_subject, CSV_NAME, file_name)



############
main()   ###
############
