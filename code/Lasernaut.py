import herepy
from geopy import geocoders
import json
import mysql.connector
import configparser
import pandas as pd
import requests
import os


def getcoords(apikey, address):
    geocoderapi = herepy.GeocoderApi(apikey)
    response = str(geocoderapi.free_form(address))
    obj = (json.loads(response))
    lnglat = [(obj['items'][0]['access'][0]['lat']), (obj['items'][0]['access'][0]['lng'])]
    return lnglat


def heregeocoder(apikey, df):
    #print(df)
    geocoderapi = herepy.GeocoderApi(apikey)
    # iterate over dataframe
    for i in df.index:
        address = df.iloc[i, 5] + ' ' + df.iloc[i, 6] + ' ' + df.iloc[i, 7] + ' ' + df.iloc[i, 8] + ' ' \
                  + ' UK ' + df.iloc[i, 9] + df.iloc[i, 10]  # concatenate address from data frame

        response = str(geocoderapi.free_form(address))
        obj = (json.loads(response))
        print(address)
        print(obj)
        try:
            latlng = [(obj['items'][0]['position']['lat']), (obj['items'][0]['position']['lng'])]
            print(obj)
            df.at[i, 21] = latlng[0]
            df.at[i, 22] = latlng[1]
        except:
            print('XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')
    return df


def googlegeocoder(df):
    #print(df)
    g = geocoders.GoogleV3(api_key='xxxxxxxxc')
    # iterate over dataframe
    for i in df.index:
        address = df.iloc[i, 5] + ' ' + df.iloc[i, 6] + ' ' + df.iloc[i, 7] + ' ' + df.iloc[i, 8] + ' ' \
                  + df.iloc[i, 9] + df.iloc[i, 10] # concatenate address from data frame
        location = g.geocode(address, timeout=10) # do geocoding
        # print (address)
        try:
            df.at[i, 21] = location.latitude
            df.at[i, 22] = location.longitude
            print(str(location.latitude) + ' ' + str(location.longitude))
        except:
            print('XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')
    return df




def getjobsdone(df):
    dfdone = df.loc[df[18] == '1']
    dfdone.reset_index(inplace=True, drop=True)
    return(dfdone)


def getjobsnotdone(df):
    dftodo = df.loc[df[18] == '0']
    dftodo.reset_index(inplace=True, drop=True)
    return(dftodo)

def delcurrentlocs(group):
    api_user = os.environ.get('MN_USER')
    api_pass = os.environ.get('MN_PASS')
    auth = (api_user, api_pass)

    currentlocresp = requests.get("https://api.masternautconnect.com/"
                            "connect-webservices/services/public/v1/customer/27172/location/current?category=" + group +
                            "&pageIndex=0&pageSize=500"
                            ,auth=auth)

    response_dict = json.loads(currentlocresp.text)
    print(json.dumps(response_dict, indent=2))
    for i in response_dict["items"]:
        locid = i["id"]
        delloc = requests.delete("https://api.masternautconnect.com/"
                                  "connect-webservices/services/public/v1/customer/27172/location/" + locid, auth=auth)

        print(delloc)


def masternautplot(df, group):
    api_user = os.environ.get('MN_USER')
    api_pass = os.environ.get('MN_PASS')
    auth = (api_user, api_pass)
    print(auth)

    for i in df.index:
        mnaddlocationurl = "https://api.masternautconnect.com/" \
                            "connect-webservices/services/public/v1/customer/27172/location"
        jobref = df.iloc[i, 11]
        addrroadnumber = df.iloc[i, 5]
        addrroad = df.iloc[i, 6]
        addrcity = df.iloc[i, 8]
        addrpostcode = df.iloc[i, 9] + ' ' + df.iloc[i, 10]

        address = {"name": jobref,
                   "categoryName": group,
                   "radius": 0.001,
                   "address": {
                                "roadNumber": addrroadnumber,
                                "road": addrroad,
                                "city": addrcity,
                                "postCode": addrpostcode,
                                "country": "United Kingdom"
                                }
                   }
        print(address)
        response = requests.post(mnaddlocationurl, json=address, auth=auth)
        print(response.text)


def jprint(jsondata):
    json_object = json.loads(str(jsondata))
    json_formatted_str = json.dumps(json_object, indent=2)
    print(json_formatted_str)


def getfcldata(query, host, user, password, db):
    cnx = mysql.connector.connect(user=user, password=password,
                                  host=host,
                                  database=db)
    cursor = cnx.cursor(prepared=True)
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    cnx.close()
    return data

# ---------------- START OF MAIN PROGRAM ----------------
# Read INI file
config = configparser.ConfigParser()
config.read('Lasernaut.ini')
fclhost = config['FCL DB']['host']
fcluser = config['FCL DB']['user']
fclpassword = config['FCL DB']['password']
fcldb = config['FCL DB']['db']
hereapikey = config['HERE MAPS']['apikey']
mndonegroup = config['MASTERNAUT']['donegroup']
mnnotdonegroup = config['MASTERNAUT']['notdonegroup']

fclvrsqry = ("""select  TRANS_MODULE_TRANS_INSTRUCTION.TRN_VRSINT$$ AS 'VRS',
                        TRANS_MODULE_TRANS_INSTRUCTION.TRN_TI$$ AS 'TI',
                        TRANS_MODULE_TRANS_INSTRUCTION.VRS_POS AS 'VRS POS',
                        TRANS_MODULE_TRANS_INSTRUCTION.EQUIP_CODE$$_VRS AS 'REG NO.',
                        TRANS_MODULE_TRANS_INSTRUCTION.TRN_TITYPE$$ AS 'TYPE',
                        TRANS_MODULE_TRANS_INSTRUCTION.POINTA_ADD_1 AS 'ADD1',
                        TRANS_MODULE_TRANS_INSTRUCTION.POINTA_ADD_2 AS 'ADD2',
                        TRANS_MODULE_TRANS_INSTRUCTION.POINTA_ADD_3 AS 'ADD3',
                        TRANS_MODULE_TRANS_INSTRUCTION.POINTA_ADD_4 AS 'ADD4',
                        TRANS_MODULE_TRANS_INSTRUCTION.AREA$$_POINTA_PFX AS 'PCODE_PFX',
                        TRANS_MODULE_TRANS_INSTRUCTION.POINTA_AREA_SFX AS 'PCODE_SFX',
                        TRANS_MODULE_TRANS_INSTRUCTION.OPSREF$$_LOCAL AS 'JOB REF',
                        TRANS_MODULE_TRANS_INSTRUCTION.TOTAL_PACKAGES AS 'PACKAGES',
                        TRANS_MODULE_TRANS_INSTRUCTION.CHARGEABLE_WEIGHT AS 'WEIGHT',
                        TRANS_MODULE_TRANS_INSTRUCTION.INSTRUCTIONS_1 as 'INSTRUCTIONS 1',
                        TRANS_MODULE_TRANS_INSTRUCTION.INSTRUCTIONS_2 AS 'INSTRUCTIONS 2',
                        TRANSPORT_MODULE_VRS.TRN_DRIVER$$_1 AS 'DRIVER',
                        TRANS_MODULE_TRANS_INSTRUCTION.VRS_ACTIVITY_DATE AS 'VRS DATE',
                        TRANS_MODULE_TRANS_INSTRUCTION.MOBILE_APP_LOCKED AS 'STATUS',
                        TRANS_MODULE_TRANS_INSTRUCTION.POD_SIGNATORY AS 'POD SIGNED BY',
                        TRANS_MODULE_TRANS_INSTRUCTION.POD_TIME AS 'POD TIME'
                from    TRANS_MODULE_TRANS_INSTRUCTION join TRANSPORT_MODULE_VRS
                on      TRANS_MODULE_TRANS_INSTRUCTION.TRN_VRSINT$$=TRANSPORT_MODULE_VRS.TRN_VRSINT$$
                where   TRANS_MODULE_TRANS_INSTRUCTION.VRS_ACTIVITY_DATE = CURDATE()
                and     TRANS_MODULE_TRANS_INSTRUCTION.EQUIP_CODE$$_VRS REGEXP '(^[A-Z]{2}[0-9]{2}[A-Z]{3})'""")

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

vrsdata = getfcldata(fclvrsqry, fclhost, fcluser, fclpassword, fcldb)

dataframe = pd.DataFrame(vrsdata)

# heregeocoder(hereapikey, dataframe)
# googlegeocoder(dataframe)
print(dataframe)
delcurrentlocs(mnnotdonegroup)
delcurrentlocs(mndonegroup)

jobsdonedf = getjobsdone(dataframe)
print(jobsdonedf)
jobsnotdonedf = getjobsnotdone(dataframe)
print(jobsnotdonedf)

masternautplot(jobsdonedf, mndonegroup)
masternautplot(jobsnotdonedf, mnnotdonegroup)

