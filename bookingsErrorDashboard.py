import gspread
import gspread_dataframe as gd
import os
import pandas as pd
import datetime
from datetime import timedelta
from pandas import DataFrame
import collections
import json
import numpy as np
from oauth2client.service_account import ServiceAccountCredentials
import sys

sys.stdout = open("bookingsErrorDashboard.log", "a")

scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('silver-charmer-333011-c7b0e3d22c84.json', scope)
client = gspread.authorize(creds)

def export_to_sheets(sheet_name,worksheet_name,df,mode='r'):
    ws = client.open(sheet_name).worksheet(worksheet_name)
    str_list = list(filter(None, ws.col_values(1)))

    if(mode=='w'):
        ws.clear()
        gd.set_with_dataframe(worksheet=ws,dataframe=df,include_index=False,include_column_header=True,resize=True)
        return True
    elif(mode=='a'):
        ws.add_rows(df.shape[0])
        gd.set_with_dataframe(worksheet=ws,dataframe=df,include_index=False,include_column_header=False,row=len(str_list)+1,resize=False)
        return True
    else:
        return gd.get_as_dataframe(worksheet=ws)

directory = '/appl/commwr/BookingReport/'
bvError,tuiError,dcError,bookedAlreadyError,combinationError,partnerError,totalErrors = 0,0,0,0,0,0,0
totalBookingsDirectory = '/appl/commwr/tmp/'
today = datetime.date.today()
yesterday = today - timedelta(days = 1)
yesterday = str(yesterday)
yesterday_formatted1 = datetime.datetime.strptime(yesterday, '%Y-%m-%d').strftime("%d/%m/%y")
yesterday_formatted = datetime.datetime.strptime(yesterday, '%Y-%m-%d').strftime("%m/%d/%y")
print (yesterday_formatted)
errorscount = []

for filename in os.listdir(directory):
    if not filename.startswith('.') and filename.startswith(yesterday):
        f = os.path.join(directory, filename)
        if os.path.isfile(f):
            bvError,tuiError,dcError,bookedAlreadyError,combinationError,partnerError,totalErrors = 0,0,0,0,0,0,0
            dashboard_df = pd.read_table(f, sep="\t")
            dashboard_df.columns = dashboard_df.columns.str.lower()
            output = list(zip(dashboard_df.brand,dashboard_df.error))
            for x in output:
                if (x[0] in ['BVAG','DC','TUI']):
                    if (x[0] == 'BVAG'):
                        bvError = bvError + 1
                    elif (x[0] == 'TUI'):
                        tuiError = tuiError + 1
                    elif (x[0] == 'DC'):
                        dcError = dcError + 1
                if ("ReservationId already known." in x[1] or "Reservation already exists" in x[1] or "being booked" in x[1]):
                    bookedAlreadyError = bookedAlreadyError + 1
                elif ("is not available" in x[1] or "Accommodation not available" in x[1]):
                    combinationError = combinationError + 1
                elif ("Not available at our Partner" in x[1] or "Accommodation not available at our Partner" in x[1]):
                    partnerError = partnerError + 1
            rowobj = {}
            rowobj["Date"] = yesterday_formatted1
            rowobj["bvError"] = bvError
            rowobj["tuiError"] = tuiError
            rowobj["dcError"] = dcError
            rowobj["bookedAlreadyError"] = bookedAlreadyError
            rowobj["combinationError"] = combinationError
            rowobj["partnerError"] = partnerError
            errorscount.append(rowobj)

df1 = pd.DataFrame(errorscount)
aggregation_functions = {'Date': 'first', 'bvError': 'sum', 'tuiError': 'sum', 'dcError': 'sum', 'bookedAlreadyError': 'sum', 'combinationError': 'sum', 'partnerError': 'sum'}
df3 = df1.groupby(df1['Date']).aggregate(aggregation_functions).reset_index(drop=True)
f = os.path.join(totalBookingsDirectory, "huovCount.csv")
if os.path.isfile(f):
    df2 = pd.read_csv(f)
    df3 = df3.merge(df2, on="Date").reset_index(drop=True)
    df3['Date'] = yesterday_formatted
    df3['totalBookingsCount'] = df2['BVCount'] + df2['TUICount'] + df2['DCCount '] + df3['bvError'] + df3['dcError'] + df3['tuiError']
    df3['totalErrors'] = df3['bookedAlreadyError'] + df3['combinationError'] + df3['partnerError']
    df3['bvErrorRate'] = round(df3['bvError']/ (df3['BVCount'] + df3['bvError']) ,4)
    df3['dcErrorRate'] = round(df3['dcError']/ (df3['DCCount '] + df3['dcError']) ,4)
    df3['tuiErrorRate'] = round(df3['tuiError']/ (df3['TUICount'] + df3['tuiError']) ,4)
    df3.drop(['bvError', 'BVCount', 'dcError', 'DCCount ', 'tuiError', 'TUICount'], axis=1, inplace=True)
    df3 = df3[['Date','totalBookingsCount','totalErrors','bvErrorRate', 'dcErrorRate', 'tuiErrorRate', 'partnerError', 'combinationError', 'bookedAlreadyError']]
print ("ABB_BDC")
print (df3)
export_to_sheets("Copy of Belvilla - Bookings Error Dashboard","AirBnb + BDC",df3,'a')

errorscount = []
yesterday = datetime.datetime.strptime(yesterday, '%Y-%m-%d').strftime("%y%m%d")

paths = ["/appl/log/jsonrpc-intern/"]
for mypath in paths:
    onlyfiles = [i for i in os.listdir(mypath) if os.path.isfile(mypath+i)]
    for filename in onlyfiles:
        if not filename.startswith('.') and filename.startswith(yesterday) and filename.endswith("insertrentalcontractv2"):
            reqcount, bvCount, bvError, tfError, dcError,tfCount, dcCount, partnerError, totalErrors  = 0,0,0,0,0,0,0,0,0
            beginlist = []
            with open(mypath+filename, encoding = "ISO-8859-1") as fh:
                for num,line in enumerate(fh,1):
                    if line.startswith("----Begin"):
                        beginlist.append(num)
                    if "insertrentalcontractv2" in line.lower():
                        reqcount = reqcount + 1
                    if '"brand":"bv"' in line.lower():
                        bvCount = bvCount + 1
                    if '"brand":"tf"' in line.lower():
                        tfCount = tfCount + 1
                    if '"brand":"dc"' in line.lower():
                        dcCount = dcCount + 1
                        

            finallist = []
            fh = open(mypath+filename, encoding = "ISO-8859-1")  
            lines = fh.read().splitlines()
            for i in range(len(lines)):
                if lines[i].startswith("ERROR"):
                    index = beginlist.index(i-1)
                    prevbeginindex = index - 1
                    string1 = ""
                    dict1 = {}
                    for j in range(beginlist[prevbeginindex]+2,beginlist[index]):
                        string1 = string1 + lines[j-1]
                    dict1["time"] = lines[beginlist[prevbeginindex]].split("  ")[1]
                    dict1["payload"] = string1
                    dict1["error"] = lines[i]
                    finallist.append(dict1)

            errorlist = []
            for d in finallist:
                errorobj ={}
                jsonob = json.loads(d["payload"])["params"]
                try:
                    errorobj["Brand"] = jsonob["Brand"]
                    if (errorobj["Brand"].lower() == "bv"):
                        bvError = bvError + 1
                    if (errorobj["Brand"].lower() == "tf"):
                        tfError = tfError + 1
                    if (errorobj["Brand"].lower() == "dc"):
                        dcError = dcError + 1
                except Exception: # Replace Exception with something more specific.
                    continue

                errorobj["Error"] = d["error"].split(":")[-1].replace('\n','')
                errorobj["Time"] = d["time"]
                errorlist.append(errorobj)

            df = pd.DataFrame(errorlist)
            partnerError = df['Error'].str.contains('Availability record not available at our Partner').sum()
            totalErrors = len(df.index)

            rowobj = {}
            rowobj["Date"] = yesterday_formatted
            rowobj["ReqCount"] = reqcount
            rowobj["TotalError"] = str(totalErrors)
            rowobj["ErrorRate"] = str(round ((totalErrors * 100)/ reqcount,2))
            rowobj["Partner"] = str(partnerError)
            rowobj["Combination"] = df['Error'].str.contains(' is not available').sum()
            rowobj["BookedAlready"] = df['Error'].str.contains('House is being booked already').sum()
            rowobj["partnerErrorRate"] = str(round ((partnerError/ (reqcount)) * 100,2)) + "%"
            rowobj["OtherErrorRate"] = str(round (((totalErrors - partnerError)/ (reqcount)) * 100,2)) + "%"
            rowobj["bvError"] = str(round ((bvError/ (bvCount+1)) * 100,2)) + "%"
            rowobj["dcError"] = str(round ((dcError/ (dcCount+1)) * 100,2)) + "%"
            rowobj["tfError"] = str(round ((tfError/ (tfCount+1)) * 100,2)) + "%"
            fh.close()
            errorscount.append(rowobj)

df1 = pd.DataFrame(errorscount)
print ("Website")
print (df1)

errorscount = []
paths = ["/appl/log/jsonrpc-partner/"]
for mypath in paths:
    onlyfiles = [i for i in os.listdir(mypath) if os.path.isfile(mypath+i)]
    for filename in onlyfiles:
        if not filename.startswith('.') and filename.startswith(yesterday) and filename.endswith("placebookingv1"):
            reqcount, combinationError = 0,0
            beginlist = []
            with open(mypath+filename, encoding = "ISO-8859-1") as fh:
                print (mypath+filename)              
                for num,line in enumerate(fh,1):
                    if line.startswith("----Begin"):
                        beginlist.append(num)
                    if "placebookingv1" in line.lower():
                        reqcount = reqcount + 1
            finallist = []
            fh = open(mypath+filename,encoding = "ISO-8859-1")
            lines = fh.read().splitlines()
            for i in range(len(lines)):
                if lines[i].startswith("ERROR"):
                    index = beginlist.index(i-1)
                    prevbeginindex = index - 1
                    string1 = ""
                    dict1 = {}
                    for j in range(beginlist[prevbeginindex]+2,beginlist[index]):
                        string1 = string1 + lines[j-1]
                    dict1["time"] = lines[beginlist[prevbeginindex]].split("  ")[1]
                    dict1["payload"] = string1
                    dict1["error"] = lines[i]
                    finallist.append(dict1)

            errorlist = []
            for d in finallist:
                errorobj ={}
                jsonob = json.loads(d["payload"])["params"]
                errorobj["HouseCode"] = jsonob["HouseCode"]
                errorobj["Error"] = d["error"].split(":")[-1].replace('\n','')
                errorobj["Time"] = d["time"]
                errorlist.append(errorobj)

            df = pd.DataFrame(errorlist)
            combinationError = df['Error'].str.contains('Combination House-ArrivalDate-DepartureDate not available').sum()
            rowobj = {}
            rowobj["Date"] = yesterday_formatted
            rowobj["ErrorRate"] = str(round((combinationError * 100)/ reqcount,2)) + "%"
            rowobj["ReqCount"] = str(reqcount)
            rowobj["TotalError"] = str(len(df.index))
            rowobj["Partner"] = df['Error'].str.contains('Availability record not available at our Partner').sum()
            rowobj["Combination"] = str(combinationError)
            rowobj["BookedAlready"] = df['Error'].str.contains('House is being booked already').sum()
            fh.close()
            errorscount.append(rowobj)

df2 = pd.DataFrame(errorscount)
print ("Long Tail OTA's")
print (df2)
export_to_sheets("Copy of Belvilla - Bookings Error Dashboard","AirBnb + BDC",df3,'a')
export_to_sheets("Copy of Belvilla - Bookings Error Dashboard","Website",df1,'a')
export_to_sheets("Copy of Belvilla - Bookings Error Dashboard","Long Tail OTAs",df2,'a')
