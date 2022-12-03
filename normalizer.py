from enum import unique
from math import isnan
import sys
from numpy import insert
import pypyodbc as odbc
import pandas as pd
from IPython.display import display
from random import randint, randrange
import datetime
import re

#Imports excel File excludes Rows Q,R,S
df1 = pd.read_excel('ProactivePotholing_SkinPatching_FY21-22.xlsx', usecols="A:P", skiprows=[2])

#Creating dataframe to be inserted into sql database
df2 = pd.DataFrame(columns=["UniqueNumber", "PotholeDate", "TruckNumber", "PotholeZone", "PotholeCrew1", 
                            "PotholeCrew2", "PotholeLaborHrs", "PotholeNumLocations", "PotholeAsphaltApplied",
                            "PotholeNumberLoads", "PotholeMaintDistrict", "PotholeCDList", "PotholeCD",
                            "PotholeAsphaltAppliedEach", "PotholeLocationWorking", "PotholeComments"])

#Removes Empty Rows from dataframe
df1.dropna(subset=["Truck #"], inplace=True)

#Used to create unique number
def createNo(date):
    stringDate = str(date).split(" ")[0].split("-") #Creates list of parts of date with it starting with year
    stringDate.append(str(randrange(1000000, 10000000))) #Appends randomly generated number to end of date list
    uniqueNo = "".join(stringDate)
    return uniqueNo

#Checks format of CD column data 
def checkCDFormat(num):
    try:
        num = int(num)
        if num > 15 or num < 0:
            return 0
        else:
            return num
    except ValueError:
        return 0

#Used to find number of council districts, returns that number as an array
def findCD(cdString):
    if isinstance(cdString, str):
        cdString = cdString.replace(" ", "")
        if(len(cdString) != 0):
            #Add recognition of period as delimiter
            cdList = cdString.split(',')
            cdList = [i for i in cdList if i.isnumeric()] #Remove elements of list if it is not a number
            cdList = list(map(checkCDFormat, cdList))
            if sum(cdList) > 0:
                return cdList
            else:
                return [0]
        else:
            return [0]
    else:
        return [0]

#Used to format maintenance district
def formatMD(mdNum):
    mdString = str(mdNum)
    return '/'.join(mdString[i:i+3] for i in range(0, len(mdString), 3))

recNum = 0
#Adds rows from excel dataframe to new data frame
def formatRow(row):
    global recNum
    recNum += 1
    cdList = findCD(row["CD"])
    string_ints = [str(int) for int in cdList]
    cdString = ",".join(string_ints)
    eachPothole = round(row['SQFT Asphalt Applied'] / len(cdList), 2)
    md = formatMD(row['Maintenance District'])
    uniqueNo = createNo(row['\n'])
    if type(row['# of Locations ']) == str:
        print(row['# of Locations '])
    global dictList
    for cd in cdList:
        newRow = {"RecNumber": recNum, "UniqueNumber": uniqueNo, "PotholeDate": row['\n'], "TruckNumber": row['Truck #'], "PotholeZone": row['Zone \n(M or V)'], "PotholeCrew1": row['Name of Crew Leader'], 
                            "PotholeCrew2": row['Name of 2nd Crew Member'], "PotholeLaborHrs": row['Total Labor Hours'], "PotholeNumLocations": row['# of Locations '], "PotholeAsphaltApplied": row['SQFT Asphalt Applied'],
                            "PotholeNumberLoads": row['# loads'], "PotholeMaintDistrict": md if md != "nan" else None, "PotholeCDList": cdString, "PotholeCD": cd,
                            "PotholeAsphaltAppliedEach": None if isnan(eachPothole) else eachPothole, "PotholeLocationWorking": "", "PotholeComments": row['Clock In'] if type(row['Clock In']) == str else None}
        dictList.append(newRow)
    
    
dictList = []
df1.apply(lambda row: formatRow(row), axis = 1)
combined = pd.DataFrame(dictList)
combined["PotholeDate"] = pd.to_datetime(combined["PotholeDate"]).dt.strftime('%Y-%m-%d')
print(combined)
combined.to_csv("prunedData.csv")

#Connect python script to SQL Server

DRIVER_NAME = 'SQL SERVER'
SERVER_NAME = 'A8605099'
DATABASE_NAME = 'model'

connection_string = f"""
    DRIVER={{{DRIVER_NAME}}};
    SERVER={SERVER_NAME};
    DATABASE={DATABASE_NAME};
    Trust_Connection=yes;
"""
try:
    conn = odbc.connect(connection_string)
except Exception as e:
    print(e)
    print('task is terminated')
    sys.exit()
else:
    cursor = conn.cursor()

def debug(row):
    print(row)
    cursor.execute("""
        INSERT INTO PotholeTable
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, row.values.flatten().tolist())

record = [23456, '1231231', '2019-02-15', 23, 'C', 'MIKE', 'PAM', 12, 3, 12.23, 2, '3', '3,5,6', 4, 2.5, 'YES', 'NO']

try:
    combined.apply(lambda row: debug(row), axis=1)
except Exception as e:
    cursor.rollback()
    print(e.value)
    print("transaction rolled back")
else:
    cursor.commit()
    cursor.close()
finally:
    if conn.connected == 1:
        conn.close()

print(conn.connected)
