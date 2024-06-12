from enum import unique
from math import isnan
import sys
import numpy as np
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
    
#Checks if value is a string, then checks if it is longer than sql permits.
def truncateString(string, length):
    if type(string) == str:
        if len(string) > length:
            return string[:length]
        else:
            return string
    else:
        return None

#Used to format maintenance district
def formatMD(mdNum):
    if type(mdNum) == int:
        mdString = str(mdNum)
        return '/'.join(mdString[i:i+3] for i in range(0, len(mdString), 3))
    elif type(mdNum) == str:
        return mdNum
    else:
        return None

recNum = 0
#Adds rows from excel dataframe to new data frame
def formatRow(row):
    global recNum
    recNum += 1
    cdList = findCD(row["CD"])
    string_ints = [str(int) for int in cdList]
    cdString = ",".join(string_ints)
    eachPothole = round(row['SQFT Asphalt Applied'] / len(cdList), 2)
    truckNo = row['Truck #'] if type(row['Truck #']) == int else None
    md = formatMD(row['Maintenance District'])
    potholeLocations = row['# of Locations ']
    potholeCrew1 = truncateString(row['Name of Crew Leader'], 25)
    potholeCrew2 = truncateString(row['Name of 2nd Crew Member'], 25)
    potholeMaintDistrict = truncateString(md, 30)
    asphaltApplied = row['SQFT Asphalt Applied'] 
    global dictList
    for cd in cdList:
        uniqueNo = createNo(row['\n'])
        newRow = {"RecNumber": recNum, "UniqueNumber": uniqueNo, "PotholeDate": row['\n'], "TruckNumber": truckNo, "PotholeZone": row['Zone \n(M or V)'], "PotholeCrew1": potholeCrew1, 
                            "PotholeCrew2": potholeCrew2, "PotholeLaborHrs": row['Total Labor Hours'] if isinstance(row['Total Labor Hours'], float) and not isnan(row['Total Labor Hours']) else None,
                            "PotholeNumLocations": potholeLocations, "PotholeAsphaltApplied": asphaltApplied,
                            "PotholeNumberLoads": None if type(row['# loads']) == str else row['# loads'], "PotholeMaintDistrict": potholeMaintDistrict, "PotholeCDList": cdString, "PotholeCD": cd,
                            "PotholeAsphaltAppliedEach": eachPothole, "PotholeLocationWorking": "", "PotholeComments": row['Clock In'] if type(row['Clock In']) == str else None}
        dictList.append(newRow)
    
    
dictList = []
df1.apply(lambda row: formatRow(row), axis = 1)
combined = pd.DataFrame(dictList)
combined["PotholeDate"] = pd.to_datetime(combined["PotholeDate"]).dt.strftime('%Y-%m-%d')
combined = combined.replace(({np.nan: None}))
print(combined)
combined.to_csv("prunedData.csv")

#Connect python script to SQL Server

DRIVER_NAME = 'SQL SERVER'
SERVER_NAME = 'localhost'
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
    print("\n")
    print('task is terminated')
    sys.exit()
else:
    cursor = conn.cursor()
    if cursor.tables(table='PotholeTable', tableType='TABLE').fetchone():
        print("PotholeTable exists")
    else:
        cursor.execute(
            """CREATE TABLE PotholeTable(RecNumber integer, UniqueNumber varchar(20), PotholeDate varchar(20), TruckNumber float, 
                PotholeZone varchar(2), PotholeCrew1 varchar(30), PotholeCrew2 varchar(30), PotholeLaborHrs float,
                PotholeNumLocations integer, PotholeAsphaltApplied float, PotholeNumberLoads float, PotholeMaintDistrict varchar(30),
                PotholeCDList varchar(20), PotholeCD integer, PotholeAsphaltAppliedEach float, PotholeLocationWorking varchar(20),
                PotholeComments varchar(MAX))"""
        )
def debug(row):
    rowList = row.values.flatten().tolist()
    print(row)
    cursor.execute("""
        INSERT INTO PotholeTable
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rowList)

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
