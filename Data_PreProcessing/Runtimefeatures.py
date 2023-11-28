import pandas, sys
from datetime import datetime
from RemoveoutlierTrips import *
import numpy, openpyxl, json, os, multiprocessing

def getJsonInformation():
    #---- Grbas all information from the information.json file ----
    with open("../Config.json") as configInformation:
        configData = json.load(configInformation)
    return configData

def runtimefeatures(INPUT_FILE, OUTPUT_FILE, jsonData, githubtest=False):
    finalDataFrame = pandas.read_csv(INPUT_FILE, on_bad_lines='skip').dropna(how="any").reset_index(drop=True).drop_duplicates()
    print(finalDataFrame)
    #---- Change runtime period dataframe to timestamp format similiar to the logged data ----
    runtimeperiods = pandas.read_excel(jsonData.get("RunTimePeriodFile"), engine="openpyxl")
    daysofcolumn = jsonData.get("DaysColumn")
    bigData = []
    for x in range(len(daysofcolumn)):
        firstDataFrame = runtimeperiods[daysofcolumn[x]]
        #print("first",firstDataFrame)
        firstdatatime = pandas.to_datetime(firstDataFrame, format="%H%M")
        firstdatatime = firstdatatime.dropna()
        bigData.append((daysofcolumn[x], firstdatatime))

    
    #---- Append day of week ----
    days = {0:"M", 1:"Tu", 2:"W", 3:"Th", 4:"F", 5:"Sa", 6:"Su"}
    arrivaldataFrame = finalDataFrame["arrive_time"]
    timestamplist = []
    for x in arrivaldataFrame:
        timestamp = datetime.strptime(x.split(" ")[0], "%Y-%m-%d")
        timestamplist.append(days.get(timestamp.weekday()))
    #print(timestamplist)
    finalDataFrame["dayofweek"] = timestamplist
    

    #---- Append the time Sequence ----
    finalDataFrame["Sequence"] = [0 for x in range(len(timestamplist))]
    for name, timeDataFrame in bigData: 
        timesubset = finalDataFrame[finalDataFrame["dayofweek"].apply(lambda x: x in name)]
        arrivaldataFrame = pandas.to_datetime(timesubset["arrive_time"], format="%Y-%m-%d %H:%M:%S")
        arrivaldataFrame1 = arrivaldataFrame.apply(lambda x: x.replace(year=1990, month=1, day=1))
        timeDataFrame = timeDataFrame.apply(lambda x: x.replace(year=arrivaldataFrame1.dt.year.iloc[0], month=arrivaldataFrame1.dt.month.iloc[0], day=arrivaldataFrame1.dt.day.iloc[0]))
        for x in range(len(timeDataFrame)-1):
            subset_value = arrivaldataFrame1[(arrivaldataFrame1 >= timeDataFrame.iloc[x]) & (arrivaldataFrame1 <= timeDataFrame.iloc[x+1])]
            finalDataFrame.loc[subset_value.index.tolist(), "Sequence"] = str(datetime.strftime(timeDataFrame.iloc[x], "%H:%M:%S")) + " - " + str(datetime.strftime(timeDataFrame.iloc[x+1], "%H:%M:%S"))
        subset_value = arrivaldataFrame1[(arrivaldataFrame1 >= timeDataFrame.iloc[-1])]
        finalDataFrame.loc[subset_value.index.tolist(), "Sequence"] = str(datetime.strftime(timeDataFrame.iloc[-1], "%H:%M:%S"))
        subset_value = arrivaldataFrame1[(arrivaldataFrame1 <= timeDataFrame.iloc[0])]
        finalDataFrame.loc[subset_value.index.tolist(), "Sequence"] = str(datetime.strftime(timeDataFrame.iloc[0], "%H:%M:%S"))
    finalDataFrame.to_csv(OUTPUT_FILE, index=False, header=True)
    print("---- Added useful Features ----")
    print("Created Filename with more useful features: \n", OUTPUT_FILE)
    print("Final Data Frame: \n", finalDataFrame)
    return finalDataFrame


if __name__ == "__main__":
    print(len(sys.argv))
    if(len(sys.argv) != 3):
        print("Usage {} [INPUT CSV FILE] [OUTPUT CSV FILE]"  .format(sys.argv[0]))
        exit(1)
    INPUT_FILE = sys.argv[1]
    OUTPUT_FILE = sys.argv[2]
    configData = getJsonInformation()
    runtimefeatures(INPUT_FILE, OUTPUT_FILE, configData)