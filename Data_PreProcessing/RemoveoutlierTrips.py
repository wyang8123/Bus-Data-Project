from datetime import datetime
from datetime import timedelta
from scipy.stats import zscore
import pandas, openpyxl, json, sys, numpy
import multiprocessing

def getJsonInformation():
    #---- Grbas all information from the information.json file ----
    with open("Config.json") as configInformation:
        configData = json.load(configInformation)
    return configData

#---- Parallel Processing to make the code faster using multiprocess ----
def parallelprocessTimeSegments(argv):
    try: 
        x, y, stopsequenceids, row_numbers_with_ones, groupid, listrow, listvalue, ArrivalDataFrameList, dictionarystopsequences, DepartureDataFrameList, TimeType, stopsequencetimes, stopsequencerows = [argv[i] for i in range(0, len(argv))]
        CalculateRunTimeSegments(x, stopsequenceids, row_numbers_with_ones, groupid, listrow, listvalue, ArrivalDataFrameList, dictionarystopsequences, DepartureDataFrameList, TimeType, stopsequencetimes, stopsequencerows)
    except Exception as e:
        print(e)
        x, y, stopsequenceids, row_numbers_with_ones, groupid, listrow, listvalue, ArrivalDataFrameList, dictionarystopsequences, DepartureDataFrameList, TimeType, stopsequencetimes, stopsequencerows = [argv[i] for i in range(0, len(argv))]
        CalculateRunTimeSegments(x, stopsequenceids, row_numbers_with_ones, groupid, listrow, listvalue, ArrivalDataFrameList, dictionarystopsequences, DepartureDataFrameList, TimeType, stopsequencetimes, stopsequencerows)


#---- Calculates the Run Time Segments ----
def CalculateRunTimeSegments(x, y, stopsequenceids, row_numbers_with_ones, groupid, listrow, listvalue, ArrivalDataFrameList, dictionarystopsequences, DepartureDataFrameList, TimeType, stopsequencetimes, stopsequencerows):
    # ---- Checks if the first possible timepoint is the first stop sequence id ----
        if float(groupid.loc[row_numbers_with_ones[y]]) == stopsequenceids[0]:
            #print("first group", groupid.loc[row_numbers_with_ones[y]])
            #print("stopid", stopsequenceids[0])
            #rint("Row Number", row_numbers_with_ones[y])
            #---- Calculates the firsttime and appends the row number and calcualtes the arrival time or depature time as the first time ----
            listrow.append(row_numbers_with_ones[y])
            firstime = datetime.timestamp(datetime.strptime(ArrivalDataFrameList.loc[row_numbers_with_ones[y]], "%Y-%m-%d %H:%M:%S"))
            if dictionarystopsequences.get(x)[0] == "arrive":
                listvalue.append("arrive")
                firstime = datetime.timestamp(datetime.strptime(ArrivalDataFrameList.loc[row_numbers_with_ones[y]], "%Y-%m-%d %H:%M:%S"))
            if dictionarystopsequences.get(x)[0] == "depart":
                listvalue.append("depart")
                firstime = datetime.timestamp(datetime.strptime(DepartureDataFrameList.loc[row_numbers_with_ones[y]], "%Y-%m-%d %H:%M:%S"))
            #print(stopsequenceids)
            #---- Checks if the stop id sequence contains a group exception value ----
            if len(stopsequenceids) == 3:
                #---- Checks to see if ignoring the group excpetion and going to the next stop id doesn't exceed the length ----
                if y+2 != len(row_numbers_with_ones):
                    #---- Checks to see if the current second stop id is equalivalent to the group exception stop id ---
                    if float(groupid.loc[row_numbers_with_ones[y+1]]) == stopsequenceids[2]:
                        #---- Checks to see if the next group ignoring the group exception is actual the stop id we are looking for ----
                        if float(groupid.loc[row_numbers_with_ones[y+2]]) == stopsequenceids[1]:
                            #print("second group", groupid.loc[row_numbers_with_ones[y+1]])
                            #print("stopidlen3", stopsequenceids[2])
                            #print("Row Number", row_numbers_with_ones[y+1])
                            #---- Appends the second time and accuarely appends the values. Also takes secondtime - firsttime to calculate the accurate time difference ----
                            listrow.append(row_numbers_with_ones[y+2])
                            secondtime = datetime.timestamp(datetime.strptime(ArrivalDataFrameList.loc[row_numbers_with_ones[y+2]], "%Y-%m-%d %H:%M:%S"))
                            if dictionarystopsequences.get(x)[-1] == "arrive":
                                listvalue.append("arrive")
                                secondtime = datetime.timestamp(datetime.strptime(ArrivalDataFrameList.loc[row_numbers_with_ones[y+2]], "%Y-%m-%d %H:%M:%S"))
                            if dictionarystopsequences.get(x)[-1] == "depart":
                                listvalue.append('depart')
                                secondtime = datetime.timestamp(datetime.strptime(DepartureDataFrameList.loc[row_numbers_with_ones[y+2]], "%Y-%m-%d %H:%M:%S"))
                            #print("Second time", secondtime)
                            #print("First time", firstime)
                            TimeType.get(x).append(listvalue)
                            stopsequencetimes.get(x).append(secondtime-firstime)
                            stopsequencerows.get(x).append(listrow)
                            #print("stoptimes1 ", stopsequencetimes)
                            #print("stoprows1 ", stopsequencerows)
            #---- Checks if the length doesn't equal 3 ----
            if len(stopsequenceids) != 3:
                #---- Checks if the next stop id is equalvalent to the group id ----
                if float(groupid.loc[row_numbers_with_ones[y+1]]) == stopsequenceids[1]:
                    #print("second group", groupid.loc[row_numbers_with_ones[y+1]])
                    #print("stopid", stopsequenceids[1])
                    #print("Row Number", row_numbers_with_ones[y+1])
                    #---- Appends the valuees appropriatly for the secondTime ----
                    listrow.append(row_numbers_with_ones[y+1])
                    secondtime = datetime.timestamp(datetime.strptime(ArrivalDataFrameList.loc[row_numbers_with_ones[y+1]], "%Y-%m-%d %H:%M:%S"))
                    if dictionarystopsequences.get(x)[-1] == "arrive":
                        listvalue.append("arrive")
                        secondtime = datetime.timestamp(datetime.strptime(ArrivalDataFrameList.loc[row_numbers_with_ones[y+1]], "%Y-%m-%d %H:%M:%S"))
                    if dictionarystopsequences.get(x)[-1] == "depart":
                        listvalue.append("depart")
                        secondtime = datetime.timestamp(datetime.strptime(DepartureDataFrameList.loc[row_numbers_with_ones[y+1]], "%Y-%m-%d %H:%M:%S"))
                    #print("Second time", secondtime)
                    #print("First time", firstime)
                    TimeType.get(x).append(listvalue)
                    stopsequencetimes.get(x).append(secondtime-firstime)
                    stopsequencerows.get(x).append(listrow)
                    #print("stoptimes2 ", stopsequencetimes)
                    #print("stoprows2 ", stopsequencerows)



def removeOutliers(INPUT_FILE, OUTPUT_FILE, jsonData):
    dictionarystopsequences = {}
    #---- Grabs all information from the input file, reset index, dropna ----
    csvdataframe = pandas.read_csv(INPUT_FILE, on_bad_lines='skip').dropna(how="any").reset_index(drop=True).drop_duplicates()
    #---- Grabs all information from the stop sequence file to generate stop sequences ----
    stopsequences = pandas.read_excel(jsonData.get("StopSequenceFile"), engine="openpyxl").reset_index(drop=True).drop_duplicates()
    print("----- Removing Outliers -----")
    print("Removing Outliers from: \n", INPUT_FILE)
    print("Inital Data Frame: \n", csvdataframe)
    print("stop sequence Data Frame: \n", stopsequences)
    #sequenceName = stopsequences["sequence_name"].to_list()
    #print("SequenceName: ", sequenceName)




    for x in range(len(stopsequences)):
        smalllist = [stopsequences["first_time_type"][x], stopsequences["first_group_id"][x], stopsequences["second_group_id"][x], stopsequences["group_exception"][x], stopsequences["second_time_type"][x]]
        dictionarystopsequences.update({stopsequences["sequence_name"][x]:smalllist})
    print("Dictionary:", dictionarystopsequences)

    #---- Grabs the arrival and departure time for the input dataframe ----
    ArrivalDataFrameList = csvdataframe["arrive_time"]
    DepartureDataFrameList = csvdataframe["depart_time"]
    #FleetIDDataFrameList = csvdataframe["fleet_id"].to_list()
    timepoint = csvdataframe["Timepoint"]
    groupid = csvdataframe["group_id"]
    #fleetNumberList = csvdataframe["Fleet_Number"].to_list()
    stopsequencetimes = {}
    TimeType = {}
    stopsequencerows = {}
    row_numbers_with_ones = timepoint[timepoint == 1].index.tolist()
    for x in dictionarystopsequences:
        TimeType.update({x:[]})
        stopsequencetimes.update({x:[]})
        stopsequencerows.update({x:[]})
    #for y in range(len(row_numbers_with_ones)-1):
    processinglist = []
    for y in range(len(row_numbers_with_ones)-1):
        for x in dictionarystopsequences:
            listvalue = []
            listrow = []
            print("group", x)
            #print("First possible group", groupid.loc[row_numbers_with_ones[y]])
            #print("Second possible group", groupid.loc[row_numbers_with_ones[y+1]])
            stopsequenceids = [float(id) for id in dictionarystopsequences.get(x)[1:-1] if not numpy.isnan(id)]
            print("stop sequence", stopsequenceids)
            #processinglist.append([x,y, stopsequenceids, row_numbers_with_ones, groupid, listrow, listvalue, ArrivalDataFrameList, dictionarystopsequences, DepartureDataFrameList, TimeType, stopsequencetimes, stopsequencerows])
            CalculateRunTimeSegments(x,y, stopsequenceids, row_numbers_with_ones, groupid, listrow, listvalue, ArrivalDataFrameList, dictionarystopsequences, DepartureDataFrameList, TimeType, stopsequencetimes, stopsequencerows)
    numofcpus = len(processinglist)
    if len(processinglist) >= multiprocessing.cpu_count()-4:
        numofcpus = multiprocessing.cpu_count()-4
    #p = multiprocessing.Pool(numofcpus)
    #p.map(parallelprocessTimeSegments, processinglist)
        #grabdayinformation(dayofweeklist, daylist, bigData, finalDataFrame, listofgroups, jsonData)
    #exitinput = "exit"
    
    '''for x in stopsequences.columns.to_list():
        dictionarystopsequences.update({x:stopsequences[x].dropna(how="any").to_list()})
    #---- Grabs the arrival and departure time for the input dataframe ----
    ArrivalDataFrameList = csvdataframe["arrive_time"].to_list()
    DepartureDataFrameList = csvdataframe["depart_time"].to_list()
    #FleetIDDataFrameList = csvdataframe["fleet_id"].to_list()
    groupid = csvdataframe["group_id"].to_list()
    #fleetNumberList = csvdataframe["Fleet_Number"].to_list()
    stopsequencetimes = {}
    stopsequencerows = {}

    for x in dictionarystopsequences:
        stopsequencetimes.update({x:[]})
        stopsequencerows.update({x:[]})
        stopsequenceids = [float(id) for id in dictionarystopsequences.get(x)[1:-1]]
        for y in range(0, len(groupid), len(stopsequenceids)):
            if groupid[y:y+len(stopsequenceids)] == stopsequenceids:
                firstime = datetime.strptime(DepartureDataFrameList[0], "%Y-%m-%d %H:%M:%S")
                secondtime = datetime.strptime(DepartureDataFrameList[0], "%Y-%m-%d %H:%M:%S")
                #print("firststoptype", dictionarystopsequences.get(x)[0])
                #print("laststoptype", dictionarystopsequences.get(x)[len(stopsequenceids)+1])
                if dictionarystopsequences.get(x)[0] == "depart":
                    #print("seconddepart")
                    #stringtime = DepartureDataFrameList[y].split(" ")[1]
                    secondtime = datetime.timestamp(datetime.strptime(DepartureDataFrameList[y], "%Y-%m-%d %H:%M:%S"))
                    #secondtime = datetime.timestamp(datetime.strptime(stringtime, "%H:%M:%S"))
                if dictionarystopsequences.get(x)[0] == "arrive":
                    #print("secondarrive")
                    stringtime = ArrivalDataFrameList[y].split(" ")[1]
                    secondtime = datetime.timestamp(datetime.strptime(ArrivalDataFrameList[y], "%Y-%m-%d %H:%M:%S"))
                    #secondtime = datetime.timestamp(datetime.strptime(stringtime, "%H:%M:%S"))
                if dictionarystopsequences.get(x)[len(stopsequenceids)+1] == "depart":
                    stringtime = DepartureDataFrameList[y+len(stopsequenceids)].split(" ")[1]
                    #print("first depature")
                    #firstime = datetime.timestamp(datetime.strptime(stringtime, "%H:%M:%S"))
                    firstime = datetime.timestamp(datetime.strptime(DepartureDataFrameList[y+len(stopsequenceids)], "%Y-%m-%d %H:%M:%S"))
                if dictionarystopsequences.get(x)[len(stopsequenceids)+1] == "arrive":
                    #print("firstarrive")
                    #firstime = datetime.timestamp(datetime.strptime(ArrivalDataFrameList[y+len(stopsequenceids)].split(" ")[1], "%H:%M:%S"))
                    firstime = datetime.timestamp(datetime.strptime(ArrivalDataFrameList[y+len(stopsequenceids)], "%Y-%m-%d %H:%M:%S"))
                stopsequencetimes.get(x).append(secondtime-firstime)
                #print("Secondtime", secondtime)
                #print("firsttime", firstime)
                #print("timesdifferencecalc", stopsequencetimes)
                stopsequencerows.get(x).append([y, y+len(stopsequenceids)])'''
    





    #---- Does a z-score estimation and removes any large outlier ----
    #print("Stop sequences", stopsequencetimes)
    #print("Stop sequence rows", stopsequencerows)
    
    finalstopsequnecetimes = {}
    for x in stopsequencetimes:
        normalizedtime = []
        stopsequencenormalize = zscore(stopsequencetimes.get(x))
        for y in range(len(stopsequencenormalize)):
            if abs(stopsequencenormalize[y]) < 4:
                normalizedtime.append(stopsequencerows.get(x)[y])
                #timedifference.append(stopsequencetimes.get(x)[y])
        finalstopsequnecetimes.update({x:normalizedtime})
    #print("StopSequences", stop)
    #print("Time", timedifference)
    #print("finalStop",finalstopsequnecetimes)
    #---- Adds all group names to the dataframe and creates a larger data frame ----
    finalDataFrame = pandas.DataFrame()
    for x in finalstopsequnecetimes:
        largedataframe = pandas.DataFrame()
        for y in range(len(finalstopsequnecetimes.get(x))):
            SmallDataFrame = csvdataframe.loc[finalstopsequnecetimes.get(x)[y][0]:finalstopsequnecetimes.get(x)[y][1]] 
            timedifference = [timedelta(seconds = abs(stopsequencetimes.get(x)[y])) for z in range(finalstopsequnecetimes.get(x)[y][1]-finalstopsequnecetimes.get(x)[y][0]+1)]
            timetpye = [None for z in range(finalstopsequnecetimes.get(x)[y][1]-finalstopsequnecetimes.get(x)[y][0] +1)]
            timetpye[0] = TimeType.get(x)[y][0]
            timetpye[-1] = TimeType.get(x)[y][-1]
            #print(timetpye)
            listofx = [x for z in range(finalstopsequnecetimes.get(x)[y][1]-finalstopsequnecetimes.get(x)[y][0] + 1)]
            typedataframe = pandas.DataFrame(timetpye, columns=["Type Time"])
            xdataframe = pandas.DataFrame(listofx, columns=["Group_Name"])
            timediffdataframe = pandas.DataFrame(timedifference, columns=["Time Difference"])
            #typedataframe.to_csv("test.csv")
            #ydataframe = pandas.concat([typedataframe.reset_index(drop=True), xdataframe.reset_index(drop=True)], axis=1)
            xdataframe = pandas.concat([timediffdataframe.reset_index(drop=True), typedataframe.reset_index(drop=True), xdataframe.reset_index(drop=True)], axis=1)
            SmallDataFrame = pandas.concat([SmallDataFrame.reset_index(drop=True), xdataframe.reset_index(drop=True)], axis=1)
            largedataframe = pandas.concat([largedataframe, SmallDataFrame], axis=0)
        finalDataFrame = pandas.concat([finalDataFrame, largedataframe.reset_index(drop=True)], axis=0)
        finalDataFrame.drop_duplicates()
    finalDataFrame.to_csv(OUTPUT_FILE, index=False, header=True)
    print("---- Removed all outliers ----")
    print("Created Filename with all removed outliers: \n", OUTPUT_FILE)
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
    removeOutliers(INPUT_FILE, OUTPUT_FILE, configData)