from geopy import distance
import pytz
import pandas
import datetime
import sys
import json

'''import importlib

def import_module(module_name):
    try:
        return importlib.import_module(module_name)
    except ImportError:
        print(f"Module '{module_name}' not found. Installing it now...")
        import pip
        pip.main(["install", module_name])
        return importlib.import_module(module_name)

pandas = import_module("pandas")'''


#---- User Chanageable Parameters ----
 #change as needed
#UNIX_TIMEZONE_OFFSET_SECONDS = timezone_PST.utcoffset(datetime.datetime.now()).total_seconds()
#print(UNIX_TIMEZONE_OFFSET_SECONDS)

Similiar_Stop_ids = {1341: 1341, 122: 122, 1342: 1342, 2679: 1342, 200: 200, 2375: 2375, 1510: 1510, 2374: 1510, 2739: 2739, 1385: 1385, 2328: 2328, 102: 102, 1505: 1505, 2670: 1505, 2516: 2516, 2448: 2448, 2671: 2448, 2672: 2672, 2673: 1509, 1509: 1509, 1615: 1615, 2674: 1615, 1616: 1616, 2675: 1616, 1617: 1617, 2102: 2102, 2676: 2102, 2101: 2101, 2677: 1501, 1501: 1501, 2669: 2669, 2678: 2669, 120: 120, 101: 101, 100: 100, 201: 201, 299: 201, 2750: 2749, 2749: 2749, 2744: 2744}

#Similiar_Stop_ids = {1341: 1341, 122: 122, 200: 122, 1342: 1342, 2679: 1342, 2375: 1341, 1510: 1510, 2374: 1510, 2739: 2739, 1385: 1385, 2328: 1385, 102: 102, 1505: 1505, 2670: 1505, 2516: 2516, 2448: 2448, 2671: 2448, 2672: 2672, 2673: 1509, 1509: 1509, 1615: 1615, 2674: 1615, 1616: 1616, 2675: 1616, 1617: 1617, 2102: 2102, 2676: 2102, 2101: 2101, 2677: 1501, 1501: 1501, 2669: 2669, 2678: 2669, 101: 101, 100: 100, 201: 201, 299: 201, 2750: 2749, 2749: 2749, 2744: 2744}

def getJsonInformation():
    #---- Grbas all information from the information.json file ----
    with open("Config.json") as configInformation:
        configData = json.load(configInformation)
    return configData

#---- Reads the CSV File places into a pandas dataframe ----
def stopSequence(INPUT_FILE, OUTPUT_FILE, jsonData):
    meters_arrive_threshold = 40
    meters_depart_threshold = 20
    #timezone_PST = pytz.timezone(jsonData.get("TimeZone"))
    #groupids = pandas.read_excel("C:\Users\garfi\Downloads\group_ids.xlsx", engine="openpyxl")
    similiarstopDataFrame = pandas.read_excel(jsonData.get("SimiliarStopids"), engine="openpyxl").reset_index(drop=True)
    groupid, timepointTable = getGroupIDSTimepoint(similiarstopDataFrame)
    stopdetection = pandas.read_csv(INPUT_FILE, on_bad_lines='skip').dropna(how="any").replace(groupid).reset_index(drop=True)
    print("------ Reading Stop Detection File -----")
    print("Filename: \n%s"%(INPUT_FILE))
    print("Inital Data Frame: \n", stopdetection)
    print("Group_id: \n", groupid)
    print("Timepoint_Table:\n", timepointTable)
    list_Actual_Column = stopdetection.columns.tolist()
    #----- Column Names (Just ensures the text is actually what it is)-----
    Date_Column, Time_Column, Stop_lat_Column, Stop_lon_Column, lat_Column, lon_Column, stop_id_Column = getColumnName(list_Actual_Column)
    #---- Grabs all the rows with the column stop_id ----
    stop_group_dataframe = stopdetection[stop_id_Column].dropna(how="all").reset_index(drop=True)
    #---- Compares the stop_id rows and if they are different then place the index of the change into a list ----
    change_points = stopdetection.loc[stop_group_dataframe.shift() != stop_group_dataframe].index.tolist()
    FinalStops= []
    #---- Creates a subset at the points where the stop id changes ----
    for x in range(len(change_points)-1):
        subset = stopdetection.loc[int(change_points[x]):int(change_points[x+1])-1]
        Distance_KM_List_Routes = []
        #---- Finds the distance using geopy and lat and lon and appends it into a a temp list ----
        for y in range(subset.shape[0]):
            LocatedRow = subset.iloc[y]
            KM_Distance = distance.distance((LocatedRow[Stop_lat_Column], LocatedRow[Stop_lon_Column]), (LocatedRow[lat_Column], LocatedRow[lon_Column])).km * 1000
            Distance_KM_List_Routes.append(float(KM_Distance))
        #---- Finds the minimum and if its less then the arrive threshold then arrived bus ----
        #---- Calcualtes the arrival and depature time ----
        if min(Distance_KM_List_Routes) <= meters_arrive_threshold:
            #---- Finds the first row of the subset dataframe and finds the date and time and creates a timestamp (Which is arrival)----
            Arrival_dataframe= subset.iloc[0]
            stopid = Arrival_dataframe[stop_id_Column]
            Arrival_Date = Arrival_dataframe[Date_Column]
            Arrival_Time = Arrival_dataframe[Time_Column]
            try: 
                Arrival_Formated_Date = datetime.datetime.strptime(str(Arrival_Date) + " " + str(Arrival_Time), "%Y-%m-%d %H:%M:%S")
            except Exception as e:
                try: 
                    Arrival_Formated_Date = datetime.datetime.strptime(str(Arrival_Date) + " " + str(Arrival_Time) + ":00", "%Y-%m-%d %H:%M:%S")
                except Exception as e:
                    Arrival_Formated_Date = None
            Arrival_Unix_Timestamp = datetime.datetime.timestamp(Arrival_Formated_Date)
            #---- Determines the fleet Number through the filename and finds the second(first_index) of "_" and splits the . to take out the file ending ----
            fleetNumber = INPUT_FILE.split("_")[1].split(".")[0]
            try:
                int(fleetNumber)
            except ValueError as e:
                print(e)
                fleetNumber = INPUT_FILE.split("_")[0].split("clean")[1]
            #---- Determines if a stopid is timpoint and if it is makes the timepoint 1 ----
            is_timepoint = 0
            if stopid in timepointTable:
                is_timepoint = 1
            #print(INPUT_FILE)
            #print(fleetNumber)
            #print("Time", Arrival_Unix_Timestamp)
            #print("Time2", datetime.datetime.fromtimestamp(Arrival_Unix_Timestamp))
            #---- Finds the last row of the subset dataframe and finds the date and time and creates a timestamp (Which is depature) ----
            Departure_dataframe = subset.iloc[-1]
            #---- Gets the Date and time of the depature_dataframe and formats the string readable time to a timestamp ----
            Departure_Date = Departure_dataframe[Date_Column]
            Departure_Time = Departure_dataframe[Time_Column]
            Departure_Formated_Date = datetime.datetime.strptime(str(Departure_Date) + " " + str(Departure_Time), "%Y-%m-%d %H:%M:%S")
            Departure_Unix_Timestamp = datetime.datetime.timestamp(Departure_Formated_Date)
            #---- Grabs the final timestamp for the depature and arrival and places into a list ----
            Final_Stop_Groups = [Arrival_dataframe[stop_id_Column], str(datetime.datetime.fromtimestamp(Arrival_Unix_Timestamp)), str(datetime.datetime.fromtimestamp(Departure_Unix_Timestamp)), fleetNumber, is_timepoint]
            FinalStops.append(Final_Stop_Groups)
    #---- Creates a dataframe with the following columns and creates a CSV file----
    Output_DataFrame = pandas.DataFrame(FinalStops, columns=["group_id", "arrive_time", "depart_time", "Fleet_Number", "Timepoint"])
    Output_DataFrame.drop_duplicates()
    Output_DataFrame.to_csv(OUTPUT_FILE, index=False)
    print("----- OUTPUT Data Frame -----")
    print("Filename: \n%s"%(OUTPUT_FILE))
    print("Stop Sequence Data Frame: \n", Output_DataFrame)

def getGroupIDSTimepoint(SimiliarIdsDataFrame):
    timepoint_stops = SimiliarIdsDataFrame.loc[SimiliarIdsDataFrame['timepoint'] == 1, 'primary_stop'].tolist()
    groupids = SimiliarIdsDataFrame.iloc[:,2:]
    # Create an empty dictionary to store the matches
    stop_dict = {}
    groupids_columns = list(groupids.columns)
    # Iterate over the rows of the DataFrame
    for index, row in groupids.iterrows():
        primary_stop = int(row['primary_stop'])
        # Add the primary_stop to the dictionary with itself as the value
        stop_dict[primary_stop] = primary_stop
        for x in range(1,len(groupids_columns)):
            if pandas.notnull(row[groupids_columns[x]]):
                stop_dict[int(row[groupids_columns[x]])] = primary_stop
    Similiar_Stop_ids = stop_dict
    groupid = {}
    for key, value in Similiar_Stop_ids.items():
        new_key = float(key)
        groupid[new_key] = value
    #---- Returns group_id and all stop_ids that are valid timepoints ----
    return groupid, timepoint_stops

def getColumnName(list_Actual_Column):
    #---- Gets all column names (Ensures the column names are correctly labeled) ----
    Date_Column = ""
    Time_Column = ""
    Stop_lat_Column = ""
    Stop_lon_Column = ""
    lat_Column = ""
    lon_Column = ""
    stop_id_Column = ""
    for x in list_Actual_Column:
        if "time" in str(x):
            Time_Column = str(x)
        elif "date" in str(x):
            Date_Column = str(x)
        elif "stop_lat" in str(x):
            Stop_lat_Column = str(x)
        elif "stop_lon" in str(x):
            Stop_lon_Column = str(x)
        elif "lat" in str(x):
            lat_Column = str(x)
        elif "lon" in str(x):
            lon_Column = str(x)
        elif "stop_id" in str(x):
            stop_id_Column = str(x)
    return Date_Column, Time_Column, Stop_lat_Column, Stop_lon_Column, lat_Column, lon_Column, stop_id_Column

if __name__ == "__main__":
    if(len(sys.argv) != 3):
        print("Usage {} [INPUT CSV FILE] [OUTPUT CSV FILE]"  .format(sys.argv[0]))
        exit(1)
    INPUT_FILE = sys.argv[1]
    OUTPUT_FILE = sys.argv[2]
    jsonData = getJsonInformation
    stopSequence(INPUT_FILE, OUTPUT_FILE, jsonData)