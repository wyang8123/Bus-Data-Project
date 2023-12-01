# Go through a Stop Detection File and make a new CSV File with a correct columns and remove any lines without a stop id
import pandas
import sys, json

def getJsonInformation():
    #---- Grbas all information from the information.json file ----
    with open("../Config.json") as configInformation:
        configData = json.load(configInformation)
    return configData

def cleandata(INPUT_FILE, OUTPUT_FILE, jsonData):
    #---- Reads all the INPUT_FILE CSV file and drop any missing values and resets the index (Ensure index is in chronlogical order) ----
    csvFile = pandas.read_csv(INPUT_FILE, on_bad_lines='skip',).reset_index(drop=True).drop_duplicates()
    if "sleep_pin_state" in csvFile.columns:
        csvFile = csvFile.drop(columns=["sleep_pin_state", "fleet_id"])
    csvFile = csvFile.dropna(how="any")
    #csvFile = pandas.read_csv(INPUT_FILE)
    print("----- Cleaning the CSV Data (Removing 0 and empty Columns) -----")
    print("File Being Cleaned: \n", INPUT_FILE)
    print("Initial Data Frame: \n", csvFile)

    #csvFile = csvFile.drop(csvFile.columns[jsonData.get("Drop Columns")], axis=1)
    #csvFile = csvFile.drop(csvFile.columns[-1], axis=1)
    #---- Replaces all 0 or missing data cells with NaN(none) and then drop all None values in the dataset ----
    csvFile.replace(0, float("NaN"), inplace=True)
    csvFile.replace("", float("NaN"), inplace=True)
    csvFile.dropna(how="all", inplace=True)
    first_column = csvFile.iloc[:, 0]
    #---- Finds the length of how many logged table columns and skips the date/time column, which is the first  ----
    csvFile = csvFile.iloc[:, 1:len(jsonData.get("LoggedTableColumns"))]
    print(csvFile)
    #---- Splits the frst column that has combined date/time to two different columns with Date and time ----
    firstcolumnResult = first_column.str.split(" ", expand=True)
    firstcolumnResult.columns = ["date", "time"]
    #print(jsonData.get("LoggedTableColumns")[1:len(jsonData.get("LoggedTableColumns"))])
    #---- Grabs the column from the list stored in the json file ----
    csvFile.columns = jsonData.get("LoggedTableColumns")[1:len(jsonData.get("LoggedTableColumns"))]
    #---- Adds the firstcolumnResult, which contains the date/time from the dropped date/time combined column dataframe ----
    resultcsv = pandas.concat([firstcolumnResult, csvFile], axis=1)
    #---- Writes the datafrmae into a csv file -----
    resultcsv.drop_duplicates()
    resultcsv.to_csv(OUTPUT_FILE, index=False, header=True)
    print("----- Finished Cleaning the CSV Data (Removed all 0s and empty columns) -----")
    print("Fully Cleaned File: \n", OUTPUT_FILE)
    print("Cleaned Data Frame: \n", resultcsv)

if __name__ == "__main__":
    if(len(sys.argv) != 3):
        print("Usage {} [INPUT CSV FILE] [OUTPUT CSV FILE]"  .format(sys.argv[0]))
        exit(1)
    INPUT_FILE = sys.argv[1]
    OUTPUT_FILE = sys.argv[2]
    configData = getJsonInformation()
    cleandata(INPUT_FILE, OUTPUT_FILE, configData)