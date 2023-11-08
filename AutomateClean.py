import sys, os, shutil
from StopSequnceGenerator import *
#from AIDataAnalysis import *
from CombinedData import *
from CleanData import *
#from CleaningAggregation import *

import json, multiprocessing

def processFolderInformation(argv):
    try: 
        root, filename, ArchivedFolder, CombinedOutputCSVFIle, jsonData = [argv[i] for i in range(0, len(argv))]
        cleandata(os.path.join(root, filename), os.path.join(ArchivedFolder, "clean" + filename), jsonData)
        #pruningData(os.path.join(ArchivedFolder, "clean" + filename), os.path.join(ArchivedFolder+"/Direction", "CleanDirection" + filename))
        stopSequence(os.path.join(ArchivedFolder, "clean" + filename), os.path.join(ArchivedFolder, "stopdetect" + filename), jsonData)
        combinedFiles(os.path.join(ArchivedFolder, "stopdetect" + filename), os.path.join(ArchivedFolder, CombinedOutputCSVFIle))
    except Exception as e:
        print(e)
        root, filename, ArchivedFolder, CombinedOutputCSVFIle, jsonData = [argv[i] for i in range(0, len(argv))]
        cleandata(os.path.join(root, filename), os.path.join(ArchivedFolder, "clean" + filename), jsonData)
        #pruningData(os.path.join(ArchivedFolder, "clean" + filename), os.path.join(ArchivedFolder+"/Direction", "CleanDirection" + filename))
        stopSequence(os.path.join(ArchivedFolder, "clean" + filename), os.path.join(ArchivedFolder, "stopdetect" + filename), jsonData)
        combinedFiles(os.path.join(ArchivedFolder, "stopdetect" + filename), os.path.join(ArchivedFolder, CombinedOutputCSVFIle))
    

def main(Folderbustop, ArchivedFolder, CombinedOutputCSVFIle, outputcsv, jsonData, githubTest=False):
    #---- Attemps to make directory for ArchivedFolder (Catches error if folder already exists and continues) ----
    try:
        os.mkdir(ArchivedFolder)
    except OSError as error:
        print(error)
        print("Folder Already exists")
    try: 
        os.mkdir(ArchivedFolder+"/Direction")
    except OSError as error:
        print("Folder already exists")
    numofcpus = 0
    processinglist = []
    #---- Goes through the full directory for the folderbusstop (Grabs the root(which is parent path) and the files in the directory) ----
    for root, dirs, files in os.walk(Folderbustop):
        for filename in files:
            #---- Grabs all files with a .csv filename ----
            if ".csv" in filename:
                if not githubTest:
                    processinglist.append([root, filename, ArchivedFolder, CombinedOutputCSVFIle, jsonData])
                #processFolderInformation([root, filename, ArchivedFolder, CombinedOutputCSVFIle, jsonData])
                #---- Checks to see if it was previous months (Ex: 2023-04-28 logged Data (As filename was very much different)) ----
                if githubTest:
                    if "_StopDetectionReads" in filename:
                        #---- Grabs the fleetNumber from the beginning of the filename ----
                        fleetNumber = filename.split("_")[0]
                        #---- Makes a copy of the filename and chagnes it to current filenames being used ----
                        shutil.copyfile(os.path.join(root, filename), os.path.join(root,"StopDetectionReads_" + fleetNumber + ".csv"))
                        #---- Uses that file instead to continue with the cleandata. ----
                        cleandata(os.path.join(root,"StopDetectionReads_" + fleetNumber + ".csv"), os.path.join(ArchivedFolder, "clean" + filename), jsonData)
                        #shutil.move(os.path.join(root, fleetNumber + "_StopDetectionReads.csv"), os.path.join(ArchivedFolder, fleetNumber + "_StopDetectionReads.csv"))
                    #print(os.path.join(root, filename))
                    else:
                        #---- More highly recommmend with this method ----
                        #---- Calls each method in the list and performs the actions (located in different files) ----
                        cleandata(os.path.join(root, filename), os.path.join(ArchivedFolder, "clean" + filename), jsonData)
                    stopSequence(os.path.join(ArchivedFolder, "clean" + filename), os.path.join(ArchivedFolder, "stopdetect" + filename), jsonData)
                    combinedFiles(os.path.join(ArchivedFolder, "stopdetect" + filename), os.path.join(ArchivedFolder, CombinedOutputCSVFIle))
        #---- Splits the process to multiple cpus for faster creation of files ----
        numofcpus = len(processinglist)
        if len(processinglist) >= multiprocessing.cpu_count()-2:
            numofcpus = multiprocessing.cpu_count()-2
        if not githubTest:
            p = multiprocessing.Pool(numofcpus)
            p.map(processFolderInformation, processinglist)
       #---- Copys all folders in the folderbusstop folder given in the config.json to the archviedbusStop Folder to ensure multiple copies ----
        for item in os.listdir(Folderbustop):
            source = os.path.join(Folderbustop, item)
            destination = os.path.join(ArchivedFolder, item)
            shutil.copy2(source, destination)
            print("Copied", source, "to", destination)
        #---- Calls the data analysis method and file to handle data analysis ----
        #removeOutliers(os.path.join(ArchivedFolder, CombinedOutputCSVFIle), os.path.join(ArchivedFolder, outputcsv), jsonData)
        #regression(os.path.join(ArchivedFolder, outputcsv), jsonData, githubTest)

if __name__ == "__main__":
    print(sys.argv)
    #if(len(sys.argv) != 5):
    #    print("Usage {} [FOLDER FOR BUS STOP DETECTIONS] [ARCHIVED FOLDER] [COMBINED CSV FILE] [OUTPUT CSV FILE]"  .format(sys.argv[0]))
    #    exit(1)
    #Folderbustop = sys.argv[1]
    #ArchivedFolder = sys.argv[2]
    #CombinedOutputCSVFIle = sys.argv[3]
    #outputcsv = sys.argv[4]
    githubtest = False
    if len(sys.argv) > 1:
        githubteststring = sys.argv[1].lower()
        if githubteststring == "true":
            githubtest = True
    configData = getJsonInformation()
    Folderbustop = configData.get("Stop Detection Folder")
    ArchivedFolder = configData.get("Archived Folder")
    CombinedOutputCSVFIle = configData.get("Combined CSV File")
    outputcsv = configData.get("Output CSV File")
    main(Folderbustop, ArchivedFolder, CombinedOutputCSVFIle, outputcsv, configData, githubtest)
