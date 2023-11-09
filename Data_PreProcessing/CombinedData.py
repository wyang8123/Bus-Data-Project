import pandas
import sys, os

def combinedFiles(FIRST_FILE, OUTPUT_FILE):
    #---- Reads the csv file and skips all bad lines (just in case) ----
    firstcsvdataframe = pandas.read_csv(FIRST_FILE, on_bad_lines='skip').reset_index(drop=True).drop_duplicates()
    print("------ Combining Multiple CSV Files ------")
    print("File needed to be added: \n", FIRST_FILE)
    print("Inital Data Frame: \n", firstcsvdataframe)

    if os.path.exists(OUTPUT_FILE):
        #---- Checks if OUTPUT_FILE exists as it will need to read the csv file and then concat the value to the output csv file ----
        secondcsvdataframe = pandas.read_csv(OUTPUT_FILE, on_bad_lines='skip').reset_index(drop=True).drop_duplicates()
        #---- Concats the input dataframe to the output dataframe read from the output CSV file ----
        combinedDataFrmae = pandas.concat([firstcsvdataframe, secondcsvdataframe], axis=0)
    else: 
        #---- Just concat the input dataframe if no output file currently exists
        combinedDataFrmae = pandas.concat([firstcsvdataframe], axis=0)
    #---- Drop any duplicates in the dataframe and writes it to the output csv file ----
    combinedDataFrmae.drop_duplicates()
    combinedDataFrmae.to_csv(OUTPUT_FILE, index=False, header=True)
    print("----- Finished Combining the files -----")
    print("Combined Filename: \n", OUTPUT_FILE)
    print("Combined Data Frame: \n", combinedDataFrmae)

if __name__ == "__main__":
    if(len(sys.argv) != 3):
        print("Usage {} [FIRST CSV FILE] [OUTPUT CSV FILE]"  .format(sys.argv[0]))
        exit(1)
    FIRST_FILE = sys.argv[1]
    OUTPUT_FILE = sys.argv[2]
    combinedFiles(FIRST_FILE, OUTPUT_FILE)