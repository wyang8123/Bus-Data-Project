import sys
import pandas as pd

def filtertrain(INPUT_FILE, TRAINING_FILE):
    filtered_training_data = pd.read_csv(INPUT_FILE)
    filtered_training_data = filtered_training_data[filtered_training_data["arrive_time"].apply(lambda x: x.split("-")[1] in ["10", "11"])]
    filtered_training_data = filtered_training_data[filtered_training_data["depart_time"].apply(lambda x: x.split("-")[1] in ["10", "11"])]
    filtered_training_data.to_csv(TRAINING_FILE, index=False, header=True)


if __name__ == "__main__":
    print(len(sys.argv))
    if(len(sys.argv) != 3):
        print("Usage {} [INPUT CSV FILE] [OUTPUT CSV FILE]"  .format(sys.argv[0]))
        exit(1)
    INPUT_FILE = sys.argv[1]
    OUTPUT_FILE = sys.argv[2]
    #configData = getJsonInformation()
    filtertrain(INPUT_FILE, OUTPUT_FILE)