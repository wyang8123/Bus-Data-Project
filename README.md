## **Bus Schedule Analysis**
It is important to have schedules that represent the data from the bus. We will generate an accurate bus schedule for TAPS based on the time and the day of the week. 

### **Project Directory**
- [Data_PreProcessing](./Data_PreProcessing/)
    - Folder containing all the preprocessing for the TAPS Bus Data
    - [CleanData.py](./Data_PreProcessing/CleanData.py)
        - Cleans all the data by dropping any 0 or empty values in the logged dataframe
    - [CombinedData.py](./Data_PreProcessing/CombinedData.py)
        - Combines multiple dataframes to one another and appends all the different dataframes into one giant CSV File
    - [RemoveoutlierTrips.py](./Data_PreProcessing/RemoveoutlierTrips.py)
        - Remove all outlier trips and determines the whether the bus is departing or arriving based on the bus stop sequence
    - [StopSequenceGenerator.py](./Data_PreProcessing/StopSequnceGenerator.py)
        - Splits the log data to arrival and depature time 
- [AutomateClean.py](./AutomateClean.py)
    - Automates the data preprocessing as the training and testing set is very large and goes through around 35 different files with around 100000 - 10000000 rows of data. 

### **Project References**
- [kaggle forcasting bus demand](https://www.kaggle.com/code/serdargundogdu/forecasting-bus-demand-with-time-series)
- [kaggle predicting bus delay](https://www.kaggle.com/code/asit78/predicting-the-bus-delays-on-any-given-day/notebook)
- [kaggle NYC Bus Traffic Analysis](https://www.kaggle.com/code/sachinxshrivastav/nyc-bus-traffic-analysis/notebook)


### **Group Resources Internal**
- [Training Raw Folder](https://drive.google.com/drive/folders/1pOEjtb1TFz1i7-1fmpwMigLnDeLY_Qp8?usp=drive_link)
- [Testing Raw Folder](https://drive.google.com/drive/folders/1pSLZrU5neqJUVNWVVT3CGrxBIBYI47Wp?usp=drive_link)
- [Testing Dataset](https://drive.google.com/file/d/1tELkS1DEUddnpKkguctOj6t8LvgJYDIz/view?usp=drive_link)
- [Training Dataset](https://drive.google.com/file/d/1uVq3dwf4CKwLd5U0Y8uMOjxkjjXmXfvO/view?usp=drive_link)

### **Setting up GIT**
- Add SSH Key for github: [here](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent)
```python
git init .
git add .
git remote add origin <SSH_GIT>
git commit -m "Commit_MESSAGE"
git push -u origin Master
```