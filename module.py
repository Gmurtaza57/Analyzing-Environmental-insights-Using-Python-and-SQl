import pandas as pd
import warnings
warnings.filterwarnings("ignore")

#Task1
#Loading the data
def read_data_from_csv():
    #df =read the 'iot_telemetry_data.csv' file
    df = pd.read_csv('iot_telemetry_data.csv')
    return df


#Task 2: Renaming the Columns
def rename_columns():
    # do not edit the predefined function name
    
    df=read_data_from_csv()
    #rename the columns according to the description
    #write your code here
    column_mapping = {
        'ts': 'timestamp',
        'device': 'device_id',
        'co': 'carbon_monoxide',
        'lpg': 'liquefied_petroleum_gas',
        'temp': 'temperature'
    }
    df = df.rename(columns=column_mapping)
    return df

#Task 3: check for null values
def null_values_check():
    # do not edit the predefined function name
    df=rename_columns()
    #write your code here to check for the null values in the dataset
    null_counts = df.isnull().sum()
    return null_counts



#Task4 :Removing Duplicates

def remove_duplicates():
    # do not edit the predefined function name
    df=rename_columns()
    #write your code here to drop the duplicates in the dataset and return the dataframe 'df' without the duplicates
    df = df.drop_duplicates()
    return df


#Task 5:Handling Missing Values:
def handle_missing_values():
    # do not edit the predefined function name
    df=remove_duplicates()
    df = df.dropna()
    #write your code here to drop the missing values in the dataset which returns the dataframe 'df' without missing values
    return df

#Task 6:Data Type Conversion:

def convert_data_types():
    # do not edit the predefined function name
    df= handle_missing_values()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['humidity'] = df['humidity'].astype(float)
    df['light'] = df['light'].astype(bool)
    df['motion'] = df['motion'].astype(bool)
    df['temperature'] = df['temperature'].astype(float)
    #write your code to change the datatype of each column to respective datatype mentioned in the task
    #description and  return the dataframe'df' which contains updated columns datatypes.

    return df

#Task 7: Export the cleaned dataset to "cleaned_environemnt.csv"
def export_the_dataset():
    # do not edit the predefined function name
    df=convert_data_types()
    df.to_csv('cleaned_environment.csv', index=False)
    #write your code to export the cleaned dataset and set the index=false and return the same as 'df'



#TASK 8: Load the Cleaned dataset 'cleaned_environment.csv' to the database provided.
#follow the instruction in the Task 8 description and complete the task as per it.

#check if mysql table is created using "cleaned_environment"
#Use this final dataset and upload it on the provided database for performing analysis in  MySQL
#To run this task click on the terminal and click on the run project
