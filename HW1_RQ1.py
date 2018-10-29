
# coding: utf-8

# In[27]:


# Load the Pandas libraries with alias 'pd' 
import pandas as pd 
from sqlalchemy import create_engine
import os as os
from datetime import datetime
import traceback

#classes this will be on seperate file
class EachMonthTrips:
    totalTrips = 0
    total_number_days_in_month=0
    each_day_total_trips = {}
    each_day_total_boroughs_trips = {}
    
    

#filesname variables
yellow_tripdata_2018 = 'yellow_tripdata_2018'
taxi_zone_lookup = 'taxi_zone_lookup.csv'

#This function will generate the chunk and yeild it where it is demanded
def chunck_generator(filename, header=False,chunk_size = 10 ** 5): # notice the default value for chunk_size and header
    for chunk in pd.read_csv(filename,delimiter=',', iterator=True, chunksize=chunk_size, parse_dates=[1] ): 
        yield (chunk)
        
#This function will also used for generating the chunk but it will yeild out the single row which being used for calculation
def _generator( filename, header=False,chunk_size = 10 ** 5): # notice the default value for chunk_size and header
    chunk = chunck_generator(filename, header=False,chunk_size = 10 ** 5)
    for row in chunk: #go over the chunk file and loop through rows and yeild them
        yield row

#This function will be used to read zone_lookup file
def read_full_csv_file(zone_lookup):
    df = pd.read_csv(zone_lookup)
    return df 


#combinning the data with zone    
def combine_file_with_zone(tripdata_filename,zone_lookup):
    filename = tripdata_filename
    generator = _generator(filename=filename)
    
    continue_iteration = True # this will make iteration to stop
    # now I am going to assume that data I have is for 2018
    list_of_months = {"01":EachMonthTrips(),
                      "02":EachMonthTrips(),
                      "03":EachMonthTrips(),
                      "04":EachMonthTrips(),
                      "05":EachMonthTrips(),
                      "06":EachMonthTrips() }
    
    #I know I can automate this process too but Just leave it for now
    list_of_months["01"].total_number_days_in_month= 31
    list_of_months["02"].total_number_days_in_month= 28
    list_of_months["03"].total_number_days_in_month= 31
    list_of_months["04"].total_number_days_in_month= 30
    list_of_months["05"].total_number_days_in_month= 31
    list_of_months["06"].total_number_days_in_month= 30
    
    
    # assigning value to each object of list_of_months for dictionary purposes
    for key, value in list_of_months.items():
        for i in range(value.total_number_days_in_month):
            day_value_in_double_digit='%02d' % i
            list_of_months[key].each_day_total_trips[day_value_in_double_digit] = 0
            
            
    while continue_iteration:
        try:
#             if(index == 1):
#                 break
            yellow_tripdata_chunk = next(generator)
            taxi_zone_data_set = read_full_csv_file(taxi_zone_lookup)
            if isinstance(yellow_tripdata_chunk, pd.DataFrame) and isinstance(taxi_zone_data_set, pd.DataFrame):
                combined_data_set = pd.merge(yellow_tripdata_chunk, taxi_zone_data_set, on= None, left_on = 'PULocationID', right_on = 'LocationID')
                 #finding answers

            for index, row in combined_data_set.iterrows(): # iterating through dataframe
                date = row["tpep_pickup_datetime"] # It is already in DateTime format. Great!
                month_name = '%02d' % date.month #getting month name
                day_name = '%02d' % date.day
                try:
                    list_of_months[month_name].totalTrips = list_of_months[month_name].totalTrips + 1
                    list_of_months[month_name].each_day_total_trips[day_name]= list_of_months[month_name].each_day_total_trips[day_name] + 1
                except Exception as e:
                    traceback.print_exc() #ignore month that is not between 1-6 of 2018 and days that are not followed the above limit 
        except Exception as e:
            continue_iteration=False
            traceback.print_exc()
            print("Iterator finished: ex:"+ str(e))
        index = index + 1
        
    for key, value in list_of_months.items():
        print(value.totalTrips)
        
        
        
files = os.listdir(os.curdir)  #files and directories
index=0
# files.reverse()
for filename in files:
    if (index == 1):
        break
    if yellow_tripdata_2018 in filename:
        combine_file_with_zone(filename,taxi_zone_lookup)
    index = index + 1



        
        

