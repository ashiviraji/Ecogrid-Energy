
#import libraries

import csv    #to read csv file
from datetime import datetime      #to datetime data type
from models import MeterReading    #to import data model


class MeterService:
    #csv file path to intialize the service
    def __init__(self, file_path="meter_readings.csv"):
        self.file_path = file_path

    # Read smart meter data from CSV file
    def read_meter_readings(self):
        readings = []      #list to store smart meter readings

        with open(self.file_path, mode="r") as file:
            reader = csv.DictReader(file)     #read the rows in the csv file as dictionary

            #read each row
            for row in reader:
                print(row)

        return readings