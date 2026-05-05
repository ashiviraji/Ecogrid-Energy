
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
                # create objects for meter readings from csv
                reading = MeterReading(
                    meter_id=row["meter_id"],
                    household_id=row["household_id"],
                    timestamp=datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M"),
                    generated_kwh=float(row["generated_kwh"]),
                    consumed_kwh=float(row["consumed_kwh"])
                )
                readings.append(reading) #add to the list

        return readings
    
    #process the readings and identify households with excess energy 
    def process_meter_readings(self):
        readings = self.read_meter_readings() #get all readings

        #get each reading fromthe loop
        for reading in readings:
            #Check if household has excess energy
            if reading.has_excess_energy():
                print(
                    f"[INFO] {reading.household_id} has "
                    f"{round(reading.excess_energy(), 2)} kWh excess energy"
                )
            else:
                print(f"[INFO] No excess energy for {reading.household_id}")
    