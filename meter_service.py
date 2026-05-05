#import libraries
import csv
from datetime import datetime
from models import MeterReading


class MeterService:
    def __init__(self, file_path="meter_readings.csv"):
        self.file_path = file_path