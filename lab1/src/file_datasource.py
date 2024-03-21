from csv import reader
from datetime import datetime
from domain.accelerometer import Accelerometer
from domain.gps import Gps
from domain.aggregated_data import AggregatedData
from domain.parking import Parking
import config

class FileDatasource:
    def __init__(self, accelerometer_filename: str, gps_filename: str, parking_filename: str,) -> None:
        self.accelerometer_filename = accelerometer_filename
        self.gps_filename = gps_filename
        self.parking_filename = parking_filename
        self.accelerometer_file = None
        self.gps_file = None
        self.parking_file = None

    def read(self) -> AggregatedData:
        #Read data from accelerometer, parking and GPS files, and create an AggregatedData object.
        accelerometer_data = self.accelerometer_file.readline().strip().split(',')
        gps_data = self.gps_file.readline().strip().split(',')
        parking_data = self.parking_file.readline().strip().split(',')

        if not accelerometer_data or not gps_data or not parking_data:
            return None

        try:
            accelerometer = Accelerometer(*map(int, accelerometer_data[:3]))
            gps = Gps(*map(float, gps_data[:2]))
            parking = Parking(float(parking_data[2]), gps)
            time = datetime.now()
            return AggregatedData(accelerometer, gps, parking, time, config.USER_ID)
        except (IndexError, ValueError) as e:
            self.accelerometer_file.seek(0)
            self.accelerometer_file.readline()
            self.gps_file.seek(0)
            self.gps_file.readline()
            self.parking_file.seek(0)
            self.parking_file.readline()
            self.read()

    def startReading(self, *args, **kwargs):

        #Open accelerometer, parking and GPS files for reading.

        self.accelerometer_file = open(self.accelerometer_filename, 'r')
        self.gps_file = open(self.gps_filename, 'r')
        self.parking_file = open(self.parking_filename, 'r')
        next(self.accelerometer_file)  # Skip header line
        next(self.gps_file)  # Skip header line
        next(self.parking_file)

    def stopReading(self, *args, **kwargs):

        #Close accelerometer, parking and GPS files.

        self.accelerometer_file.close()
        self.gps_file.close()
        self.parking_file.close()
