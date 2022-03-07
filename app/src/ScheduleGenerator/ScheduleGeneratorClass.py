import os
import csv
import time
from time import sleep
import random
from datetime import datetime, timedelta

import geopy.distance


class ScheduleGeneratorClass(object):
    def __init__(self, logger):
        self.aircraft_types = {
            'B733': 430,
            'PAY2': 230,
            'C500': 250,
            'B738': 440,
            'PA34': 130,
            'A320': 420
        }
        self.knots2ms = 0.51444
        self.logger = logger
        self.seed = int(time.time())
        self.airports_coord = []
        self._files = None
        self.flight_number = 1000

    def load(self, data_path: str) -> None:
        self._files = [filenames for dirpath, dirnames, filenames in os.walk(data_path)]
        for filename in self._files[0]:
            filepath = os.path.join(data_path, filename)
            with open(filepath, 'r', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                self.logger.info(f"Reading {filepath}")
                for row in reader:
                    self.airports_coord.append({row[0]: (float(row[-2]), float(row[-1]))})
        self.logger.info("List of airports has been successfully uploaded to RAM")

    def _schedule(self) -> str:
        week_days = list("MTWTFSS")
        schedule = []
        for d in week_days:
            schedule.append(random.choice(["-", d, "-"]))
        if len(set(schedule)) == 1 and list(set(schedule))[0] == "-":
            i = random.randint(0, len(week_days)-1)
            schedule[i] = week_days[i]
        return "".join(schedule)

    def calculate_time(self, distance: float, aircraft_type: str) -> int:
        t = distance * 1000 / (self.aircraft_types[aircraft_type] * self.knots2ms * 60)  # min
        return int(t)

    def off_block_time(self, off_block_buffer: list) -> datetime:
        off_block_time = None
        off_block_tmp = datetime(2022, 3, 7, random.randint(17, 23), random.randint(0, 59), 0)
        if len(off_block_buffer) == 0:
            off_block_buffer.append(off_block_tmp)
            off_block_time = off_block_tmp
        else:
            while True:
                interrupt = True
                for off_block in off_block_buffer:
                    delta = (abs(off_block_tmp - off_block)).total_seconds() / 60
                    if delta >= 30:
                        continue
                    else:
                        off_block_tmp = datetime(2022, 3, 7, random.randint(17, 23), random.randint(0, 59), 0)
                        interrupt = False
                        break
                if interrupt:
                    off_block_time = off_block_tmp
                    off_block_buffer.append(off_block_time)
                    break
        return off_block_time

    def generate(self, target_path) -> str:
        current_ts = datetime.now().strftime("%Y-%m-%dT%H%M%S")
        filename = f"flight_schedule_{current_ts}.csv"
        filepath = os.path.join(target_path, filename)
        number_of_airports = len(self.airports_coord) - 1
        self.logger.info("Generating Flight Schedule...")
        with open(filepath, 'w', encoding='utf-8') as file_out:
            writer = csv.writer(file_out)
            for airport in self.airports_coord:
                off_block_buffer = []
                for i in range(random.randint(3, 7)-1):
                    origin_airport_code = list(airport.keys())[0]
                    destination_airport = self.airports_coord[random.randint(0, number_of_airports)]
                    destination_airport_code = list(destination_airport.keys())[0]

                    aircraft_type = random.choice(list(self.aircraft_types.items()))[0]

                    distance = geopy.distance.distance(
                            airport[origin_airport_code],
                            destination_airport[destination_airport_code]
                        ).km

                    delta_min = self.calculate_time(distance, aircraft_type)
                    off_block = self.off_block_time(off_block_buffer)
                    on_block = off_block + timedelta(hours=int(delta_min/60), minutes=int(delta_min % 60))

                    flight_unit = {
                        'flight_number': self.flight_number,
                        'origin_airport_code': origin_airport_code,
                        'destination_airport_code': destination_airport_code,
                        'aircraft_type': aircraft_type,
                        'schedule': self._schedule(),
                        'off_block_time': off_block.time().strftime("%H:%M"),
                        'on_block_time': on_block.time().strftime("%H:%M")
                    }
                    self.flight_number += 1
                    writer.writerow(flight_unit.values())
        self.logger.info(f"Flight Schedule has been successfully generated into {filepath}")
        return filepath


