import os
import csv
import random
import calendar
from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta


class FlightsGeneratorClass(object):
    """
    Class for flights report generation
    self.flights_by_day contains list of all flights tha are scheduled for certain day of week
    """
    def __init__(self, logger, generate_months: int):
        self.logger = logger
        self._generate_months = generate_months
        self._schedule_path = None
        self.flights_by_day = {
            "Monday": [],
            "Tuesday": [],
            "Wednesday": [],
            "Thursday": [],
            "Friday": [],
            "Saturday": [],
            "Sunday": []
        }
        self._files = None
        self._start_dt = datetime(2020, 1, 1)
        self._end_dt = self._start_dt + relativedelta(months=self._generate_months)
        self.percentage_of_flights = 0.95
        self.percentage_of_late = 0.2
        self.percentage_of_late_depart = 0.5
        self.percentage_of_intime_late_offtime = 0.3
        self.percentage_of_early = 0.05
        self.movement_type = ("arrival", "departure")

    def load(self, schedule_path: str) -> None:
        """
        Entry point. Wrapper around files iterator in dir and csv writer
        Populates self.flights_by_day by reading schedule file
        :param schedule_path: path to schedule file
        :return:
        """
        self._schedule_path = schedule_path
        self._files = [filenames for dirpath, dirnames, filenames in os.walk(self._schedule_path)]
        for filename in self._files[0]:
            filepath = os.path.join(self._schedule_path, filename)
            self.logger.info(f"Loading {filepath}...")
            with open(filepath, 'r', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                for row in reader:
                    schedule = row[4]
                    for i in range(len(schedule)):
                        if schedule[i] != "-":
                            self.flights_by_day[calendar.day_name[i]].append(row)
            self.logger.info(f"Successfully loaded {filepath}...")
        for k, v in self.flights_by_day.items():
            self.logger.info(f"Day: {k} - Number of flights: {len(v)}")
        return None

    def generate(self, target_path: str) -> str:
        """
        Messed up function, sorry. But logic of generation as simple as possible.
        First, calculate dates range for report with build-in datetime class
        Second, we find name of the calculated day and copy from self.flights_by_day list of all flights in this day
        Third, we build separate lists of flights - flights that late for departure and arrival, flights that
        late for arrival because of strong wing, flights that early etc
        Basic instrument for building report - pop method of list
        Fourth, we generate  random fluctuations of time in flights schedule. Tricky part - watch out for day change
        when we add us some delta - 2020-01-01 23:55 + 20 min -> 2020-01-02 00:15. Conditions messy and stupid, but work
        :param target_path: path to store data
        :return: path to the formed file
        """
        current_ts = datetime.now().strftime("%Y-%m-%dT%H%M%S")
        filename = f"flights_{current_ts}.csv"
        filepath = os.path.join(target_path, filename)
        self.logger.info("Generating Flights...")
        with open(filepath, 'w', encoding='utf-8') as file_out:
            writer = csv.writer(file_out)
            delta = self._end_dt - self._start_dt
            for i in range(delta.days):
                dt = self._start_dt + relativedelta(days=i)
                day_of_week = calendar.day_name[dt.weekday()]

                #  total number of flights
                flights_schedule = self.flights_by_day[day_of_week][:]
                number_of_flights_schedule = len(flights_schedule)
                number_of_flights_abort = int(number_of_flights_schedule * (1 - self.percentage_of_flights))
                for abort in range(number_of_flights_abort):
                    try:
                        abort_flight_index = random.randint(0, len(flights_schedule)-1)
                        flights_schedule.pop(abort_flight_index)
                    except IndexError:
                        self.logger.warning("Got IndexError in POP method of flights_late!")
                        pass
                self.logger.debug(f"Total number of flights for {dt.date()}/{day_of_week} is {len(flights_schedule)}")
                number_of_flights_schedule = len(flights_schedule)

                #  list of late arrival flights
                number_of_flights_late = int(number_of_flights_schedule * self.percentage_of_late)
                flights_late = []
                for late_index in range(number_of_flights_late):
                    try:
                        late_flight_index = random.randint(0, len(flights_schedule)-1)
                        flights_late.append(flights_schedule.pop(late_flight_index))
                    except IndexError:
                        self.logger.warning("Got IndexError in POP method of flights_late!")
                        pass
                number_of_flights_schedule = len(flights_schedule)

                #  list of late departure
                number_of_late_departure = int(len(flights_late) * self.percentage_of_late_depart)
                late_departure_flights = []
                for late_departure in range(number_of_late_departure):
                    try:
                        late_departure_index = random.randint(0, len(flights_late)-1)
                        late_departure_flights.append(flights_late.pop(late_departure_index))
                    except IndexError:
                        self.logger.warning("Got IndexError in POP method of late_departure!")
                        pass

                #  list of late_off_arrive_in_time
                late_off_arrive_in_time = []
                number_of_late_off_arrive_in_time = int(number_of_flights_schedule * self.percentage_of_intime_late_offtime)
                for intime_index in range(number_of_late_off_arrive_in_time):
                    try:
                        intime_flight_index = random.randint(0, len(flights_schedule)-1)
                        late_off_arrive_in_time.append(flights_schedule.pop(intime_flight_index))
                    except IndexError:
                        self.logger.warning("Got IndexError in POP method of intime_index!")
                        pass
                number_of_flights_schedule = len(flights_schedule)

                #  list of early flights
                early_flights = []
                number_of_early_flights = int(number_of_flights_schedule * self.percentage_of_early)
                for early_index in range(number_of_early_flights):
                    try:
                        early_flights_index = random.randint(0, len(flights_schedule)-1)
                        early_flights.append(flights_schedule.pop(early_flights_index))
                    except IndexError:
                        self.logger.warning("Got IndexError in POP method of number_of_early_flights!")
                        pass
                number_of_flights_schedule = len(flights_schedule)

                self.logger.debug(f"number_of_flights_late: {len(flights_late)}")
                self.logger.debug(f"late_off_arrive_in_time: {len(late_off_arrive_in_time)}")
                self.logger.debug(f"early_flights: {len(early_flights)}")
                self.logger.debug(f"late_departure_flights: {len(late_departure_flights)}")
                self.logger.debug(f"flights_schedule: {len(flights_schedule)}")

                #  Write flights on schedule
                for flight in flights_schedule:
                    departure_time = datetime(2022, 3, 7, *[int(num) for num in flight[-2].split(":")], 0)
                    departure_date = dt
                    arrival_time = datetime(2022, 3, 7, *[int(num) for num in flight[-1].split(":")], 0)
                    if departure_time > arrival_time:
                        arrival_date = dt + timedelta(days=1)
                    else:
                        arrival_date = dt
                    writer.writerow([flight[0], 'departure', departure_date.date(), departure_time.time().strftime("%H:%M")])
                    writer.writerow([flight[0], 'arrival', arrival_date.date(), arrival_time.time().strftime("%H:%M")])

                #  Write late flights because of late departure
                for flight in late_departure_flights:
                    delta = timedelta(minutes=random.randint(15, 59))
                    departure_time_base = datetime(2022, 3, 7, *[int(num) for num in flight[-2].split(":")], 0)
                    departure_time = departure_time_base + delta
                    arrival_time_base = datetime(2022, 3, 7, *[int(num) for num in flight[-1].split(":")], 0)
                    arrival_time = arrival_time_base + delta
                    arrival_date = dt
                    if departure_time.date() > departure_time_base.date():
                        departure_date = dt + timedelta(days=1)
                        arrival_date = departure_date
                    else:
                        departure_date = dt
                    if departure_time > arrival_time:
                        arrival_date = dt + timedelta(days=1)
                    writer.writerow([flight[0], 'departure', departure_date.date(), departure_time.time().strftime("%H:%M")])
                    writer.writerow([flight[0], 'arrival', arrival_date.date(), arrival_time.time().strftime("%H:%M")])

                #  Writes late flights because of wind
                for flight in flights_late:
                    delta = timedelta(minutes=random.randint(15, 59))
                    departure_time_base = datetime(2022, 3, 7, *[int(num) for num in flight[-2].split(":")], 0)
                    departure_time = departure_time_base
                    arrival_time_base = datetime(2022, 3, 7, *[int(num) for num in flight[-1].split(":")], 0)
                    arrival_time = arrival_time_base + delta
                    departure_date = dt
                    if departure_time > arrival_time or departure_time.date() < arrival_time.date():
                        arrival_date = dt + timedelta(days=1)
                    else:
                        arrival_date = dt
                    writer.writerow([flight[0], 'departure', departure_date.date(), departure_time.time().strftime("%H:%M")])
                    writer.writerow([flight[0], 'arrival', arrival_date.date(), arrival_time.time().strftime("%H:%M")])

                #  write in time flights, but late take off
                for flight in late_off_arrive_in_time:
                    delta = timedelta(minutes=random.randint(17, 23))
                    departure_time_base = datetime(2022, 3, 7, *[int(num) for num in flight[-2].split(":")], 0)
                    departure_time = departure_time_base + delta
                    arrival_time_base = datetime(2022, 3, 7, *[int(num) for num in flight[-1].split(":")], 0)
                    arrival_time = arrival_time_base
                    departure_date = dt
                    arrival_date = departure_date
                    if departure_time.date() > departure_time_base.date():
                        if departure_time > arrival_time:
                            continue
                        else:
                            departure_date = dt + timedelta(days=1)
                            arrival_date = departure_date
                    if departure_time > arrival_time:
                        arrival_date = dt + timedelta(days=1)
                    writer.writerow([flight[0], 'departure', departure_date.date(), departure_time.time().strftime("%H:%M")])
                    writer.writerow([flight[0], 'arrival', arrival_date.date(), arrival_time.time().strftime("%H:%M")])

                #  writes early flights
                for flight in early_flights:
                    delta_off = timedelta(minutes=random.randint(17, 23))
                    delta_in = timedelta(minutes=random.randint(17, 23))
                    departure_time_base = datetime(2022, 3, 7, *[int(num) for num in flight[-2].split(":")], 0)
                    departure_time = departure_time_base - delta_off
                    arrival_time_base = datetime(2022, 3, 7, *[int(num) for num in flight[-1].split(":")], 0)
                    arrival_time = arrival_time_base - delta_in
                    departure_date = dt
                    if departure_time > arrival_time:
                        arrival_date = dt + timedelta(days=1)
                    else:
                        arrival_date = dt
                    writer.writerow([flight[0], 'departure', departure_date.date(), departure_time.time().strftime("%H:%M")])
                    writer.writerow([flight[0], 'arrival', arrival_date.date(), arrival_time.time().strftime("%H:%M")])
        self.logger.info(f"Generated flights have been successfully writen into {filepath}")
        return filepath







