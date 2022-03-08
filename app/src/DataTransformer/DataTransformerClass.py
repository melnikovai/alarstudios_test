import re
import csv
import os.path
import sys
import json
import typing
from os import walk
from datetime import datetime

from Connector.PGConnectorClass import PGConnectorClass

sys.path.append("..")


class DataTransformerClass(object):
    """
    Transformer is required for cleaning the data befor replicating it to DB
    """
    def __init__(self, logger, db: PGConnectorClass) -> None:
        """
        Constructor.
        self._continents placed for getting first letter of airport code
        exceptions are made for specific airports that need to transform
        :param logger: logger
        :param db: PGConnectorClass
        """
        self.logger = logger
        self.db = db
        self._staging_path = None
        self._data_path = None
        self._continents = ['E', 'L']
        self._airports = ['BKPR']
        self._files = None

    @property
    def staging_path(self) -> str: return self._staging_path

    @property
    def data_path(self) -> str: return self._data_path

    @property
    def continents(self) -> list: return self._continents

    @property
    def airports(self) -> list: return self._airports

    @property
    def files(self) -> list: return self._files

    def find_coordinates_by_id(self, airport_id) -> tuple:
        """
        Dataset has gaps on some values, that nowhere else to find.
        To avoid misses I download external dictionary of all airports and pre-downloaded it in DB
        Function searches missing fields (log/lat) by airport id if no data found in dataset
        :param airport_id: 4 symbol code of airport
        :return: airport's coordinates
        """
        self.db.execute("select coordinates from alar.airports_dict where airport_id = %s", (airport_id,))
        data = self.db.fetchall()
        return eval(data[0][0])

    def _cleared_lines_transformer(self, writer, line: str) -> None:
        """
        Writes text string to the CSV file
        :param writer: CSV writer
        :param line: text string
        :return:
        """
        line_json = json.loads(line)
        records = line_json["data"]

        for record in records:
            airport_id = record["id"]
            continent = airport_id[0]
            if continent in self.continents or airport_id in self.airports:
                if not all([isinstance(record["lat"], (int, float)), isinstance(record["lon"], (int, float))]):
                    coordinates = self.find_coordinates_by_id(airport_id)
                    record["lon"] = coordinates[0]
                    record["lat"] = coordinates[1]
                writer.writerow(record.values())
        return None

    def _transform_lines(self, writer, line: str) -> None:
        """
        Clears text string from staging dir by regular expressions line by line
        and then sends it for writer function
        :param writer: CSV writer
        :param line: text string
        :return:
        """
        if re.findall(r'(?<=[a-zA-Z\)\s]),\"', line):
            line = re.sub(r'(?<=[a-zA-Z\)\s]),\"', '\",\"', line)
        if re.findall(r'\":\s', line):
            line = re.sub(r'\":\s', '\":\"', line)
        if re.findall(r'(?<=[a-zA-Z])\}', line):
            line = re.sub(r'(?<=[a-zA-Z])\}', '\"}', line)
        if re.findall(r'(?<=-[0-9]),\"', line):
            line = re.sub(r'(?<=-[0-9]),\"', '\",\"', line)

        self._cleared_lines_transformer(writer, line)

        return None

    def transform(self, staging_path: str, data_path: str) -> str:
        """
        Entry point. Wrapper around files iterator, cleaner and writer
        Files masks are hardcoded but can be configured from config file
        :param staging_path: path to the staging dir
        :param data_path: path to store cleared data
        :return: path to the file with cleared data
        """
        self._staging_path = staging_path
        self._data_path = data_path
        self._files = [filenames for dirpath, dirnames, filenames in walk(self._staging_path)]
        current_ts = datetime.now().strftime("%Y-%m-%dT%H%M%S")
        raw_filename = f"raw_{current_ts}.csv"
        filepath_out = os.path.join(self._data_path, raw_filename)

        with open(filepath_out, "w") as file_out:
            for filename in self._files[0]:
                filepath_in = os.path.join(self._staging_path, filename)
                self.logger.info(f"Starting to transform {filepath_in}...")
                with open(filepath_in, "r") as file_in:
                    writer = csv.writer(file_out)
                    for line in file_in:
                        try:
                            self._transform_lines(writer, line)
                        except ValueError:
                            self.logger.warning(f"Can not decode line: {line}")
                    self.logger.info(f"All records from STAGING have been loaded to {filepath_out}")
        return filepath_out


