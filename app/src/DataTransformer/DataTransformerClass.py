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
    def __init__(self, logger, db: PGConnectorClass) -> None:
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
        self.db.execute("select coordinates from alar.airports_dict where airport_id = %s", (airport_id,))
        data = self.db.fetchall()
        return eval(data[0][0])

    def _cleared_lines_transformer(self, writer, line: str) -> None:
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

    def transform(self, staging_path: str, data_path: str) -> None:
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
        return None


