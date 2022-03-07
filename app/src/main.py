import logging
from os import environ
from datetime import datetime

from Auth.AuthClass import AuthClass
from Connector.PGConnectorClass import PGConnectorClass
from DataExtractor.DataExtractorClass import DataExtractorClass
from DataTransformer.DataTransformerClass import DataTransformerClass
from FlightsGenerator.FlightsGeneratorClass import FlightsGeneratorClass
from ScheduleGenerator.ScheduleGeneratorClass import ScheduleGeneratorClass


def main():
    current_date = datetime.now().date()
    user = environ["ALAR_USERNAME"]
    password = environ["ALAR_PASSWORD"]
    pg_password = environ["PG_PASSWORD"]

    logging.basicConfig(filename=f"/app/log/log_{current_date}.log",
                        format='%(asctime)s,%(msecs)d %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filemode='w',
                        level=logging.DEBUG)

    logger = logging.getLogger("logger")

    logger.info("Logger has been initiated")

    db = PGConnectorClass(logger=logger)
    db.connect(host="db", port=5432, user="postgres", password=pg_password, db="alardb")
    db.execute("SET search_path TO alar")
    auth = AuthClass(logger, user, password)
    extractor = DataExtractorClass(logger=logger, auth_client=auth, pages_to_scan=-1, default=False)
    transformer = DataTransformerClass(logger=logger, db=db)
    schedule = ScheduleGeneratorClass(logger=logger)
    try:
        extractor.extract(staging_path="/app/staging")

        staging_file = transformer.transform(staging_path="/app/staging", data_path="/app/raw")
        db.cursor.copy_from(
            open(staging_file, "r"),
            "alar_api_airports",
            ",",
            columns=("airport_id", "city", "country", "latitude", "longitude")
        )
        db.connection.commit()
        logger.info("Raw data from external source have been successfully replicated to DB!")

        schedule.load("/app/raw")
        schedule_file = schedule.generate("/app/schedule")
        db.cursor.copy_from(
            open(schedule_file, "r"),
            "alar_staging_schedule",
            ",",
            columns=(
                "flight_number",
                "origin_airport_code",
                "destination_airport_code",
                "aircraft_type",
                "weekly_schedule",
                "off_block_time",
                "on_block_time"
            )
        )
        db.connection.commit()
        logger.info("Flights schedule have been successfully replicated to DB")

        flights = FlightsGeneratorClass(logger=logger, generate_months=3)
        flights.load("/app/schedule")
        flights_file = flights.generate("/app/flights")
        db.cursor.copy_from(
            open(flights_file, "r"),
            "alar_generate_flight",
            ",",
            columns=(
                "flight_number",
                "movement_type",
                "event_date",
                "event_time"
            )
        )
        db.connection.commit()
        logger.info("Flight Dataset have been successfully replicated to DB")
    except Exception as e:
        logger.exception(str(e))
    finally:
        db.close()


if __name__ == "__main__":
    main()

