import logging
from os import environ
from datetime import datetime

from Auth.AuthClass import AuthClass
from Connector.PGConnectorClass import PGConnectorClass
from DataExtractor.DataExtractorClass import DataExtractorClass
from DataTransformer.DataTransformerClass import DataTransformerClass


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
    #auth = AuthClass(logger, user, password)
    #extractor = DataExtractorClass(logger=logger, auth_client=auth, pages_to_scan=-1, default=False)
    transformer = DataTransformerClass(logger=logger, db=db)
    try:
        #extractor.extract(staging_path="/app/staging")
        transformer.transform(staging_path="/app/staging", data_path="/app/raw")
        db.close()
    except Exception as e:
        logger.exception(str(e))


if __name__ == "__main__":
    main()

