import logging
from os import environ
from datetime import datetime

from Auth.AuthClass import AuthClass


def main():
    current_date = datetime.now().date()
    user = environ["ALAR_USERNAME"]
    password = environ["ALAR_PASSWORD"]

    logging.basicConfig(filename=f"/app/log/log_{current_date}.log",
                        format='%(asctime)s,%(msecs)d %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filemode='w',
                        level=logging.DEBUG)

    logger = logging.getLogger("logger")

    logger.info("Logger has been initiated")

    auth = AuthClass(logger, user, password)
    try:
        auth.get_response()
        logger.info(auth)
    except Exception as e:
        logger.exception(str(e))


if __name__ == "__main__":
    main()

