import typing

import psycopg2


class PGConnectorClass(object):
    """
    Simple class to handle DB. Based on psycopg2
    """
    def __init__(self, logger):
        self.logger = logger
        self._host = None
        self._port = None
        self._user = None
        self._db = None
        self._password = None
        self.connection = None
        self.cursor = None

    def connect(self, host: str, port: int, db: str, user: str, password: str):
        self._host = host
        self._port = port
        self._user = user
        self._db = db
        self._password = password
        self.connection = psycopg2.connect(
            host=self._host,
            port=self._port,
            database=self._db,
            user=self._user,
            password=self._password
        )
        self.cursor = self.connection.cursor()
        self.logger.info("Connection has been initiated!")

    def close(self):
        self.connection.close()
        self.logger.info("Connection has been closed!")

    def execute(self, query: str, *args, **kwargs):
        if self.connection:
            if len(args) > 0:
                query_fmt = self.cursor.mogrify(query, *args)
            else:
                query_fmt = query
            self.logger.info(f"Executing query {query_fmt}")
            self.cursor.execute(query_fmt)

    def fetchall(self):
        return self.cursor.fetchall()



