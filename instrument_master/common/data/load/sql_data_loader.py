import psycopg2

import instrument_master.common.data.load.data_loader as vcdlda


# TODO(*): Move it to data_loader.py
# TODO(*): SQL -> Sql
class AbstractSqlDataLoader(vcdlda.AbstractDataLoader):
    """
    Interface for class which loads the data for a security from an SQL backend.
    """

    def __init__(
        self, dbname: str, user: str, password: str, host: str, port: int
    ):
        self.conn: psycopg2.extensions.connection = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
        )
