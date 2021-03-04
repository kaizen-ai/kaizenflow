import psycopg2

import vendors_amp.common.data.load.data_loader as vcdlda


class AbstractSQLDataLoader(vcdlda.AbstractDataLoader):
    """
    Interface for SQL data loader.
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
