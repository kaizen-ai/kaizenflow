"""
Produce transformer objects.

Import as:
import vendors_amp.app.services.sql_writer_factory as wfac
"""
from typing import Union, Any
import vendors_amp.common.sql_writer_backend as mt
import vendors_amp.kibot.sql_writer_backend as kt

class SqlWriterFactory:
    @staticmethod
    def get_sql_writer_backend(provider: str, dbname: str, user:str, password:str, host:str, port:str) -> mt.AbstractSQLWriterBackend:
        """
        Get sql writer backend for provider.

        :param provider: provider (kibot, ...)
        :raises ValueError: if sql writer backend is not implemented for provider
        """

        transformer: mt.AbstractSQLWriterBackend
        if provider == "kibot":
            transformer = kt.SQLWriterKibotBackend(dbname=dbname, user=user, password=password, host=host, port=port)
        else:
            raise ValueError("SQL writer backend for %s is not implemented" % provider)
        return transformer

