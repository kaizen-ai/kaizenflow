"""
Import as: import vendors_amp.app.services.sql_writer_factory as vassql
"""
import vendors_amp.common.sql_writer_backend as vcsqlw
import vendors_amp.kibot.sql_writer_backend as vksqlw


class SqlWriterFactory:
    """
    Builds an SQLWriter to write data from a specific provider in an SQL backend.
    """

    @staticmethod
    def get_sql_writer_backend(
        provider: str, dbname: str, user: str, password: str, host: str, port: str
    ) -> vcsqlw.AbstractSQLWriterBackend:
        """
        Get sql writer backend for provider.

        :param provider: provider (kibot, ...)
        :raises ValueError: if sql writer backend is not implemented for provider
        """
        transformer: vcsqlw.AbstractSQLWriterBackend
        if provider == "kibot":
            transformer = vksqlw.SQLWriterKibotBackend(
                dbname=dbname, user=user, password=password, host=host, port=port
            )
        else:
            raise ValueError(
                "SQL writer backend for %s is not implemented" % provider
            )
        return transformer