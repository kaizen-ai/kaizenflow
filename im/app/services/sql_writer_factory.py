"""
Import as:

import im.app.services.sql_writer_factory as iassql
"""
import im.common.sql_writer as icsqlw


class SqlWriterFactory:
    """
    Build an SqlWriter to write data from a specific provider into an SQL
    backend.
    """

    @staticmethod
    def get_sql_writer_backend(
        provider: str, dbname: str, user: str, password: str, host: str, port: int
    ) -> icsqlw.AbstractSqlWriter:
        """
        Get sql writer backend for provider.

        :param provider: provider (kibot, ...)
        :raises ValueError: if sql writer backend is not implemented for provider
        """
        transformer: icsqlw.AbstractSqlWriter
        if provider == "kibot":
            import im.kibot.sql_writer as ikkibo

            transformer = ikkibo.KibotSqlWriter(
                dbname=dbname, user=user, password=password, host=host, port=port
            )
        elif provider == "ib":
            import im.ib.sql_writer as iiibsq

            transformer = iiibsq.IbSqlWriter(
                dbname=dbname, user=user, password=password, host=host, port=port
            )
        else:
            raise ValueError(
                "SQL writer backend for %s is not implemented" % provider
            )
        return transformer
