"""
Import as:
import instrument_master.app.services.sql_writer_factory as vassql
"""
import instrument_master.common.sql_writer_backend as vcsqlw


class SqlWriterFactory:
    """
    Build an SqlWriter to write data from a specific provider into an SQL backend.
    """

    @staticmethod
    def get_sql_writer_backend(
        provider: str, dbname: str, user: str, password: str, host: str, port: int
    ) -> vcsqlw.AbstractSqlWriterBackend:
        """
        Get sql writer backend for provider.

        :param provider: provider (kibot, ...)
        :raises ValueError: if sql writer backend is not implemented for provider
        """
        transformer: vcsqlw.AbstractSqlWriterBackend
        if provider == "kibot":
            import instrument_master.kibot.kibot_sql_writer_backend as vksqlw
            transformer = vksqlw.KibotSqlWriterBackend(
                dbname=dbname, user=user, password=password, host=host, port=port
            )
        elif provider == "ib":
            import instrument_master.ib.ib_sql_writer_backend as visqlw
            transformer = visqlw.IbSqlWriterBackend(
                dbname=dbname, user=user, password=password, host=host, port=port
            )
        else:
            raise ValueError(
                "SQL writer backend for %s is not implemented" % provider
            )
        return transformer
