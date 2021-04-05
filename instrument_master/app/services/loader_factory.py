"""
Import as: import instrument_master.app.services.loader_factory as vasloa.
"""
from typing import Any

# TODO: Move it out to app/

import instrument_master.common.data.load.data_loader as vcdlda
import instrument_master.common.data.load.s3_data_loader as vcdls3
import instrument_master.common.data.load.sql_data_loader as vcdlsq
import instrument_master.ib.data.load.ib_s3_data_loader as vidls3
import instrument_master.ib.data.load.ib_sql_data_loader as vidlsq
import instrument_master.kibot.data.load.kibot_s3_data_loader as vkdls3
import instrument_master.kibot.data.load.kibot_sql_data_loader as vkdlsq


class LoaderFactory:
    """
    Builds AbstractDataLoader objects based on different criteria (e.g.,
    provider and storage type).
    """

    @classmethod
    def get_loader(
        cls, storage_type: str, provider: str, **kwargs: Any
    ) -> vcdlda.AbstractDataLoader:
        """
        Return a data loader for the requested `storage_type` and `provider`.

        :param storage_type: load from where (e.g., s3, sql)
        :param provider: provider (e.g., kibot, ib)
        :param kwargs: additional parameters for loader instantiation
        :raises ValueError: `storage_type` loader is not implemented for provider
        """
        if storage_type == "sql":
            loader = cls._get_sql_loader(provider, **kwargs)
        elif storage_type == "s3":
            loader = cls._get_s3_loader(provider)
        else:
            raise ValueError("Storage type %s is not supported" % storage_type)
        return loader

    @staticmethod
    def _get_s3_loader(provider: str) -> vcdls3.AbstractS3DataLoader:
        """
        Return a data loader from S3 for the requested `provider`.

        :param provider: provider (e.g., kibot)
        :raises ValueError: if loader is not implemented for provider
        """
        loader: vcdls3.AbstractS3DataLoader
        if provider == "kibot":
            loader = vkdls3.KibotS3DataLoader()
        elif provider == "ib":
            loader = vidls3.IbS3DataLoader()
        else:
            raise ValueError("S3 loader for %s is not implemented" % provider)
        return loader

    @staticmethod
    def _get_sql_loader(
        provider: str, dbname: str, user: str, password: str, host: str, port: int
    ) -> vcdlsq.AbstractSqlDataLoader:
        """
        Return a data loader from SQL for the requested `provider`.

        :param provider: provider (e.g., kibot)
        :param dbname: database name to connect
        :param user: authorization user
        :param password: authorization password
        :param host: database host
        :param port: database port
        :raises ValueError: if SQL loader is not implemented for provider
        """
        loader: vcdlsq.AbstractSqlDataLoader
        if provider == "kibot":
            loader = vkdlsq.KibotSqlDataLoader(
                dbname=dbname, user=user, password=password, host=host, port=port
            )
        elif provider == "ib":
            loader = vidlsq.IbSqlDataLoader(
                dbname=dbname, user=user, password=password, host=host, port=port
            )
        else:
            raise ValueError("SQL loader for %s is not implemented" % provider)
        return loader
