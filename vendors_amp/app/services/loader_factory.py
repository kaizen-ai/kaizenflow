"""
Produce loader objects.

Import as:
import vendors_amp.app.services.loader_factory as loadfac
"""
from typing import Any

import vendors_amp.common.data.load.data_loader as vcdlda
import vendors_amp.common.data.load.s3_data_loader as vcdls3
import vendors_amp.common.data.load.sql_data_loader as vcdlsq
import vendors_amp.kibot.data.load.s3_data_loader as vkdls3
import vendors_amp.kibot.data.load.sql_data_loader as vkdlsq


class LoaderFactory:
    @classmethod
    def get_loader(
        cls, storage_type: str, provider: str, **kwargs: Any
    ) -> vcdlda.AbstractDataLoader:
        """
        Get `storage_type` loader for provider.

        Supported storages:
            - s3
            - sql

        :param storage_type: load from where
        :param provider: provider (kibot, ...)
        :param kwargs: additional parameters for loader instantiation
        :return: loader instance with `read_data()` method
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
        Get S3 loader for provider.

        :param provider: provider (kibot, ...)
        :return: loader instance with `read_data()` method
        :raises ValueError: if S3 loader is not implemented for provider
        """
        loader: vcdls3.AbstractS3DataLoader
        if provider == "kibot":
            loader = vkdls3.S3KibotDataLoader()
        else:
            raise ValueError("S3 loader for %s is not implemented" % provider)
        return loader

    @staticmethod
    def _get_sql_loader(
        provider: str, dbname: str, user: str, password: str, host: str, port: str
    ) -> vcdlsq.AbstractSQLDataLoader:
        """
        Get SQL loader for provider.

        :param provider: provider (kibot, ...)
        :param dbname: database name to connect
        :param user: authorization user
        :param password: authorization password
        :param host: database host
        :param port: database port
        :return: loader instance with `read_data()` method
        :raises ValueError: if SQL loader is not implemented for provider
        """
        loader: vcdlsq.AbstractSQLDataLoader
        if provider == "kibot":
            loader = vkdlsq.SQLKibotDataLoader(
                dbname=dbname, user=user, password=password, host=host, port=port
            )
        else:
            raise ValueError("SQL loader for %s is not implemented" % provider)
        return loader
