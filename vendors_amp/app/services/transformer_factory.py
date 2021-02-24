"""
Produce transformer objects.

Import as:
import vendors_amp.app.services.transformer_factory as loadfac
"""
from typing import Union, Any
import vendors_amp.common.data.transform.s3_to_sql_transformer as mt
import vendors_amp.kibot.data.transform.s3_to_sql_transformer as kt

class TransformerFactory:
    @classmethod
    def get_s3_to_sql_transformer(cls, provider: str) -> mt.AbstractS3ToSqlTransformer:
        """
        Get s3 data to sql data transformer for provider.

        :param provider: provider (kibot, ...)
        :raises ValueError: if s3-to-sql transformer is not implemented for provider
        """
        transformer: mt.AbstractS3ToSqlTransformer
        if provider == "kibot":
            transformer = kt.S3ToSqlTransformer()
        else:
            raise ValueError("S3 to SQL transformer for %s is not implemented" % provider)
        return transformer

