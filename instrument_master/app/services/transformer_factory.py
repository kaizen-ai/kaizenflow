"""
Produce transformer objects.

Import as:
import instrument_master.app.services.transformer_factory as vastra
"""

import instrument_master.common.data.transform.s3_to_sql_transformer as vcdts3

# TODO(*): -> S3ToSqlTransformerFactory
class TransformerFactory:
    @classmethod
    def get_s3_to_sql_transformer(
        cls, provider: str
    ) -> vcdts3.AbstractS3ToSqlTransformer:
        """
        Get S3 data to SQL data transformer for provider.

        :param provider: provider (kibot, ...)
        :raises ValueError: if s3-to-sql transformer is not implemented for provider
        """
        transformer: vcdts3.AbstractS3ToSqlTransformer
        if provider == "kibot":
            import instrument_master.kibot.data.transform.kibot_s3_to_sql_transformer as vkdts3
            transformer = vkdts3.S3ToSqlTransformer()
        elif provider == "ib":
            import instrument_master.ib.data.transform.ib_s3_to_sql_transformer as vidts3
            transformer = vidts3.IbS3ToSqlTransformer()
        else:
            raise ValueError(
                "S3 to SQL transformer for provider '%s' is not implemented" % provider
            )
        return transformer
