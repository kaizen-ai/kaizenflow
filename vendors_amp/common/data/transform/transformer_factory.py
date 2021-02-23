"""
Produce transformer objects.

Import as: import vendors_amp.common.data.transformer_factory as vcdtra
"""
import vendors_amp.common.data.transform.s3_to_sql_transformer as vcdts3
import vendors_amp.kibot.data.transform.s3_to_sql_transformer as vkdts3


class TransformerFactory:
    @classmethod
    def get_s3_to_sql_transformer(
        cls, provider: str
    ) -> vcdts3.AbstractS3ToSqlTransformer:
        """
        Get s3 data to sql data transformer for provider.

        :param provider: provider (kibot, ...)
        :raises ValueError: if s3-to-sql transformer is not implemented for provider
        """
        transformer: vcdts3.AbstractS3ToSqlTransformer
        if provider == "kibot":
            transformer = vkdts3.S3ToSqlTransformer()
        else:
            raise ValueError(
                "S3 to SQL transformer for %s is not implemented" % provider
            )
        return transformer
