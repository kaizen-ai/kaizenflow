"""
Produce transformer objects.

Import as: import im.app.services.transformer_factory as
iastra
"""

import im.common.data.transform.s3_to_sql_transformer as icdts3


# TODO(*): -> S3ToSqlTransformerFactory
class TransformerFactory:
    @classmethod
    def get_s3_to_sql_transformer(
        cls, provider: str
    ) -> icdts3.AbstractS3ToSqlTransformer:
        """
        Get S3 data to SQL data transformer for provider.

        :param provider: provider (kibot, ...)
        :raises ValueError: if s3-to-sql transformer is not implemented for provider
        """
        transformer: icdts3.AbstractS3ToSqlTransformer
        if provider == "kibot":
            import im.kibot.data.transform.kibot_s3_to_sql_transformer as ikdtki

            transformer = ikdtki.S3ToSqlTransformer()
        elif provider == "ib":
            import im.ib.data.transform.ib_s3_to_sql_transformer as iidtib

            transformer = iidtib.IbS3ToSqlTransformer()
        else:
            raise ValueError(
                "S3 to SQL transformer for provider '%s' is not implemented"
                % provider
            )
        return transformer
