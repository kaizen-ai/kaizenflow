import pandas as pd
import im.common.data.types as vcdtyp

_LOG = logging.getLogger(__name__)


class CcxtTransformer:
    def __init__(self) -> None:
        return
    @classmethod
    def transform(cls,
                  data: pd.DataFrame,
                  exchange: str,
                  currency: str,
                  frequency: vcdtyp.Frequency):
        """
        Transform IB data loaded from S3 to load to SQL.

        :param df: dataframe with data from S3
        :param trade_symbol_id: symbol id in SQL database
        :param frequency: dataframe frequency
        :return: processed dataframe
        """
        return transformed_data