import pandas as pd

import im.ccxt.data.load.s3_file_path_generator as s3_gen


class CCXTLoader:
    """
    Load CCXT data.
    """
    def read_data(
        self,
        exchange: str,
        currency: str,
        data_type: str,
    ) -> pd.DataFrame:
        """
        Load data from s3 and transform it.

        :param exchange: CCXT exchange id (e.g. "binance")
        :param currency: currency pair (e.g. "BTC/USDT")
        :param data_type: OHLCV or trade, bid/ask data
        :return: transformed CCXT data
        """
        # Get path to the file.
        file_path = s3_gen.CCXTFilePathGenerator().get(exchange, currency)
        # Read data.
        data = pd.read_csv(file_path)
        # It is assumed that the "transform" part is done by Danya
        # in the same class below.
        transformed_data = self._transform(data, exchange, currency, data_type)
        return transformed_data


