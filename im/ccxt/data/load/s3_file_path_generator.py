import os

import helpers.s3 as hs3

_CCXT_PATH = "/data/ccxt"


class CCXTFilePathGenerator:
    """
    Generate a file path for CCXT data.
    """
    # Simply "get" to avoid stuttering.
    def get(self, exchange: str, currency: str) -> str:
        """
        Get a file path to CCXT data.

        :param exchange: CCXT exchange id (e.g. "binance")
        :param currency: currency pair (e.g. "BTC/USDT")
        :return: path to CCXT data
        """
        # Get s3 bucket path.
        s3_bucket_path = hs3.get_path()
        # Get file name from exchange id, currency pair.
        file_name = self.get_file_name(exchange, currency)
        # Get full path to the file.
        s3_file_path = os.path.join(s3_bucket_path, file_name)
        return s3_file_path

    @staticmethod
    def get_file_name(exchange: str, currency: str) -> str:
        """
        Get a name for file with CCXT data.

        File name is constructed in the following way:
        <exchange>_<currency1>_<currency2>.csv.gz

        :param exchange: CCXT exchange id (e.g. "binance")
        :param currency: currency pair (e.g. "BTC/USDT")
        :return: name of a file with CCXT data
        """
        file_name = f"{exchange}_{currency.replace('/', '_')}.csv.gz"
        return file_name
