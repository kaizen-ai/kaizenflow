"""
Import as:

import im.ccxt.data.load.loader as cdlloa
"""

import logging
import os
from typing import Optional

import pandas as pd

import core.pandas_helpers as pdhelp
import helpers.dbg as dbg
import helpers.io_ as hio
import helpers.s3 as hs3

_LOG = logging.getLogger(__name__)

# Path to the data about downloaded currencies from the spreadsheet in CMTask41.
_DOWNLOADED_CURRENCIES_PATH = "im/data/downloaded_currencies.json"
# Latest historical data snapsot.
_LATEST_DATA_SNAPSHOT = "20210924"


def _get_file_name(exchange_id: str, currency: str) -> str:
    """
    Get path to a file with CCXT data from a content root.

    File name is constructed in the following way:
    `<exchange>_<currency1>_<currency2>.csv.gz.`

    :param exchange: CCXT exchange id
    :param currency: currency pair `<currency1>/<currency2>` (e.g. "BTC/USDT")
    :return: name for a file with CCXT data
    """
    # Extract data about downloaded currencies for CCXT.
    downloaded_currencies_info = hio.from_json(_DOWNLOADED_CURRENCIES_PATH)["CCXT"]
    # Verify that data for the input exchange id was downloaded.
    dbg.dassert_in(
        exchange_id,
        downloaded_currencies_info.keys(),
        msg="Data for exchange id='%s' was not downloaded" % exchange_id,
    )
    # Verify that data for the input exchange id and currency pair was
    # downloaded.
    downloaded_currencies = downloaded_currencies_info[exchange_id]
    dbg.dassert_in(
        currency_pair,
        downloaded_currencies,
        msg="Data for exchange='%s', currency pair='%s' was not downloaded"
        % (exchange, currency),
    )
    file_path = f"ccxt/{data_snapshot}/{exchange_id}/{currency_pair.replace('/', '_')}.csv.gz"
    return file_path


class CcxtLoader:
    def __init__(self, root_dir: str, aws_profile: Optional[str] = None) -> None:
        """
        Load CCXT data.

        :param: root_dir: either a local root path (e.g., "/app/im") or
            an S3 root path ("s3://alphamatic-data/data) to CCXT data
        :param: aws_profile: AWS profile name (e.g., "am")
        """
        self._root_dir = root_dir
        self._aws_profile = aws_profile

    # TODO(Dan): Dassert `data_type` value before reading data from S3.
    def read_data(
        self, exchange: str, currency: str, data_type: str
    ) -> pd.DataFrame:
        """
        Load data from S3 and process it in the common format used by the models.

        :param exchange_id: CCXT exchange id, e.g. "binance"
        :param currency_pair: currency pair, e.g. "BTC/USDT"
        :param data_type: OHLCV or trade, bid/ask data
        :param data_snapshot: snapshot of datetime when data was loaded, e.g. "20210924"
        :return: processed CCXT data
        """
        data_snapshot = data_snapshot or _LATEST_DATA_SNAPSHOT
        # Get absolute file path for a CCXT file.
        file_path = os.path.join(
            self._root_dir,
            _get_file_path(data_snapshot, exchange_id, currency_pair),
        )
        # Initialize kwargs dict for further CCXT data reading.
        read_csv_kwargs = {}
        # TODO(Dan): Remove asserts below after CMTask108 is resolved.
        # Verify that the file exists and fill kwargs if needed.
        if hs3.is_s3_path(file_path):
            s3fs = hs3.get_s3fs(self._aws_profile)
            hs3.dassert_s3_exists(file_path, s3fs)
            # Add s3fs argument to kwargs.
            read_csv_kwargs["s3fs"] = s3fs
        else:
            dbg.dassert_file_exists(file_path)
        # Read raw CCXT data.
        _LOG.info(
            "Reading CCXT data for exchange id='%s', currencies='%s' from file='%s'...",
            exchange_id,
            currency_pair,
            file_path,
        )
        data = cphelp.read_csv(file_path, **read_csv_kwargs)
        # Apply transformation to raw data.
        _LOG.info(
            "Processing CCXT data for exchange id='%s', currencies='%s'...",
            exchange_id,
            currency_pair,
        )
        transformed_data = self._transform(
            data, exchange_id, currency_pair, data_type
        )
        return transformed_data

    def _transform(
        self, data: pd.DataFrame, exchange: str, currency: str, data_type: str
    ):
        """
        Transform CCXT data loaded from S3.

        :param data: dataframe with CCXT data from S3
        :param exchange: CCXT exchange id
        :param currency: currency pair (e.g. "BTC/USDT")
        :param data_type: OHLCV or trade, bid/ask data
        :return: processed dataframe
        """
        transformed_data = self._apply_ccxt_transformation(
            data, exchange, currency
        )
        if data_type.lower() == "ohlcv":
            transformed_data = self._apply_ohlcv_transformation(transformed_data)
        else:
            dbg.dfatal("Incorrect data type. Acceptable types: ohlcv")
        return transformed_data

    @staticmethod
    def _apply_ccxt_transformation(
        data: pd.DataFrame, exchange: str, currency: str
    ):
        """
        Apply transform common to all CCXT data.

        This includes:
        - datetime format assertion
        - Converting epoch ms timestamp to pd.Timestamp
        - Adding exchange_id and currency_pair columns
        :return:
        """
        return transformed_data

    @staticmethod
    def _apply_ohlcv_transformation(transformed_data: pd.DataFrame):
        """
        Apply transformations for OHLCV data.

        This includes:
        - Assertion of present columns
        - Assertion of data types
        - Renaming and rearranging of OHLCV columns, namely:
            ["timestamp",
             "open",
             "high",
             "low",
             "close"
             "volume",
             "epoch",
             "currency_pair",
             "exchange"]
        :return:
        """
        return transformed_ohlcv
