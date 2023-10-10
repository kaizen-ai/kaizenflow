"""
Binance native data extractor. https://github.com/binance/binance-public-data/

Import as:

import im_v2.binance.data.extract.extractor as imvbdexex
"""
import enum
import glob
import json
import logging
import os
import sys
import urllib.error as urlerr
import urllib.request as urlreq
import uuid
from typing import Any, Iterator, List, Optional, Union

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hio as hio
import im_v2.common.data.extract.extractor as ivcdexex

_LOG = logging.getLogger(__name__)
BASE_URL = "https://data.binance.vision/"
CHUNK_SIZE = 20000
TRADES_COLUMNS = {
    "futures": ["id", "price", "qty", "quote_qty", "time", "is_buyer_maker"],
    "spot": [
        "id",
        "price",
        "qty",
        "quote_qty",
        "time",
        "is_buyer_maker",
        "is_best_match",
    ],
}


class BinanceNativeTimePeriod(enum.Enum):
    """
    Binance native data time period.
    """

    DAILY = "daily"
    MONTHLY = "monthly"
    YEARLY = "yearly"


class BinanceExtractor(ivcdexex.Extractor):
    """
    Extracts data from the Binance native data API.
    """

    def __init__(
        self,
        contract_type: str,
        time_period: BinanceNativeTimePeriod,
        allow_data_gaps: Optional[bool] = False,
    ) -> None:
        """
        Construct Binance native data extractor.

        :param contract_type: spot or futures contracts to extract
        :param time_period: time period of the data to extract
            By the nature of Binance data, we can extract data only for a:
            - daily
            - monthly
            - yearly
            time period.
            The choice of the time period is depends on the data we want to
            extract. For example, if we want to extract data for a few days,
            then we should choose daily time period. If we want to extract
            data for a few months, then we should choose monthly time period.
        :param allow_data_gaps: whether to allow data gaps
            Example: if we have data for 2020-01-01 and 2020-01-03, but not for
            2020-01-02, then we have a data gap.
            So, if allow_data_gaps is True, we will just log a warning and
            continue. If allow_data_gaps is False, we will raise an exception.
        """
        self.base_dir = "./tmp/binance"
        self.allow_data_gaps = allow_data_gaps
        hdbg.dassert_in(
            contract_type,
            ["futures", "spot"],
            msg="Supported contract types: futures, spot",
        )
        self.contract_type = contract_type
        self.vendor = "Binance"
        self.time_period = time_period
        super().__init__()

    def __del__(self) -> None:
        """
        Destructor of the Binance Extractor.
        """
        # Remove the temporary directory.
        if hasattr(self, "tmp_dir_path") and os.path.exists(self.tmp_dir_path):
            hio.delete_dir(self.tmp_dir_path)

    def convert_currency_pair(self, currency_pair: str, **kwargs) -> str:
        """
        Convert currency pair to Binance native format.

        :param currency_pair: currency pair to convert, e.g. `BTC_USDT`
        :return: converted currency pair, e.g. `BTCUSDT`
        """
        return currency_pair.replace("_", "")

    def _make_tmp_dir(self) -> str:
        """
        Make temporary directory.

        :return: path to temporary directory
        """
        dir_id = uuid.uuid4()
        self.tmp_dir_path = os.path.join(self.base_dir, str(dir_id))
        hio.create_dir(self.tmp_dir_path, incremental=False)
        return self.tmp_dir_path

    def _download_single_binance_file(
        self,
        file_url: str,
        file_name: str,
        dst_path: Optional[str] = None,
    ) -> bool:
        """
        Download a single file from Binance.

        :param file_url: url of the file to download
        :param file_name: name of the file to download
        :param dst_path: path to save the file to
        :return: True if the file was downloaded, False otherwise
        """
        if dst_path is None:
            dst_path = self.tmp_dir_path
        save_path = os.path.join(dst_path, file_name)
        if os.path.exists(save_path):
            raise FileExistsError(save_path)
        try:
            download_url = f"{BASE_URL}{file_url}{file_name}"
            dl_file = urlreq.urlopen(download_url)
            length = dl_file.getheader("content-length")
            if length:
                length = int(length)
                blocksize = max(4096, length // 100)
            else:
                raise ValueError(
                    "No content-length header in the file: %s", download_url
                )
            with open(save_path, "wb") as out_file:
                dl_progress = 0
                # TODO(Vlad): Refactor this to use tqdm.
                print(f"\nFile Download: {download_url}")
                while True:
                    buf = dl_file.read(blocksize)
                    if not buf:
                        print("\n")
                        break
                    dl_progress += len(buf)
                    out_file.write(buf)
                    done = int(50 * dl_progress / length)
                    sys.stdout.write(f"\r[{'#' * done}{'.' * (50 - done)}]")
                    sys.stdout.flush()
        except urlerr.HTTPError:
            if self.allow_data_gaps:
                _LOG.warning("File not found: %s", download_url)
                return False
            else:
                raise FileNotFoundError(download_url)
        return True

    def _get_binance_file_path(
        self,
        trading_type: str,
        market_data_type: str,
        time_period: str,
        symbol: str,
        interval: Optional[str] = None,
    ) -> str:
        """
        Get the path to the file on Binance.

        :param trading_type: spot or futures
        :param market_data_type: trades or klines
        :param time_period: daily, monthly or yearly
        :param symbol: currency pair
        :param interval: time interval. Example: 1m, 1h, 1d
        :return: path to the data on Binance
            Example: data/futures/daily/trades/BTCUSDT/
        """
        trading_type_path = "data/spot"
        if trading_type != "spot":
            trading_type_path = f"data/futures/{trading_type}"
        if interval is not None:
            path = (
                f"{trading_type_path}/"
                f"{time_period}/"
                f"{market_data_type}/"
                f"{symbol.upper()}/"
                f"{interval}/"
            )
        else:
            path = (
                f"{trading_type_path}/"
                f"{time_period}/"
                f"{market_data_type}/"
                f"{symbol.upper()}/"
            )
        return path

    def _get_file_name_by_period(
        self,
        time_period: BinanceNativeTimePeriod,
        currency_pair: str,
        period: pd.Timestamp,
    ) -> str:
        """
        Get the file name based on the time period.

        :param time_period: time period
        :param currency_pair: currency pair to download
        :param period: current period
        :return: file name
        """
        if time_period == BinanceNativeTimePeriod.DAILY:
            file_name = (
                f"{currency_pair.upper()}-trades"
                f"-{period.strftime('%Y-%m-%d')}.zip"
            )
        elif time_period == BinanceNativeTimePeriod.MONTHLY:
            file_name = (
                f"{currency_pair.upper()}-trades"
                f"-{period.year}"
                f"-{period.month:02d}.zip"
            )
        else:
            raise ValueError(f"Unsupported time period: {self.time_period}")
        return file_name

    def _download_binance_files(
        self,
        currency_pair: str,
        start_timestamp: pd.Timestamp,
        end_timestamp: pd.Timestamp,
        dst_path: str,
    ) -> None:
        """
        Download files from Binance.

        :param currency_pair: currency pair to download
        :param start_timestamp: start date
        :param end_timestamp: end date
        :param dst_path: path to save the files to
        """
        # Get the list of periods to download.
        binance_to_pandas_time_period = {
            BinanceNativeTimePeriod.DAILY: "D",
            BinanceNativeTimePeriod.MONTHLY: "M",
        }
        periods = pd.date_range(
            start=start_timestamp,
            end=end_timestamp,
            freq=binance_to_pandas_time_period[self.time_period],
        )
        # TODO(Vlad): Refactor this when we add more data types.
        # Get the path to the data on Binance.
        if self.contract_type == "futures":
            trading_type = "um"
        else:
            trading_type = "spot"
        market_data_type = "trades"
        path = self._get_binance_file_path(
            trading_type, market_data_type, self.time_period.value, currency_pair
        )
        # Download files date by date.
        for period in periods:
            file_name = self._get_file_name_by_period(
                self.time_period, currency_pair, period
            )
            self._download_single_binance_file(path, file_name, dst_path)

    def _read_trades_from_csv_file(self, file_path: str) -> pd.DataFrame:
        """
        Read trades from a file.

        :param file_path: path to the file to read trades from
        :return: trades
        """
        # Check if the file has headers.
        two_rows = pd.read_csv(file_path, nrows=2)
        # Compare the values from the first line with the trades columns.
        if set(two_rows.columns) == set(TRADES_COLUMNS[self.contract_type]):
            trades = pd.read_csv(file_path)
        else:
            trades = pd.read_csv(
                file_path, names=TRADES_COLUMNS[self.contract_type]
            )
        return trades

    def _read_trades_from_chunks_by_days(
        self,
        data: Iterator[pd.DataFrame],
    ) -> Iterator[pd.DataFrame]:
        """
        Read trades from chunks by days.

        :param data: iterator of chunks
        :return: iterator of days
        """
        current_day = 0
        trades = []
        timestamp_column = "time"
        # Go through the chunks.
        for chunk in data:
            # Get days from the chunk.
            days_from_chunk = (
                chunk[timestamp_column]
                .apply(pd.Timestamp, unit="ms")
                .dt.day.unique()
            )
            if len(days_from_chunk) == 1:
                if days_from_chunk[0] == current_day:
                    trades.append(chunk)
                else:
                    if len(trades) > 0:
                        yield pd.concat(trades)
                    trades = [chunk]
                    current_day = days_from_chunk[0]
            # If there are more than one day in the chunk we need to split it.
            elif len(days_from_chunk) >= 2:
                # Get the first day.
                first_day = days_from_chunk[0]
                if first_day == current_day:
                    trades.append(
                        chunk[
                            chunk[timestamp_column]
                            .apply(pd.Timestamp, unit="ms")
                            .dt.day
                            == current_day
                        ]
                    )
                else:
                    if len(trades) > 0:
                        yield pd.concat(trades)
                    trades = [
                        chunk[
                            chunk[timestamp_column]
                            .apply(pd.Timestamp, unit="ms")
                            .dt.day
                            == first_day
                        ]
                    ]
                    current_day = first_day
                # Get the rest of the days.
                for day in days_from_chunk[1:]:
                    if len(trades) > 0:
                        yield pd.concat(trades)
                    trades = [
                        chunk[
                            chunk[timestamp_column]
                            .apply(pd.Timestamp, unit="ms")
                            .dt.day
                            == day
                        ]
                    ]
                    current_day = day
        # Yield the last day.
        if len(trades) > 0:
            yield pd.concat(trades)

    def _read_trades_from_csv_file_by_chunks(
        self, file_path: str
    ) -> Iterator[pd.DataFrame]:
        """
        Read trades from a file.

        :param file_path: path to the file to read trades from
        :return: trades
        """
        # TODO(Vlad): Extract the logic to define the header to a separate
        #  function.
        # Check if the file has headers.
        two_rows = pd.read_csv(file_path, nrows=2)
        # Compare the values from the first line with the trades columns.
        if set(two_rows.columns) == set(TRADES_COLUMNS[self.contract_type]):
            trades = pd.read_csv(file_path, chunksize=CHUNK_SIZE)
        else:
            trades = pd.read_csv(
                file_path,
                names=TRADES_COLUMNS[self.contract_type],
                chunksize=CHUNK_SIZE,
            )
        return trades

    def _extract_data_from_binance_files(
        self,
        src_path: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Extract data from the local Binance files.

        :param src_path: path to the files to extract data from
        :return: binance data
        """
        data = []
        if src_path is None:
            src_path = self.tmp_dir_path
        search_path = f"{src_path}/**/*.zip"
        for file_name in glob.glob(search_path, recursive=True):
            _LOG.info("Extracting %s", file_name)
            data.append(self._read_trades_from_csv_file(file_name))
        if len(data) == 0 and self.allow_data_gaps:
            _LOG.warning("No data found in %s", src_path)
            return pd.DataFrame(columns=TRADES_COLUMNS[self.contract_type])
        return pd.concat(data).reset_index(drop=True)

    def _post_process_binance_data(
        self,
        data: pd.DataFrame,
        start_timestamp: pd.Timestamp,
        end_timestamp: pd.Timestamp,
    ) -> pd.DataFrame:
        """
        Post-process Binance data.
            It's necessary to post-process the data because the data from
            Binance is not in the correct format.
            We need to:
                - rename the columns
                - add the side column
                - add the end_download_timestamp column
                - filter the data by the start and end timestamps

        :param data: data to post-process
        :param start_timestamp: start date
        :param end_timestamp: end date
        :return: post-processed data
            Example:
                timestamp     | price | amount | side | end_download_timestamp
                1681131717000 | 0.001 | 1.0    | buy  | 1681131717000
        """
        start_timestamp = hdateti.convert_timestamp_to_unix_epoch(
            start_timestamp, unit="ms"
        )
        end_timestamp = hdateti.convert_timestamp_to_unix_epoch(
            end_timestamp, unit="ms"
        )
        data["end_download_timestamp"] = str(hdateti.get_current_time("UTC"))
        data.rename(columns={"time": "timestamp", "qty": "amount"}, inplace=True)
        data["side"] = data.is_buyer_maker.map({False: "sell", True: "buy"})
        data = data[
            ["timestamp", "price", "amount", "side", "end_download_timestamp"]
        ]
        return data[data.timestamp.between(start_timestamp, end_timestamp)]

    def _get_binance_data_iterator(
        self,
        src_path: str,
    ) -> Iterator[pd.DataFrame]:
        """
        Get iterator over the Binance data.

        :param src_path: path to the files to extract data from
        :return: iterator over Binance data that return pandas
            DataFrame with the data splitted by days
        """
        search_path = f"{src_path}/**/*.zip"
        # There is 3 levels of iteration:
        # 1. Iterate over files.
        # 2. Iterate over chunks in the file.
        # 3. Group chunks by days.
        data_iterator = (
            df
            for file_name in glob.glob(search_path, recursive=True)
            for df in self._read_trades_from_chunks_by_days(
                self._read_trades_from_csv_file_by_chunks(file_name)
            )
        )
        return data_iterator

    def _get_trades_iterator(
        self,
        currency_pair: str,
        start_timestamp: pd.Timestamp,
        end_timestamp: pd.Timestamp,
    ) -> Iterator[pd.DataFrame]:
        """
        Get the iterator that retrieve trades from Binance.

        :param currency_pair: currency pair to download
        :param start_timestamp: start timestamp
        :param end_timestamp: end timestamp
        :return: iterator over trades grouped by days that return
            pandas DataFrame with the data splitted by days
        """
        # Make temporary directory.
        tmp_dir = self._make_tmp_dir()
        # Download files locally.
        self._download_binance_files(
            currency_pair, start_timestamp, end_timestamp, tmp_dir
        )
        # Get iterator over the files and chunks.
        data_iterator = self._get_binance_data_iterator(tmp_dir)
        # Wrap the iterator with the data post-processing.
        data_iterator = (
            self._post_process_binance_data(data, start_timestamp, end_timestamp)
            for data in data_iterator
        )
        return data_iterator

    def _fetch_trades(
        self,
        currency_pair: str,
        start_timestamp: pd.Timestamp,
        end_timestamp: pd.Timestamp,
    ) -> pd.DataFrame:
        """
        Fetch trades from Binance.

        :param currency_pair: currency pair to download
        :param start_timestamp: start timestamp
        :param end_timestamp: end timestamp
        :return: trades data
        """
        # Make temporary directory.
        tmp_dir = self._make_tmp_dir()
        # Download files locally.
        self._download_binance_files(
            currency_pair, start_timestamp, end_timestamp, tmp_dir
        )
        # Extract data from files.
        trades_df = self._extract_data_from_binance_files(tmp_dir)
        # Remove temporary directory.
        hio.delete_dir(tmp_dir)
        # Filter data by timestamp.
        start_timestamp_epoch = hdateti.convert_timestamp_to_unix_epoch(
            start_timestamp
        )
        end_timestamp_epoch = hdateti.convert_timestamp_to_unix_epoch(
            end_timestamp
        )
        trades_df = trades_df[
            trades_df.time.between(start_timestamp_epoch, end_timestamp_epoch)
        ]
        # Convert data.
        trades_df["end_download_timestamp"] = str(hdateti.get_current_time("UTC"))
        trades_df.rename(
            columns={"time": "timestamp", "qty": "amount"}, inplace=True
        )
        trades_df["side"] = trades_df.is_buyer_maker.map(
            {False: "sell", True: "buy"}
        )
        trades_df = trades_df[
            ["timestamp", "price", "amount", "side", "end_download_timestamp"]
        ]
        # Return data.
        return trades_df

    def _get_currency_pairs(
        self,
        data_type: str,
    ) -> List[str]:
        """
        Get list of currency pairs.

        :param data_type: data type to get currency pairs for
        :return: list of currency pairs
        """
        if data_type == "trades":
            response = urlreq.urlopen(
                "https://fapi.binance.com/fapi/v1/exchangeInfo"
            ).read()
        else:
            raise ValueError(f"Data type {data_type} not supported")
        pairs = [symbol["symbol"] for symbol in json.loads(response)["symbols"]]
        return pairs

    def _download_trades(
        self,
        exchange_id: str,
        currency_pair: str,
        *,
        start_timestamp: Optional[pd.Timestamp] = None,
        end_timestamp: Optional[pd.Timestamp] = None,
        **kwargs: Any,
    ) -> Union[Iterator[pd.DataFrame], pd.DataFrame]:
        """
        Download trades from Binance.

        :param exchange_id: parameter for compatibility with other extractors
          always set to `binance`
        :param currency_pair: currency pair to get data for, e.g. `BTC_USDT`
        :param start_timestamp: start timestamp
        :param end_timestamp: end timestamp
        :return: exchange data:
            - if `time_period` is `BinanceNativeTimePeriod.MONTHLY` then
                iterator over monthly trades grouped by days that return
                pandas DataFrame with the data splitted by days
            - if `time_period` is `BinanceNativeTimePeriod.DAILY` then
                pandas DataFrame with the data
        """
        # Verify that exchange_id is correct.
        hdbg.dassert_eq(exchange_id, "binance")
        # Convert symbol to Binance Native format, e.g. "BTC_USDT" -> "BTCUSDT".
        currency_pair = self.convert_currency_pair(
            currency_pair,
        )
        data_type = "trades"
        currency_pairs = self._get_currency_pairs(data_type)
        hdbg.dassert_in(
            currency_pair,
            currency_pairs,
            "Currency pair is not present in exchange",
        )
        # Verify that date parameters are of correct format.
        hdbg.dassert_isinstance(end_timestamp, pd.Timestamp)
        hdbg.dassert_isinstance(start_timestamp, pd.Timestamp)
        hdbg.dassert_lte(start_timestamp, end_timestamp)
        # Fetch trades.
        if self.time_period in (
            BinanceNativeTimePeriod.MONTHLY,
            BinanceNativeTimePeriod.DAILY,
        ):
            trades = self._get_trades_iterator(
                currency_pair,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
            )
        else:
            raise ValueError(f"Time period {self.time_period} not supported")
        return trades

    def _download_bid_ask(
        self,
        exchange_id: str,
        currency_pair: str,
        *,
        start_timestamp: Optional[pd.Timestamp] = None,
        end_timestamp: Optional[pd.Timestamp] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Download bid-ask data from Binance.

        :param exchange_id: parameter for compatibility with other extractors
          always set to `binance`
        :param currency_pair: currency pair to get data for, e.g. `BTC_USDT`
        :param start_timestamp: start timestamp
        :param end_timestamp: end timestamp
        :return: exchange data
        """
        raise NotImplementedError("This method is not implemented yet.")

    def _download_ohlcv(
        self,
        exchange_id: str,
        currency_pair: str,
        *,
        start_timestamp: Optional[pd.Timestamp] = None,
        end_timestamp: Optional[pd.Timestamp] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Download OHLCV data from Binance.

        :param exchange_id: parameter for compatibility with other extractors
          always set to `binance`
        :param currency_pair: currency pair to get data for, e.g. `BTC_USDT`
        :param start_timestamp: start timestamp
        :param end_timestamp: end timestamp
        :return: exchange data
        """
        raise NotImplementedError("This method is not implemented yet.")

    def _download_websocket_bid_ask(
        self,
        exchange_id: str,
        currency_pair: str,
        *,
        start_timestamp: Optional[pd.Timestamp] = None,
        end_timestamp: Optional[pd.Timestamp] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Download bid-ask data from Binance.

        :param exchange_id: parameter for compatibility with other extractors
          always set to `binance`
        :param currency_pair: currency pair to get data for, e.g. `BTC_USDT`
        :param start_timestamp: start timestamp
        :param end_timestamp: end timestamp
        :return: exchange data
        """
        raise NotImplementedError("This method is not implemented yet.")

    def _download_websocket_ohlcv(
        self,
        exchange_id: str,
        currency_pair: str,
        *,
        start_timestamp: Optional[pd.Timestamp] = None,
        end_timestamp: Optional[pd.Timestamp] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Download OHLCV data from Binance.

        :param exchange_id: parameter for compatibility with other extractors
          always set to `binance`
        :param currency_pair: currency pair to get data for, e.g. `BTC_USDT`
        :param start_timestamp: start timestamp
        :param end_timestamp: end timestamp
        :return: exchange data
        """
        raise NotImplementedError("This method is not implemented yet.")

    def _download_websocket_trades(
        self,
        exchange_id: str,
        currency_pair: str,
        *,
        start_timestamp: Optional[pd.Timestamp] = None,
        end_timestamp: Optional[pd.Timestamp] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Download trades data from Binance.

        :param exchange_id: parameter for compatibility with other extractors
          always set to `binance`
        :param currency_pair: currency pair to get data for, e.g. `BTC_USDT`
        :param start_timestamp: start timestamp
        :param end_timestamp: end timestamp
        :return: exchange data
        """
        raise NotImplementedError(
            "This extractor for historical data download only. "
            "It can't be used for the real-time data downloading."
        )

    def _subscribe_to_websocket_bid_ask(
        self,
        exchange_id: str,
        currency_pair: str,
        *,
        start_timestamp: Optional[pd.Timestamp] = None,
        end_timestamp: Optional[pd.Timestamp] = None,
        **kwargs: Any,
    ) -> None:
        """
        Subscribe to bid-ask data from Binance.

        :param exchange_id: parameter for compatibility with other extractors
          always set to `binance`
        :param currency_pair: currency pair to get data for, e.g. `BTC_USDT`
        :param start_timestamp: start timestamp
        :param end_timestamp: end timestamp
        """
        raise NotImplementedError("This method is not implemented yet.")

    def _subscribe_to_websocket_ohlcv(
        self,
        exchange_id: str,
        currency_pair: str,
        *,
        start_timestamp: Optional[pd.Timestamp] = None,
        end_timestamp: Optional[pd.Timestamp] = None,
        **kwargs: Any,
    ) -> None:
        """
        Subscribe to OHLCV data from Binance.

        :param exchange_id: parameter for compatibility with other extractors
          always set to `binance`
        :param currency_pair: currency pair to get data for, e.g. `BTC_USDT`
        :param start_timestamp: start timestamp
        :param end_timestamp: end timestamp
        """
        raise NotImplementedError("This method is not implemented yet.")

    def _subscribe_to_websocket_trades(
        self,
        exchange_id: str,
        currency_pair: str,
        *,
        start_timestamp: Optional[pd.Timestamp] = None,
        end_timestamp: Optional[pd.Timestamp] = None,
        **kwargs: Any,
    ) -> None:
        """
        Subscribe to trades data from Binance.

        :param exchange_id: parameter for compatibility with other extractors
          always set to `binance`
        :param currency_pair: currency pair to get data for, e.g. `BTC_USDT`
        :param start_timestamp: start timestamp
        :param end_timestamp: end timestamp
        """
        raise NotImplementedError("This method is not implemented yet.")
