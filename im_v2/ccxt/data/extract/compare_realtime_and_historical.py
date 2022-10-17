#!/usr/bin/env python
"""
Compare data on DB and S3, raising when difference was found.

Use as:
# Compare daily S3 and realtime data for binance.
> im_v2/ccxt/data/extract/compare_realtime_and_historical.py \
   --db_stage 'dev' \
   --start_timestamp 20221008-000000 \
   --end_timestamp 20221013-000000 \
   --exchange_id 'binance' \
   --data_type 'ohlcv' \
   --contract_type 'futures' \
   --db_table 'ccxt_ohlcv_preprod' \
   --aws_profile 'ck' \
   --resample_1min 'True' \
   --s3_path 's3://cryptokaizen-data/reorg/daily_staged.airflow.pq'

Import as:

import im_v2.ccxt.data.extract.compare_realtime_and_historical as imvcdecrah
"""
import argparse
import logging
from typing import List
import os

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hparser as hparser
import helpers.hs3 as hs3
import helpers.hsql as hsql
import im_v2.ccxt.data.client as icdcl
import im_v2.crypto_chassis.data.client as iccdc
import im_v2.im_lib_tasks as imvimlita

_LOG = logging.getLogger(__name__)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--start_timestamp",
        action="store",
        required=True,
        type=str,
        help="Beginning of the compared period",
    )
    parser.add_argument(
        "--end_timestamp",
        action="store",
        required=True,
        type=str,
        help="End of the compared period",
    )
    parser.add_argument(
        "--db_stage",
        action="store",
        required=True,
        type=str,
        help="DB stage to use",
    )
    parser.add_argument(
        "--exchange_id",
        action="store",
        required=True,
        type=str,
        help="Exchange for which the comparison should be done",
    )
    parser.add_argument(
        "--data_type",
        action="store",
        required=True,
        type=str,
        help="Data type to compare: 'bid_ask' or 'ohlcv'",
    )
    parser.add_argument(
        "--contract_type",
        action="store",
        required=True,
        type=str,
        help="Contract type: 'spot' or 'futures'",
    )
    parser.add_argument(
        "--db_table",
        action="store",
        required=False,
        default="ccxt_ohlcv",
        type=str,
        help="(Optional) DB table to use, default: 'ccxt_ohlcv'",
    )
    parser.add_argument(
        "--resample_1min",
        action="store",
        required=False,
        default=True,
        type=bool,
        help="(Optional) If the data should be resampled to 1 min'",
    )
    parser = hparser.add_verbosity_arg(parser)
    parser = hs3.add_s3_args(parser)
    return parser  # type: ignore[no-any-return]


class RealTimeHistoricalReconciler:
    def __init__(self, args) -> None:
        hdbg.dassert_in(args.data_type, ["bid_ask", "ohlcv"])
        self.data_type = args.data_type
        # Set DB connection.
        db_connection = hsql.get_connection(
            *hsql.get_connection_info_from_env_file(
                imvimlita.get_db_env_path("dev")
            )
        )
        # Initialize CCXT client.
        self.ccxt_rt_im_client = icdcl.CcxtSqlRealTimeImClient(
            args.resample_1min, db_connection, args.db_table
        )
        # Initialize CC client.
        universe_version = None
        partition_mode = "by_year_month"
        data_snapshot = ""
        self.cc_daily_pq_client = iccdc.CryptoChassisHistoricalPqByTileClient(
            universe_version,
            args.resample_1min,
            args.s3_path,
            partition_mode,
            args.data_type,
            args.contract_type,
            data_snapshot,
            aws_profile=args.aws_profile,
        )
        # Process time period.
        self.start_ts = pd.Timestamp(args.start_timestamp, tz="UTC")
        self.end_ts = pd.Timestamp(args.end_timestamp, tz="UTC")
        #
        self.universe = self._get_universe()
        self.expected_columns = {
            "ohlcv": [
                "timestamp",
                "full_symbol",
                "open",
                "high",
                "low",
                "close",
                "volume",
            ],
            "bid_ask": [
                "timestamp",
                "full_symbol",
                "bid_price",
                "bid_size",
                "ask_price",
                "ask_size",
            ],
        }

    @staticmethod
    def clean_data_for_orderbook_level(
        df: pd.DataFrame, level: int = 1
    ) -> pd.DataFrame:
        """
        Specify the order level in CCXT bid ask data.

        :param df: Data with multiple levels (e.g., bid_price_1, bid_price_2, etc.)
        :return: Data where specific level has common name (i.e., bid_price)
        """
        level_cols = [col for col in df.columns if col.endswith(f"_{level}")]
        level_cols_cleaned = [elem[:-2] for elem in level_cols]
        #
        zip_iterator = zip(level_cols, level_cols_cleaned)
        col_dict = dict(zip_iterator)
        #
        df = df.rename(columns=col_dict)
        #
        return df

    def run(self) -> None:
        """
        Compare real time and daily data. 
        """
        # Get CCXT data.
        ccxt_rt = self.ccxt_rt_im_client.read_data(
            self.universe, self.start_ts, self.end_ts, None, "assert"
        )
        if self.data_type == "bid_ask":
            # CCXT timestamp data goes up to milliseconds, so one needs to round it to minutes.
            ccxt_rt.index = ccxt_rt.reset_index()["timestamp"].apply(
                lambda x: x.round(freq="T")
            )
            # Choose the specific order level (first level by default).
            ccxt_rt = self.clean_data_for_orderbook_level(ccxt_rt)
        # Remove duplicated columns in real time data.
        ccxt_rt = self._filter_duplicates(ccxt_rt)
        # Get CC data.
        cc_daily = self.cc_daily_pq_client.read_data(
            self.universe, self.start_ts, self.end_ts, None, "assert"
        )
        # Remove duplicated columns in daily data.
        cc_daily = self._filter_duplicates(cc_daily)
        expected_columns = self.expected_columns[self.data_type]
        # Reindex real time data.
        ccxt_rt = ccxt_rt[expected_columns]
        ccxt_rt_reindex = ccxt_rt.set_index(
            ["timestamp", "full_symbol"]
        )
        # Reindex daily data.
        cc_daily = cc_daily[expected_columns]
        cc_daily_reindex = cc_daily.set_index(
            ["timestamp", "full_symbol"]
        )
        # Compare real time and daily data.
        if self.data_type == "ohlcv":
            self._compare_ohlcv(ccxt_rt_reindex, cc_daily_reindex)
        else:
            self._compare_bid_ask(ccxt_rt_reindex, cc_daily_reindex)
        return

    def _compare_ohlcv(self, rt_data: pd.DataFrame, daily_data: pd.DataFrame) -> None:
        """
        Compare OHLCV real time and daily data.

        :param rt_data: real time data
        :param daily_data: daily data
        :return:
        """
        # Inform if both dataframes are empty,
        # most likely there is a wrong arg value given.
        if rt_data.empty and daily_data.empty:
            message = (
                "Both realtime and staged data are missing, \n"
                + "Did you provide correct arguments?"
            )
            hdbg.dfatal(message=message)
        # Get missing data.
        rt_missing_data, daily_missing_data = hpandas.find_gaps_in_dataframes(
            rt_data, daily_data
        )
        # Compare dataframe contents.
        data_difference = hpandas.compare_dataframe_rows(rt_data, daily_data)
        # Show difference and raise if one is found.
        error_message = []
        if not rt_missing_data.empty:
            error_message.append("Missing real time data:")
            error_message.append(
                hpandas.get_df_signature(
                    rt_missing_data, num_rows=len(rt_missing_data)
                )
            )
        if not daily_missing_data.empty:
            error_message.append("Missing daily data:")
            error_message.append(
                hpandas.get_df_signature(
                    daily_missing_data, num_rows=len(daily_missing_data)
                )
            )
        if not data_difference.empty:
            error_message.append("Differing table contents:")
            error_message.append(
                hpandas.get_df_signature(
                    data_difference, num_rows=len(data_difference)
                )
            )
        if error_message:
            hdbg.dfatal(message="\n".join(error_message))
        _LOG.info("No differences were found between real time and daily data")

    def _compare_bid_ask(self, rt_data: pd.DataFrame, daily_data: pd.DataFrame) -> None:
        """
        Compare order book real time and daily data.

        :param rt_data: real time data
        :param daily_data: daily data
        :return:
        """
        data = rt_data.merge(
            daily_data,
            how="outer",
            left_index=True,
            right_index=True,
            suffixes=("_ccxt", "_cc"),
        )
        # Move the same metrics from two vendors together.
        data = data.reindex(sorted(data.columns), axis=1)
        #
        _LOG.info(
            "Found %s missing rows for both vendors",
            len(data[data.isna().all(axis=1)]),
        )
        # METRICS HERE

        return
        
    def _filter_duplicates(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicates from data based on exchange id and timestamp.

        Keeps the row with the latest 'knowledge_timestamp' value.

        :param data: Dataframe to process
        :return: data with duplicates removed
        """
        duplicate_columns = ["full_symbol", "timestamp"]
        # Sort values.
        data = data.sort_values("knowledge_timestamp", ascending=False)
        use_index = False
        _LOG.info("Dataframe length before duplicate rows removed: %s", len(data))
        # Reset `timestamp` index to use timestamp as a column.
        data = data.reset_index()
        # Remove duplicates.
        data = hpandas.drop_duplicates(
            data, use_index, subset=duplicate_columns
        ).sort_index()
        _LOG.info("Dataframe length after duplicate rows removed: %s", len(data))
        return data

    
        

    def _get_universe(self) -> List[str]:
        """ 
        """
        # DB universe.
        ccxt_universe = self.ccxt_rt_im_client.get_universe()
        # CC universe.
        cc_universe = self.cc_daily_pq_client.get_universe()
        # Intersection of universes that will be used for analysis.
        universe = list(set(ccxt_universe) & set(cc_universe))
        return universe


def _run(args: argparse.Namespace) -> None:
    #
    reconciler = RealTimeHistoricalReconciler(args)
    #
    reconciler.run()


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    _run(args)


if __name__ == "__main__":
    _main(_parse())
