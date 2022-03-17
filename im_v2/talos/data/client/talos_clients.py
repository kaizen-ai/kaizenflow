"""
Import as:

import im_v2.talos.data.client.talos_clients as imvtdctacl
"""

import logging
import os
from typing import Any, Dict, List, Optional

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hparquet as hparque
import im_v2.common.data.client as icdc

_LOG = logging.getLogger(__name__)


# #############################################################################
# TalosParquetByTileClient
# #############################################################################


class TalosParquetByTileClient(icdc.HistoricalPqByTileClient):
    """
    Read historical data for 1 `Talos` asset stored as Parquet dataset.

    It can read data from local or S3 filesystem as backend.
    """

    def __init__(
        self,
        root_dir: str,
        partition_mode: str,
        *,
        aws_profile: Optional[str] = None,
    ) -> None:
        """
        Load `Talos` data from local or S3 filesystem.
        """
        super().__init__(
            "talos", "N/A", root_dir, partition_mode, aws_profile=aws_profile
        )

    def get_metadata(self) -> pd.DataFrame:
        """
        See description in the parent class.
        """
        raise NotImplementedError

    def get_universe(self) -> List[icdc.FullSymbol]:
        """
        See description in the parent class.
        """
        # TODO(Dan): CmTask1379.
        return []

    def _read_data_for_multiple_symbols(
        self,
        full_symbols: List[icdc.FullSymbol],
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        full_symbol_col_name: str = "N/A",
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        See description in the parent class.
        """
        # Split full symbols list on tuples of exchange and currency pair.
        exchange_currency_pairs_tuples = [
            icdc.parse_full_symbol(full_symbol) for full_symbol in full_symbols
        ]
        # Get a dictionary with exchange ids as keys and lists of currency pairs
        # that belong to them as values.
        exchange_currency_pairs_dict = {}
        for exchange, currency_pair in exchange_currency_pairs_tuples:
            exchange_currency_pairs_dict.setdefault(exchange, []).append(
                currency_pair
            )
        # Load data for each exchange separately since every exchange has its
        # own directory in filesystem and save it in list for all data.
        all_data = []
        for exchange, currency_pairs in exchange_currency_pairs_dict.items():
            # Get path to a dir with all the data for specified exchange id.
            exchange_dir_path = os.path.join(
                self._root_dir_name, self._vendor, exchange
            )
            # Build Parquet filtering conditions.
            currency_pair_filter = ("currency_pair", "in", currency_pairs)
            filters = hparque.get_parquet_filters_from_timestamp_interval(
                self._partition_mode,
                start_ts,
                end_ts,
                additional_filter=currency_pair_filter,
            )
            # Specify column names to load.
            columns = ["open", "high", "low", "close", "volume"]
            # Load data.
            data = hparque.from_parquet(
                exchange_dir_path,
                columns=columns,
                filters=filters,
                aws_profile=self._aws_profile,
            )
            hdbg.dassert(not data.empty)
            # Convert index to datetime.
            data.index = pd.to_datetime(data.index)
            # Trim data.
            ts_col_name = None
            left_close = True
            right_close = True
            data = hpandas.trim_df(
                data, ts_col_name, start_ts, end_ts, left_close, right_close
            )
            all_data.append(data)
        all_data_df = pd.concat(all_data)
        all_data_df.index.name = None
        return all_data_df
