"""
Import as:

import im_v2.common.data.client.multiple_symbols_client as imvcdcmscl
"""

import abc
import logging
import re
from typing import Any, Dict, List, Optional, Union

import pandas as pd

import helpers.dbg as hdbg
import im_v2.common.data.client.abstract_data_loader as imvcdcadlo

_LOG = logging.getLogger(__name__)


class MultipleSymbolsClient:
    """
    Object compatible with `AbstractImClient` interface which reads data for multiple full symbols.
    """
    def __init__(self, class_: imvcdcadlo.AbstractImClient, mode: str) -> None:
        """
        :param class_: `AbstractImClient` object
        :param mode: output mode
            - concat: store data for multiple full symbols in one dataframe
            - dict: store data in a dict of a type `Dict[full_symbol, data]`
        """
        # Store an object from `AbstractImClient`.
        self._class = class_
        # Specify output mode.
        hdbg.dassert_in(mode, ("concat", "dict"))
        self._mode = mode

    @abc.abstractmethod
    def read_data(
        self,
        full_symbols: List[imvcdcadlo.FullSymbol],
        *,
        full_symbol_col_name: str = "full_symbol",
        start_ts: Optional[pd.Timestamp] = None,
        end_ts: Optional[pd.Timestamp] = None,
        **kwargs: Dict[str, Any],
    ) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """
        Read data for multiple full symbols.

        None `start_ts` and `end_ts` means the entire period of time available.

        :param full_symbols: list of full symbols, e.g.
            `['binance::BTC_USDT', 'kucoin::ETH_USDT']`
        :param full_symbol_col_name: name of the column with full symbols
        :param start_ts: the earliest date timestamp to load data for
        :param end_ts: the latest date timestamp to load data for
        :return: combined data for provided symbols
        """
        # Verify that all the provided full symbols are unique.
        hdbg.dassert_no_duplicates(full_symbols)
        # Initialize results dict.
        full_symbol_to_df = {}
        for full_symbol in sorted(full_symbols):
            # Read data for each given full symbol.
            df = self._class.read_data(
                full_symbol=full_symbol,
                start_ts=start_ts,
                end_ts=end_ts,
                **kwargs,
            )
            # Insert column with full symbol to the dataframe.
            df.insert(0, full_symbol_col_name, full_symbol)
            # Add full symbol data to the results dict.
            full_symbol_to_df[full_symbol] = df
        if self._mode == "concat":
            # Combine results dict in a dataframe if specified.
            ret = pd.concat(full_symbol_to_df.values())
            # Sort results dataframe by increasing index and full symbol.
            ret = ret.sort_index().sort_values(by=full_symbol_col_name)
        elif self._mode == "dict":
            # Return results dict if specified.
            ret = full_symbol_to_df
        else:
            raise ValueError("Invalid mode=%s", self._mode)
        return ret

    # TODO(Grisha/Dan): Decide if we want to also implement other methods of the base class.
    # TODO(Grisha/Dan): Decide if we want to add get_start(end)_ts_available() methods.
