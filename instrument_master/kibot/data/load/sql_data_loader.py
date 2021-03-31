import functools
from typing import Optional

import pandas as pd

import helpers.dbg as dbg
import instrument_master.common.data.load.sql_data_loader as vcdlsq
import instrument_master.common.data.types as vcdtyp


class KibotSqlDataLoader(vcdlsq.AbstractSqlDataLoader):

    # TODO(plyq): Uncomment once #1047 will be resolved.
    # @hcache.cache
    # Use lru_cache for now.
    @functools.lru_cache(maxsize=64)
    def read_data(
        self,
        exchange: str,
        symbol: str,
        asset_class: vcdtyp.AssetClass,
        frequency: vcdtyp.Frequency,
        contract_type: Optional[vcdtyp.ContractType] = None,
        unadjusted: Optional[bool] = None,
        nrows: Optional[int] = None,
        normalize: bool = True,
    ) -> pd.DataFrame:
        """
        Read Kibot data.

        :param exchange: name of the exchange
        :param symbol: symbol to get the data for
        :param asset_class: asset class
        :param frequency: `D` or `T` for daily or minutely data respectively
        :param contract_type: required for asset class of type: `futures`
        :param unadjusted: required for asset classes of type: `stocks` & `etfs`
        :param nrows: if not None, return only the first nrows of the data
        :param normalize: whether to normalize the dataframe by frequency
        :return: a dataframe with the symbol data
        """
        return self._read_data(
            exchange=exchange,
            symbol=symbol,
            frequency=frequency,
            nrows=nrows,
        )

    @staticmethod
    def _get_table_name_by_frequency(frequency: vcdtyp.Frequency) -> str:
        """
        Get table name by predefined frequency.

        :param frequency: a predefined frequency
        :return: table name in DB
        """
        table_name = ""
        if frequency == vcdtyp.Frequency.Minutely:
            table_name = "KibotMinuteData"
        elif frequency == vcdtyp.Frequency.Daily:
            table_name = "KibotDailyData"
        elif frequency == vcdtyp.Frequency.Tick:
            table_name = "KibotTickData"
        dbg.dassert(table_name, f"Unknown frequency {frequency}")
        return table_name
