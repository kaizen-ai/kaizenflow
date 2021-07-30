"""
Import as:

import core.dataflow_source_nodes as dtfsn
"""
import datetime
import logging
from typing import Any, Dict, List, Optional, Union

import pandas as pd

import core.dataflow as cdataf
import core.finance as cfinan
import helpers.dbg as dbg
import helpers.printing as hprint
import im.kibot as vkibot

_LOG = logging.getLogger(__name__)

_PANDAS_DATE_TYPE = Union[str, pd.Timestamp, datetime.datetime]


# #############################################################################


def DataSourceNodeFactory(
    nid: str, source_node_name: str, source_node_kwargs: Dict[str, Any]
) -> cdataf.DataSource:
    """
    Initialize the appropriate data source node.

    The use case for this function is to create nodes depending on config parameters
    leaving the pipeline DAG unchanged.

    :param nid: node identifier
    :param source_node_name: short name for data source node type
    :param source_node_kwargs: kwargs for data source node
    :return: data source node of appropriate type instantiated with kwargs
    """
    dbg.dassert(source_node_name)
    # TODO(gp): To simplify we can use the name of the class (e.g., "ArmaGenerator"
    #  instead of "arma"), so we don't have to use another level of mnemonics.
    if source_node_name == "arma":
        ret = cdataf.ArmaGenerator(nid, **source_node_kwargs)
    elif source_node_name == "crypto_data_download":
        import core_lem.dataflow.nodes.sources as cldns

        ret = cldns.CryptoDataDownload_DataReader(nid, **source_node_kwargs)
    elif source_node_name == "disk":
        ret = cdataf.DiskDataSource(nid, **source_node_kwargs)
    elif source_node_name == "kibot":
        ret = KibotDataReader(nid, **source_node_kwargs)
    elif source_node_name == "kibot_equities":
        ret = KibotEquityReader(nid, **source_node_kwargs)
    elif source_node_name == "kibot_multi_col":
        ret = KibotColumnReader(nid, **source_node_kwargs)
    elif source_node_name == "DataLoader":
        ret = cdataf.DataLoader(nid, **source_node_kwargs)
    elif source_node_name == "multivariate_normal":
        ret = cdataf.MultivariateNormalGenerator(nid, **source_node_kwargs)
    else:
        raise ValueError(f"Unsupported data source node {source_node_name}")
    return ret


# #############################################################################


# TODO(gp): Move all the nodes somewhere else (e.g., sources.py)?


def process_timestamp(
    timestamp: Optional[_PANDAS_DATE_TYPE],
) -> Optional[pd.Timestamp]:
    if timestamp is pd.NaT:
        timestamp = None
    if timestamp is not None:
        timestamp = pd.Timestamp(timestamp)
        dbg.dassert_is(timestamp.tz, None)
    return timestamp


def load_kibot(
    symbol: str,
    frequency: Union[str, vkibot.Frequency],
    contract_type: Union[str, vkibot.ContractType],
    start_date: Optional[_PANDAS_DATE_TYPE] = None,
    end_date: Optional[_PANDAS_DATE_TYPE] = None,
    nrows: Optional[int] = None,
) -> pd.DataFrame:
    frequency = (
        vkibot.Frequency(frequency) if isinstance(frequency, str) else frequency
    )
    contract_type = (
        vkibot.ContractType(contract_type)
        if isinstance(contract_type, str)
        else contract_type
    )
    start_date = process_timestamp(start_date)
    end_date = process_timestamp(end_date)
    df_out = vkibot.KibotS3DataLoader().read_data(
        exchange="CME",
        asset_class=vkibot.AssetClass.Futures,
        frequency=frequency,
        contract_type=contract_type,
        symbol=symbol,
        nrows=nrows,
    )
    df_out = df_out.loc[start_date:end_date]
    return df_out


# TODO(gp): Maybe consolidate KibotDataReader and KibotColumnReader.

# TODO(gp): -> KibotFuturesDataReader
class KibotDataReader(cdataf.DataSource):
    def __init__(
        self,
        nid: str,
        symbol: str,
        frequency: Union[str, vkibot.Frequency],
        contract_type: Union[str, vkibot.ContractType],
        start_date: Optional[_PANDAS_DATE_TYPE] = None,
        end_date: Optional[_PANDAS_DATE_TYPE] = None,
        nrows: Optional[int] = None,
    ) -> None:
        """
        Create data source node outputting single instrument data from Kibot.

        :param symbol, frequency, contract_type:
            define the Kibot data to load with the same meaning as in get_kibot_path
        :param start_date: data start date in ET, included
        :param end_date: data end date in Et, included
        :param nrows: same as Kibot read_data
        """
        super().__init__(nid)
        self._symbol = symbol
        self._frequency = frequency
        self._contract_type = contract_type
        self._start_date = start_date
        self._end_date = end_date
        self._nrows = nrows

    def fit(self) -> Optional[Dict[str, pd.DataFrame]]:
        """
        :return: training set as df
        """
        self._lazy_load()
        return super().fit()

    def predict(self) -> Optional[Dict[str, pd.DataFrame]]:
        self._lazy_load()
        return super().predict()

    def _lazy_load(self) -> None:
        if self.df is not None:
            return
        df = load_kibot(
            symbol=self._symbol,
            frequency=self._frequency,
            contract_type=self._contract_type,
            start_date=self._start_date,
            end_date=self._end_date,
            nrows=self._nrows,
        )
        self.df = df


# #############################################################################


# TODO(gp): Move reading only a subset of columns into KibotS3DataLoader.
#  If we use Parquet we can avoid to read useless data for both time and
#  columns.


class KibotColumnReader(cdataf.DataSource):
    def __init__(
        self,
        nid: str,
        symbols: List[str],
        frequency: Union[str, vkibot.Frequency],
        contract_type: Union[str, vkibot.ContractType],
        col: str,
        start_date: Optional[_PANDAS_DATE_TYPE] = None,
        end_date: Optional[_PANDAS_DATE_TYPE] = None,
        nrows: Optional[int] = None,
    ) -> None:
        """
        Same interface as KibotDataReader but with multiple symbols.
        """
        super().__init__(nid)
        self._symbols = symbols
        self._frequency = (
            vkibot.Frequency(frequency)
            if isinstance(frequency, str)
            else frequency
        )
        self._contract_type = (
            vkibot.ContractType(contract_type)
            if isinstance(contract_type, str)
            else contract_type
        )
        self._col = col
        self._start_date = start_date
        self._end_date = end_date
        self._nrows = nrows

    def fit(self) -> Optional[Dict[str, pd.DataFrame]]:
        self._lazy_load()
        return super().fit()

    def predict(self) -> Optional[Dict[str, pd.DataFrame]]:
        self._lazy_load()
        return super().predict()

    def _lazy_load(self) -> None:
        if self.df is not None:
            return
        dict_df = {}
        for s in self._symbols:
            data = vkibot.KibotS3DataLoader().read_data(
                exchange="CME",
                asset_class=vkibot.AssetClass.Futures,
                frequency=self._frequency,
                contract_type=self._contract_type,
                symbol=s,
                nrows=self._nrows,
            )[self._col]
            data = data.loc[self._start_date : self._end_date]
            dict_df[s] = data
        self.df = pd.DataFrame.from_dict(dict_df)


# #############################################################################


class KibotEquityReader(cdataf.DataSource):
    def __init__(
        self,
        nid: str,
        symbols: List[str],
        frequency: Union[str, vkibot.Frequency],
        start_date: Optional[_PANDAS_DATE_TYPE] = None,
        end_date: Optional[_PANDAS_DATE_TYPE] = None,
        nrows: Optional[int] = None,
    ) -> None:
        """
        Read equity OHLCV data.
        """
        super().__init__(nid)
        dbg.dassert_isinstance(symbols, list)
        self._symbols = symbols
        self._frequency = (
            vkibot.Frequency(frequency)
            if isinstance(frequency, str)
            else frequency
        )
        self._start_date = start_date
        self._end_date = end_date
        self._nrows = nrows

    def fit(self) -> Optional[Dict[str, pd.DataFrame]]:
        self._lazy_load()
        return super().fit()

    def predict(self) -> Optional[Dict[str, pd.DataFrame]]:
        self._lazy_load()
        return super().predict()

    def _lazy_load(self) -> None:
        if self.df is not None:
            return
        dfs = {}
        for symbol in self._symbols:
            data = vkibot.KibotS3DataLoader().read_data(
                # TODO(*): This is required, but is it used?
                exchange="NYSE",
                asset_class=vkibot.AssetClass.Stocks,
                frequency=self._frequency,
                symbol=symbol,
                # TODO(*): Pass this through as an option.
                unadjusted=False,
                nrows=self._nrows,
            )
            data = data.loc[self._start_date : self._end_date]
            # Rename column for volume so that it adheres with our conventions.
            data = data.rename(columns={"vol": "volume"})
            # Print some info about the data.
            _LOG.debug(hprint.df_to_short_str("data", data))
            # Ensure data is on a uniform frequency grid.
            data = cfinan.resample_ohlcv_bars(data, rule=self._frequency.value)
            dfs[symbol] = data
        # Create a dataframe with multiindexed columns.
        df = pd.concat(dfs.values(), axis=1, keys=dfs.keys())
        # Swap column levels so that symbols are leaves.
        df = df.swaplevel(i=0, j=1, axis=1)
        df.sort_index(axis=1, level=0, inplace=True)
        self.df = df
