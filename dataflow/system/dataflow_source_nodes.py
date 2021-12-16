"""
Import as:

import dataflow.system.dataflow_source_nodes as dtfsdtfsono
"""

import logging
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd

import core.finance as cofinanc
import dataflow.core.node as dtfcornode
import dataflow.core.nodes.base as dtfconobas
import dataflow.core.nodes.sources as dtfconosou
import helpers.datetime_ as hdateti
import helpers.dbg as hdbg
import helpers.printing as hprint
import im.kibot as vkibot
import market_data.market_data_interface as mdmadain

_LOG = logging.getLogger(__name__)


# #############################################################################


def data_source_node_factory(
    nid: dtfcornode.NodeId,
    source_node_name: str,
    source_node_kwargs: Dict[str, Any],
) -> dtfconobas.DataSource:
    """
    Initialize the appropriate data source node.

    The use case for this function is to create nodes depending on config parameters
    leaving the pipeline DAG unchanged.

    There are several types of nodes:
    - synthetic data generators, e.g.,
        - `ArmaGenerator`
        - `MultivariateNormalGenerator`
    - real-time data sources e.g.,
        - `RealTimeDataSource` (which uses a full-fledged `AbstractPriceInterface`)
    - data generators using data from disk
        - `DiskDataSource` (which reads CSV and PQ files)
    - data generators using pluggable functions
        - `DataLoader` (which uses a passed function to create data)

    - Note that the same goal can be achieved using different nodes in multiple
      ways, e.g.,
      - Synthetic data or data from disk can be generated using the specialized
        node or passing a function to `DataLoader`
      - One could inject synthetic data in the IM and go through the
        high-fidelity data pipeline or mock a later interface

    - In general we want to funnel data sources through `RealTimeDataSource`
      since these nodes have the closest behavior to real-time data sources

    :param nid: node identifier
    :param source_node_name: short name for data source node type
    :param source_node_kwargs: kwargs for data source node
    :return: data source node of appropriate type instantiated with kwargs
    """
    hdbg.dassert_ne(source_node_name, "")
    # TODO(gp): To simplify we can use the name of the class (e.g., "ArmaGenerator"
    #  instead of "arma"), so we don't have to use another level of mnemonics.
    if source_node_name == "arma":
        ret = dtfconosou.ArmaGenerator(nid, **source_node_kwargs)
    elif source_node_name == "multivariate_normal":
        ret = dtfconosou.MultivariateNormalGenerator(nid, **source_node_kwargs)
    elif source_node_name == "RealTimeDataSource":
        ret = RealTimeDataSource(nid, **source_node_kwargs)
    elif source_node_name == "disk":
        ret = dtfconosou.DiskDataSource(nid, **source_node_kwargs)
    elif source_node_name == "DataLoader":
        ret = dtfconosou.DataLoader(nid, **source_node_kwargs)
    elif source_node_name == "crypto_data_download":
        # TODO(gp): This should go through RealTimeDataSource.
        import core_lem.dataflow.nodes.sources as cldns

        ret = cldns.CryptoDataDownload_DataReader(nid, **source_node_kwargs)
    elif source_node_name == "kibot":
        # TODO(gp): This should go through RealTimeDataSource.
        ret = KibotDataReader(nid, **source_node_kwargs)
    elif source_node_name == "kibot_equities":
        # TODO(gp): This should go through RealTimeDataSource.
        ret = KibotEquityReader(nid, **source_node_kwargs)
    elif source_node_name == "kibot_multi_col":
        # TODO(gp): This should go through RealTimeDataSource.
        ret = KibotColumnReader(nid, **source_node_kwargs)
    else:
        raise ValueError(f"Unsupported data source node {source_node_name}")
    return ret


# #############################################################################

# TODO(gp): Not sure about all these nodes below. We have too many ways of
#  doing the same thing. They go through IM. Then there are other nodes that
#  go directly to disk bypassing IM.

# TODO(gp): Move all the nodes somewhere else (e.g., sources.py)?


def _process_timestamp(
    timestamp: Optional[hdateti.Datetime],
) -> Optional[pd.Timestamp]:
    if timestamp is pd.NaT:
        timestamp = None
    if timestamp is not None:
        timestamp = pd.Timestamp(timestamp)
        hdbg.dassert_is(timestamp.tz, None)
    return timestamp


def load_kibot_data(
    symbol: str,
    frequency: Union[str, vkibot.Frequency],
    contract_type: Union[str, vkibot.ContractType],
    start_date: Optional[hdateti.Datetime] = None,
    end_date: Optional[hdateti.Datetime] = None,
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
    start_date = _process_timestamp(start_date)
    end_date = _process_timestamp(end_date)
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
class KibotDataReader(dtfconobas.DataSource):
    def __init__(
        self,
        nid: dtfcornode.NodeId,
        symbol: str,
        frequency: Union[str, vkibot.Frequency],
        contract_type: Union[str, vkibot.ContractType],
        start_date: Optional[hdateti.Datetime] = None,
        end_date: Optional[hdateti.Datetime] = None,
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
        self._lazy_load()
        return super().fit()  # type: ignore[no-any-return]

    def predict(self) -> Optional[Dict[str, pd.DataFrame]]:
        self._lazy_load()
        return super().predict()  # type: ignore[no-any-return]

    def _lazy_load(self) -> None:
        if self.df is not None:
            return
        df = load_kibot_data(
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


class KibotColumnReader(dtfconobas.DataSource):
    def __init__(
        self,
        nid: dtfcornode.NodeId,
        symbols: List[str],
        frequency: Union[str, vkibot.Frequency],
        contract_type: Union[str, vkibot.ContractType],
        col: str,
        start_date: Optional[hdateti.Datetime] = None,
        end_date: Optional[hdateti.Datetime] = None,
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
        return super().fit()  # type: ignore[no-any-return]

    def predict(self) -> Optional[Dict[str, pd.DataFrame]]:
        self._lazy_load()
        return super().predict()  # type: ignore[no-any-return]

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


class KibotEquityReader(dtfconobas.DataSource):
    def __init__(
        self,
        nid: dtfcornode.NodeId,
        symbols: List[str],
        frequency: Union[str, vkibot.Frequency],
        start_date: Optional[hdateti.Datetime] = None,
        end_date: Optional[hdateti.Datetime] = None,
        nrows: Optional[int] = None,
    ) -> None:
        """
        Read equity OHLCV data.
        """
        super().__init__(nid)
        hdbg.dassert_isinstance(symbols, list)
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
        return super().fit()  # type: ignore[no-any-return]

    def predict(self) -> Optional[Dict[str, pd.DataFrame]]:
        self._lazy_load()
        return super().predict()  # type: ignore[no-any-return]

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
            n_rows = data.shape[0]
            _LOG.debug("Read %d rows for symbol=%s", n_rows, symbol)
            data = data.loc[self._start_date : self._end_date]
            _LOG.debug(
                "Retained %s rows for symbol=%s after time filtering (%.2f)",
                data.shape[0],
                symbol,
                data.shape[0] / n_rows,
            )
            hdbg.dassert(
                not data.empty, "No data for %s in requested time range", symbol
            )
            # Rename column for volume so that it adheres with our conventions.
            data = data.rename(columns={"vol": "volume"})
            # Print some info about the data.
            _LOG.debug(hprint.df_to_short_str("data", data))
            # Ensure data is on a uniform frequency grid.
            data = cofinanc.resample_ohlcv_bars(data, rule=self._frequency.value)
            dfs[symbol] = data
        # Create a dataframe with multiindexed columns.
        df = pd.concat(dfs.values(), axis=1, keys=dfs.keys())
        # Swap column levels so that symbols are leaves.
        df = df.swaplevel(i=0, j=1, axis=1)
        df.sort_index(axis=1, level=0, inplace=True)
        self.df = df


class RealTimeDataSource(dtfconobas.DataSource):
    """
    A RealTimeDataSource is a node that:

    - has a wall clock (replayed or not, simulated or real)
    - emits different data based on the value of a clock
      - This represents the fact the state of a DB is updated over time
    - has a blocking behavior
      - E.g., the data might not be available immediately when the data is
        requested and thus we have to wait
    """

    def __init__(
        self,
        nid: dtfcornode.NodeId,
        market_data_interface: mdmadain.AbstractMarketDataInterface,
        period: str,
        asset_id_col: Union[int, str],
        multiindex_output: bool,
    ) -> None:
        """
        Constructor.

        :param period: how much history is needed from the real-time node
        """
        super().__init__(nid)
        hdbg.dassert_isinstance(
            market_data_interface, mdmadain.AbstractMarketDataInterface
        )
        self._market_data_interface = market_data_interface
        self._period = period
        self._asset_id_col = asset_id_col
        self._multiindex_output = multiindex_output

    # TODO(gp): Can we use a run and move it inside fit?
    async def wait_for_latest_data(
        self,
    ) -> Tuple[pd.Timestamp, pd.Timestamp, int]:
        ret = await self._market_data_interface.is_last_bar_available()
        return ret  # type: ignore[no-any-return]

    def fit(self) -> Optional[Dict[str, pd.DataFrame]]:
        # TODO(gp): This approach of communicating params through the state
        #  makes the code difficult to understand.
        self.df = self._market_data_interface.get_data(self._period)
        if self._multiindex_output:
            self._convert_to_multiindex()
        return super().fit()  # type: ignore[no-any-return]

    def predict(self) -> Optional[Dict[str, pd.DataFrame]]:
        self.df = self._market_data_interface.get_data(self._period)
        if self._multiindex_output:
            self._convert_to_multiindex()
        return super().predict()  # type: ignore[no-any-return]

    def _convert_to_multiindex(self) -> None:
        # From _load_multiple_instrument_data().
        _LOG.debug(
            "Before multiindex conversion\n:%s",
            hprint.dataframe_to_str(self.df.head()),
        )
        dfs = {}
        # TODO(Paul): Pass the column name through the constructor, so we can make it
        # programmable.
        for asset_id, df in self.df.groupby(self._asset_id_col):
            dfs[asset_id] = df
        # Reorganize the data into the desired format.
        df = pd.concat(dfs.values(), axis=1, keys=dfs.keys())
        df = df.swaplevel(i=0, j=1, axis=1)
        df.sort_index(axis=1, level=0, inplace=True)
        self.df = df
        _LOG.debug(
            "After multiindex conversion\n:%s",
            hprint.dataframe_to_str(self.df.head()),
        )
