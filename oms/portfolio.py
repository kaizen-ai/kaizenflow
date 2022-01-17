"""
Import as:

import oms.portfolio as omportfo
"""
import abc
import collections
import logging
import os
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

import helpers.hasyncio as hasynci
import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hprint as hprint
import helpers.hsql as hsql
import oms.broker as ombroker

_LOG = logging.getLogger(__name__)


# #############################################################################
# AbstractPortfolio
# #############################################################################


class AbstractPortfolio(abc.ABC):
    """
    Store holdings over time, e.g., many shares of each asset are owned at any
    time.

    Cash is treated as any other asset to keep code uniform.

    The tables are indexed by knowledge time, i.e., when this information became
    known by this object.
    """

    # ID of asset representing cash.
    CASH_ID: int = -1

    # An `holding_df` represents the holdings over time, e.g.,
    # ```
    #                            asset_id  curr_num_shares
    # 2000-01-01 09:35:00-05:00        -1        1000000.0
    # 2000-01-01 09:35:00-05:00       314        1000000.0
    # ...
    # 2000-01-01 09:30:00-05:00       214       -1000000.0
    # ```
    # Columns required in a `holding_df`.
    HOLDINGS_COLS = ["asset_id", "curr_num_shares"]

    # Columns that a dataframe with prices should have.
    PRICE_COLS = ["price", "value"]

    def __init__(
        self,
        strategy_id: str,
        account: str,
        broker: ombroker.AbstractBroker,
        asset_id_col: str,
        mark_to_market_col: str,
        timestamp_col: str,
        initial_holdings: pd.Series,
    ):
        """
        Constructor.

        :param strategy_id, account: back office information about the strategy
            driving this portfolio
        :param asset_id_col: column name in the output df of `market_data`
            storing the asset id
        :param mark_to_market_col: column name used as price to mark holdings to
            market
        :param timestamp_col: column to use when accessing price data
        """
        _LOG.debug(
            hprint.to_str(
                "strategy_id account asset_id_col mark_to_market_col timestamp_col"
            )
        )
        self._strategy_id = strategy_id
        self._account = account
        # Set and unpack broker.
        hdbg.dassert_issubclass(broker, ombroker.AbstractBroker)
        self.broker = broker
        # Extract `market_data` from `broker`.
        self.market_data = broker.market_data
        # Extract `get_wall_clock_time` from `market_data`.
        self._get_wall_clock_time = broker.market_data.get_wall_clock_time
        self._asset_id_col = asset_id_col
        self._mark_to_market_col = mark_to_market_col
        self._timestamp_col = timestamp_col
        # Initialize universe and holdings.
        self._validate_initial_holdings(initial_holdings)
        initial_holdings.index.name = "asset_id"
        initial_holdings.name = "curr_num_shares"
        # Get the initialization timestamp from the wall clock.
        initial_timestamp = self._get_wall_clock_time()
        _LOG.debug("initial_timestamp=%s" % initial_timestamp)
        self._initial_timestamp = initial_timestamp
        # At each call to `mark_to_market()`, we capture `wall_clock_time` and
        # perform a sequence of updates to the following dictionaries.
        # We initialize the collection of dictionaries from `holdings_df`.
        # - timestamp to pd.Series of holdings in shares (indexed by asset_id).
        _LOG.debug("Initializing asset_holdings...")
        self._asset_holdings = collections.OrderedDict()
        asset_holdings = initial_holdings[
            initial_holdings.index != AbstractPortfolio.CASH_ID
        ]
        asset_holdings.name = initial_timestamp
        hdbg.dassert_isinstance(asset_holdings, pd.Series)
        self._sequential_insert(
            initial_timestamp, asset_holdings, self._asset_holdings
        )
        _LOG.debug("asset_holdings initialized.")
        # - timestamp to float.
        _LOG.debug("Initializing cash...")
        self._cash = collections.OrderedDict()
        cash = initial_holdings.iloc[AbstractPortfolio.CASH_ID]
        self._sequential_insert(initial_timestamp, cash, self._cash)
        _LOG.debug("cash initialized.")
        # - timestamp to pd.DataFrame of price, value (indexed by asset_id).
        _LOG.debug("Initializing assets_marked_to_market...")
        self._assets_marked_to_market = collections.OrderedDict()
        # Price the assets at the initial timestamp.
        self._price_assets(asset_holdings)
        _LOG.debug("assets_marked_to_market initialized.")
        _LOG.debug("Initializing statistics...")
        # - timestamp to pd.Series of statistics.
        self._statistics = collections.OrderedDict()
        # Compute the initial portfolio statistics.
        self._compute_statistics()
        _LOG.debug("statistics initialized.")
        #
        self._initial_universe = asset_holdings.index

    def __str__(self) -> str:
        """
        Return the state of the Portfolio in terms of the holdings as a string.
        """
        act = []
        act.append(
            "# historical holdings=\n%s"
            % hprint.dataframe_to_str(self.get_historical_holdings())
        )
        act.append(
            "# historical holdings marked to market=\n%s"
            % hprint.dataframe_to_str(
                self.get_historical_holdings_marked_to_market()
            )
        )
        act.append(
            "# historical statistics=\n%s"
            % hprint.dataframe_to_str(self.get_historical_statistics())
        )
        act = "\n".join(act)
        return act

    @classmethod
    def from_cash(
        cls,
        *args: Any,
        initial_cash: float,
        asset_ids: Optional[List[int]] = None,
        **kwargs: Any,
    ) -> "AbstractPortfolio":
        """
        Initialize with no non-cash assets.
        """
        hdbg.dassert_lt(0, initial_cash)
        if not asset_ids:
            asset_ids = []
        hdbg.dassert_not_in(AbstractPortfolio.CASH_ID, asset_ids)
        holdings_dict = {asset_id: 0 for asset_id in asset_ids}
        holdings_dict[AbstractPortfolio.CASH_ID] = initial_cash
        portfolio = cls.from_dict(
            *args,
            holdings_dict=holdings_dict,
            **kwargs,
        )
        return portfolio

    @classmethod
    def from_dict(
        cls,
        *args: Any,
        holdings_dict: Dict[int, float],
        **kwargs: Any,
    ) -> "AbstractPortfolio":
        """
        Initialize from a dict of holdings and initial timestamp.

        :param holdings_dict: dictionary from asset_id to position
        """
        hdbg.dassert_isinstance(holdings_dict, dict)
        initial_holdings = pd.Series(holdings_dict)
        portfolio = cls(
            *args,
            initial_holdings,
            **kwargs,
        )
        return portfolio

    @property
    def universe(self) -> List[int]:
        # TODO(Paul): Consider making this time-dependent.
        return self._initial_universe.to_list()

    def get_asset_ids_not_in_universe(self, asset_ids: List[int]) -> List[int]:
        """
        Get subset of asset ids not in universe. Empty list if null.

        :param asset_ids: query asset_ids
        :return: asset ids in `asset_ids` not in portfolio universe.
        """
        idx = pd.Index(asset_ids)
        diff = idx.difference(self._initial_universe)
        return diff.to_list()

    def mark_to_market(self) -> pd.DataFrame:
        """
        Mark the portfolio to market.

        This function checks the portfolio state at `wall_clock_time` and
        updates the internal state.
          - Holdings are as of `wall_clock_time` (e.g., any updates from fills)
          - Uses `market_data` to price holdings
          - Computes portfolios statistics such as leverage, exposure, etc.

        # TODO(Paul): Add a dataframe snippet.

        :return: dataframe with HOLDINGS and PRICE columns
        """
        _LOG.debug("Marking to market...")
        # Update asset_holdings, cash.
        self._observe_holdings()
        # Get the latest timestamp.
        ts = next(reversed(self._asset_holdings))
        # Price the assets.
        self._price_assets(self._asset_holdings[ts])
        # Calculate statistics.
        self._compute_statistics()
        # Perform sanity-checks.
        # These four dictionaries should have the same keys. Here we check that
        # they have the same length.
        # TODO(Paul): Maybe check keys instead of length.
        hdbg.dassert_eq(
            len(self._asset_holdings), len(self._assets_marked_to_market)
        )
        hdbg.dassert_eq(len(self._asset_holdings), len(self._cash))
        hdbg.dassert_eq(len(self._asset_holdings), len(self._statistics))
        #
        df = self.get_cached_mark_to_market()
        _LOG.debug("mark_to_market_df=\n%s", hprint.dataframe_to_str(df))
        return df

    def get_cached_mark_to_market(self) -> pd.DataFrame:
        """
        Retrieve the last cached mark-to-market dataframe.

        This does not perform a new observation of the market.

        return: same as `mark_to_market()`
        """
        # Get latest timestamp available.
        timestamp = next(reversed(self._asset_holdings))
        _LOG.debug("Retrieving holdings at timestamp=%s", timestamp)
        # Create a `holdings_df` with assets and cash.
        holdings_df = self._asset_holdings[timestamp].reset_index()
        cash_df = AbstractPortfolio._create_holdings_df_from_cash(
            self._cash[timestamp], timestamp
        )
        holdings_df.columns = AbstractPortfolio.HOLDINGS_COLS
        holdings_df.index = [timestamp] * holdings_df.shape[0]
        holdings_df = pd.concat([holdings_df, cash_df])
        holdings_df = holdings_df.convert_dtypes()
        # Get mark-to-market values.
        marked_to_market = self._assets_marked_to_market[timestamp]
        cash_row = pd.DataFrame(
            index=[AbstractPortfolio.CASH_ID],
            columns=AbstractPortfolio.PRICE_COLS,
            data=[[1.0, self._cash[timestamp]]],
        )
        marked_to_market = marked_to_market.append(cash_row)
        marked_to_market.index.name = "asset_id"
        marked_to_market = marked_to_market.reset_index()
        marked_to_market.index = [timestamp] * marked_to_market.index.shape[0]
        # Merge holdings and mark-to-market dataframes.
        df = holdings_df.merge(marked_to_market, on="asset_id")
        df["wall_clock_timestamp"] = [timestamp] * df.shape[0]
        df.index = [timestamp] * df.shape[0]
        df = df.convert_dtypes()
        return df

    def get_historical_statistics(self) -> pd.DataFrame:
        """
        Return a dataframe of portfolio statistics over time.
        """
        df = pd.DataFrame(self._statistics).transpose()
        # Add `pnl` by diffing the snapshots of `net_wealth`.
        df["pnl"] = df["net_wealth"].diff()
        df["realized_pnl"] = df["cash"].diff()
        df["unrealized_pnl"] = df["net_asset_holdings"].diff()
        return df

    def get_historical_holdings(self) -> pd.DataFrame:
        """
        Return a dataframe of portfolio holdings in shares over time.
        """
        asset_holdings = pd.DataFrame(self._asset_holdings).transpose()
        cash = pd.Series(self._cash)
        asset_holdings[AbstractPortfolio.CASH_ID] = cash
        return asset_holdings

    def get_historical_holdings_marked_to_market(self) -> pd.DataFrame:
        """
        Return a dataframe of portfolio holdings in dollars over time.
        """
        asset_values = collections.OrderedDict()
        for k, v in self._assets_marked_to_market.items():
            asset_values[k] = v["value"]
        asset_values = pd.DataFrame(asset_values).transpose()
        cash = pd.Series(self._cash)
        asset_values[AbstractPortfolio.CASH_ID] = cash
        return asset_values

    def log_state(self, log_dir: str) -> None:
        hdbg.dassert(log_dir, "Must specify `log_dir` to log state.")
        #
        wall_clock_time = self._get_wall_clock_time()
        wall_clock_time_str = wall_clock_time.strftime("%Y%m%d_%H%M%S")
        filename = f"{wall_clock_time_str}.csv"
        #
        holdings_df = self.get_historical_holdings()
        holdings_filename = os.path.join(log_dir, "holdings", filename)
        hio.create_enclosing_dir(holdings_filename, incremental=True)
        holdings_df.to_csv(holdings_filename)
        #
        holdings_mtm = self.get_historical_holdings_marked_to_market()
        holdings_mtm_filename = os.path.join(
            log_dir, "holdings_marked_to_market", filename
        )
        hio.create_enclosing_dir(holdings_mtm_filename, incremental=True)
        holdings_mtm.to_csv(holdings_mtm_filename)
        #
        stats = self.get_historical_statistics()
        stats_filename = os.path.join(log_dir, "statistics", filename)
        hio.create_enclosing_dir(stats_filename, incremental=True)
        stats.to_csv(stats_filename)

    def price_assets(
        self,
        asset_ids: List[int],
    ) -> pd.Series:
        """
        Wrap `portfolio.market_data` and packages output.

        :param asset_ids: as in `market_data.get_data_at_timestamp()`
        :return: series of prices at `as_of_timestamp` indexed by asset_id
        """
        prices = self.market_data.get_last_price(
            self._mark_to_market_col, asset_ids
        )
        hdbg.dassert_eq(self._mark_to_market_col, prices.name)
        prices.index.name = "asset_id"
        prices.name = "price"
        hdbg.dassert(not prices.index.has_duplicates)
        return prices

    @abc.abstractmethod
    def _observe_holdings(
        self,
    ) -> None:
        """
        Update holdings and cash at the current wall clock time.
        """
        ...

    def _price_assets(
        self,
        asset_ids: pd.Series,
    ) -> None:
        """
        Access the underlying market_data to price assets.

        :param asset_ids: series of share counts indexed by asset id
        :return: series of asset values
        """
        as_of_timestamp = next(reversed(self._asset_holdings))
        _LOG.debug("_price_assets `as_of_timestamp`=%s" % as_of_timestamp)
        hdbg.dassert_isinstance(asset_ids, pd.Series)
        asset_ids_list = asset_ids.index.to_list()
        # TODO(*): Get the market as-of timestamp.
        if not asset_ids_list:
            # prices = pd.Series(name=as_of_timestamp)
            assets_marked_to_market = pd.DataFrame(
                columns=AbstractPortfolio.PRICE_COLS
            )
        else:
            # TODO(gp): A bit weird that we are calling the public method from the
            # private.
            prices = self.price_assets(asset_ids_list)
            assets_marked_to_market = asset_ids * prices
            assets_marked_to_market.name = "value"
            assets_marked_to_market = pd.concat(
                [prices, assets_marked_to_market], axis=1
            )
            assets_marked_to_market.columns = AbstractPortfolio.PRICE_COLS
        hdbg.dassert_isinstance(assets_marked_to_market, pd.DataFrame)
        hdbg.dassert(not assets_marked_to_market.index.has_duplicates)
        self._sequential_insert(
            as_of_timestamp,
            assets_marked_to_market,
            self._assets_marked_to_market,
        )

    def _compute_statistics(self) -> None:
        """
        Compute various portfolio statistics using latest holdings and prices.

        Return asset/cash values, net wealth, exposure, and leverage for
        the portfolio at a given timestamp.
        """
        cash_ts = next(reversed(self._cash))
        assets_ts = next(reversed(self._assets_marked_to_market))
        hdbg.dassert_eq(cash_ts, assets_ts)
        hdbg.dassert_not_in(cash_ts, self._statistics)
        # Compute value of holdings.
        asset_holdings = self._assets_marked_to_market[assets_ts]["value"]
        holdings_net_value = asset_holdings.sum()
        hdbg.dassert(
            np.isfinite(holdings_net_value),
            "holdings_net_value=%s",
            holdings_net_value,
        )
        # Get the cash available.
        cash = self._cash[cash_ts]
        hdbg.dassert(np.isfinite(cash), "cash=%s", cash)
        # Cash should always be positive.
        hdbg.dassert_lte(0.0, cash, "cash should always be positive")
        # Compute the net wealth (AKA "total value" AKA "NAV").
        net_wealth = holdings_net_value + cash
        hdbg.dassert(np.isfinite(net_wealth), "net_value=%s", net_wealth)
        # Compute the gross exposure.
        gross_exposure = asset_holdings.abs().sum()
        # Compute the portfolio leverage.
        leverage = gross_exposure / net_wealth
        dict_ = {
            "net_asset_holdings": holdings_net_value,
            "cash": cash,
            "net_wealth": net_wealth,
            "gross_exposure": gross_exposure,
            "leverage": leverage,
        }
        statistics = pd.Series(dict_, name=cash_ts)
        self._statistics[cash_ts] = statistics

    @staticmethod
    def _create_holdings_df_from_cash(
        cash: float, timestamp: pd.Timestamp
    ) -> pd.DataFrame:
        # The holdings_df has a single row about cash.
        row = [AbstractPortfolio.CASH_ID, cash]
        holdings_df = pd.DataFrame(
            [row],
            index=[timestamp],
            columns=AbstractPortfolio.HOLDINGS_COLS,
        )
        return holdings_df

    @staticmethod
    def _validate_mark_to_market_df(
        df: pd.DataFrame,
    ) -> None:
        """
        Ensure that `df` passes basic sanity checks for `mark_to_market()`.
        """
        # The input should be a dataframe satisfying the following column
        # constraints.
        hdbg.dassert_isinstance(df, pd.DataFrame)
        hdbg.dassert_is_subset(AbstractPortfolio.HOLDINGS_COLS, df.columns)
        hdbg.dassert_not_in("price", df.columns)
        hdbg.dassert_not_in("value", df.columns)
        # The columns should be of the correct types. Skip if the dataframe is
        # empty (since the correct types are not inferred in that case).
        if not df.empty:
            hdbg.dassert_eq(
                df["asset_id"].dtype.type,
                np.int64,
                "The column `asset_id` should only contain integer ids.",
            )
            hdbg.dassert_in(
                df["curr_num_shares"].dtype.type,
                [np.float64, np.int64],
                "The column `curr_num_shares` should be a float column.",
            )
        # There should be no more than one row per asset.
        hdbg.dassert_no_duplicates(df["asset_id"].to_list())
        # All share values should be finite.
        hdbg.dassert(
            np.isfinite(df["curr_num_shares"]).all(),
            "All share values must be finite.",
        )

    @staticmethod
    def _validate_initial_holdings(
        initial_holdings: pd.Series,
    ) -> None:
        """
        Ensure that `initial_holdings` passes basic sanity checks.
        """
        idx = initial_holdings.index
        # The index must be an Int64Index (asset_id must be an int) with no
        # duplicates. The ID for cash must be present.
        hdbg.dassert(not idx.has_duplicates)
        hdbg.dassert_isinstance(idx, pd.Int64Index)
        hdbg.dassert_in(
            AbstractPortfolio.CASH_ID,
            idx,
            "No cash holdings available.",
        )
        # The holdings must be numerical. They represent number of shares.
        hdbg.dassert_in(
            initial_holdings.dtype.type,
            [np.float64, np.int64],
            "The initial holdings should consist of numerical values (in shares).",
        )
        # All share values should be finite.
        hdbg.dassert(
            np.isfinite(initial_holdings).all(),
            "All share values must be finite.",
        )
        initial_cash = initial_holdings.iloc[AbstractPortfolio.CASH_ID]
        hdbg.dassert_lte(0, initial_cash)

    @staticmethod
    def _sequential_insert(
        key: pd.Timestamp,
        obj: Any,
        odict: Dict[pd.Timestamp, Any],
    ) -> None:
        """
        Insert `(key, obj)` in `odict` ensuring that keys are in increasing
        order.

        Assume that `odict` is a dict maintaining the insertion order of
        the keys.
        """
        hdbg.dassert_isinstance(key, pd.Timestamp)
        hdbg.dassert_isinstance(odict, collections.OrderedDict)
        # Ensure that timestamps are inserted in increasing order.
        if odict:
            last_key = next(reversed(odict))
            hdbg.dassert_lt(last_key, key)
        if isinstance(obj, pd.Series) or isinstance(obj, pd.DataFrame):
            hdbg.dassert(not obj.index.has_duplicates)
        odict[key] = obj


# #############################################################################
# SimulatedPortfolio
# #############################################################################


# TODO(gp): -> InMemoryPortfolio, DataFramePortfolio?
class SimulatedPortfolio(AbstractPortfolio):

    # A `fills_df` represents orders that have been executed (e.g., how many shares,
    # at how much).
    # Columns required in a `fills_df`.
    FILLS_COLS = [
        "asset_id",
        "timestamp",
        "num_shares",
        "price",
    ]

    def _observe_holdings(
        self,
    ) -> None:
        """
        Update holdings at the current wall clock time using fills information.
        """
        wall_clock_timestamp = self._get_wall_clock_time()
        # Round the time in simulation to nearest second.
        # TODO: consider changing the resolution.
        # wall_clock_timestamp = wall_clock_timestamp.round("S")
        # Get fills.
        fills_df = self._get_fills(wall_clock_timestamp)
        # Get latest holdings
        prev_asset_holdings_ts = next(reversed(self._asset_holdings))
        prev_asset_holdings = self._asset_holdings[prev_asset_holdings_ts]
        hdbg.dassert_lt(prev_asset_holdings_ts, wall_clock_timestamp)
        prev_cash_holdings_ts = next(reversed(self._cash))
        prev_cash = self._cash[prev_cash_holdings_ts]
        hdbg.dassert_lt(prev_cash_holdings_ts, wall_clock_timestamp)
        new_cash = prev_cash
        # Update holdings using the `fills_df`.
        new_asset_holdings_srs = prev_asset_holdings.copy()
        if fills_df is not None:
            SimulatedPortfolio._validate_fills_df(fills_df)
            # last_timestamp <= fills_df.index <= timestamp
            hdbg.dassert_lte(prev_asset_holdings_ts, fills_df["timestamp"].min())
            hdbg.dassert_lte(fills_df["timestamp"].max(), wall_clock_timestamp)
            holdings_diff = fills_df.set_index("asset_id")["num_shares"]
            cash_diff = -1 * (fills_df["price"] * fills_df["num_shares"]).sum()
            hdbg.dassert(np.isfinite(cash_diff))
            new_asset_holdings_srs = new_asset_holdings_srs.add(
                holdings_diff, fill_value=0
            )
            new_cash += cash_diff
        hdbg.dassert(not new_asset_holdings_srs.index.has_duplicates)
        self._sequential_insert(
            wall_clock_timestamp, new_asset_holdings_srs, self._asset_holdings
        )
        self._sequential_insert(wall_clock_timestamp, new_cash, self._cash)

    def _get_fills(
        self,
        wall_clock_timestamp: pd.Timestamp,
    ) -> pd.DataFrame:
        """
        Get the fills from the broker and convert it into a `fills_df`.

        :return: fills_df
        """
        _LOG.debug("")
        # Get the fills from the broker.
        fills = self.broker.get_fills()
        # Convert the fills into a `fills_df`.
        fill_rows = []
        for fill in fills:
            _LOG.debug("# Processing fill=%s", fill)
            fill_row: Dict[str, Any] = collections.OrderedDict()
            # Copy contents of the fill.
            fill_row.update(fill.to_dict())
            fill_rows.append(pd.Series(fill_row))
        if fill_rows:
            fills_df = pd.concat(fill_rows, axis=1).transpose()
            fills_df = fills_df.convert_dtypes()
        else:
            fills_df = None
        _LOG.debug("fills_df=\n%s", hprint.dataframe_to_str(fills_df))
        return fills_df

    @staticmethod
    def _validate_fills_df(
        fills_df: pd.DataFrame,
    ) -> None:
        """
        Ensure that `fills_df` passes basic sanity checks.
        """
        # The input should be a nonempty dataframe.
        hdbg.dassert_isinstance(fills_df, pd.DataFrame)
        hdbg.dassert(not fills_df.empty, "The dataframe must be nonempty.")
        # The dataframe must have the correct columns.
        hdbg.dassert_is_subset(
            SimulatedPortfolio.FILLS_COLS,
            fills_df.columns.to_list(),
            "Columns do not conform to requirements.",
        )
        # The columns should be of the correct types.
        hdbg.dassert_eq(
            fills_df["asset_id"].dtype.type,
            np.int64,
            "The column `asset_id` should only contain integer ids.",
        )
        hdbg.dassert_eq(
            fills_df["num_shares"].dtype.type,
            np.float64,
            "The column `curr_num_shares` should be a float column.",
        )
        #
        hdbg.dassert_eq(fills_df["timestamp"].dtype.type, pd.Timestamp)
        hdbg.dassert(hasattr(fills_df["timestamp"].dtype, "tz"))
        # The dataframe must not contain a row for cash.
        hdbg.dassert_not_in(
            SimulatedPortfolio.CASH_ID,
            fills_df["asset_id"].to_list(),
            "Order for cash detected.",
        )
        # There should be no more than one row per asset.
        hdbg.dassert_no_duplicates(fills_df["asset_id"].to_list())
        # All share values should be finite.
        _LOG.debug("fills_df=%s", fills_df)
        hdbg.dassert(
            np.isfinite(fills_df["num_shares"]).all(),
            "All share values must be finite.",
        )


# #############################################################################
# MockedPortfolio
# #############################################################################


# TODO(gp): -> DatabasePortfolio, DbPortfolio, DbBasedPortfolio
#  The important characteristic is how it's implemented rather than that it's a
#  mocked version of the implemented system.
#  In fact the implemented Portfolio and Broker descend from this class because
#  they implement the logic talking to the DB.
class MockedPortfolio(AbstractPortfolio):
    """
    Portfolio class using a DB to store the state of the holdings.

    A `snapshot_df` contains the image of the current holdings (excluding cash)
    in the account and it is maintained by an external DB, e.g.,

    ```
     tradedate           published_dt   curr_pos
               asset_id         target_pos       net_cost
    2021-12-09    10005  ... 11:54:28  0.0     0      ...
    2021-12-09    10006  ... 11:54:28  0.0     0      ...
    2021-12-09    10009  ... 00:00:00  0.0     0      ...
    ```
    """

    def __init__(
        self,
        *args: Any,
        db_connection: hsql.DbConnection,
        # TODO(gp): -> position_table_name
        table_name: str,
        poll_kwargs: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ):
        """
        Constructor.
        """
        super().__init__(*args, **kwargs)
        #
        self._db_connection = db_connection
        self._table_name = table_name
        #
        if poll_kwargs is None:
            poll_kwargs = hasynci.get_poll_kwargs(self._get_wall_clock_time)
        self._poll_kwargs = poll_kwargs
        # wall clock timestamp -> snapshot_df (i.e., the image of the holdings in
        # the account, without cash).
        self._timestamp_to_snapshot_df = collections.OrderedDict()
        # wall clock timestamp -> total net cost of transactions since the BOD.
        self._net_cost = collections.OrderedDict()
        self._net_cost[self._initial_timestamp] = 0.0

    def _observe_holdings(self) -> None:
        # The current positions table has the following fields:
        # - tradedate (e.g., 2021-10-28)
        # - id (e.g., 10005)
        # - strategyid (e.g., SAU1)
        # - account (e.g., SAU1_CAND)
        # - published_dt (e.g., 2021-10-28 12:01:49.41)
        # - target_position (e.g., 300)
        #   = the fully realized portfolio position
        #   - E.g., the value is 300 if we own 100 AAPL and we want get 200 more so
        #     we send an order for 200
        # - current_position (e.g., 100)
        #   = what we own (e.g., 100 AAPL)
        # - open_quantity (e.g., 200)
        #   = how many shares we have orders open in the market. In other words,
        #     open quantity reflects how much is out getting executed in the market
        #   - Note that it's not always
        #     `open_quantity = target_position - current_position`
        #     since orders might have been cancelled. If `open_quantity = 0` it
        #     means that there are no order in the market
        #   - E.g., if we send orders for 200 AAPL, then current_position = 200, but
        #     if we cancel the orders, current_position = 0, even if target_position
        #     reports what we were targeting 200 shares
        # - net_cost (e.g., 0.0)
        #   = fill-quantity * signed fill_price with respect to the BOD price
        #   - In practice it is the average price paid
        # - currency (e.g., USD)
        # - fx (e.g., 1.0)
        # - action (e.g., BOD)
        #   - TODO(gp): Find out what it means and if interesting
        # - bod_position (e.g., 0)
        #   - = number of shares at BOD
        # - bod_price (e.g., 0.0)
        #   - = price of a share at BOD
        # Wait until the portfolio is stable.
        # TODO(gp): We can either assert or wait until the portfolio is stable.
        # Get the current positions.
        query = []
        query.append(f"SELECT * FROM {self._table_name}")
        wall_clock_timestamp = self._get_wall_clock_time()
        _LOG.debug("wall_clock_timestamp=%s" % wall_clock_timestamp)
        trade_date = wall_clock_timestamp.date()
        # Restrict query to portfolio universe.
        hdbg.dassert(self.universe, "Universe is empty.")
        _LOG.debug("universe=\n%s", self.universe)
        universe = tuple(self.universe)
        if len(universe) == 1:
            universe = str(universe)[:-2] + ")"
        else:
            universe = universe
        # TODO(Paul): Make sure that we do not exclude IDs with a nonzero current/target position.
        #  (ensure that these are included in the universe).
        query.append(
            f"WHERE account='{self._account}' AND tradedate='{trade_date}' AND {self._asset_id_col} IN {universe}"
        )
        query.append(f"ORDER BY {self._asset_id_col}")
        query = "\n".join(query)
        _LOG.debug("query=%s", query)
        # Retrieve the data from the DB.
        snapshot_df = hsql.execute_query_to_df(self._db_connection, query)
        snapshot_df.rename(columns={self._asset_id_col: "asset_id"}, inplace=True)
        # `snapshot_df` looks like:
        # ```
        #  tradedate asset_id        published_dt  target_position  current_position
        # 2021-12-09    10005 2021-12-09 11:54:28  0.0              0
        # 2021-12-09    10006 2021-12-09 11:54:28  0.0              0
        # 2021-12-09    10009 1970-01-01 00:00:00  0.0              0
        # ```
        _LOG.debug("snapshot_df=\n%s", hprint.dataframe_to_str(snapshot_df))
        if not snapshot_df.empty:
            hdbg.dassert_no_duplicates(
                snapshot_df["asset_id"],
                "Each asset_id should be unique in a snapshot_df",
            )
        if False:
            file_name = f"snapshot_df.{wall_clock_timestamp}.csv"
            file_name = file_name.replace(" ", "_")
            _LOG.debug("Saving %s", file_name)
            snapshot_df.to_csv(file_name)
        # Update cash from snapshot_df.
        self._update_cash(snapshot_df, wall_clock_timestamp)
        # Update asset holdings from snapshot_df.
        asset_holdings = snapshot_df[["asset_id", "current_position"]].set_index(
            "asset_id"
        )["current_position"]
        hdbg.dassert_isinstance(asset_holdings, pd.Series)
        asset_holdings.name = wall_clock_timestamp
        self._sequential_insert(
            wall_clock_timestamp, asset_holdings, self._asset_holdings
        )
        # Update snapshot_df.
        self._sequential_insert(
            wall_clock_timestamp, snapshot_df, self._timestamp_to_snapshot_df
        )

    def _convert_to_holdings_df(
        self, snapshot_df: pd.DataFrame, as_of_timestamp: pd.Timestamp
    ) -> pd.DataFrame:
        """
        Convert a snapshot_df from SQL query into a `holdings_df`.
        """
        holdings_df = snapshot_df[[self._asset_id_col, "current_position"]]
        holdings_df.columns = AbstractPortfolio.HOLDINGS_COLS
        holdings_df.index = [as_of_timestamp] * snapshot_df.shape[0]
        holdings_df = holdings_df.convert_dtypes()
        return holdings_df

    @staticmethod
    def _get_net_cost(snapshot_df: pd.DataFrame) -> float:
        """
        Return the `net_cost` of assets stored in a `snapshot_df`.

        This is a helper for `_update_cash()`.
        """
        if snapshot_df.empty:
            return 0.0
        hdbg.dassert_in("net_cost", snapshot_df.columns)
        net_cost = snapshot_df["net_cost"].sum()
        _LOG.debug("net_cost=%f", net_cost)
        return net_cost

    def _update_cash(
        self,
        snapshot_df: pd.DataFrame,
        wall_clock_timestamp: pd.Timestamp,
    ) -> None:
        """
        Compute the current cash amount using:

        - the net cost information from `snapshot_df`; and
        - the previous cash amount
        """
        # `snapshot_df` should not have CASH_ID.
        hdbg.dassert_not_in(AbstractPortfolio.CASH_ID, snapshot_df["asset_id"])
        # Get the cash at the previous timestamp.
        prev_cash_ts = next(reversed(self._cash))
        hdbg.dassert_lt(prev_cash_ts, wall_clock_timestamp)
        prev_cash = self._cash[prev_cash_ts]
        _LOG.debug("prev_cash=%s", prev_cash)
        hdbg.dassert(np.isfinite(prev_cash), "prev_cash=%s", prev_cash)
        # Get the net cost at the previous timestamp.
        prev_net_cost_ts = next(reversed(self._net_cost))
        prev_net_cost = self._net_cost[prev_net_cost_ts]
        hdbg.dassert_eq(prev_net_cost_ts, prev_cash_ts)
        # Get the current net cost.
        current_net_cost = self._get_net_cost(snapshot_df)
        hdbg.dassert(
            np.isfinite(current_net_cost), "current_net_cost=%s", current_net_cost
        )
        # The cost of the previous transactions is the difference of net cost.
        cost = current_net_cost - prev_net_cost
        hdbg.dassert(np.isfinite(cost), "cost=%s", cost)
        # The current cash is given by the previous cash and the cash spent in the
        # previous transactions.
        updated_cash = prev_cash - cost
        _LOG.debug("updated_cash=%s", updated_cash)
        # Update the cash and net cost.
        self._cash[wall_clock_timestamp] = updated_cash
        self._net_cost[wall_clock_timestamp] = current_net_cost
