"""
Import as:

import oms.portfolio as omportfo
"""
import abc
import collections
import logging
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

import helpers.datetime_ as hdateti
import helpers.dbg as hdbg
import helpers.printing as hprint
import helpers.sql as hsql
import market_data.market_data_interface as mdmadain
import oms.broker as ombroker

_LOG = logging.getLogger(__name__)


# #############################################################################
# AbstractPortfolio
# #############################################################################


class AbstractPortfolio(abc.ABC):
    """
    Store holdings over time, e.g., many shares of each asset are owned at any
    time.

    Cash is treated as any other asset to keep code uniform. The tables
    are indexed by knowledge time, i.e., when this information became
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
        market_data_interface: mdmadain.AbstractMarketDataInterface,
        # TODO(*): Get the wall clock time from market data interface, if it
        #  is guaranteed to always be available.
        get_wall_clock_time: hdateti.GetWallClockTime,
        asset_id_col: str,
        mark_to_market_col: str,
        timestamp_col: str,
        holdings_df: pd.DataFrame,
    ):
        """
        Constructor.

        :param strategy_id, account: back office information about the strategy
            driving this portfolio
        :param asset_id_col: column name in the output df of `market_data_interface`
            storing the asset id
        :param mark_to_market_col: column name used as price to mark holdings to
            market
        :param timestamp_col: column to use when accessing price data
        :param holding_df: the initial state of the portfolio in terms of (cash
            and non-cash) holdings
        """
        _LOG.debug(
            hprint.to_str(
                "strategy_id account asset_id_col mark_to_market_col timestamp_col"
            )
        )
        self._strategy_id = strategy_id
        self._account = account
        #
        hdbg.dassert_issubclass(
            market_data_interface, mdmadain.AbstractMarketDataInterface
        )
        self.market_data_interface = market_data_interface
        self._get_wall_clock_time = get_wall_clock_time
        self._asset_id_col = asset_id_col
        self._mark_to_market_col = mark_to_market_col
        self._timestamp_col = timestamp_col
        #
        self._validate_initial_holdings_df(holdings_df)
        self._holdings_df = holdings_df
        # At each call to `mark_to_market()`, we capture `wall_clock_time` and
        # perform a sequence of updates to the following dictionaries.
        # - timestamp to pd.Series of holdings in shares (indexed by asset_id).
        self._asset_holdings = collections.OrderedDict()
        # - timestamp to pd.DataFrame of price, value (indexed by asset_id).
        self._assets_marked_to_market = collections.OrderedDict()
        # - timestamp to float.
        self._cash = collections.OrderedDict()
        # - timestamp to pd.Series of statistics.
        self._statistics = collections.OrderedDict()
        # We initialize the collection of dictionaries from `holdings_df`.
        initial_timestamp = holdings_df.index[0]
        self._initial_timestamp = initial_timestamp
        # Initialize cash.
        cash = holdings_df.set_index("asset_id").loc[AbstractPortfolio.CASH_ID][
            "curr_num_shares"
        ]
        _sequential_insert(initial_timestamp, cash, self._cash)
        # Initialize asset share holdings.
        asset_holdings = holdings_df[
            holdings_df["asset_id"] != AbstractPortfolio.CASH_ID
        ]
        asset_holdings = asset_holdings.set_index("asset_id")["curr_num_shares"]
        asset_holdings.name = initial_timestamp
        hdbg.dassert_isinstance(asset_holdings, pd.Series)
        _sequential_insert(
            initial_timestamp, asset_holdings, self._asset_holdings
        )
        # Price the assets at the initial timestamp.
        self._price_assets(initial_timestamp, asset_holdings)
        # Compute the initial portfolio statistics.
        self._compute_statistics()

    def __str__(self) -> str:
        """
        Return the state of the Portfolio in terms of the holdings as a string.
        """
        act = []
        act.append("# holdings=\n%s" % hprint.dataframe_to_str(self._holdings_df))
        act = "\n".join(act)
        return act

    @classmethod
    def from_cash(
        cls,
        *args: Any,
        initial_cash: float,
        initial_timestamp: pd.Timestamp,
        **kwargs: Any,
    ) -> "AbstractPortfolio":
        """
        Initialize a portfolio with no non-cash assets.
        """
        hdbg.dassert_lt(0, initial_cash)
        holdings_df = cls._create_holdings_df_from_cash(
            initial_cash, initial_timestamp
        )
        # Build the portfolio.
        portfolio = cls(
            *args,
            holdings_df,
            **kwargs,
        )
        return portfolio

    @classmethod
    def from_dict(
        cls,
        *args: Any,
        holdings_dict: Dict[int, float],
        initial_timestamp: pd.Timestamp,
        **kwargs: Any,
    ) -> "AbstractPortfolio":
        """
        Initialize from a dict of holdings and initial timestamp.
        """
        # Convert the dictionary into a df with `asset_id`, `curr_num_shares`.
        hdbg.dassert_isinstance(holdings_dict, dict)
        holdings_df = pd.Series(holdings_dict).to_frame().reset_index()
        holdings_df.columns = AbstractPortfolio.HOLDINGS_COLS
        hdbg.dassert_isinstance(initial_timestamp, pd.Timestamp)
        size = len(holdings_dict)
        holdings_df.index = [initial_timestamp] * size
        # Build the portfolio.
        portfolio = cls(
            *args,
            holdings_df,
            **kwargs,
        )
        return portfolio

    def mark_to_market(self) -> pd.DataFrame:
        """
        Mark the portfolio to market.

        This function checks the portfolio state at `wall_clock_time` and
        updates the internal state.
          - Holdings are as of `wall_clock_time` (e.g., any updates from fills)
          - Uses `market_data_interface` to price holdings
          - Computes portfolios statistics such as leverage, exposure, etc.

        # TODO(Paul): Add a dataframe snippet.

        :return: dataframe with HOLDINGS and PRICE columns
        """
        # Update asset_holdings, cash.
        self._observe_holdings()
        # Get the latest timestamp.
        ts = next(reversed(self._asset_holdings))
        # Price the assets.
        self._price_assets(ts, self._asset_holdings[ts])
        # Calculate statistics.
        self._compute_statistics()
        # Perform sanity-checks.
        # These four dictionaries should have the same keys. Here we check that
        # they have the same length.
        # TODO(Paul): Check keys instead of length.
        hdbg.dassert_eq(
            len(self._asset_holdings), len(self._assets_marked_to_market)
        )
        hdbg.dassert_eq(len(self._asset_holdings), len(self._cash))
        hdbg.dassert_eq(len(self._asset_holdings), len(self._statistics))
        #
        df = self.get_cached_mark_to_market()
        _LOG.debug("mark_to_market_df=\n%s", hprint.dataframe_to_str(df))
        # Update the internal holdings_df.
        holdings_df = df[AbstractPortfolio.HOLDINGS_COLS]
        self._holdings_df = pd.concat([holdings_df, self._holdings_df])
        self._holdings_df = self._holdings_df.convert_dtypes()
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
        return pd.DataFrame(self._statistics).transpose()

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

    # TODO(gp): -> _get_price_assets_helper?
    def price_assets(
        self,
        as_of_timestamp: pd.Timestamp,
        asset_ids: List[int],
    ) -> pd.Series:
        """
        Wrap `portfolio.market_data_interface` and packages output.

        :param as_of_timestamp: as in `market_data_interface.get_data_at_timestamp()`
        :param asset_ids: as in `market_data_interface.get_data_at_timestamp()`
        :return: series of prices at `as_of_timestamp` indexed by asset_id
        """
        price_df = self.market_data_interface.get_data_at_timestamp(
            as_of_timestamp, self._timestamp_col, asset_ids
        )
        columns = [self._asset_id_col, self._mark_to_market_col]
        hdbg.dassert_is_subset(columns, price_df.columns)
        price_df = price_df[columns]
        price_srs = price_df.set_index("asset_id")[self._mark_to_market_col]
        price_srs.name = "price"
        hdbg.dassert(not price_srs.index.has_duplicates)
        return price_srs

    # TODO(gp): -> _get_price_assets?
    def _price_assets(
        self,
        as_of_timestamp: pd.Timestamp,
        asset_ids: pd.Series,
    ) -> None:
        """
        Access the underlying market_data_interface to price assets.

        :param as_of_timestamp: timestamp to forward to `market_data_interface`
        :param asset_ids: series of share counts indexed by asset id
        :return: series of asset values
        """
        # as_of_timestamp, asset_holdings = next(reversed(self._asset_holdings))
        hdbg.dassert_isinstance(asset_ids, pd.Series)
        asset_ids_list = asset_ids.index.to_list()
        # TODO(*): Get the market as-of timestamp.
        if not asset_ids_list:
            # prices = pd.Series(name=as_of_timestamp)
            assets_marked_to_market = pd.DataFrame(
                columns=AbstractPortfolio.PRICE_COLS
            )
        else:
            prices = self.price_assets(as_of_timestamp, asset_ids_list)
            assets_marked_to_market = asset_ids * prices
            assets_marked_to_market.name = "value"
            assets_marked_to_market = pd.concat(
                [prices, assets_marked_to_market], axis=1
            )
            assets_marked_to_market.columns = AbstractPortfolio.PRICE_COLS
        hdbg.dassert_isinstance(assets_marked_to_market, pd.DataFrame)
        hdbg.dassert(not assets_marked_to_market.index.has_duplicates)
        # _sequential_insert(as_of_timestamp, prices, self._asset_prices)
        _sequential_insert(
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
    def _validate_holdings_df(
        holdings_df: pd.DataFrame,
    ) -> None:
        """
        Ensure that `holdings_df` passes basic sanity checks.
        """
        # The input should be a nonempty dataframe with a datetime index.
        hdbg.dassert_isinstance(holdings_df, pd.DataFrame)
        hdbg.dassert_isinstance(holdings_df.index, pd.DatetimeIndex)
        hdbg.dassert(hasattr(holdings_df.index.dtype, "tz"))
        hdbg.dassert(not holdings_df.empty, "The dataframe must be nonempty.")
        # The dataframe must have the correct columns.
        hdbg.dassert_eq_all(
            holdings_df.columns.to_list(),
            AbstractPortfolio.HOLDINGS_COLS,
            "Columns do not conform to requirements.",
        )
        # The columns should be of the correct types.
        hdbg.dassert_eq(
            holdings_df["asset_id"].dtype.type,
            np.int64,
            "The column `asset_id` should only contain integer ids.",
        )
        hdbg.dassert_in(
            holdings_df["curr_num_shares"].dtype.type,
            [np.float64, np.int64],
            "The column `curr_num_shares` should be a float column.",
        )
        # The dataframe must contain a row for cash.
        hdbg.dassert_in(
            AbstractPortfolio.CASH_ID,
            holdings_df["asset_id"].to_list(),
            "No cash holdings available.",
        )
        # All share values should be finite.
        hdbg.dassert(
            np.isfinite(holdings_df["curr_num_shares"]).all(),
            "All share values must be finite.",
        )

    @staticmethod
    def _validate_initial_holdings_df(holdings_df: pd.DataFrame) -> None:
        """
        Ensure that `holdings_df` qualifies as an initial holdings df.

        The dataframe should specify asset holdings (in shares) at a
        single point in time. Cash must be included and be non-negative.
        """
        AbstractPortfolio._validate_holdings_df(holdings_df)
        # Initialization should be at a single point in time.
        hdbg.dassert_eq(
            holdings_df.index.nunique(),
            1,
            "Encountered too many unique index values in initial holdings dataframe",
        )
        # At initialization there should be no more than one row per asset.
        hdbg.dassert_no_duplicates(holdings_df["asset_id"].to_list())
        # Initial cash must be non-negative.
        mask = holdings_df["asset_id"] == AbstractPortfolio.CASH_ID
        cash_holdings = holdings_df[mask]
        hdbg.dassert_eq(cash_holdings.shape[0], 1)
        cash = cash_holdings["curr_num_shares"].values[0]
        hdbg.dassert_lte(0, cash)


# #############################################################################
# SimulatedPortfolio
# #############################################################################


class SimulatedPortfolio(AbstractPortfolio):
    def __init__(
        self,
        *args: Any,
        # In Python parameters after *args are always keywords-only.
        # TODO(gp): Looks like a broker is needed for all the implementation. So
        #  move it up.
        broker: ombroker.SimulatedBroker,
    ):
        """
        Constructor.
        """
        super().__init__(*args)
        self.broker = broker

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
        Update holdings at `timestamp` using fills information in `fill_df`.
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
        _sequential_insert(
            wall_clock_timestamp, new_asset_holdings_srs, self._asset_holdings
        )
        _sequential_insert(wall_clock_timestamp, new_cash, self._cash)

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


class MockedPortfolio(AbstractPortfolio):
    def __init__(
        self,
        *args: Any,
        broker: ombroker.MockedBroker,
        db_connection: hsql.DbConnection,
        table_name: str,
        asset_id_col: str,
        poll_kwargs: Optional[Dict[str, Any]] = None,
    ):
        """
        Constructor.
        """
        super().__init__(*args)
        hdbg.dassert_issubclass(
            broker,
            ombroker.AbstractBroker,
        )
        self.broker = broker
        self._db_connection = db_connection
        self._table_name = table_name
        # TODO(gp): Initialize with a default.
        self._poll_kwargs = poll_kwargs
        hdbg.dassert_isinstance(asset_id_col, str)
        self._asset_id_col = asset_id_col
        # Keep a mapping between the wall clock timestamp and the image of the
        # holdings in the account (without cash).
        self._timestamp_to_snapshot_df = collections.OrderedDict()
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
        # TODO(gp): This query returns all the asset IDs. Should we need to subset to our
        #  universe?
        # Get the current positions.
        query = []
        query.append(f"SELECT * FROM {self._table_name}")
        wall_clock_timestamp = self._get_wall_clock_time()
        trade_date = wall_clock_timestamp.date()
        query.append(
            f"WHERE account='{self._account}' AND tradedate='{trade_date}'"
        )
        query.append(f"ORDER BY {self._asset_id_col}")
        query = "\n".join(query)
        _LOG.debug("query=%s", query)
        snapshot_df = hsql.execute_query_to_df(self._db_connection, query)
        if not snapshot_df.empty:
            hdbg.dassert(not snapshot_df["asset_id"].duplicated().any())
        # Update snapshot_df.
        # self._timestamp_to_snapshot_df[wall_clock_timestamp] = snapshot_df
        # snapshot_df looks like:
        # ```
        #  tradedate asset_id         published_dt   target_position  current_position
        # 2021-12-09    10005  2021-12-09 11:54:28   0.0              0
        # 2021-12-09    10006  2021-12-09 11:54:28   0.0              0
        # 2021-12-09    10009  1970-01-01 00:00:00   0.0              0
        # ```
        _LOG.debug("snapshot_df=\n%s", hprint.dataframe_to_str(snapshot_df))
        #
        # Update cash.
        self._update_cash(snapshot_df, wall_clock_timestamp)
        # Update asset holdings.
        asset_holdings = snapshot_df[["asset_id", "current_position"]].set_index(
            "asset_id"
        )["current_position"]
        hdbg.dassert_isinstance(asset_holdings, pd.Series)
        asset_holdings.name = wall_clock_timestamp
        _sequential_insert(
            wall_clock_timestamp, asset_holdings, self._asset_holdings
        )
        # Update snapshot_df.
        _sequential_insert(
            wall_clock_timestamp, snapshot_df, self._timestamp_to_snapshot_df
        )

    def _convert_to_holdings_df(
        self, snapshot_df: pd.DataFrame, as_of_timestamp: pd.Timestamp
    ) -> pd.DataFrame:
        """
        Convert df from SQL query into a holdings_df.
        """
        holdings_df = snapshot_df[[self._asset_id_col, "current_position"]]
        holdings_df.columns = AbstractPortfolio.HOLDINGS_COLS
        holdings_df.index = [as_of_timestamp] * snapshot_df.shape[0]
        holdings_df = holdings_df.convert_dtypes()
        return holdings_df

    def _get_net_cost(self, snapshot_df: pd.DataFrame) -> float:
        """
        Return `net_cost` of assets at `as_of_timestamp`.

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
        # `snapshot_df` should not have CASH_ID.
        hdbg.dassert_not_in(
            AbstractPortfolio.CASH_ID, snapshot_df[self._asset_id_col]
        )
        prev_cash_ts = next(reversed(self._cash))
        prev_cash = self._cash[prev_cash_ts]
        hdbg.dassert_lt(prev_cash_ts, wall_clock_timestamp)
        _LOG.debug("prev_cash=%s", prev_cash)
        hdbg.dassert(np.isfinite(prev_cash), "prev_cash=%s", prev_cash)
        prev_net_cost_ts = next(reversed(self._net_cost))
        prev_net_cost = self._net_cost[prev_net_cost_ts]
        hdbg.dassert_eq(prev_net_cost_ts, prev_cash_ts)
        current_net_cost = self._get_net_cost(snapshot_df)
        cost = current_net_cost - prev_net_cost
        hdbg.dassert(np.isfinite(cost), "cost=%s", cost)
        updated_cash = prev_cash - cost
        _LOG.debug("updated_cash=%s", updated_cash)
        self._cash[wall_clock_timestamp] = updated_cash
        self._net_cost[wall_clock_timestamp] = current_net_cost


def _sequential_insert(
    key: pd.Timestamp,
    obj: Any,
    odict: Dict[pd.Timestamp, Any],
) -> None:
    """
    Insert `(key, obj)` in `odict` ensuring that keys are in increasing order.

    Assume that `odict` is a dict maintaining the insertion order of the
    keys.
    """
    hdbg.dassert_isinstance(key, pd.Timestamp)
    hdbg.dassert_isinstance(odict, collections.OrderedDict)
    # Ensure that timestamps are inserted in increasing order.
    if odict:
        last_key = next(reversed(odict))
        hdbg.dassert_lt(last_key, key)
    # TODO(Paul): If `obj` is a series or dataframe, ensure that the index is
    #  unique.
    odict[key] = obj
