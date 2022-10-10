"""
Import as:

import oms.portfolio as omportfo
"""

import abc
import collections
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from tqdm.autonotebook import tqdm

import core.key_sorted_ordered_dict as cksoordi
import helpers.hasyncio as hasynci
import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hobject as hobject
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import helpers.hsql as hsql
import helpers.hwall_clock_time as hwacltim
import oms.broker as ombroker

_LOG = logging.getLogger(__name__)


# #############################################################################
# Portfolio
# #############################################################################


# TODO(gp): Ideally log_dir should be passed to the constructor.
class Portfolio(abc.ABC, hobject.PrintableMixin):
    """
    Store holdings_shares over time, e.g., how many shares of each asset are
    owned at any time. Cash is treated as just another asset to keep code
    uniform.

    The data is indexed by knowledge time, i.e., when this information became
    known to this object.

    A `Portfolio` tracks a fixed universe of asset ids.

    A `Portfolio` needs a `Broker` to:
    - connect to the `MarketData` to receive prices
    - receive the fills and update the holdings_shares.
    """

    # ID of asset representing cash.
    CASH_ID: int = -1

    # An `holding_df` represents the holdings_shares over time, e.g.,
    # ```
    #                            asset_id  curr_num_shares
    # 2000-01-01 09:35:00-05:00        -1        1000000.0
    # 2000-01-01 09:35:00-05:00       314        1000000.0
    # ...
    # 2000-01-01 09:30:00-05:00       214       -1000000.0
    # ```
    # Columns required in a `holdings_shares_df`.
    # TODO(Paul): Change "curr_num_shares" to "holdings_shares".
    HOLDINGS_COLS = ["asset_id", "curr_num_shares"]

    # Columns that a dataframe with prices should have.
    # TODO(Paul): Change "value" to "holdings_notional".
    PRICE_COLS = ["price", "value"]

    def __init__(
        self,
        broker: ombroker.Broker,
        mark_to_market_col: str,
        pricing_method: str,
        initial_holdings_shares: pd.Series,
        *,
        retrieve_initial_holdings_from_db: bool = False,
        max_num_bars: Optional[int] = 100,
    ):
        """
        Constructor.

        :param broker: the `Broker` object used to retrieve prices and fills
        :param mark_to_market_col: column name used as price to mark holdings_shares to
            market
        :param pricing_method: pricing methodology to use for valuing assets.
            If e.g. "twap", then we also include the bar duration as a
            pandas-style suffix: "twap.5T"
        :param initial_holdings_shares: initial positions in shares indexed by integer
            asset_ids; no NaNs are allowed unless
            `retrieve_initial_holdings_from_db=True`, in which case all values
            must be NaN.
        :param retrieve_initial_holdings_from_db: `True` iff holdings_shares are
            initialized via an external database. The asset ids of nonzero
            holdings_shares must be a subset of the index of `initial_holdings`.
        :param max_num_bars: maximum number of market data bars to store in memory;
            if `None`, then impose no restriction.
        """
        _LOG.debug(
            hprint.to_str(
                "broker mark_to_market_col pricing_method initial_holdings_shares "
                "retrieve_initial_holdings_from_db max_num_bars"
            )
        )
        # Set and unpack broker.
        hdbg.dassert_issubclass(broker, ombroker.Broker)
        self.broker = broker
        self._account = broker.account
        self._timestamp_col = broker.timestamp_col
        # Extract `MarketData` object from `Broker` and extract other information.
        self.market_data = broker.market_data
        self._get_wall_clock_time = self.market_data.get_wall_clock_time
        self._asset_id_col = self.market_data.asset_id_col
        self._mark_to_market_col = mark_to_market_col
        # Parse `pricing_method`.
        self._pricing_type, self._bar_duration = self._parse_pricing_method(
            pricing_method
        )
        # Initialize bookkeeping dictionaries.
        # At each call to `mark_to_market()`, we capture `wall_clock_time` and
        # perform a sequence of updates to the following dictionaries.
        self._max_num_bars = max_num_bars
        # We use `KeySortedOrderedDict` keyed `timestamp` to:
        # - enforce that inserted new keys are always increasing according to the
        #   key order (i.e., increasing in time)
        # - simplify extracting the last timestamp
        # We initialize the collection of dictionaries from `holdings_shares_df`.
        # - timestamp to pd.Series of holdings_shares in shares (indexed by asset_id)
        # - this does not include the "cash asset".
        self._holdings_shares = cksoordi.KeySortedOrderedDict(
            pd.Timestamp, self._max_num_bars
        )
        # - timestamp to float value of cash
        self._cash = cksoordi.KeySortedOrderedDict(
            pd.Timestamp, self._max_num_bars
        )
        # - timestamp to pd.DataFrame of price, value (indexed by asset_id)
        self._holdings_notional = cksoordi.KeySortedOrderedDict(
            pd.Timestamp, self._max_num_bars
        )
        # - timestamp to pd.Series of notional trades (indexed by asset_id)
        self._executed_trades_notional = cksoordi.KeySortedOrderedDict(
            pd.Timestamp, self._max_num_bars
        )
        # - timestamp to pd.Series of statistics
        self._statistics = cksoordi.KeySortedOrderedDict(
            pd.Timestamp, self._max_num_bars
        )
        # Validate universe and holdings_shares.
        self._retrieve_initial_holdings_shares_from_db = (
            retrieve_initial_holdings_from_db
        )
        if not self._retrieve_initial_holdings_shares_from_db:
            # The client passed initial holdings_shares and not just the allowed universe,
            # so we need to make sure that the holdings_shares are valid (e.g., contain
            # no NaNs).
            self._validate_initial_holdings(initial_holdings_shares)
        # TODO(Paul): these should be kept in alignment with `HOLDING_COLS`.
        initial_holdings_shares.index.name = "asset_id"
        initial_holdings_shares.name = "curr_num_shares"
        self._initial_holdings = initial_holdings_shares
        # Set the initial universe.
        self._initial_universe = initial_holdings_shares.index.drop(
            Portfolio.CASH_ID
        )
        #
        _LOG.debug("After initialization:\n%s", repr(self))

    def __str__(self, num_periods: Optional[int] = None) -> str:
        """
        Return the state of the Portfolio in terms of the holdings_shares as a
        string.
        """
        txt = []
        # <...portfolio at 0x>
        txt.append(hprint.to_object_repr(self))
        # Print the rest of the data in a more readable format.
        txt_tmp = []
        if num_periods:
            hdbg.dassert_lte(1, num_periods)
        precision = 2
        txt_tmp.append(
            "# holdings_shares=\n%s"
            % hpandas.df_to_str(
                self.get_historical_holdings_shares(num_periods),
                num_rows=None,
                precision=precision,
            )
        )
        txt_tmp.append(
            "# holdings_notional=\n%s"
            % hpandas.df_to_str(
                self.get_historical_holdings_notional(num_periods),
                num_rows=None,
                precision=precision,
            )
        )
        txt_tmp.append(
            "# executed_trades_shares=\n%s"
            % hpandas.df_to_str(
                self.get_historical_executed_trades_shares(num_periods),
                num_rows=None,
                precision=precision,
            )
        )
        txt_tmp.append(
            "# executed_trades_notional=\n%s"
            % hpandas.df_to_str(
                self.get_historical_executed_trades_notional(num_periods),
                num_rows=None,
                precision=precision,
            )
        )
        txt_tmp.append(
            "# pnl=\n%s"
            % hpandas.df_to_str(
                self.get_historical_pnl(num_periods),
                num_rows=None,
                precision=precision,
            )
        )
        txt_tmp.append(
            "# statistics=\n%s"
            % hpandas.df_to_str(
                self.get_historical_statistics(num_periods),
                num_rows=None,
                precision=precision,
            )
        )
        txt_tmp = "\n".join(txt_tmp)
        txt.append(hprint.indent(txt_tmp))
        # Assemble in a single string.
        txt = "\n".join(txt)
        return txt

    # TODO(gp): We could share some code from PrintableMixin.
    def __repr__(self) -> str:
        """
        Print a detailed state of the Portfolio.
        """
        txt = []
        # Same content as `str()`.
        txt.append(str(self))
        # Add details about each attribute.
        txt_tmp = []
        txt_tmp.append("broker=%s" % hprint.to_object_repr(self.broker))
        txt_tmp.append("market_data=%s" % hprint.to_object_repr(self.market_data))
        txt_tmp.append("_account=%s" % self._account)
        txt_tmp.append("_timestamp_col=%s" % self._timestamp_col)
        # txt_tmp.append("_get_wall_clock_time=%s" % self._get_wall_clock_time)
        txt_tmp.append("_asset_id_col=%s" % self._asset_id_col)
        txt_tmp.append("_mark_to_market_col=%s" % self._mark_to_market_col)
        txt_tmp.append("_pricing_type=%s" % self._pricing_type)
        txt_tmp.append("_bar_duration=%s" % self._bar_duration)
        txt_tmp.append("_max_num_bars=%s" % self._max_num_bars)
        txt_tmp = "\n".join(txt_tmp)
        txt.append(hprint.indent(txt_tmp))
        # Assemble in a single string.
        txt = "\n".join(txt)
        return txt

    # /////////////////////////////////////////////////////////////////////////////
    # Builders
    # /////////////////////////////////////////////////////////////////////////////

    @staticmethod
    def read_state(
        log_dir: str,
        *,
        tz: str = "America/New_York",
        cast_asset_ids_to_int: bool = True,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Read and process logged Portfolio state.

        :param log_dir: store the state of a Portfolio in terms of its components,
            one per dir
        """
        holdings_shares_df = Portfolio._load_df_from_files(
            log_dir, "holdings_shares", tz
        )
        holdings_notional_df = Portfolio._load_df_from_files(
            log_dir, "holdings_notional", tz
        )
        executed_trades_shares_df = Portfolio._load_df_from_files(
            log_dir, "executed_trades_shares", tz
        )
        executed_trades_notional_df = Portfolio._load_df_from_files(
            log_dir, "executed_trades_notional", tz
        )
        # Cast asset ids to int for all the dfs, if needed.
        if cast_asset_ids_to_int:
            holdings_shares_df.columns = holdings_shares_df.columns.astype(
                "int64"
            )
            holdings_notional_df.columns = holdings_notional_df.columns.astype(
                "int64"
            )
            executed_trades_shares_df.columns = (
                executed_trades_shares_df.columns.astype("int64")
            )
            executed_trades_notional_df.columns = (
                executed_trades_notional_df.columns.astype("int64")
            )
        pnl_df = Portfolio._compute_pnl(
            holdings_notional_df, executed_trades_notional_df
        )
        dfs = {
            "holdings_shares": holdings_shares_df,
            "holdings_notional": holdings_notional_df,
            "executed_trades_shares": executed_trades_shares_df,
            "executed_trades_notional": executed_trades_notional_df,
            "pnl": pnl_df,
        }
        portfolio_df = pd.concat(dfs.values(), axis=1, keys=dfs.keys())
        #
        stats_df = Portfolio._load_df_from_files(log_dir, "statistics", tz)
        return portfolio_df, stats_df

    @classmethod
    def from_cash(
        cls,
        *args: Any,
        initial_cash: float,
        asset_ids: Optional[List[int]] = None,
        **kwargs: Any,
    ) -> "Portfolio":
        """
        Initialize a Portfolio with only cash (i.e., without non-cash assets).

        :param initial_cash: the initial desired cash, typically a non-negative
            amount
        :param asset_ids: the non-cash assets that the Portfolio should track
        :param *args, **kwargs: params passed to the Portfolio constructor of the
            derived class
        """
        if initial_cash < 0:
            _LOG.warning("Initial cash=%0.2f", initial_cash)
        # Build a dict with all asset_ids set to 0 and the given cash.
        if not asset_ids:
            asset_ids = []
        hdbg.dassert_not_in(Portfolio.CASH_ID, asset_ids)
        holdings_shares_dict = {asset_id: 0 for asset_id in asset_ids}
        holdings_shares_dict[Portfolio.CASH_ID] = initial_cash
        # Initialize the Portfolio from the holdings_shares.
        portfolio = cls.from_dict(
            *args,
            holdings_shares_dict=holdings_shares_dict,
            **kwargs,
        )
        return portfolio

    @classmethod
    def from_dict(
        cls,
        *args: Any,
        holdings_shares_dict: Dict[int, float],
        **kwargs: Any,
    ) -> "Portfolio":
        """
        Initialize a Portfolio from a dict of holdings_shares.

        :param holdings_shares_dict: dictionary from `asset_id` to position
        :param *args, **kwargs: params passed to the Portfolio constructor of the
            derived class
        """
        hdbg.dassert_isinstance(holdings_shares_dict, dict)
        initial_holdings_shares = pd.Series(holdings_shares_dict)
        # Initialize the Portfolio.
        portfolio = cls(
            *args,
            initial_holdings_shares,
            **kwargs,
        )
        return portfolio

    # /////////////////////////////////////////////////////////////////////////////
    # Accessors
    # /////////////////////////////////////////////////////////////////////////////

    @property
    def universe(self) -> List[int]:
        """
        Return the list of the asset_ids composing the universe tracked by the
        Portfolio.
        """
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

    def has_no_holdings(self) -> bool:
        """
        Return whether the Portfolio contains only cash and no holdings_shares.
        """
        # Get the dictionary of the last holdings_shares, excluding cash.
        num_periods = 1
        holdings_shares_odict = self._holdings_shares.get_ordered_dict(
            num_periods
        )
        _LOG.debug(hprint.to_str("holdings_shares_odict"))
        # The
        hdbg.dassert_isinstance(holdings_shares_odict, dict)
        hdbg.dassert_eq(len(holdings_shares_odict), 1)
        timestamp, holdings_srs = holdings_shares_odict.popitem()
        _LOG.debug(hprint.to_str("timestamp"))
        hdbg.dassert_isinstance(timestamp, pd.Timestamp)
        hdbg.dassert_isinstance(holdings_srs, pd.Series)
        # Test whether all holdings_shares in shares are exactly zero.
        has_no_holdings_ = (holdings_srs == 0).all()
        return has_no_holdings_

    def get_last_timestamp(self) -> pd.Timestamp:
        """
        Return the last timestamp of Portfolio internal state.
        """
        num_periods = 1
        holdings_shares_odict = self._holdings_shares.get_ordered_dict(
            num_periods
        )
        timestamp, _ = holdings_shares_odict.popitem()
        return timestamp

    # /////////////////////////////////////////////////////////////////////////////
    # Mark to market
    # /////////////////////////////////////////////////////////////////////////////

    def mark_to_market(self) -> pd.DataFrame:
        """
        Mark the portfolio of holdings_shares to market.

        This function checks the portfolio state at `wall_clock_time` and
        updates the internal state.
          - Holdings are as of `wall_clock_time` (e.g., any updates from fills)
          - Uses `market_data` to price holdings_shares
          - Computes portfolios statistics such as leverage, exposure, etc.

        # TODO(Paul): Add a dataframe snippet.

        :return: dataframe with HOLDINGS and PRICE columns
        """
        _LOG.debug("Marking to market...")
        # The first time this function is called we set the "initial holdings_shares".
        # TODO(Paul): See if `_set_holdings_shares()` can call into `mark_to_market()`
        #     or vice versa.
        if not self._holdings_shares:
            self._set_holdings_shares(self._initial_holdings)
        else:
            # Update `asset_holdings` and cash.
            self._observe_holdings_shares()
            # Get the latest timestamp.
            timestamp, holdings_shares = self._holdings_shares.peek()
            # Price the assets.
            self._price_assets(holdings_shares)
            # Calculate statistics.
            self._compute_statistics()
            # Perform sanity-checks.
            # These four dictionaries should have the same keys. Here we check that
            # they have the same length.
            # TODO(Paul): Maybe check keys instead of length.
            hdbg.dassert_eq(
                len(self._holdings_shares), len(self._holdings_notional)
            )
            hdbg.dassert_eq(len(self._holdings_shares), len(self._cash))
            hdbg.dassert_eq(len(self._holdings_shares), len(self._statistics))
        #
        df = self.get_cached_mark_to_market()
        _LOG.debug("mark_to_market_df=\n%s", hpandas.df_to_str(df, num_rows=None))
        return df

    def get_cached_mark_to_market(self) -> pd.DataFrame:
        """
        Retrieve the last cached mark-to-market dataframe.

        This function does not perform a new observation of the market.

        return: same as `mark_to_market()`
        """
        # TODO(Paul): Gracefully fail instead of assert.
        hdbg.dassert(self._holdings_shares, "No cached information available.")
        # Get latest timestamp available.
        timestamp, holdings_shares = self._holdings_shares.peek()
        _LOG.debug("Retrieving holdings_shares at timestamp=%s", timestamp)
        # Create a `holdings_df` with assets and cash.
        holdings_shares_df = holdings_shares.reset_index()
        cash_df = Portfolio._create_holdings_df_from_cash(
            self._cash[timestamp], timestamp
        )
        holdings_shares_df.columns = Portfolio.HOLDINGS_COLS
        holdings_shares_df.index = [timestamp] * holdings_shares_df.shape[0]
        holdings_shares_df = pd.concat([holdings_shares_df, cash_df])
        holdings_shares_df = holdings_shares_df.convert_dtypes()
        # Get mark-to-market values.
        marked_to_market = self._holdings_notional[timestamp]
        cash_row = pd.DataFrame(
            index=[Portfolio.CASH_ID],
            columns=Portfolio.PRICE_COLS,
            data=[[1.0, self._cash[timestamp]]],
        )
        marked_to_market = pd.concat([marked_to_market, cash_row])
        marked_to_market.index.name = "asset_id"
        marked_to_market = marked_to_market.reset_index()
        marked_to_market.index = [timestamp] * marked_to_market.index.shape[0]
        # Merge holdings_shares and mark-to-market dataframes.
        df = holdings_shares_df.merge(marked_to_market, on="asset_id")
        df["wall_clock_timestamp"] = [timestamp] * df.shape[0]
        df.index = [timestamp] * df.shape[0]
        df = df.convert_dtypes()
        return df

    # /////////////////////////////////////////////////////////////////////////////
    # Historical accessors
    # /////////////////////////////////////////////////////////////////////////////

    def get_historical_statistics(
        self, num_periods: Optional[int] = 10
    ) -> pd.DataFrame:
        """
        Return a dataframe of portfolio statistics over time.
        """
        stats_odict = self._statistics.get_ordered_dict(num_periods)
        df = pd.DataFrame(stats_odict).transpose()
        # Add `pnl` by diffing the snapshots of `net_wealth`.
        # ```
        # pnl = df["net_wealth"].diff().rename("pnl").to_frame()
        # ```
        # In principle, the PnL calculations should agree. However, if
        # a price for a bar is missing, this second method is more stable.
        pnl = (
            self.get_historical_pnl(num_periods=num_periods)
            .sum(axis=1, min_count=1)
            .rename("pnl")
            .to_frame()
        )
        df = pnl.merge(df, how="outer", left_index=True, right_index=True)
        df = df.astype("float")
        return df

    def get_historical_holdings_shares(
        self, num_periods: Optional[int] = 10
    ) -> pd.DataFrame:
        """
        Return a dataframe of portfolio holdings_shares in shares over time.
        """
        asset_holdings_shares_odict = self._holdings_shares.get_ordered_dict(
            num_periods
        )
        asset_holdings_shares = pd.DataFrame(
            asset_holdings_shares_odict
        ).transpose()
        # # TODO(gp): @all there is a little repetition that we would like to remove.
        # # Explicitly cast to float. This makes the string representation of
        # # the dataframe more uniform and better.
        # # We replace NaNs with numpy NaNs, since numpy NaNs are recognized as
        # # having "float" type.
        # asset_holdings_shares.fillna(np.nan, inplace=True)
        asset_holdings_shares = asset_holdings_shares.astype("float")
        return asset_holdings_shares

    def get_historical_holdings_notional(
        self, num_periods: Optional[int] = 10
    ) -> pd.DataFrame:
        """
        Return a dataframe of portfolio holdings_shares in dollars over time.
        """
        holdings_notional = collections.OrderedDict()
        for k, v in self._holdings_notional.get_ordered_dict(num_periods).items():
            holdings_notional[k] = v["value"]
        holdings_notional = pd.DataFrame(holdings_notional).transpose()
        holdings_notional.columns.name = self._asset_id_col
        # Explicitly cast to float. This makes the string representation of
        # the dataframe more uniform and better.
        # We replace NaNs with numpy NaNs, since numpy NaNs are recognized as
        # having "float" type.
        holdings_notional.fillna(np.nan, inplace=True)
        holdings_notional = holdings_notional.astype("float")
        return holdings_notional

    def get_historical_executed_trades_shares(
        self, num_periods: Optional[int] = 10
    ) -> pd.DataFrame:
        """
        Return a dataframe of executed trades in shares over time.
        """
        # Because the pnl calculation takes a diff, we extract an extra period
        # for this operation.
        if num_periods is not None:
            num_periods += 1
        # Get snapshots of assets marked to market.
        holdings_shares = self.get_historical_holdings_shares(num_periods)
        executed_trades_shares = holdings_shares.subtract(
            holdings_shares.shift(1), fill_value=0
        )
        executed_trades_shares.columns.name = self._asset_id_col
        executed_trades_shares = executed_trades_shares.astype("float")
        if num_periods is not None:
            executed_trades_shares = executed_trades_shares.tail(num_periods - 1)
        return executed_trades_shares

    def get_historical_executed_trades_notional(
        self, num_periods: Optional[int] = 10
    ) -> pd.DataFrame:
        """
        Return a dataframe of notional executed trades over time.
        """
        executed_trades_notional_odict = (
            self._executed_trades_notional.get_ordered_dict(num_periods)
        )
        executed_trades_notional = pd.DataFrame(
            executed_trades_notional_odict
        ).transpose()
        executed_trades_notional.columns.name = self._asset_id_col
        executed_trades_notional.fillna(np.nan, inplace=True)
        executed_trades_notional = executed_trades_notional.astype("float")
        return executed_trades_notional

    def get_historical_pnl(self, num_periods: Optional[int] = 10) -> pd.DataFrame:
        """
        Return a dataframe of per-asset PnL over time.
        """
        # Because the pnl calculation takes a diff, we extract an extra period
        # for this operation.
        if num_periods is not None:
            num_periods += 1
        # Get snapshots of assets marked to market.
        holdings_notional = self.get_historical_holdings_notional(num_periods)
        executed_trades_notional = self.get_historical_executed_trades_notional(
            num_periods
        )
        # Compute PnL.
        pnl = self._compute_pnl(holdings_notional, executed_trades_notional)
        #
        pnl.columns.name = self._asset_id_col
        pnl = pnl.astype("float")
        if num_periods is not None:
            pnl = pnl.tail(num_periods - 1)
        return pnl

    # /////////////////////////////////////////////////////////////////////////////

    def log_state(self, log_dir: str, *, num_periods: Optional[int] = 1) -> str:
        hdbg.dassert(log_dir, "Must specify `log_dir` to log state.")
        #
        bar_timestamp = hwacltim.get_current_bar_timestamp(as_str=True)
        #
        wall_clock_time = self._get_wall_clock_time()
        wall_clock_time_str = wall_clock_time.strftime("%Y%m%d_%H%M%S")
        file_name = f"{bar_timestamp}.{wall_clock_time_str}.csv"
        #
        holdings_shares_df = self.get_historical_holdings_shares(num_periods)
        Portfolio._write_df(
            holdings_shares_df, log_dir, "holdings_shares", file_name
        )
        #
        holdings_notional_df = self.get_historical_holdings_notional(num_periods)
        Portfolio._write_df(
            holdings_notional_df, log_dir, "holdings_notional", file_name
        )
        #
        executed_trades_shares = self.get_historical_executed_trades_shares(
            num_periods
        )
        Portfolio._write_df(
            executed_trades_shares, log_dir, "executed_trades_shares", file_name
        )
        executed_trades_notional_df = (
            self.get_historical_executed_trades_notional(num_periods)
        )
        Portfolio._write_df(
            executed_trades_notional_df,
            log_dir,
            "executed_trades_notional",
            file_name,
        )
        #
        stats_df = self.get_historical_statistics(num_periods)
        Portfolio._write_df(stats_df, log_dir, "statistics", file_name)
        return file_name

    def price_assets(self, asset_ids: List[int]) -> pd.Series:
        """
        Wrap `portfolio.market_data()` and packages output.

        :param asset_ids: as in `market_data.get_data_at_timestamp()`
        :return: series of prices at `as_of_timestamp` indexed by asset_id
        """
        if self._pricing_type == "last":
            prices_df = self.market_data.get_last_price(
                self._mark_to_market_col, asset_ids
            )
        elif self._pricing_type == "twap":
            prices_df = self.market_data.get_last_twap_price(
                self._bar_duration,
                self._timestamp_col,
                asset_ids,
                self._mark_to_market_col,
            )
        else:
            raise ValueError(f"Invalid pricing_type='{self._pricing_type}'")
        hdbg.dassert_isinstance(prices_df, pd.DataFrame)
        _LOG.debug("prices_df=%s", hpandas.df_to_str(prices_df))
        # Convert to series.
        prices_srs = self.market_data.to_price_series(
            prices_df, self._mark_to_market_col
        )
        prices_srs.index.name = "asset_id"
        prices_srs.name = "price"
        hdbg.dassert(not prices_srs.index.has_duplicates)
        return prices_srs

    # //////////////////////////////////////////////////////////////////////////////
    # Read / write state.
    # //////////////////////////////////////////////////////////////////////////////

    @staticmethod
    def _load_df_from_files(
        log_dir: str,
        name: str,
        tz: str,
    ) -> pd.DataFrame:
        # Find the files under `log_dir/{name}`.
        dir_name = os.path.join(log_dir, name)
        pattern = "*"
        only_files = True
        use_relative_paths = True
        files = hio.listdir(dir_name, pattern, only_files, use_relative_paths)
        files.sort()
        # Read each file as dataframe.
        dfs = []
        for file_name in tqdm(files, desc=f"Loading `{name}` files..."):
            df = Portfolio._read_df(log_dir, name, file_name, tz)
            dfs.append(df)
        # Concatenate.
        df = pd.concat(dfs)
        hdbg.dassert(
            not df.index.has_duplicates,
            "Duplicated indices for `%s`=\n%s",
            name,
            df.index[df.index.duplicated()],
        )
        return df

    @staticmethod
    def _read_df(
        log_dir: str,
        name: str,
        file_name: str,
        tz: str,
    ) -> pd.DataFrame:
        path = os.path.join(log_dir, name, file_name)
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        # TODO(Paul): Add better checks. The first trades dataframes do not
        #  have rows, and so when parsed do not have a DatetimeIndex.
        if isinstance(df.index, pd.DatetimeIndex):
            df.index = df.index.tz_convert(tz)
        return df

    @staticmethod
    def _write_df(
        df: pd.DataFrame,
        log_dir: str,
        name: str,
        file_name: str,
    ) -> None:
        path = os.path.join(log_dir, name, file_name)
        hio.create_enclosing_dir(path, incremental=True)
        df.to_csv(path)

    # //////////////////////////////////////////////////////////////////////////////

    @staticmethod
    def _parse_pricing_method(pricing_method: str) -> Tuple[str, Optional[str]]:
        """
        Parse a pricing method string (e.g., `last`, `twap.5T`) in terms of:

        - Pricing type (e.g., `last`, `twap`, `vwap`)
        - Bar duration as a Pandas duration string (e.g., `5T`)
        """

        hdbg.dassert_isinstance(pricing_method, str)
        if pricing_method == "last":
            pricing_type = "last"
            bar_duration = None
        else:
            split_str = pricing_method.split(".")
            hdbg.dassert_eq(len(split_str), 2)
            #
            pricing_type = split_str[0]
            hdbg.dassert_in(pricing_type, ["twap", "vwap"])
            #
            bar_duration = split_str[1]
            hdbg.dassert(
                pd.Timedelta(bar_duration),
                "Cannot convert %s to `pd.Timedelta`",
                bar_duration,
            )
        return pricing_type, bar_duration

    @staticmethod
    def _compute_pnl(
        holdings_notional: pd.DataFrame,
        executed_trades_notional: pd.DataFrame,
    ) -> pd.DataFrame:
        hdbg.dassert_not_in(Portfolio.CASH_ID, holdings_notional.columns)
        hdbg.dassert_not_in(Portfolio.CASH_ID, executed_trades_notional.columns)
        #
        cols = holdings_notional.columns.union(executed_trades_notional.columns)
        holdings_notional = holdings_notional.reindex(columns=cols, fill_value=0)
        executed_trades_notional = executed_trades_notional.reindex(
            columns=cols, fill_value=0
        )
        # Get per-bar flows and compute PnL.
        pnl = holdings_notional.diff().subtract(executed_trades_notional)
        return pnl

    @staticmethod
    def _create_holdings_df_from_cash(
        cash: float, timestamp: pd.Timestamp
    ) -> pd.DataFrame:
        # The holdings_df has a single row about cash.
        row = [Portfolio.CASH_ID, cash]
        holdings_df = pd.DataFrame(
            [row],
            index=[timestamp],
            columns=Portfolio.HOLDINGS_COLS,
        )
        return holdings_df

    @staticmethod
    def _validate_mark_to_market_df(df: pd.DataFrame) -> None:
        """
        Ensure that `df` passes basic sanity checks for `mark_to_market()`.
        """
        # The input should be a dataframe satisfying the following column
        # constraints.
        hdbg.dassert_isinstance(df, pd.DataFrame)
        hdbg.dassert_is_subset(Portfolio.HOLDINGS_COLS, df.columns)
        hdbg.dassert_not_in("price", df.columns)
        hdbg.dassert_not_in("value", df.columns)
        # The columns should be of the correct types. Skip if the dataframe is
        # empty (since the correct types are not inferred in that case).
        if not df.empty:
            hpandas.dassert_series_type_is(
                df["asset_id"],
                np.int64,
                "The column `asset_id` should only contain integer ids.",
            )
            hpandas.dassert_series_type_in(
                df["curr_num_shares"],
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
    def _validate_initial_holdings(initial_holdings: pd.Series) -> None:
        """
        Ensure that `initial_holdings_shares` passes basic sanity checks.
        """
        idx = initial_holdings.index
        # The index must be an Int64Index (asset_id must be an int) with no
        # duplicates. The ID for cash must be present.
        hdbg.dassert(not idx.has_duplicates)
        hdbg.dassert_isinstance(idx, pd.Int64Index)
        hdbg.dassert_in(
            Portfolio.CASH_ID,
            idx,
            "No cash holdings_shares available.",
        )
        # The holdings_shares must be numerical. They represent number of shares.
        hdbg.dassert_in(
            initial_holdings.dtype.type,
            [np.float64, np.int64],
            "The initial holdings_shares should consist of numerical values (in shares).",
        )
        # All share values should be finite.
        hdbg.dassert(
            np.isfinite(initial_holdings).all(),
            "All share values must be finite.",
        )
        initial_cash = initial_holdings.iloc[Portfolio.CASH_ID]
        if initial_cash < 0:
            _LOG.warning("Initial cash balance=%0.2f", initial_cash)

    def _set_holdings_shares(self, holdings_shares: pd.Series) -> None:
        """
        Set portfolio holdings_shares in shares and price the portfolio.

        This covers two main use cases:
        - Lazy initialization of the portfolio holdings_shares
        - Resetting portfolio holdings_shares overnight to account for splits/dividends
        """
        # Get the timestamp from the wall clock.
        timestamp = self._get_wall_clock_time()
        _LOG.debug("timestamp=%s", timestamp)
        _LOG.debug("Setting asset_holdings...")
        asset_holdings = holdings_shares[
            holdings_shares.index != Portfolio.CASH_ID
        ]
        asset_holdings.name = timestamp
        hdbg.dassert_isinstance(asset_holdings, pd.Series)
        _LOG.debug("`asset_holdings`=\n%s", hpandas.df_to_str(asset_holdings))
        hdbg.dassert(not asset_holdings.index.has_duplicates)
        self._holdings_shares[timestamp] = asset_holdings
        _LOG.debug("asset_holdings set.")
        _LOG.debug("Setting cash...")
        cash = holdings_shares.loc[Portfolio.CASH_ID]
        _LOG.debug("`cash`=%0.2f", cash)
        self._cash[timestamp] = cash
        _LOG.debug("cash set.")
        _LOG.debug("Setting assets_marked_to_market...")
        # Price the assets at the initial timestamp.
        self._price_assets(asset_holdings)
        _LOG.debug("assets_marked_to_market set.")
        _LOG.debug("Calculating statistics...")
        # Compute the initial portfolio statistics.
        self._compute_statistics()
        _LOG.debug("statistics calculated.")

    @abc.abstractmethod
    def _observe_holdings_shares(self) -> None:
        """
        Update holdings_shares and cash at the current wall clock time.
        """
        ...

    @abc.abstractmethod
    def _initialize_holdings_from_db(
        self, initial_holdings: pd.Series
    ) -> pd.Series:
        """
        Initialize holdings_shares after querying an external db.

        Use for:
        - BOD initialization
        - Intraday recovery
        """
        ...

    def _price_assets(self, holding_shares: pd.Series) -> None:
        """
        Access the underlying market_data to price assets.

        :param holding_shares: series of share counts indexed by asset id
        :return: series of asset values
        """
        # This is the timestamp of the last snapshot.
        as_of_timestamp, _ = self._holdings_shares.peek()
        _LOG.debug("as_of_timestamp=%s", as_of_timestamp)
        hdbg.dassert_isinstance(holding_shares, pd.Series)
        holding_shares_list = holding_shares.index.to_list()
        if not holding_shares_list:
            assets_marked_to_market = pd.DataFrame(columns=Portfolio.PRICE_COLS)
        else:
            # TODO(gp): A bit weird that we are calling the public method from the
            #  private.
            prices = self.price_assets(holding_shares_list)
            assets_marked_to_market = holding_shares * prices
            assets_marked_to_market.name = "value"
            assets_marked_to_market = pd.concat(
                [prices, assets_marked_to_market], axis=1
            )
            assets_marked_to_market.columns = Portfolio.PRICE_COLS
        hdbg.dassert_isinstance(assets_marked_to_market, pd.DataFrame)
        hdbg.dassert(not assets_marked_to_market.index.has_duplicates)
        if Portfolio.CASH_ID in assets_marked_to_market.columns:
            assets_marked_to_market = assets_marked_to_market.drop(
                columns=Portfolio.CASH_ID
            )
        self._holdings_notional[as_of_timestamp] = assets_marked_to_market

    def _compute_statistics(self) -> None:
        """
        Compute various portfolio statistics using latest holdings_shares and
        prices.

        Return asset/cash values, net wealth, exposure, and leverage for
        the portfolio at a given timestamp.
        """
        cash_timestamp, _ = self._cash.peek()
        assets_ts, holdings_notional = self._holdings_notional.peek()
        hdbg.dassert_eq(cash_timestamp, assets_ts)
        hdbg.dassert_not_in(cash_timestamp, self._statistics)
        # Compute value of holdings_shares.
        holdings_notional = holdings_notional["value"]
        is_finite = holdings_notional.apply(np.isfinite)
        net_holdings_notional = holdings_notional[is_finite].sum()
        hdbg.dassert(
            np.isfinite(net_holdings_notional),
            "net_holdings_notional=%s",
            net_holdings_notional,
        )
        # Get the cash available.
        cash = self._cash[cash_timestamp]
        hdbg.dassert(np.isfinite(cash), "cash=%s", cash)
        # Compute the net wealth (AKA "total value" AKA "NAV").
        net_wealth = net_holdings_notional + cash
        hdbg.dassert(np.isfinite(net_wealth), "net_value=%s", net_wealth)
        # Compute the gross exposure.
        gross_exposure = holdings_notional[is_finite].abs().sum()
        # Compute the portfolio leverage.
        leverage = gross_exposure / net_wealth
        # Compute the gross and net volume.
        if assets_ts in self._executed_trades_notional:
            traded_volume = self._executed_trades_notional[assets_ts]
            gross_volume = traded_volume.abs().sum()
            net_volume = traded_volume.sum()
        else:
            gross_volume = 0
            net_volume = 0
        dict_ = {
            "gross_volume": gross_volume,
            "net_volume": net_volume,
            "gmv": gross_exposure,
            "nmv": net_holdings_notional,
            "cash": cash,
            "net_wealth": net_wealth,
            "leverage": leverage,
        }
        statistics = pd.Series(dict_, name=cash_timestamp)
        self._statistics[cash_timestamp] = statistics


# #############################################################################
# DataFramePortfolio
# #############################################################################


class DataFramePortfolio(Portfolio):
    """
    An implementation of `Portfolio` using a DataFrame to store the
    information.
    """

    # A `fills_df` represents orders that have been executed (e.g., how many shares,
    # at how much).

    # Columns required in a `fills_df`.
    FILLS_COLS = [
        "asset_id",
        "timestamp",
        "num_shares",
        "price",
    ]

    @staticmethod
    def _validate_fills_df(fills_df: pd.DataFrame) -> None:
        """
        Ensure that `fills_df` passes basic sanity checks.
        """
        # The input should be a nonempty dataframe.
        hdbg.dassert_isinstance(fills_df, pd.DataFrame)
        hdbg.dassert(not fills_df.empty, "The dataframe must be nonempty.")
        # The dataframe must have the correct columns.
        hdbg.dassert_is_subset(
            DataFramePortfolio.FILLS_COLS,
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
            DataFramePortfolio.CASH_ID,
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

    def _observe_holdings_shares(self) -> None:
        """
        Update holdings_shares at the current wall clock time using fills
        information.
        """
        # Get fills.
        fills_df = self._get_fills()
        # _LOG.debug("fills_df=%s", fills_df)
        # Get latest holdings_shares
        (
            prev_asset_holdings_ts,
            prev_holdings_shares,
        ) = self._holdings_shares.peek()
        prev_cash_holdings_ts, prev_cash = self._cash.peek()
        wall_clock_timestamp = self._get_wall_clock_time()
        hdbg.dassert_lt(prev_cash_holdings_ts, wall_clock_timestamp)
        new_cash = prev_cash
        # Update holdings_shares using the `fills_df`.
        new_holdings_shares = prev_holdings_shares.copy()
        executed_trades_notional = pd.Series([], dtype="float64")
        if fills_df is not None:
            DataFramePortfolio._validate_fills_df(fills_df)
            # last_timestamp <= fills_df.index <= timestamp
            hdbg.dassert_lte(prev_asset_holdings_ts, fills_df["timestamp"].min())
            hdbg.dassert_lte(fills_df["timestamp"].max(), wall_clock_timestamp)
            executed_trades_shares = fills_df.set_index("asset_id")["num_shares"]
            # _LOG.debug("executed_trades_shares=%s", executed_trades_shares)
            trades_price_per_share = fills_df.set_index("asset_id")["price"]
            # _LOG.debug("trades_price_per_shares=%s", trades_price_per_share)
            executed_trades_notional = (
                executed_trades_shares * trades_price_per_share
            )
            # _LOG.debug("executed_trades_notional=%s", executed_trades_notional)
            cash_diff = -executed_trades_notional.sum()
            hdbg.dassert(np.isfinite(cash_diff))
            new_holdings_shares = new_holdings_shares.add(
                executed_trades_shares, fill_value=0
            )
            new_cash += cash_diff
        hdbg.dassert(not new_holdings_shares.index.has_duplicates)
        self._holdings_shares[wall_clock_timestamp] = new_holdings_shares
        hdbg.dassert(not executed_trades_notional.index.has_duplicates)
        self._executed_trades_notional[
            wall_clock_timestamp
        ] = executed_trades_notional
        self._cash[wall_clock_timestamp] = new_cash

    def _initialize_holdings_from_db(self) -> pd.Series:
        # For now we don't assume that `DataFramePortfolio` is saved on permanent
        # storage, since it is typically used for simulations.
        # TODO(Paul, GP): Consider allowing saving and retrieval of
        #  `DataFramePortfolio`.
        _ = self
        raise NotImplementedError

    def _get_fills(self) -> pd.DataFrame:
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
            # Copy contents of the fill.
            fill_row: Dict[str, Any] = collections.OrderedDict()
            fill_row.update(fill.to_dict())
            #
            fill_rows.append(pd.Series(fill_row))
        #
        if fill_rows:
            fills_df = pd.concat(fill_rows, axis=1).transpose()
            fills_df = fills_df.convert_dtypes()
        else:
            fills_df = None
        _LOG.debug(
            "fills_df=\n%s",
            hpandas.df_to_str(fills_df, num_rows=None, precision=2),
        )
        return fills_df


# #############################################################################
# DatabasePortfolio
# #############################################################################


class DatabasePortfolio(Portfolio):
    """
    An implementation of `Portfolio` using a DB to store the state of the
    holdings_shares.

    A `snapshot_df` contains the image of the current holdings_shares (excluding cash)
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
        table_name: str,
        poll_kwargs: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ):
        """
        Constructor.

        :param table_name: current positions table name
        :param poll_kwargs: polling instruction when waiting for stable current
            positions
        """
        _LOG.debug(hprint.to_str("table_name poll_kwargs"))
        super().__init__(*args, **kwargs)
        #
        self._db_connection = self.broker._db_connection
        self._table_name = table_name
        if poll_kwargs is None:
            poll_kwargs = hasynci.get_poll_kwargs(self._get_wall_clock_time)
        self._poll_kwargs = poll_kwargs
        # wall clock timestamp -> snapshot_df (i.e., the image of the holdings_shares in
        # the account, without cash).
        self._timestamp_to_snapshot_df = collections.OrderedDict()
        # wall clock timestamp -> total net cost of transactions since the BOD.
        self._net_cost = cksoordi.KeySortedOrderedDict(pd.Timestamp)
        if self._retrieve_initial_holdings_shares_from_db:
            self._initial_holdings = self._initialize_holdings_from_db(
                self._initial_holdings
            )
            self._validate_initial_holdings(self._initial_holdings)
        #
        _LOG.debug("After initialization:\n%s", repr(self))

    def log_state(self, log_dir: str, *, num_periods: Optional[int] = 1) -> str:
        super().log_state(log_dir, num_periods=num_periods)
        hdbg.dassert(log_dir, "Must specify `log_dir` to log state.")
        #
        bar_timestamp = hwacltim.get_current_bar_timestamp(as_str=True)
        #
        wall_clock_time = self._get_wall_clock_time()
        wall_clock_time_str = wall_clock_time.strftime("%Y%m%d_%H%M%S")
        f"{bar_timestamp}.{wall_clock_time_str}.csv"
        #
        # snapshot_df = self.get_historical_snapshot_dfs(num_periods)
        # Portfolio._write_df(snapshot_df, log_dir, "snapshot_df", file_name)

    # //////////////////////////////////////////////////////////////////////////

    def _get_snapshot_df(self, restrict_to_universe: bool) -> pd.DataFrame:
        """
        Return a snapshot df like:
        ```
         tradedate asset_id        published_dt  target_position  current_position ...
        2021-12-09    10005 2021-12-09 11:54:28  0.0              0
        2021-12-09    10006 2021-12-09 11:54:28  0.0              0
        2021-12-09    10009 1970-01-01 00:00:00  0.0              0
        ```
        """
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
        # Build the SQL query to retrieve the current positions.
        query = []
        query.append(f"SELECT * FROM {self._table_name}")
        # Get the trade date.
        wall_clock_timestamp = self._get_wall_clock_time()
        trade_date = wall_clock_timestamp.date()
        _LOG.debug(hprint.to_str("wall_clock_timestamp trade_date"))
        where_clause = [f"WHERE tradedate='{trade_date}'"]
        # Restrict query to portfolio universe.
        if restrict_to_universe:
            hdbg.dassert(self.universe, "Universe is empty.")
            _LOG.debug("universe=\n%s", self.universe)
            universe = tuple(self.universe)
            if len(universe) == 1:
                universe = str(universe)[:-2] + ")"
            # TODO(Paul): Make sure that we do not exclude IDs with a nonzero current /
            #  target position (ensure that these are included in the universe).
            where_clause.append(f"AND {self._asset_id_col} IN {universe}")
        # Restrict by account if needed.
        if self._account is not None:
            where_clause.append(f"AND account='{self._account}'")
        query.append(" ".join(where_clause))
        #
        query.append(f"ORDER BY {self._asset_id_col}")
        query = "\n".join(query)
        # Retrieve the data from the DB.
        snapshot_df = hsql.execute_query_to_df(self._db_connection, query)
        snapshot_df.rename(columns={self._asset_id_col: "asset_id"}, inplace=True)
        _LOG.debug(
            "snapshot_df=\n%s",
            hpandas.df_to_str(snapshot_df, num_rows=None, precision=2),
        )
        if not snapshot_df.empty:
            hdbg.dassert_no_duplicates(
                snapshot_df["asset_id"],
                "Each asset_id should be unique in a snapshot_df",
            )
        return snapshot_df

    def _is_stable_snapshot_df(
        self, *args: Any, **kwargs: Any
    ) -> Tuple[bool, pd.DataFrame]:
        snapshot_df = self._get_snapshot_df(*args, **kwargs)
        is_stable = (snapshot_df["open_quantity"] == 0).all()
        return is_stable, snapshot_df

    def _get_stable_snapshot_df(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
        """
        Wait until the current positions are stable and then return them.

        The current positions are stable when no outstanding orders are
        open.
        """
        # TODO(gp): Enable this.
        if False:
            polling_func = lambda: self._is_stable_snapshot_df(*args, **kwargs)
            tag = "wait_for_stable_snapshot_df"
            # Conceptually this should be an async polling. To avoid that the async
            # behavior spreads to all the methods, we resort to a sync implementation.
            rc, snapshot_df = hasynci.sync_poll(
                polling_func, tag=tag, **self._poll_kwargs
            )
            _ = rc
        else:
            snapshot_df = self._get_snapshot_df(*args, **kwargs)
        return snapshot_df

    def _observe_holdings_shares(self) -> None:
        """
        Observe the holdings_shares stored in the external DB and update the
        internal data structure to account for them.
        """
        restrict_to_universe = True
        snapshot_df = self._get_stable_snapshot_df(restrict_to_universe)
        _LOG.debug("snapshot_df=%s", snapshot_df)
        # 1) Update cash from snapshot_df.
        wall_clock_timestamp = self._get_wall_clock_time()
        self._update_executed_trades_notional_and_cash(
            snapshot_df, wall_clock_timestamp
        )
        # 2) Update asset holdings_shares from snapshot_df.
        holdings_shares = snapshot_df[["asset_id", "current_position"]].set_index(
            "asset_id"
        )["current_position"]
        _LOG.debug(hprint.to_str("holdings_shares"))
        hdbg.dassert_isinstance(holdings_shares, pd.Series)
        holdings_shares = holdings_shares.reindex(
            index=self._initial_universe, copy=False
        )
        holdings_shares.name = wall_clock_timestamp
        # If the database does not have an entry for an asset (e.g., as in
        # a mock database without universe initialization), then a NaN is
        # returned.
        _LOG.debug(
            "Number of NaN holdings_shares=%d", holdings_shares.isna().sum()
        )
        holdings_shares.fillna(0, inplace=True)
        hdbg.dassert(not holdings_shares.index.has_duplicates)
        self._holdings_shares[wall_clock_timestamp] = holdings_shares
        # 3) Update snapshot_df.
        hdbg.dassert(not snapshot_df.index.has_duplicates)
        self._timestamp_to_snapshot_df[wall_clock_timestamp] = snapshot_df

    # TODO(gp): -> _get_initial_holdings_from_db since we are not assigning it.
    def _initialize_holdings_from_db(
        self, initial_holdings: pd.Series
    ) -> pd.Series:
        """
        Retrieve and validate the holdings_shares stored in an external DB.

        :param initial_holdings: the nonzero holdings_shares from the DB need to be a
            subset of the index in `initial_holdings_shares`
        """
        # All initial holdings_shares must be NaN when we invoke this method.
        hdbg.dassert_eq(initial_holdings.count(), 0)
        # TODO(gp): Explain why we don't restrict to universe here like in
        #  `_observe_holdings_shares()`. Maybe it's because we expect the client to pass
        #  the universe from outside.
        restrict_to_universe = False
        snapshot_df = self._get_stable_snapshot_df(restrict_to_universe)
        # Get current nonzero positions.
        holdings = snapshot_df[["asset_id", "current_position"]].set_index(
            "asset_id"
        )["current_position"]
        _LOG.debug(hprint.to_str("initial_holdings"))
        hdbg.dassert_isinstance(holdings, pd.Series)
        #
        nonzero_holdings = holdings[holdings != 0]
        hdbg.dassert_isinstance(nonzero_holdings, pd.Series)
        # Ensure that the nonzero positions are a subset of the universe
        # specified by the index of `initial_holdings_shares`.
        hdbg.dassert_is_subset(
            nonzero_holdings.index.to_list(), initial_holdings.index.to_list()
        )
        # Set zero holdings_shares and combine with nonzero initial holdings_shares.
        initial_holdings = initial_holdings.fillna(0)
        hdbg.dassert_isinstance(initial_holdings, pd.Series)
        initial_holdings = initial_holdings.add(nonzero_holdings, fill_value=0)
        return initial_holdings

    # TODO(gp): Make it static
    def _convert_to_holdings_shares_df(
        self, snapshot_df: pd.DataFrame, as_of_timestamp: pd.Timestamp
    ) -> pd.DataFrame:
        """
        Convert a snapshot_df from SQL query into a `holdings_df`.
        """
        holdings_df = snapshot_df[["asset_id", "current_position"]]
        holdings_df.columns = Portfolio.HOLDINGS_COLS
        holdings_df.index = [as_of_timestamp] * snapshot_df.shape[0]
        holdings_df = holdings_df.convert_dtypes()
        return holdings_df

    # TODO(gp): Make it static
    def _get_net_cost(self, snapshot_df: pd.DataFrame) -> pd.Series:
        """
        Return the `net_cost` of assets stored in a `snapshot_df`.

        This is a helper for
        `_update_executed_trades_notional_and_cash()`.
        """
        if snapshot_df.empty:
            return pd.Series([], dtype="float64")
        hdbg.dassert_in("net_cost", snapshot_df.columns)
        # A long position has negative net cost.
        net_cost = -1 * snapshot_df.set_index("asset_id")["net_cost"]
        _LOG.debug("net_cost (cumulative)=%f", net_cost.sum())
        return net_cost

    def _update_executed_trades_notional_and_cash(
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
        hdbg.dassert_not_in(Portfolio.CASH_ID, snapshot_df["asset_id"])
        # Get the cash at the previous timestamp.
        prev_cash_ts, prev_cash = self._cash.peek()
        hdbg.dassert_lt(prev_cash_ts, wall_clock_timestamp)
        _LOG.debug("prev_cash=%s", prev_cash)
        hdbg.dassert(np.isfinite(prev_cash), "prev_cash=%s", prev_cash)
        # Get the net cost at the previous timestamp.
        if self._net_cost:
            prev_net_cost_ts, prev_net_costs = self._net_cost.peek()
            hdbg.dassert_eq(prev_net_cost_ts, prev_cash_ts)
        else:
            _, holdings_shares = self._holdings_shares.peek()
            idx = holdings_shares.index
            prev_net_costs = pd.Series(0, idx)
        prev_net_cost = prev_net_costs.sum()
        # Get the current net cost.
        current_net_costs = self._get_net_cost(snapshot_df)
        current_net_cost = current_net_costs.sum()
        hdbg.dassert(
            np.isfinite(current_net_cost),
            "current_net_cost (cumulative)=%f",
            current_net_cost,
        )
        # The cost of the previous transactions is the difference of net cost.
        cost = current_net_cost - prev_net_cost
        _LOG.debug("cost (net_cost diff)=%f", cost)
        hdbg.dassert(np.isfinite(cost))
        # The current cash is given by the previous cash and the cash spent in the
        # previous transactions.
        updated_cash = prev_cash - cost
        _LOG.debug("updated_cash=%s", updated_cash)
        # Update the cash and net cost.
        self._cash[wall_clock_timestamp] = updated_cash
        self._net_cost[wall_clock_timestamp] = current_net_costs
        executed_trades_notional = current_net_costs.subtract(
            prev_net_costs, fill_value=0.0
        )
        hdbg.dassert(not executed_trades_notional.index.has_duplicates)
        self._executed_trades_notional[
            wall_clock_timestamp
        ] = executed_trades_notional