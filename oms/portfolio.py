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
import market_data.market_data_interface as mdmadain
import oms.broker as ombroker

_LOG = logging.getLogger(__name__)


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

    # Columns used in a dataframe resulting from mark to market.
    HOLDINGS_COLS = ["asset_id", "curr_num_shares"]

    # Columns that a dataframe with prices should have.
    PRICE_COLS = ["asset_id", "price"]

    def __init__(
        self,
        strategy_id: str,
        account: str,
        market_data_interface: mdmadain.AbstractMarketDataInterface,
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
        self._market_data_interface = market_data_interface
        self._get_wall_clock_time = get_wall_clock_time
        self._asset_id_col = asset_id_col
        self._mark_to_market_col = mark_to_market_col
        self._timestamp_col = timestamp_col
        #
        self._validate_initial_holdings_df(holdings_df)
        self._holdings = holdings_df
        # Dictionary from timestamp to some portfolio statistics.
        # TODO(gp): -> _statistics?
        self._characteristics = collections.OrderedDict()

    def __str__(self) -> str:
        """
        Return the state of the Portfolio in terms of the holdings as a string.
        """
        act = []
        act.append("# holdings=\n%s" % hprint.dataframe_to_str(self._holdings))
        act = "\n".join(act)
        return act

    @classmethod
    def from_cash(
        cls,
        *args: Any,
        initial_cash: float,
        initial_timestamp: pd.Timestamp,
        **kwargs: Any,
    ) -> "Portfolio":
        """
        Initialize a portfolio with no non-cash assets.
        """
        hdbg.dassert_lt(0, initial_cash)
        # The holdings_df has a single row about cash.
        row = [Portfolio.CASH_ID, initial_cash]
        holdings_df = pd.DataFrame(
            [row], index=[initial_timestamp], columns=Portfolio.HOLDINGS_COLS
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
    ) -> "Portfolio":
        """
        Initialize from a dict of holdings and initial timestamp.
        """
        # Convert the dictionary into a df with `asset_id`, `curr_num_shares`.
        hdbg.dassert_isinstance(holdings_dict, dict)
        holdings_df = pd.Series(holdings_dict).to_frame().reset_index()
        holdings_df.columns = Portfolio.HOLDINGS_COLS
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

    # TODO(Paul): This is only for debug maybe we should not expose it.
    @property
    def holdings(self) -> pd.DataFrame:
        return self._holdings

    def get_last_timestamp(self) -> pd.Timestamp:
        """
        Get the last time known to Portfolio in terms of holdings.
        """
        return self._holdings.index.max()

    def get_holdings(
        self,
        as_of_timestamp: pd.Timestamp,
        asset_id: Optional[Any],
        *,
        exclude_cash: bool = False,
    ) -> pd.DataFrame:
        """
        Return the holdings in shares at `timestamp` for one or all the assets.

        :param as_of_timestamp: as-of timestamp
        :param asset_id: consider only a specific asset. `None` means all assets.
        :param exclude_cash: exclude cash from the holdings
        :return: a `holdings_df` with columns `Portfolio.HOLDINGS_COLS`, e.g.,
            ```
                                       asset_id  curr_num_shares
            2000-01-01 09:35:00-05:00      -1.0        1000000.0
            ```
            - Return an empty dataframe if there are no holdings for the requested
              `timestamp`
        """
        return Portfolio._get_holdings(
            self._holdings, as_of_timestamp, asset_id, exclude_cash=exclude_cash
        )

    def get_asset_price_at_timestamp(
        self,
        as_of_timestamp: pd.Timestamp,
        asset_ids: List[int],
    ) -> pd.DataFrame:
        """
        Use `PriceInterface` to price assets at `timestamp`.

        `CASH_ID` may be included among `asset_ids`. If it is, it is not priced
        using `PriceInterface`, but rather is priced at `1.0`.

        :return: df with columns `Portfolio.PRICE_COLS`
        """
        # Check `asset_ids`.
        hdbg.dassert_isinstance(asset_ids, list)
        asset_ids = sorted(asset_ids)
        hdbg.dassert_no_duplicates(asset_ids)
        # Get `price_df` for all the non-cash assets from the price interface.
        non_cash_asset_ids = list(
            filter(lambda x: x != Portfolio.CASH_ID, asset_ids)
        )
        price_df = self._market_data_interface.get_data_at_timestamp(
            as_of_timestamp, self._timestamp_col, non_cash_asset_ids
        )
        hdbg.dassert_eq(
            price_df.shape[0],
            len(non_cash_asset_ids),
            "Some assets have no price. Attempted to access price for "
            "`non_cash_asset_ids=%s` and found prices for `asset_ids=%s` "
            "at timestamp=`%s`",
            non_cash_asset_ids,
            price_df["asset_id"].to_list(),
            as_of_timestamp,
        )
        _LOG.debug("price_df=\n%s", hprint.dataframe_to_str(price_df))
        # Extract subset of price information.
        columns = [self._asset_id_col, self._mark_to_market_col]
        hdbg.dassert_is_subset(columns, price_df.columns)
        price_df = price_df[columns]
        # Rename using the canonical names.
        price_df.columns = Portfolio.PRICE_COLS
        # Add the current timestamp.
        price_df.index = [as_of_timestamp] * price_df.index.size
        # Add cash, if missing.
        if len(non_cash_asset_ids) < len(asset_ids):
            cash_df = pd.DataFrame(
                {
                    Portfolio.PRICE_COLS[0]: Portfolio.CASH_ID,
                    Portfolio.PRICE_COLS[1]: 1,
                },
                index=[as_of_timestamp],
            )
            price_df = pd.concat([cash_df, price_df])
        return price_df

    def mark_to_market(
        self, as_of_timestamp: pd.Timestamp, df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Mark holdings to market prices available at `timestamp`.

        :param as_of_timestamp: as-of time for holdings and price
        :param df: a dataframe that
            - has columns HOLDINGS_COLS
            - does not have columns PRICE_COLS
            - does not have duplicate asset_ids
            - does not have NaN `curr_num_shares`
        :return: `df` merged with asset prices valued according to `price` and
            `curr_num_shares` at `timestamp`
        """
        _LOG.debug(
            "\n%s",
            hprint.frame(
                "mark_to_market: timestamp=%s" % as_of_timestamp, char1="<"
            ),
        )
        Portfolio._validate_mark_to_market_df(df)
        if df.empty:
            cols = Portfolio.HOLDINGS_COLS + ["value", "price"]
            result_df = pd.DataFrame(columns=cols)
            return result_df
        # Get asset ids.
        asset_ids = df["asset_id"].to_list()
        hdbg.dassert_lt(0, len(asset_ids))
        _LOG.debug("asset_ids=%s", asset_ids)
        price_df = self.get_asset_price_at_timestamp(as_of_timestamp, asset_ids)
        hdbg.dassert(not price_df.empty)
        # Merge the holdings with the prices.
        result_df = pd.merge(
            df,
            price_df,
            how="outer",
            left_on="asset_id",
            right_on=self._asset_id_col,
        )
        # Compute the value.
        result_df["value"] = result_df["curr_num_shares"] * result_df["price"]
        result_df.index = df.index
        # TODO(gp): We should have no nans in res_df["value"].
        return result_df

    def get_characteristics(self, as_of_timestamp: pd.Timestamp) -> pd.Series:
        """
        Return asset/cash values, net wealth, exposure, and leverage for the
        portfolio at a given timestamp.
        """
        _LOG.debug(
            "Computing portfolio characteristics for timestamp=`%s`",
            as_of_timestamp,
        )
        hdbg.dassert_in(
            as_of_timestamp,
            self._holdings.index,
            "No record of holdings at timestamp=`%s`",
            as_of_timestamp,
        )
        # Mark the current holdings to market.
        holdings = self.get_holdings(
            as_of_timestamp, asset_id=None, exclude_cash=True
        )
        holdings = self.mark_to_market(as_of_timestamp, holdings)
        # Compute value of holdings.
        holdings_net_value = holdings["value"].sum()
        hdbg.dassert(
            np.isfinite(holdings_net_value),
            "holdings_net_value=%s",
            holdings_net_value,
        )
        # Get the cash available.
        cash = self._get_holdings_as_scalar(
            as_of_timestamp, asset_id=self.CASH_ID
        )
        hdbg.dassert(np.isfinite(cash), "cash=%s", cash)
        # Cash should always be positive.
        hdbg.dassert_lte(0.0, cash, "cash should always be positive")
        # Compute the net wealth (AKA "total value" AKA "NAV").
        net_wealth = holdings_net_value + cash
        hdbg.dassert(np.isfinite(net_wealth), "net_value=%s", net_wealth)
        # Compute the gross exposure.
        gross_exposure = holdings["value"].abs().sum()
        # Compute the portfolio leverage.
        leverage = gross_exposure / net_wealth
        dict_ = {
            "net_asset_holdings": holdings_net_value,
            "cash": cash,
            "net_wealth": net_wealth,
            "gross_exposure": gross_exposure,
            "leverage": leverage,
        }
        characteristics = pd.Series(dict_, name=as_of_timestamp)
        return characteristics

    def update_state(
        self,
    ) -> None:
        """
        Update holdings at the current wall clock time.
        """
        wall_clock_timestamp = self._get_wall_clock_time()
        _LOG.debug(
            "\n%s",
            hprint.frame(
                "update_state: wall_clock_timestamp=%s" % wall_clock_timestamp,
                char1="<",
            ),
        )
        # TODO(gp): Move this to hdatetime.
        hdbg.dassert_isinstance(wall_clock_timestamp, pd.Timestamp)
        hdbg.dassert_is_not(wall_clock_timestamp.tz, None)
        hdbg.dassert_eq(
            wall_clock_timestamp.tz.zone, self._holdings.index.dtype.tz.zone
        )
        # Check that latest holdings are timestamped prior to `curr_timestamp`.
        last_timestamp = self.get_last_timestamp()
        hdbg.dassert_lte(last_timestamp, wall_clock_timestamp)
        # Log portfolio characteristics at `last_timestamp` if not done already.
        # This could happen if there is a bar with no orders (or overnight).
        if last_timestamp not in self._characteristics:
            val = self.get_characteristics(last_timestamp)
            self._characteristics[last_timestamp] = val
        #
        new_holdings = self._update_state(wall_clock_timestamp)
        # TODO(gp): Make sure that new_holdings are after self._holdings.
        # Add the information to the holdings.
        Portfolio._validate_holdings_df(new_holdings)
        updated_state = pd.concat([new_holdings, self._holdings])
        updated_state = updated_state.convert_dtypes()
        self._holdings = updated_state
        # Log portfolio characteristics at `curr_timestamp` if not done already.
        val = self.get_characteristics(wall_clock_timestamp)
        self._characteristics[wall_clock_timestamp] = val

    @abc.abstractmethod
    def _update_state(
        self,
        wall_clock_timestamp: pd.Timestamp,
        # fills_df: Optional[pd.DataFrame],
    ) -> pd.DataFrame:
        """
        :return: a holding_df with the new holdings
        """
        ...

    def _get_holdings_as_scalar(
        self,
        as_of_timestamp: pd.Timestamp,
        asset_id: int,
    ) -> float:
        """
        Return the number of shares held for a given asset.

        For cash, this is the same as the cash value.
        """
        holdings = self.get_holdings(as_of_timestamp, asset_id=asset_id)
        _LOG.debug("holdings=\n%s", holdings)
        if holdings.empty:
            value = 0
        else:
            value = holdings["curr_num_shares"].values[0]
        _LOG.debug("value=%s for asset_id=%s", value, asset_id)
        return value

    # TODO(gp): use self._holding_df and we can make it a method instead of
    # static, if possible.
    @staticmethod
    def _get_holdings(
        holdings_df: pd.DataFrame,
        as_of_timestamp: pd.Timestamp,
        asset_id: Optional[Any],
        *,
        exclude_cash: bool = False,
    ) -> pd.DataFrame:
        """
        Get holdings at time `timestamp` for the requested assets.

        :param asset_id: an asset or `None` for all available assets
        :return: holding_df
        """
        Portfolio._validate_holdings_df(holdings_df)
        _LOG.debug(hprint.to_str("as_of_timestamp asset_id exclude_cash"))
        # Extract `holdings` at the requested timestamp.
        # _LOG.debug("holdings=\n%s", hprint.dataframe_to_str(holdings_df))
        if as_of_timestamp not in holdings_df.index:
            _LOG.debug(
                "Timestamp=`%s` not found in holdings index", as_of_timestamp
            )
            empty_holdings = pd.DataFrame(
                [], index=[], columns=Portfolio.HOLDINGS_COLS
            )
            return empty_holdings
        holdings = holdings_df.loc[as_of_timestamp]
        if isinstance(holdings, pd.Series):
            holdings = holdings.to_frame().T
        # _LOG.debug("holdings=%s\n%s", str(holdings.shape), holdings)
        hdbg.dassert(
            not holdings.empty,
            "Timestamp=`%s` found in index but filtered holdings are empty!",
            as_of_timestamp,
        )
        hdbg.dassert_no_duplicates(holdings["asset_id"])
        # Filter by asset_id, if needed.
        if asset_id is not None:
            _LOG.debug("Filtering by asset_id: holdings=\n%s", holdings)
            mask = holdings["asset_id"] == asset_id
            # _LOG.debug("mask=\n%s", mask)
            holdings = holdings[mask]
            if holdings.empty:
                _LOG.debug(
                    "No holdings available at timestamp=`%s` for asset_id=`%s`",
                    as_of_timestamp,
                    asset_id,
                )
        # Exclude cash, if needed.
        if exclude_cash:
            # _LOG.debug("exclude_cash: holdings=\n%s", holdings)
            hdbg.dassert_ne(
                asset_id,
                Portfolio.CASH_ID,
                "You cannot request cash (asset_id=`%s`) and simultaneously "
                "request to exclude it",
                asset_id,
            )
            mask = holdings["asset_id"] != Portfolio.CASH_ID
            # _LOG.debug("mask=\n%s", mask)
            holdings = holdings[mask]
            if holdings.empty:
                _LOG.debug(
                    "No non-cash holdings available at timestamp=`%s`",
                    as_of_timestamp,
                )
        return holdings

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
        hdbg.dassert_is_subset(Portfolio.HOLDINGS_COLS, df.columns)
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
            hdbg.dassert_eq(
                df["curr_num_shares"].dtype.type,
                np.float64,
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
            Portfolio.HOLDINGS_COLS,
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
            Portfolio.CASH_ID,
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
        single point in time. Cash must be included and be nonnegative.
        """
        Portfolio._validate_holdings_df(holdings_df)
        # Initialization should be at a single point in time.
        hdbg.dassert_eq(
            holdings_df.index.nunique(),
            1,
            "Encountered `%s` unique index values in initial holdings dataframe",
            holdings_df.index.nunique(),
        )
        # At initialization there should be no more than one row per asset.
        hdbg.dassert_no_duplicates(holdings_df["asset_id"].to_list())
        #
        initialization_timestamp = holdings_df.index[0]
        cash_holdings = Portfolio._get_holdings(
            holdings_df,
            initialization_timestamp,
            Portfolio.CASH_ID,
            exclude_cash=False,
        )
        # Initial cash must be non-negative.
        cash = cash_holdings["curr_num_shares"].values[0]
        hdbg.dassert_lte(0, cash)


# #############################################################################


# TODO(gp): -> SimulatedPortfolio
class Portfolio(AbstractPortfolio):
    def __init__(
        self,
        *args: Any,
        # In Python parameters after *args are always keywords-only.
        broker: ombroker.Broker,
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

    def _update_state(
        self,
        wall_clock_timestamp: pd.Timestamp,
    ) -> pd.DataFrame:
        """
        Update holdings at `timestamp` using fills information in `fill_df`.
        """
        _LOG.debug("")
        last_timestamp = self.get_last_timestamp()
        _LOG.debug("last_timestamp=%s", last_timestamp)
        # Get fills.
        fills_df = self._get_fills(wall_clock_timestamp)
        # Get latest holdings
        last_holdings = self.get_holdings(last_timestamp, asset_id=None)
        last_holdings.index.name = "last_timestamp"
        last_holdings.reset_index(inplace=True)
        last_holdings_srs = last_holdings.set_index("asset_id")["curr_num_shares"]
        # Update holdings using the `fills_df`.
        new_holdings_srs = last_holdings_srs.copy()
        if fills_df is not None:
            Portfolio._validate_fills_df(fills_df)
            # last_timestamp <= fills_df.index <= timestamp
            hdbg.dassert_lte(last_timestamp, fills_df["timestamp"].min())
            hdbg.dassert_lte(fills_df["timestamp"].max(), wall_clock_timestamp)
            holdings_diff = fills_df.set_index("asset_id")["num_shares"]
            cash_diff = -1 * (fills_df["price"] * fills_df["num_shares"]).sum()
            hdbg.dassert(np.isfinite(cash_diff))
            new_holdings_srs = new_holdings_srs.add(holdings_diff, fill_value=0)
            new_holdings_srs.loc[Portfolio.CASH_ID] += cash_diff
        new_holdings_srs.name = "curr_num_shares"
        new_holdings = new_holdings_srs.to_frame().reset_index()
        new_holdings.index = [wall_clock_timestamp] * len(new_holdings)
        new_holdings = new_holdings.convert_dtypes()
        return new_holdings

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
        # TODO(gp): Ensure that this returns all the fills before
        #  wall_clock_timestamp.
        fills = self.broker.get_fills(wall_clock_timestamp)
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
            Portfolio.FILLS_COLS,
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
            Portfolio.CASH_ID,
            fills_df["asset_id"].to_list(),
            "Order for cash detected.",
        )
        # There should be no more than one row per asset.
        hdbg.dassert_no_duplicates(fills_df["asset_id"].to_list())
        # All share values should be finite.
        hdbg.dassert(
            np.isfinite(fills_df["num_shares"]).all(),
            "All share values must be finite.",
        )
