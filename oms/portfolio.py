"""
Import as:

import oms.portfolio as opor
"""


import logging
from typing import Any, Dict, List, Optional

import pandas as pd

import helpers.dbg as hdbg

_LOG = logging.getLogger(__name__)


# TODO(gp): Add an abstract interface and other implementations.
class Portfolio:
    """
    Store tables with the following info:

    - holdings over time
        - how many shares of each assets are owned at any time
        - cash is treated as any other asset to keep code uniform
    - orders executed over time
        - what orders were executed, for how many shares, and at how much

    The tables are indexed by knowledge time, i.e., when this information became
    known by this object.
    """

    # ID of asset representing cash.
    CASH_ID: int = -1

    def __init__(
        self,
        strategy_id: str,
        account: str,
        #
        price_interface: RealTimePriceInterface,
        asset_id_column: str,
        price_column: str,
        #
        initial_cash: float,
        initial_ts: pd.Timestamp,
    ):
        """
        Constructor.

        :param asset_id_column: column name in the output df of `price_interface`
            storing the asset id
        :param price_column: column name used to mark holdings to market
        """
        self._strategy_id = strategy_id
        self._account = account
        #
        hdbg.dassert_issubclass(price_interface, RealTimePriceInterface)
        self._price_interface = price_interface
        self._asset_id_column = asset_id_column
        self._price_column = price_column
        # Initialize dataframe with holdings over time.
        hdbg.dassert_lt(0, initial_cash)
        row = [Portfolio.CASH_ID, initial_cash]
        # TODO(gp): Check that initial_ts is pd.Timestamp.
        # Store `id` and `position`.
        self._holdings_columns = ["asset_id", "curr_num_shares"]
        self._holdings = pd.DataFrame(
            [row], index=[initial_ts], columns=self._holdings_columns
        )
        # Initialize dataframe with executed orders.
        self._order_columns = [
            # Order corresponding to the change in position.
            "order_id",
            # Info from the order.
            "asset_id",
            "type_",
            "ts_start",
            "ts_end",
            "num_shares",
            # Info about the order execution.
            "num_shares_filled",
            "execution_price",
            # Holdings after this order was executed.
            "holdings+1",
            # Amount of cash after this order was executed.
            "cash+1",
        ]
        self._orders = pd.DataFrame(
            [], index=[initial_ts], columns=self._order_columns
        )

    def get_last_timestamp(self) -> pd.Timestamp:
        """
        Get the last time known to Portfolio.
        """
        hdbg.dassert_eq(self._holdings.index.max(), self._orders.index.max())
        return self._holdings.index.max()

    def get_holdings(
        self,
        ts: pd.Timestamp,
        asset_id: Optional[Any],
        exclude_cash: bool = False,
    ) -> pd.DataFrame:
        """
        Return the holdings at time `ts` for one or all the assets.

        - param asset_id: consider only a specific asset. `None` means all holdings.
        - param exclude_cash: exclude cash from the holdings
        """
        # tradedate                     2021-10-28
        # id                                 10005
        # strategyid                          SAU1
        # account                        SAU1_CAND
        # published_dt      2021-10-28 12:01:49.41
        # target_position                     None    Orders still in flight?
        # current_position                       0
        # open_quantity                          0    ?
        # net_cost                             0.0    ?
        # currency                             USD
        # fx                                   1.0
        # action                               BOD    ?
        # bod_position                           0    Redundant?
        # bod_price                            0.0    Redundant?
        hdbg.assert_in(ts, self._holdings.index)
        holdings = self._holdings.loc[ts]
        hdbg.dassert_no_duplicates(holdings["asset_id"])
        # Filter by asset_id, if needed.
        if asset_id is not None:
            mask = holdings["asset_id"] == asset_id
            holdings = holdings[mask]
        # Exclude cash, if needed.
        if exclude_cash:
            hdbg.dassert_ne(
                asset_id,
                self.CASH_ID,
                "You can't ask for cash value and exclude it at the same time",
            )
            mask = holdings["asset_id"] != self.CASH_ID
            holdings = holdings[mask]
        return holdings

    def mark_holdings_to_market(
        self, ts: pd.Timestamp, holdings: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Mark holdings to market prices at the same time.

        :param holdings: a subset of the `holding` df to annotate with price
            information
        :return: return `holdings` df decorated with `value`, `position`, and `price`
        """
        # Get the prices for the assets.
        asset_ids = holdings["asset_id"].unique()
        price_df = self._price_interface.get_data_at_timestamp(ts, asset_ids)
        # Extract subset of price information.
        columns = [self._asset_id_column, self._price_column]
        hdbg.dassert_is_subset(columns, price_df.columns)
        price_df = price_df[columns]
        # Merge the holdings with the prices.
        res_df = pd.merge(
            holdinds_df,
            price_df,
            how="outer",
            left_on="asset_id",
            right_on=self._asset_id_column,
        )
        res_df.rename(columns={self._price_column: "price"}, inplace=True)
        # Compute the value.
        res_df["value"] = res_df["curr_num_shares"] * res_df["price"]
        # TODO(gp): We should have no nans in res_df["value"].
        return res_df

    def get_total_wealth(self, ts: pd.Timestamp) -> float:
        """
        Return the total value of all holdings and cash at time `ts`.
        """
        # Mark to market the current holdings.
        holdings = self.get_holdings(ts, exclude_cash=True)
        holdings = self.mark_holdings_to_market(ts, holdings)
        # Compute value of holdings.
        holdings_value = holdings["value"].sum()
        dbg.dassert(
            np.isfinite(holdings_value), "holdings_value=%s", holdings_value
        )
        # Get the cash available.
        cash = self.get_holdings(ts, asset_id=self._CASH_ID)
        dbg.dassert(np.isfinite(cash), "cash=%s", cash)
        # Cash should always be positive.
        dbg.dassert_lte(0.0, cash, "cash=%s", cash)
        # Compute the total value.
        total_value = holdings_value + cash
        dbg.dassert(np.isfinite(total_value), "total_value=%s", total_value)
        return total_value

    def place_orders(self, current_ts: timestamp, orders: List[Order]) -> None:
        """
        Change state of the portfolio based on the given orders.
        """
        last_ts = self.get_last_timestamp()
        hdbg.dassert_lt(last_ts, current_ts)
        # Get the current available cash.
        cash = self.get_holdings(last_ts, asset_id=self.CASH_ID)
        # Process the orders.
        rows = []
        for order in orders:
            row: Dict[str, Any] = {}
            _LOG.debug("order=%s", order)
            # Copy content of the order.
            row["idx"] = current_ts
            row["order_id"] = order.order_id
            row["asset_id"] = order.asset_id
            row["type_"] = order.type_
            row["ts_start"] = order.ts_start
            row["ts_end"] = order.ts_end
            row["num_shares"] = order.num_shares
            # Get the current holding for the asset of the order.
            holdings = self.get_holdings(last_ts, asset_id=order.asset_id)
            # Complete fills.
            # TODO(gp): Allow partial fills.
            holdings += order.num_shares
            row["holdings+1"] = holdings
            # Account for the executed price of the order.
            execution_price = order.get_execution_price()
            row["execution_price"] = execution_price
            #
            cash -= execution_price * order.num_shares
            row["cash+1"] = cash
            #
            rows.append(row)
        # Cash should not be negative after executing all the orders.
        hdbg.dassert_lte(0.0, cash, "cash=%s", cash)
        # Add the information to the orders.
        orders_tmp = pd.DataFrame(rows, columns=self._order_columns)
        orders_tmp.set_index("idx", drop=True, inplace=True)
        orders_tmp.sort("asset_id", inplace=True)
        self._orders = pd.concat([orders_tmp, self._orders])

    def get_pnl(self):
        """
        Return the time series of positions, PnL.
        """
        # strategyid                                  SAU1
        # tradedate                             2021-10-28
        # account                                SAU1_CAND
        # id                                         10005
        # cost_basis                                   0.0  ?
        # mark                                  795.946716  ?
        # avg_buy_px                                   0.0
        # avg_sell_px                                  0.0
        # qty_bought                                     0
        # qty_sold                                       0
        # quantity                                       0
        # position_pnl                                 0.0
        # trading_pnl                                  0.0
        # total_pnl                                    0.0
        # local_currency                               USD
        # portfolio_currency                           USD
        # fx                                           1.0
        # to_usd                                       1.0
        # timestamp_db          2021-10-28 20:40:05.517649
        return

    def get_bod_holdings(
        self, ts: pd.Timestamp, id_: Optional[Any]
    ) -> pd.DataFrame:
        """
        Return the holdings at the beginning of a day.
        """
        # tradedate       2021-10-28
        # id                   10005
        # strategyid            SAU1
        # account          SAU1_CAND
        # bod_position             0
        # bod_price              0.0
        # currency               USD
        # fx                     1.0
        # TODO(gp): Just implement from get_current_holdings.
        return
