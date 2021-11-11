"""
Import as:

import oms.place_orders as oplord
"""

# We should use a RT graph executed once step at a time.
# For now we just play back the entire thing.

def get_file_name():
    trading_mode = "cand"

    dst_dir = "s3://eglp-core-exch/files/spms/SAU1/cand/targets/YYYYMMDD000000/<filename>.csv"
    # Number of minutes from the beginning of trading day.
    #idx
    #file_name = idx.timestamp
    #file_name = "s3://eglp-core-exch/files/spms/SAU1/cand/targets/YYYYMMDD000000/<filename>.csv"


def convert_to_csv():
    # trade_date -- Must be set to the current live trade date.
    #
    # egid -- EGID the target is for.
    #
    # target_position -- The new position (in qty shares) you wish to enter. Positive for long, negative for short.
    #
    # notional_limit -- Max absolute notional allowed for an order that's sent out to move you to the target position. If the order required to move you to your target position exceeds this amount, then this target will be rejected. Concrete example: target_position=7, current_position=5. Order qty would be 2. Suppose adjprice = 12.5. Then order notional is 12.5 * 2 = $25. If 25 > the notional_limit set here, the target is rejected.
    #
    # adjprice -- The latest price your system has seen for this EGID. Used along notional_limit to perform the order notional check.
    #
    # adjprice_ccy -- Currency of last seen price. Always set to "USD" for US trading.
    #
    # algo -- Name of broker algo that should be used for the order that will move you to the desired target position.
    # The remaining columns are broker algo specific and will depend on what broker algo(s) you want to use. Examples for VWAP:
    # STARTTIME -- Start time of VWAP algo order.
    # ENDTIME -- End time of the VWAP algo order.
    # MAXPCTVOL -- Max participation rate (in %) of the VWAP algo order.

    # trade_date,egid,target_position,notional_limit,adjprice,adjprice_ccy,algo,STARTTIME,ENDTIME,MAXPCTVOL,
    # 20211027,15151,7,26,12.5,USD,VWAP,10:19:38 EST,14:30:00 EST,3


# #############################################################################


# class EgPortfolio:
#
#     def __init__(self, strategy_id: str, account: str):
#         self._strategy_id = strategy_id
#         self._account = account
#
#     def get_bod_holdings(self, trade_date: Optional[datetime.date]) -> pd.DataFrame:
#         """
#         Return the current holdings.
#         """
#         # tradedate       2021-10-28
#         # egid                 10005
#         # strategyid            SAU1
#         # account          SAU1_CAND
#         # bod_position             0
#         # bod_price              0.0
#         # currency               USD
#         # fx                     1.0
#         return
#
#     def get_current_holdings(self,
#                              trade_date: Optional[datetime.date]) -> pd.DataFrame:
#         """
#         Return the current holdings.
#         """
#         # tradedate                           2021-10-28
#         # egid                                     10005
#         # strategyid                                SAU1
#         # account                              SAU1_CAND
#         # published_dt        2021-10-28 12:01:49.414932
#         # target_position                           None
#         # current_position                             0
#         # open_quantity                                0    ?
#         # net_cost                                   0.0
#         # currency                                   USD
#         # fx                                         1.0
#         # action                                     BOD    ?
#         # bod_position                                 0
#         # bod_price                                  0.0
#         return
#
#     def get_total_wealth(ts, cash, holdings) -> float:
#         """
#         Return the value of the current holdings and cash.
#         """
#         pass
#
#     def place_orders(self, orders):
#         """
#         Place orders.
#         """
#         # EG version places trades to the S3.
#         return
#
#     def get_pnl(self):
#         """
#         Return the timeseries of positions, PnL.
#         """
#         # SELECT * FROM eod_pnl_candidate_view
#         #   WHERE account='SAU1_CAND' AND tradedate=current_date - 1
#         #   ORDER by egid LIMIT 1;
#
#         # strategyid                                  SAU1
#         # tradedate                             2021-10-28
#         # account                                SAU1_CAND
#         # egid                                       10005
#         # cost_basis                                   0.0  ?
#         # mark                                  795.946716  ?
#         # avg_buy_px                                   0.0
#         # avg_sell_px                                  0.0
#         # qty_bought                                     0
#         # qty_sold                                       0
#         # quantity                                       0
#         # position_pnl                                 0.0
#         # trading_pnl                                  0.0
#         # total_pnl                                    0.0
#         # local_currency                               USD
#         # portfolio_currency                           USD
#         # fx                                           1.0
#         # to_usd                                       1.0
#         # timestamp_db          2021-10-28 20:40:05.517649
#         return


class Locates:

    def __init__(self, strategy_id: str, account: str):
        self._strategy_id = strategy_id
        self._account = account

    def get_locates(self, trade_date) -> pd.DataFrame:
        # tradedate
        # egid
        # strategyid
        # account
        # quantity
        # rate
        # timestamp_update
        # timestamp_db
        return


# def place_orders(df_preds: pd.DataFrame):
#     """
#                                      17085 ...
#                      end_time
#     2018-07-16 09:10:00-04:00        NaN   NaN
#     2018-07-16 09:15:00-04:00        NaN   NaN
#     2018-07-16 09:20:00-04:00        NaN   NaN
#     2018-07-16 09:25:00-04:00        NaN   NaN
#     ...                              ...   ...
#     2018-07-20 16:40:00-04:00        NaN   NaN
#     2018-07-20 16:45:00-04:00        NaN   NaN
#     2018-07-20 16:50:00-04:00        NaN   NaN
#     2018-07-20 16:55:00-04:00        NaN   NaN
#     2018-07-20 17:00:00-04:00        NaN   NaN
#
#
#     """
#     _LOG.debug("pred=%s", pred)
#     dbg.dassert(np.isfinite(pred), "pred=%s", pred)
#
#     # Mark the portfolio to market.
#     _LOG.debug("# Mark portfolio to market")
#     wealth = get_total_wealth(mi, ts, cash, holdings, price_column)
#     dbg.dassert(np.isfinite(wealth), "wealth=%s", wealth)
#
#     # Get locates.
#
#     # Get holdings.
#
#     #
#     orders = []
#     for egid in egids:
#         # Compute target position.
#         price_0 = mi.get_instantaneous_price(ts, price_column)
#         target_num_shares = wealth_to_allocate / price_0
#         target_num_shares *= pred[egid]
#
#         #
#         diff = target_num_shares - holdings
#         # Create order.
#         order = Order(mi, order_type, ts_start, ts_end, diff)
#         orders.append(order)
#
#     #
#     place_orders(orders)


def _compute_target_positions(current_ts: pd.Timestamp,
                              predictions: pd.Series,
                              ) -> pd.DataFrame:
    """
    Compute the target positions using `predictions`.

    :return: a dataframe about the holdings with various information, e.g.,
    """
    # Mark the portfolio to market.
    _LOG.debug("# Mark portfolio to market")
    wealth = portfolio.get_total_wealth(current_ts)
    dbg.dassert(np.isfinite(wealth), "wealth=%s", wealth)
    # Get the current holdings.
    holdings = portfolio.get_holdings(current_ts, exclude_cash=True)
    holdings = portfolio.mark_holdings_to_market(current_ts, holdings)
    # Add the predictions.
    predictions = pd.DataFrame(predictions, columns=["preds"])
    holdings = holdings.merge(holdings, predictions, on="asset_id", how="outer")
    hdbg.dassert_lt(0, holdings["price"])
    holdings[["preds", "price"]].fillna(0.0, inplace=True)
    # Compute the target notional positions.
    scale_factor = preds.abs().sum()
    hdbg.dassert(np.isfinite(scale_factor), "scale_factor=%s", scale_factor)
    hdbg.dassert_lt(0, scale_factor)
    holdings["target_wealth"] = wealth * holdings["preds"] / scale_factor
    holdings["target_num_shares"] = holdings["target_wealth"] / holdings["price"]
    # Compute the target share positions.
    # TODO(gp): Round the number of shares to integers.
    holdings["diff_num_shares"] = holdings["num_shares"] - holdings["target_num_shares"]
    return holdings


def place_orders(
    # TODO(gp): -> dag_df
    df_5mins: pd.DataFrame,
    execution_mode: str,
    config: Dict[str, Any],
    order_id: int = 0,
) -> pd.DataFrame:
    """
    Place orders corresponding to the predictions stored in the given df.

    Orders will be realized over the span of two intervals of time (i.e., two lags).

    - The PnL is realized two intervals of time after the corresponding prediction
    - The columns reported in the df are for the beginning of the interval of time
    - The columns ending with `+1` represent what happens in the next interval
      of time

    :param predictions: a time-series indexed by the ts with the predictions for each
        assets

    :param execution_mode:
        - `batch`: place the trades for all the predictions (used in historical
           mode)
        - `real_time`: place the trades only for the last prediction as in a
           real-time set-up
    :param config:
        - `pred_column`: the column in the df from the DAG containing the predictions
           for all the assets
        - `mark_column`: the column from the PriceInterface to mark holdings to
          market
        - `portfolio_interface`: object used to store positions
        - `locate_interface`: object used to access short locates
    :return: df with information about the placed orders
    """
    # Check
    price_interface = config["price_interface"]
    hdbg.dassert_issubclass(price_interface, Portfolio)
    portfolio = config["portfolio_interface"]
    hdbg.dassert_issubclass(portfolio, Portfolio)
    # Unique id of the
    # Cache some variables used many times.
    last_index, _ = preds[-1]
    #price_mark_column = config["price_mark_column"]
    offset_5min = pd.DateOffset(minutes=5)
    order_type = config["order_type"]
    #
    tqdm_out = htqdm.TqdmToLogger(_LOG, level=logging.INFO)
    num_rows = len(preds)
    for ts, pred in tqdm(preds, total=num_rows, file=tqdm_out):
        # _LOG.debug(hprint.frame("# ts=%s" % _ts_to_str(ts)))
        _LOG.debug("pred=%s", pred)
        dbg.dassert(np.isfinite(pred), "pred=%s", pred)
        if ts == last_index:
            # For the last timestamp we only need to mark to market, but not post
            # any more orders.
            continue
        # Use current price to convert forecasts in position intents.
        _LOG.debug("# Compute trade allocation")
        # Enter position between now and the next 5 mins.
        ts_start = ts
        ts_end = ts + offset_5min
        #
        df = _compute_target_positions(ts)
        _LOG.debug("# Place orders")
        # Create order.
        orders: List[Order] = []
        for ts_tmp, row in df.iteritems():
            hdbg.dassert_eq(ts, ts_tmp)
            asset_id = row["asset_id"]
            diff_num_shares = row["diff_num_shares"]
            if diff_num_shares == 0.0:
                # No need to place trades.
                continue
            order = Order(order_id, mi, ts, asset_id, order_type, ts_start, ts_end,
                          diff_num_shares)
            order_id += 1
            _LOG.debug("order=%s", order)
            orders.append(order)
        # Execute the orders.
        # TODO(gp): We rely on the assumption that orders span only one time step
        #  so we can evaluate an order starting now and ending in the next time step.
        #  A more accurate simulation requires to attach "callbacks" representing
        #  actions to timestamp.
        portfolio.place_orders(ts, orders)

    # Update the df with intermediate results.
    df_5mins[pnl] = df_5mins[wealth].pct_change()
    return df_5mins
