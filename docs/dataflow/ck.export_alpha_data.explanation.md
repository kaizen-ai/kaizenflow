# Exporting alpha data. Explanation

<!-- toc -->

- [Package](#package)
- [Reported values](#reported-values)
  * [Format](#format)
  * [Target positions](#target-positions)
  * [Research PnL](#research-pnl)
- [Trading timing semantics](#trading-timing-semantics)
- [How to reconstruct PnL from target positions](#how-to-reconstruct-pnl-from-target-positions)
  * [Example for a single asset](#example-for-a-single-asset)

<!-- tocstop -->

# Package

The tarball `KT_alpha_package.zip` includes:

- `target_holdings_shares_and_prices.csv.gz` -- target positions and
  corresponding prices, refer to
  [the target positions section](#target-positions)
- `pnl.bar_by_bar.csv.gz` -- granular (e.g., bar-by-bar for X-minute bars) PnL, refer to
  [the pnl section](#research-pnl)
- `pnl.daily.csv.gz` -- daily PnL, refer to [the pnl section](#research-pnl)
- `load_alpha_pnl_and_trades.tutorial.ipynb` -- the runnable `ipynb` file that
  shows how to load the data
- `load_alpha_pnl_and_trades.tutorial.html` -- HTML version of the `ipynb`
  tutorial, feel free to open in a web browser

# Reported values

The goal of this flow is to export data about the "research alpha" of our
trading strategies, in terms of target positions and PnL, from which one can
extract many interesting metrics.

The research alpha is based only on forecasting price movements before
considering effects that are specific to the trading set up (e.g., market fees,
underfill rate, adverse selection, algo execution quality, mixing with other
strategies that might hedge each other). Please refer to
[`ForecastEvaluatorFromPrices`](https://github.com/cryptokaizen/cmamp/blob/master/dataflow/model/forecast_evaluator_from_prices.py#L27).

In other words, research alpha assumes that there is no cost of trading and
maximizes gross PnL. Research alpha is fed to an optimizer that uses market
impact model to throttle execution and maximize realized PnL.

## Format

The data is stored in our usual `DataFlow` format. TODO(Grisha): add a pointer
to the section in the DataFlow document.

- A snippet of the data is below:
  ```
                                            feature1                        feature2
                                            asset1            asset2        asset1          asset2
  end_ts
  2022-10-01 01:15:00+00:00                 263.15             2.943        283.876080      19370.890679
  2022-10-01 01:20:00+00:00                 191.26             2.569        284.008718      19379.697236
  2022-10-01 01:25:00+00:00                 138.68             2.326        283.873623      19375.829511
  2022-10-01 01:30:00+00:00                 171.75            -4.228        283.451532      19382.296497
  2022-10-01 01:35:00+00:00                 186.27            -4.696        283.211041      19384.764448
  ```

In brief, a DataFrame in `DataFlow` format

- Is indexed by `end_ts` which represents the end of the current trading bar
  (e.g., on a 5 minute resolution)
- Has two levels of column indices:
  - The first level (outermost) consists of features (e.g., `feature1`,
    `feature2`, `pnl`)
  - The second level consists of asset names (e.g., `asset1`,
    `binance::BTC_USDT`)

## Target positions

- A snippet of the data is below:
  ```
                                 target_holdings_shares              price
                                 binance::BNB_USDT binance::BTC_USDT binance::BNB_USDT binance::BTC_USDT
  end_ts
  2022-10-01 01:15:00+00:00                 263.15             2.943        283.876080      19370.890679
  2022-10-01 01:20:00+00:00                 191.26             2.569        284.008718      19379.697236
  2022-10-01 01:25:00+00:00                 138.68             2.326        283.873623      19375.829511
  2022-10-01 01:30:00+00:00                 171.75            -4.228        283.451532      19382.296497
  2022-10-01 01:35:00+00:00                 186.27            -4.696        283.211041      19384.764448
  ```

- Column name: `target_holdings_shares`
  - The desired amount of shares at time `T+1`, for each column representing
    `T`, which corresponds to the bar ending at `end_ts`
    - E.g., at the bar ending `2022-10-01 01:15:00` we want to reach `2.569`
      (shares) `binance::BNB_USDT` by the bar ending at `2022-10-01 01:20`
- Column name: `price`
  - Prices: volume weighted average prices aka VWAP

## Research PnL

- Column name: `pnl`
  - Represents the research PnL in dollars, i.e., it is financial gain or loss
    from buying and selling assets at `end_ts`. The `pnl` represents both
    realized and unrealized pnl.

The bar-by-bar PnL is computed at the Kaizen Tech trading frequency (e.g., 5
minutes).

E.g., at `2022-10-01 00:00:00+00:00` the per-bar PnL is `16.613688$` for
`binance::BTC_USDT`.

- An example of the data is below:
  ```
                            pnl
                            binance::BNB_USDT binance::BTC_USDT
  end_ts
  2022-10-01 00:00:00+00:00        -80.729206         16.613688
  2022-10-01 00:05:00+00:00        -33.351286          5.740454
  2022-10-01 00:10:00+00:00        -20.362921        -36.442894
  2022-10-01 00:15:00+00:00        -22.322110         97.692387
  2022-10-01 00:20:00+00:00        -25.685092        -31.682631
  ```

However, there is also a file that contains daily PnL values. The daily PnL is
the sum of per-bar PnL values for each trading bar within a day.

E.g., on `2022-10-01` (`2022-10-01 00:00:00+00:00` represents the entire day
`2022-10-01`) the model has made `16.613688$` in total for `binance::BTC_USDT`.

- An example of the data is below:
  ```
                            pnl
                            binance::BNB_USDT binance::BTC_USDT
  end_ts
  2022-10-01 00:00:00+00:00        -80.729206         16.613688
  2022-10-02 00:00:00+00:00        -33.351286          5.740454
  2022-10-03 00:00:00+00:00        -20.362921        -36.442894
  2022-10-04 00:00:00+00:00        -22.322110         97.692387
  2022-10-05 00:00:00+00:00        -25.685092        -31.682631
  ```

# Trading timing semantics

- At time `T`, a KaizenFlow system makes a forecast that between `T+1` and `T+2`
  the price of a given asset will increase or decrease
  - We want to buy or sell shares of this asset to monetize the forecast,
    depending on the sign and the magnitude of the forecast
- At time `T` we use the forecast to compute a target position `TP` that we plan
  to achieve at `T+1`
  - This approach gives the execution system an entire bar (e.g., 5 minutes) to
    enter the position
- The PnL for the forecast at `T` is realized between `T+1` and `T+2` and
  timestamped at time `T`
- Note that since a KaizenFlow system emits a forecast at every step, entering
  and exiting trades corresponding to consecutive forecasts overlap and are
  automatically merged
  - E.g., assume the portfolio is flat at `T`, the forecast for `T+1`
    corresponds to a position of +1 BTC and the forecast for `T+2` corresponds
    to a position of +1 BTC (i.e., hold the position)
  - The trade to realize forecast `T+1` requires to:
    - Go from 0 to 1 BTC in [`T+1`, `T+2`]; and
    - Go from 1 to 0 BTC in [`T+2`, `T+3`]
  - The trade to realize forecast `T+2` requires to:
    - Go from 0 to 1 BTC in [`T+2`, `T+3`]; and
    - Go from 1 to 0 BTC in [`T+3`, `T+4`]
  - Thus the opposite trades in [`T+2`, `T+3`] are internally reconciliated and
    no trade is actually performed, keeping the current and target holdings to
    +1 BTC
    - TODO(Paul, grisha): Pls double check. Where does actually happen this in
      the code?

- TODO(grisha): Can we add a plot with mermaid to show the timing?

# How to reconstruct PnL from target positions

- The PnL for the forecast at `T` is
  `pnl(T) = target_position(T) * VWAP_ret(T+2)`, i.e.,
  `pnl(T) = target_position(T) * (VWAP(T+2) - VWAP(T+1))` where
  - `pnl(T)` is the realized pnl that corresponds to the forecast for bar `T`
  - `target_position(T)` is the target position that corresponds to the forecast
    for bar `T`. This is the position that is executed within `T` and `T+1`, and
    the goal is to have `target_position(T)` in shares at time `T+1`
  - `VWAP(T)` is the vwap price for the bar starting at `T-1` and ending at
    `T`, i.e., `VWAP(T) = volume_weighted_average_price([T-1, T])`

## Example for a single asset
```
time  | price | target_position
T     | 1     | 5
T+1   | 2     | ...
T+2   | 3     | ...
```

`pnl(T+2) = (price(T+2) - price(T+1)) \* target_position(T) = (3-2) \* 5 = 5`

- TODO(grisha): let's complete this.
