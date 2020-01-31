<!--ts-->
      * [Overview](#overview)
      * [Guiding principles and conventions](#guiding-principles-and-conventions)
      * [Prices and returns](#prices-and-returns)



<!--te-->

## Overview

Each price data source (Kibot, Quandl CHRIS, Quandl SCF, ...) is idiosyncratic
with respect to, e.g.,

- What price resolution is available (e.g., 1min, 5mins, daily)
- How prices are labeled (e.g., at the beginning or at the end of the interval)
- How intervals are interpreted (e.g., [a, b) vs (a, b])
- What fields are available (close-only, OHLC, L1, L2, ...)

We want to adopt clear and uniform internal conventions to help us reason about
the data and to minimize any mistakes due to misinterpretations.

## Guiding principles and conventions

1. The primary time associated with a value should be the "knowledge time"
   1. Some time series naturally have multiple times associated with them, e.g.,
      EIA data is natively labeled according to the survey week, which is not
      made available until the following week
   1. Adopting knowledge times by default affords protection against
      future-peeking
   1. Unfortunately, knowledge times for historical data may need to be
      estimated. We may also want to impute knowledge times in the event that
      real-time collection fails on our end.
1. Knowledge times are represented as datetimes
   1. E.g., for daily closing price data with a label such as "2019-01-04", the
      label should be converted to "2019-01-04 16:00:00 ET"
   1. Additional information may be required for this conversion (e.g., trading
      calendars for instruments, data release times for government data, etc.)
   1. All datetimes should have a timezone
      1. In pandas we may not explicitly make a series `tz`-aware for
         performance reasons. In this case we encode the timezone in the column
         name
1. Time series represented in pandas Series or DataFrames are indexed by their
   knowledge datetimes
1. We use left-closed right-open time intervals (e.g., `[a, b)`) and choose the
   right endpoint (e.g., `b`) as the interval label
   1. This labeling convention respects knowledge times and behaves well under
      downsampling
   1. In the ideal setting (e.g., instantaneous computation and execution),
      information from `[a, b)` could be acted upon at time `b`
   1. Whenever we use the pandas `resample` function, we adopt the conventions
      (`closed="left", label="right"`)

## Prices and returns

1. The last price quote in an interval `[a, b)` we call the "closing" price, and
   we label it with time `b`. In series/dataframes, we label this price series
   with `close`. An "instantaneous" price at time `b` we also label in this way
   (assuming in practice that it is equivalent to the end-of-interval price of
   `[a, b)`).
1. Given a price time series following these conventions, say `prices`, we
   calculate returns using
   1. `prices` and `prices.shift(1)`
      1. We typically implicitly assume a uniform time grid
      2. In using a uniform grid, consecutive times may be represented as
         `t - 1`, `t`, `t + 1`, etc., where `1` is understood to be with respect
         to the sampling frequency
   1. Time labels from `prices`
1. We call returns calculated in this way `ret_0` by convention
   1. In general, `ret_0` at time `t` is the return realized upon exiting at
      time `t` a position held at time `t - 1`
      1. Note that `ret_0` at time `t` is observable at time `t`
      1. We use `ret_j` to denote `ret_0.shift(-j)`
   1. If, e.g., it takes one time interval to enter a position and one time
      interval to exit, then to realize `ret_0` at time `t`, a decision to enter
      must be made by time `t - 2`
1. We aim to predict forward returns, e.g., `ret_j` for `j > 0`
   1. In the ideal setting for "instantaneous" prices, we can come close to
      achieving `ret_1`
   1. Achieving `ret_2` is subject to fewer constraints (one time step to enter
      a position, one time step to exit)
   1. If prices represent aggregated prices (e.g., twap or vwap), then in the
      ideal setting `ret_2` is the earliest realizable return

## Aligning predictors and responses

1. Typically, a prediction at time `t_0` of the time `t_0` response value
   `resp_0` is not actionable. Therefore we actually want to predict forward
   response values (using the same timing conventions as `rets`; so `resp_n`
   for the forward response `n` steps ahead).
1. For convenience, we want to align (e.g., put in the same dataframe row)
   predictors with the corresponding response value that we are using the
   predictors to predict. E.g., if we are predicting `ret_2`, then we want to
   align row-wise the predictor and `ret_2` columns.
1. We have essentially two equivalent ways of performing the alignment:
   1. Shift the predictors
   1. Shift the response
1. If we shift the response:
   1. Predictor knowledge times are preserved
   1. In real-time mode, predictor timestamps correspond to "now" rather than
      the future
   1. Multiple forward returns can be used simultaneously (e.g., if we want to
      predict a forward curve / optimize how many unit steps ahead to predict)
1. If we shift (lag) the predictor:
   1. The return semantics are always clear (especially so if we ever restrict
      returns windows to ATH, etc., violating a uniform-grid assumption)
   1. Causality is respected in the sense that at any given datetime (row),
      everything in that row or preceding it is known (however, we know the
      "future" values of predictors)
1. A reasonable default would be to
   1. Enforce a uniform grid on the response variables (e.g., use `freq` for
      the dataframes)
   1. Shift and rename the response column to be explicit about what is being
      predicted and when
   1. Do not change the predictor timestamps (treat them as knowledge times)
