<!--ts-->
* [Point-in-time predictors and observed response](#point-in-time-predictors-and-observed-response)
* [Alignment of predictors and predicted response](#alignment-of-predictors-and-predicted-response)
   * [Alternative alignment of predictors and predicted response](#alternative-alignment-of-predictors-and-predicted-response)
* [Training models](#training-models)
   * [The world of sklearn](#the-world-of-sklearn)
   * [The world of gluonts](#the-world-of-gluonts)
   * [An aside](#an-aside)
<!--te-->

## Point-in-time predictors and observed response

- In `dataflow`, time series of predictors and a time series of an observed
  response is represented using a dataframe with:
  - A monotonically increasing datetime index with uniform increments. In other
    words:
    - Time `t_{i-1}` happens before `t_i`, for all `i`
    - `t_i - t_{i-1}` is equal to a constant independent of `i`
    - The time grid has no gaps, e.g., weekends and overnight periods are
      sampled uniformly in the same way that more interesting periods of time
      are
  - Point-in-time data with a left-open, right-closed, right-label convention
    - E.g., in the dataframe, the label `t_i` represents the value of the
      variables in the time interval `(t_{i-1}, t_i]`
    - This convention has the nice property that all and only information
      available at time <= `t_i` is represented in the data frame for rows with
      index `t_i`, which makes it easier to prevent future-peeking
  - A snippet appears as follows:

    | idx | x_var | ret_0 |
    | --- | ----- | ----- |
    | t_0 | x_0   | z_0   |
    | t_1 | x_1   | z_1   |
    | t_2 | x_2   | z_2   |
    | ... | ...   | ...   |

## Alignment of predictors and predicted response

- It is not useful in practice to predict `z_0` from `x_0` at `t_0`, since `z_0`
  is known at time `t_0`
- Instead, we want to predict forward response values, e.g., `z_2` from `x_0` at
  time `t_0`
- See `time_series.md` for further discussion
- To row-align the target that we want to predict with the given predictors, we
  shift the response column backwards in time (e.g., `z_2` moves to where `z_0`
  had been), e.g.

  | idx | x_var | ret_2 |
  | --- | ----- | ----- |
  | t_0 | x_0   | z_2   |
  | t_1 | x_1   | z_3   |
  | t_2 | x_2   | z_4   |
  | ... | ...   | ...   |

- Note that, e.g., at time `t_2`, only the following entries are known:

  | idx     | x_var   | ret_2 |
  | ------- | ------- | ----- |
  | t\_{-2} | x\_{-2} | z_0   |
  | t\_{-1} | x\_{-1} | z_1   |
  | t_0     | x_0     | z_2   |
  | t_1     | x_1     | ?     |
  | t_2     | x_2     | ?     |
  | ?       | ?       | ?     |

- In this representation, the emphasis is on the time at which predictions are
  made:
  - For row with time index `t_j`, the most recent predictor is `x_j`
  - In real-time mode, some (one or more) entries of the response column will be
    unknown, and one (e.g., the last) or more will be the focus of prediction

### Alternative alignment of predictors and predicted response

- The alternative to shifting the response column backwards is to shift the
  predictor columns forward, e.g.,

  | idx | x_var   | ret_0 |
  | --- | ------- | ----- |
  | t_0 | x\_{-2} | z_0   |
  | t_1 | x\_{-1} | z_1   |
  | t_2 | x_0     | z_2   |
  | ... | ...     | ...   |

- In this format, the emphasis is on the time for which (instead of at which)
  predictions are made
  - For row with time index `t_j`, we do not typically use the most recent
    predictor `x_j`, but rather predict the return (ending) at that point
  - In real-time mode, we shift our predictors into future time points for which
    we are carrying out prediction

- This approach always delays quantities in time and never "anticipates"
  quantities (i.e., move quantities from the future to the past)
- Besides these semantic details, the approaches are equivalent when the time
  grid is uniform

## Training models

- We will focus on the problem formulation where response columns are shifted
- For purposes of comparison, we will also sometimes consider the alternative
  (equivalent) alignment that instead uses lagged predictors.

### The world of `sklearn`

- `sklearn` does not support time series modeling out-of-the-box
- If we pass to `sklearn` an `x_var` column and a response column, e.g.,
  `ret_2`, then `sklearn` will only use the `x_var` value `x_j` at time `t_j` to
  predict `z_{j + 2}`
  - `sklearn` follows the typical supervised learning setup where predictors
    and predicted variables are contemporaneous and there is no "time"
  - Many time series models use time series history in predictions
  - In order to build such models in `sklearn`, we explicitly incorporate
    history by appending lagged predictor and/or response columns
- Some care must be taken in order to prevent future-peeking, e.g.,
  - In the response-shifted representation, future-peeking is respected by only
    using point-in-time or lagged columns (e.g., never a forward response
    column) as predictors
  - In the predictor-shifted representation, no future-peeking is respected
    (with respect to response columns) by only using admissible lags of the
    response variable as predictors
    - E.g., if we are predicting `z_2` for time `t_2` (but making the prediction
      at time `t_0`), then we may use `z_0` as a predictor, but not `z_1`, since
      `z_1` is not yet known at time `t_0`

### The world of `gluonts`

- `gluonts` is specifically designed for forecasting time series
- A parameter that the user must specify is `prediction_length`, which
  determines how many unit time steps into the future the model predicts
- Suppose we want to predict returns two time steps into the future (`ret_2`),
  and begin with

  | idx | x_var | ret_0 |
  | --- | ----- | ----- |
  | t_0 | x_0   | z_0   |
  | t_1 | x_1   | z_1   |
  | t_2 | x_2   | z_2   |
  | ... | ...   | ...   |

- `gluonts` requires (at least for some models, like DeepAR) that we know
  covariates (x variables) over the future window of length `prediction_length`
- This again motivates predictor/response column alignment, e.g.,

  | idx | x_var | ret_2 |
  | --- | ----- | ----- |
  | t_0 | x_0   | z_2   |
  | t_1 | x_1   | z_3   |
  | t_2 | x_2   | z_4   |
  | ... | ...   | ...   |

- So, if we want to predict `z_2` at time `t_0`, then we need to know both
  `x_{-1}` and `x_0` (which we do know when the target is `ret_2`)
- In `gluonts`, we feed `x_var` and `ret_2` into the model with the same
  starting time `t_0` and with the same frequency increment `freq`, but with
  `prediction_length = 2`. The framework itself ensures (e.g., in the case that
  we build an autoregressive model on the response) that there is no
  future-peeking (but note that the `j` in `ret_j` must equal
  `prediction_length`)
- In applying the `predict` step over historical series, we obtain two
  predictions. Combining and rearranging them, we build

  | idx | x_var | ret_2 | ret_1_hat | ret_2_hat |
  | --- | ----- | ----- | --------- | --------- |
  | t_0 | x_0   | z_2   | z_1_hat   | z_2_hat   |
  | t_1 | x_1   | z_3   | z_2_hat   | z_3_hat   |
  | t_2 | x_2   | z_4   | z_3_hat   | z_4_hat   |
  | ... | ...   | ...   | ...       | ...       |

- A distinction between `gluonts` and `sklearn` in training and prediction is
  that `gluonts` wants to ingest the entire time series up to the present time
  - Though `context_length` (for number of historical values used) may be set
    explicitly, values in the more distant past may still in fact enter in
    training
  - If performed in a naive way (e.g., converting a time slice of the input
    dataframe into the `gluonts` `ListDataset` format), there will be a large
    amount of conversion work (e.g., for each time step, create and then destroy
    potentially large `numpy` arrays)
    - The simplest approach may also be the safest
    - The naive approach can be parallelized if needed (for historical
      backtesting)

### An aside

- Classical time series models assume that the sampling frequency is uniform
- On the other hand, models that lag predictors by more than one step (or
  equivalently, predict forward returns beyond one step) are in reality
  multistep models, but are sometimes trained as though they were single-step
  (i.e., we only train and predict for the final time step)
- This distinction is important when considering autoregressive models
  - E.g., suppose we want to predict `ret_2`
  - This would mean calculating `z_2_hat` with information available at `t_0`
    and earlier
  - The one-step-ahead forecast would be `z_1_hat` (for `z_1`, which is not
    known at time `t_0`)
  - To obtain `z_2_hat`, we should carry out one additional prediction (at every
    time step) in order to obtain `z_2_hat`
- What this means is that by taking into account the mechanics of trading (e.g.,
  time to get into position and time to get out), we are already and naturally
  in the setting of multistep prediction

## Returns and volatility

### Definition

Let `p_t` denote the price of an asset as time `t`.

The _percentage return_ from time `t-1` to time `t` is defined by
`(p_t - p_{t -  1}) / p_{t - 1} = p_t / p_{t - 1} - 1`.
This is also called the _relative return_.

The _log return_ is defined by
`\log (p_t / p_{t - 1})`.

For small changes in price, the percentage return and log return are the same
up to a first order approximation (e.g., using a Taylor expansion):
`\log (p_t / p_{t - 1}) = \log (1 + (p_t / p_{t - 1} - 1) \approx p_t / p_{t -1} - 1`.

Volatility refers to the standard deviation of the return.
  - Note that this is not instantaneously observable
  - _Realized volatility_ (or _historical volatility) refers to the standard
    deviation of returns calculated over a rolling window 

### Calculating returns  

To calculate returns, we use the function `fin.compute_ret_0()`. The `0` in the
name emphasizes the fact that the return series is not shifted in time.

To translate between log and percentage returns, use
`fin.convert_log_rets_to_pct_rets()` or `fin.convert_pct_rets_to_log_rets()`.

### Units

What we typically call "returns" or "return" is really short for
"rate of return". In particular,
  - Returns have units of "return per time" (percentage or log))
  - The standard deviation of returns, or _volatility_, has units of
    "return per square root time"

To make returns and volatility comparable across instruments and
strategies, we typically annualize the quantities using
`fin.compute_annualized_return()` and
`fin.compute_annualized_volatility()`, respectively.

### Log or relative?

- (Quant Nugget 2: Linear vs. Compounded Returns)[https://papers.ssrn.com/sol3/papers.cfm?abstract_id=1586656]
  - Here "linear" refers to "relative" and "compound" to "log"

Reasons to prefer log returns include:
  - The distribution can be easily projected to any time horizon
  - Log returns naturally aggregate in time
  - The distribution is symmetric
  - They are comparable across instruments
  - The "standard" assumption in continuous-time finance and economics is that
    log returns are normally distributed (as when the underlying price process
    is given by geometric Brownian motion)
   
Reasons to prefer relative returns include:
  - Relative returns naturally aggregate cross-sectionally
  - Relative returns aggregate naturally across time under the assumption of a
    fixed capital allocation
  - Relative returns are robust to scenarios where an investment can decrease
    in value 100% or more

### PnL

- (Quant Nugget 5: Return Calculations for Leveraged Securities and Portfolios)[https://papers.ssrn.com/sol3/papers.cfm?abstract_id=1675067]

For this section, we choose dollars as the _numeraire_.

Suppose we have an instrument with prices `p_0, p_1, p_2, ...`, in dollars.
Denote our corresponding _target holdings_ (or _positions_) as `h_1, h_2, ...`
(also in dollars).

> For the purposes of this section, we assume that we know our
target holdings `n` steps ahead (typically with `n` set to `1` or `2`). To
connect this with our conventions around backtests, we would generate our target
holdings for period `j` from a prediction for the returns over period `j-1` to
`j`.

We assume that we enter `h_j`, transacting at price `p_{j - 1}` at time
`t_{j - 1}`, and exit the position at time `t_j` at price `p_j` (while
simultaneously entering the next position `h_{j + 1}`). The position `h_j`
therefore has as dollar value `h_j` at time `t_{j - 1}` and a dollar value
of `h_j \cdot (p_j / p_{j - 1} - 1)` at time `t_j`. This may be re-expressed as
`h_j \cdot r_j`, where `r_j` denotes the relative return.

Under these conventions, the strategy PnL series is a sequence of dollars,
representing profit or loss.
- Average PnL has units of dollar per time
- PnL volatility has units of dollar per square root time
- Sharpe ratio (still) has units of per square root time
  - The Sharpe ratio is invariant under changes of scale, e.g., doubling the
    target positions does not alter the Sharpe ratio
- Aggregation over time intervals is by summation

An alternative way to interpret `h_j` is as a ratio of a fixed but unspecified
dollar amount, e.g., a fixed amount of capital available for deployment daily
- We assume profits are siphoned off / losses are "replenished" to/from the
  total "available capital"
- `h_j \cdot r_j` then admits the interpretation as the percentage return with 
  respect to the capital deployed
- We may interpret this PnL curve as we would returns
  - Though the returns are relative, they are additive in time (because the
    amount of capital is fixed)
  - Return and volatility again admit percentage-like units
- The advantage of this interpretation is that it is scale invariant with
  respect to the size of the deployed capital
- To convert into dollars, we multiply `h_j` by the fixed "available capital"
  amount

## Statistics

### Sharpe ratio

The Sharpe ratio, abbreviated SR, is our key metric for evaluating returns.

A good reference for many technical nuances of the Sharpe Ratio is
(A Short Sharpe Course)[https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3036276].

Some key facts about the Sharpe ratio:
- It is equivalent to a rescaled t-statistic
- The probability of a drawdown of a certain size can be bounded in terms of
  the Sharpe ratio

It is important to understand how return characteristics and changes in units
(e.g., resampling in time) affect the Sharpe ratio: 
- SR is not a unitless quantity, but has units of per square root time
- Using relative returns instead of log returns inflates the SR 
- SR is fairly robust to non-normality assumptions
  - Strong positive autocorrelation overly inflates SR estimates
  - Vice-versa for negative autocorrelation