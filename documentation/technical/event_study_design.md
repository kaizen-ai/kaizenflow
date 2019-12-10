# Event Study Framework

## Classes of events

We consider the following two classes of events:

-   predictable events
    -   These are events that we know will happen ahead of time
    -   E.g., earnings releases, non-farm payroll
    -   We can build models that predict economic quantities both before and
        after the event
-   unpredictable events
    -   These are events whose timing or occurrence cannot be anticipated, but
        which can be recognized when the event occurs (e.g., breaking news)
    -   Predictive models can only react to the event (but we can still use
        models that cover pre/post-event times to model the effect for the
        purpose of hypothesis testing)
    -   In traders' parlance, "one can only trade the drift"

Both event types can be studied in the same framework given a list of
historical events.

## Classes of event studies

We make a distinction between the following different types of events (with
respect to active trading hours):

1.  Intraday events
    1.  We focus on events strictly within active trading hours
    1.  E.g., market-moving news breaks at 12:05pm: we want to study the
        reaction of the market in a window spanning 30 mins before and
        30 mins after the event, using a 5-min resolution
    1.  We may further restrict around the open / close, because response
        windows of uniform time are easy to work with (e.g., we may be
        interested in the 5-minute responses out to 30 minutes, but want to
        ensure that we have the same number of data points for each 5-min
        bar) 
1.  At-the-open
    1.  We focus on the effect on the market of information that has
        accumulated outside of active-trading-hours
    1.  E.g., we want to study the reaction of the market between the open and
        10 minutes after the open, conditioned on a certain event having
        happened prior to the open. We can can set up an event study between
        the open and the open + 10 minutes using a 1-minute resolution
    1.  These can be handled together with intraday events provided we treat
        the event time as the market-open
1.  At-the-close
    1.  Distinguished from vanilla intraday in that the timing of the close
        may restrict the response windows of interest
    1.  E.g., we may want to pin the target responses to specific minutes
        before the close rather than pin the window size. So we might study
        the response between the close minus 10 minutes and the close at
        1-minute increments
1.  Multi-day
    1.  Of interest if events are sparse on the scale of days; otherwise the
        proper multi-day setting is a continuous one
    1.  Days without active trading (e.g., weekends) should be excluded from
        the response variable time shifts
1.  Exotic
    1.  Suppose an event occurs 10 minutes before the close, but we believe
        that the market will not price in the information until the next day.
        This setting does not immediately fit into one of the preceding cases.
    1.  Some exotic cases can be adapted to fit into our more typical settings
        1.  One way to adapt an exotic case is to effectively shift the event
            time forward, e.g., perform an at-the-open study, but extend the
            look-back event window into time before the preceding day's close 
        1.  Another way to handle exotic cases is to abandon a uniform time
            grid for data, e.g., remove times between the close and the open
            and treat the resulting study as an intraday study

## Inputs

### What

-   A `pd.DataFrame` called `events` with a `pd.DatetimeIndex` of datetimes
        indicating event times
    -   If there is no additional data associated with an event other than that
        the event occurred, then `events` may consist of a single column, e.g.,
        an "indicator" column containing ones
    -   Downstream in the study, we want to make the simplifying assumption
        that no more than one event occurs at any given time. If multiple
        events occur simultaneously, then some aggregation should be performed,
        e.g., we may do a `resample().mean()` or a `resample().sum()` depending
        upon the circumstances
    -   We assume that the index is monotonically increasing 
    -   We assume that all events are relevant to a single financial instrument
-   A `pd.DataFrame` called `grid_data` consisting of at least one response and
        possibly one or more predictors 
    -   As suggested by the name, the data should lie on a `pd.DatetimeIndex`
        time grid of equal increments
    -   We assume that the index is monotonically increasing 
    -   We do not enforce that the time grid carry a `freq`, because we want
        to have flexibility in violating the strict time grid assumption, e.g.,
        we may have a grid of 5-min bars, but only during active trading hours 
    -   Example responses include returns and volatility for a single
        instrument
    -   For predictors, we may include features such as sentiment or event type
    -   A predictor can also be a simple coefficient indicating the magnitude
        and the sign (e.g., go short) of the position to take when event X
        happens
    -    
-   The times in the index of `events` should have offsets matching those of
    `grid_data`
    -   E.g., we resample `events` if needed so that they align with the
        with the `grid_data` index frequency 
-   We assume that `grid_data` is associated with a single instrument

### Why

-   Allowing `grid_data` to have predictors provides flexibility in the types of
    studies that may be run, e.g.,
    -   Suppose we study the effect of news
        -   The `events` consists of news events along with sentiment score
        -   We may reindex event features according to `grid_data`, apply a
            kernel to the features, and merge with `grid_data`
        -   By allowing `grid_data` to contain event features, we may regress
            on such predictors
        -   By including an event "indicator" column in `events` and merging
            with `grid_data`, we are allowing the use of an "intervention"
            variable in regression
        -   This setup enables us to perform inference on the effect of the
            news on the response
        -   We avoid needing to separately test events with different supposed
            directional effects (provided we believe that the predictors in
            `data` contain sufficient information to determine directionality)
-   Requiring that `grid_data` have monotonic datetimes with set time bars 
        -   Helps ensure that inference conclusions are valid (our procedures
            typically assume implicitly that the sampling frequency is
            uniform and need to be adjusted otherwise)
        -   Simplify and speed up the implementation
-   Requiring that `events.index` have offsets matching those of
    `grid_data.index` ensures alignment
        -   Time-shifting can be expensive, but is very efficient under certain
            simplifying assumptions 
-   Requiring that `grid_data` be single-instrument simplifies bookkeeping
    -   This is the most common case for commodities
    -   If it becomes important to study cross-sectional or aggregate effects,
        we can revisit this assumption
    
## Outputs

-   Single-instrument aggregated response before / after events
-   The simplest study investigates level changes
    -   These changes can be in series included in `data`, e.g.,
        -   returns
        -   volatility
        -   volume
        -   transformations of these (e.g., `log`, `cumsum`)
    -   In the case of returns, we also want to study cumulative returns 
    -   The change can be detected by encoding the event as an (Heaviside-like)
        indicator variable
        -   This approach also extends to the case with predictors
        -   If the event effect on the response is transient, then a different
            encoding may be more suitable (e.g., if we aren't really looking
            for a level change, but a spike)
        -   The regression determines the directionality of the event effect
            automatically
        -   This can all be done in a Bayesian way if desired
    -   An alternative is to create a baseline using pre-event data only, then
        compare post-event explicitly to the pre-event baseline
-   When predictors are involved, we may want to project to PnL using predicted
    returns, separate according to pre vs post event, and then perform a t-test
    or use BEST

## Statistical approach

-   We pool all events
    -   This is the simplest approach
    -   The number of events we expect to be large with respect to
        -   Settings where using hierarchical modeling could lead to a big
            improvement 
        -   The number of predictors 
    -   Hierarchical models may be introduced later if needed
-   For linear regression, we assume
    -   A constant term (a single pre/post-event level)
    -   A linear term based on event time offset (to detect a global trend)
        -   Maybe we should split this into two pieces, e.g.,
            - one ReLu unit zero pre-event
            - one ReLu unit zero post-event
    -   An event indicator variable
        -   This could be broken up into one variable per post-event tick
        -   We default to a single Heaviside indicator since this seems to
            cover the common case well
    -   Note that the resulting model may not be tradable at all times
        -   E.g., if the events in real life occur unpredictably, then
            knowledge about shifts before the event is not available
            pre-event 
-   The regression constitutes a learning step (so we should test OOS)

## Pipeline

1.  Data preparation
    -   The `grid_data` data should have uniform time bars
    -   The `events` dataframe should be aligned with `grid_data` in terms of
        offsets if it isn't already (e.g., using `resample`)
1.  Event/data alignment
    -   If the `events` dataframe should have an indicator column and one or
        more features. We use `reindex_event_features` to align these rows
        with `event_idx`, apply any processing steps (e.g., ema), and then
        merge the resulting dataframe with `grid_data`
    -   Applying a kernel may cause the effects of one event to "bleed" into
        another if applied blindly (the user needs to consider the distribution
        of the times between events, the time effects of the kernel, etc.) 
1.  Local time series (relative to event times)
    -   We decide how many frequency steps to look before and after each event
    -   We zero-index events and use negative integers for the past (note that
        sign conventions are opposite those used by `period` in `shift()`)
    -   The goal is to avoid timestamp arithmetic, but rather make time
        manipulations easy (e.g., by just using `shift()`)
    -   The result is a multiindex dataframe, indexed by
        -   event offset
        -   event timestamp
    -   The multiindexed dataframe supports easy analysis / modeling
        -   For regression / modeling, we drop the multiindex and use the
            relevant predictor and response columns
        -   For plotting, we may, e.g., use `groupby` on the event offset
            time, then aggregate with `mean`, to see the event effect on
            a response
    -   This step adds the linear and event indicator variables appropriately
1.  Model learning
    -   E.g., OLS or Bayesian
        -   Use Bayesian or OLS for estimating the event effect
        -   We use the framework to determine whether the effect is real, and
            if so, its direction and size
    -   Store info concerning model coefficients, etc.
    -   Generate predictions given the predictors and aggregate according to
        -   pre/post-event
        -   time shift
    -   Project to PnL using predicted returns vs. actual returns (e.g., use a
        kernel)
    -   The model should only upon
        -   predictors
        -   presence of event
        -   time shift relative to event (maybe)
    -   Project from "event space" back to linear time
