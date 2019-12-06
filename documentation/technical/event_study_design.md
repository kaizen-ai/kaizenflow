# Event Study Framework

## Different types of event studies

We make a distinction between the following different types of events (with
respect to active trading hours):

1.  Intraday events

    a)  We focus on events strictly within active trading hours
    
    b)  We may further restrict around the open / close, because response
        windows of uniform time are easy to work with (e.g., we may be
        interested in the 5-minute responses out to 30 minutes, but want to
        ensure that we have the same number of data points for each 5-min
        bar) 
        
2.  At-the-open

    a)  We focus on the effect on the market of information that has
        accumulated outside of active-trading-hours
        
    b)  These can be handled together with intraday events provided we treat
        the event time as the market-open
        
3.  At-the-close

    a)  Distinguished from vanilla intraday in that the timing of the close
        may restrict the response windows of interest
        
4.  Multi-day

    a)  Of interest if events are sparse on the scale of days; otherwise the
        proper multi-day setting is a continuous one

    b)  Days without active trading (e.g., weekends) should be excluded from
        the response variable time shifts

## Inputs

### What

-   An `pandas.DatetimeIndex` of datetimes indicating event times
    -   Let's refer to it as `event_idx` 
-   A `pd.DataFrame` of responses and possibly also predictors, e.g.,
    -   Let's refer to it as `data`
    -   For the responses, we may study returns and volatility for a single
        instrument
    -   For predictors, we may include features such as sentiment or event type
-   `data` should have a monotonic datetimes index with a specified `freq`
-   `event_idx` should consist of datetimes with offsets matching those of
    `data`
-   We assume that `data` is associated with a single instrument

### Why

-   Allowing `data` to have predictors provides flexibility in the types of
    studies that may be run, e.g.,
    -   Suppose we study the effect of news
        -   The `event_idx` consists of news events
        -   `data` may consist of news sentiment, in addition to the response
        -   By allowing `data` to contain predictors such as sentiment, we may
            regress on the predictors
        -   In this sense, the `event_idx` is determining where and how to set
            an "indicator" or "intervention" variable
        -   This setup enables us to perform inference on the effect of the
            news on the response
        -   We avoid needing to separately test events with different supposed
            directional effects (provided we believe that the predictors in
            `data` contain sufficient information to determine directionality)
-   Requiring that `data` have monotonic datetimes with a specified `freq`
        -   Helps ensure that inference conclusions are valid (our procedures
            typically assume implicitly that the sampling frequency is
            uniform and need to be adjusted otherwise)
        -   Simplify and speed up the implementation
-   Requiring that the `event_idx` has offsets matching those of `data` ensures
    alignment
        -   We might want a layer that automatically performs shifts
        -   Time-shifting can be expensive, but in certain special cases can be
            made very efficient 
-   Requiring that `data` be single-instrument simplifies bookkeeping
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
        -   transformations of these (e.g., log, cumsum)
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
    -   The `data` dataframe should be prepared prior to the study
    -   The `event_idx` may be derived externally from filters on `data` or
        come from some other channel. In any case, we assume it is given
1.  Event/data alignment
    -   We decide how many frequency steps to look before and after each event
    -   We create a dataframe per step, indexed by event time, and tracked
        (e.g., in a dict) according to step shift size
1.  Reshaping
    -   We reshape the dataframes so that a regression / model may be fit to
        the data
    -   This step adds the linear and event indicator variables appropriately
1.  Model learning
    -   E.g., OLS or Bayesian
    -   Store info concerning model coefficients, etc.
    -   Generate predictions given the predictors and aggregate according to
        -   pre/post-event
        -   time shift
    -   Project to PnL using predicted returns vs. actual returns
    -   The model should only upon
        -   predictors
        -   presence of event
        -   time shift relative to event (maybe)
1.  Fancy plots
