<!--ts-->

-   [Kibot](#kibot)
    -   [Buy](#buy)
        -   [Subscription](#subscription)
        -   [Historical data](#historical-data)
    -   [API](#api)
    -   [FAQs](#faqs)
        -   [Data delivery](#data-delivery)
        -   [Historical minute data format](#historical-minute-data-format)
        -   [Historical tick data format](#historical-tick-data-format)
        -   [Historical aggregate bid / ask data](#historical-aggregate-bid--ask-data)
        -   [Time zone](#time-zone)
        -   [Market sessions](#market-sessions)
        -   [Missing minutes](#missing-minutes)
        -   [Historical tick data](#historical-tick-data)
        -   [Prices outside bid/ask spread](#prices-outside-bidask-spread)
        -   [Historical futures data](#historical-futures-data)
        -   [Settlement price](#settlement-price)
        -   [Historical forex data](#historical-forex-data)
        -   [Indexes](#indexes)
        -   [Data quality](#data-quality)
        -   [Missing bars](#missing-bars)
        -   [Price in bars can be discontinuous](#price-in-bars-can-be-discontinuous)
        -   [Closing prices](#closing-prices)
        -   [Odd lots](#odd-lots)
        -   [Price sources](#price-sources)
        -   [Why adjusted data?](#why-adjusted-data)
        -   [Adjustment methods for splits and dividends](#adjustment-methods-for-splits-and-dividends)
        -   [Split multipliers](#split-multipliers)
        -   [Dividend multipliers](#dividend-multipliers)
        -   [Adjustment](#adjustment)
    -   [Comparing to other sources](#comparing-to-other-sources)

<!--te-->

# Kibot

-   http://www.kibot.com/

-   Kibot offers market data for:

    -   US stocks
    -   ETFs
    -   Futures
    -   Forex

-   Both 1-minute level and tick data (time and sales)

    -   From 1-minute level data Kibot creates bar data for 5, 30, 60 minutes
        data

-   History length

    -   21 years of minute-level intraday data for stocks and ETFs
    -   10 years of minute level intraday for Forex and Futures
    -   10 years of tick-by-tick stock, ETFs, Forex, and Futures

-   Historical data are verified for accuracy
    -   TODO(gp): Not clear how

## Buy

-   From [here](http://www.kibot.com/buy.aspx)

-   Do not provide real-time or streaming data
-   Daily (end-of-day) data is free / included with tick, second, or minute data
    -   TODO(gp): Probably because EOD can be regenerated from higher frequency
        data for most of the data sets (maybe not for futures?)
-   Aggregated data (1, 5, 15, 30 mins) is free / included with tick data
-   Data include
    -   pre-market and after-market data for stocks / ETFs
    -   24h trading for futures and forex
-   Stocks / ETFs adjustments
    -   Unadjusted
    -   Adjusted for splits and dividends
    -   Adjusted for splits

### Subscription

-   Premium subscription (from [here](http://www.kibot.com/updates.aspx))
    -   Daily data updates
    -   4 years of minute data for Stocks, ETFs, Futures and Forex
    -   Up to 57 years of EOD historical data
    -   \$139 / month
-   Maybe Professional subscription is fine
    -   Same as Premium but only 2 years of minute data
    -   \$69 / month

### Historical data

-   There is the list of all names included in each package

-   200 most liquid stocks

    -   200 names
    -   1 minute data, from 1998 -> \$520
    -   Tick + bid / ask, from 2009 -> \$2,250

-   SP500

    -   591 names (it seems that it has no survivorship bias)
    -   1 minute data, from 1998 -> \$750 (<--)
    -   Tick + bid / ask, from 2009 -> \$1,500

-   All stocks

    -   8300 names
    -   1 minute data, from 1998 -> \$3,000
    -   Tick + bid / ask, from 2009 -> \$9,000

-   All ETFs

    -   1950 names
    -   1 minute data, from 1998 -> \$1,200
    -   Tick + bid / ask, from 2009 -> \$3,600

-   All forex

    -   1189 names
    -   1 minute data, from 2009 -> \$350
    -   Tick + bid / ask, from 2009 -> \$990

-   All futures (continuous and individual contracts)
    -   5621 names
    -   1 minute data, from 2009 -> \$820
    -   Tick + bid / ask, from 2009 -> \$3,750

## API

-   From [here](http://www.kibot.com/api/historical_data_api_sdk.aspx)
-   All data is available after the close of the regular market session
-   US stocks and ETFs minute data is delayed 30 mins during market hours
    -   Both adjusted and unadjusted data is available
    -   Adjusted for splits only (and not dividends) with the same methodology
        of Yahoo / Google Finance

## FAQs

-   From [here](http://www.kibot.com/Support.aspx)

### Data delivery

-   Data is delivered through website or FTP

-   List of symbols
    -   [Active](http://www.kibot.com/files/2/allactive.txt)
    -   [Delisted](http://www.kibot.com/files/2/alldelisted.txt)
    -   Files are updated once per week

### Historical minute data format

-   CSV files
-   For minute (and higher) data the format is:

    -   Date `MM/DD/YYYY`, Time `hh:mm`, Open, High, Low, Close, Volume
    -   Time zone is always ET

-   Aggregated data have a time value indicating the time when the bar opened
    ```
    a time stamp of 10:00 AM is for a period between 10:00:00 AM and 10:00:59 AM.
    All records with a time stamp between 9:30:00 AM and 3:59:59 PM represent the
    regular US stock market trading session.
    ```

### Historical tick data format

-   CSV files
-   The format is:

    -   Date, Time, Bid, Ask, Size

-   The bid / ask prices are recorded when a trade occurs and represent NBBO
    across multiple exchanges and ECNs
-   Trade is recorded with transaction price and volume
-   All transactions are included, and are not aggregated

### Historical aggregate bid / ask data

-   The format is:
    -   Date, Time,
    -   BidOpen, BidHigh, BidLow, BidClose
    -   AskOpen, AskHigh, AskLow, AskClose

### Time zone

-   The time zone is always ET, even if data comes from CBOT (in the CT
    timezone)
-   Time values are conformed to EST (standard time, UTC-5) and EDT (daylight
    saving time, UTC-4)

### Market sessions

-   Our stocks and ETFs have:
    -   pre-market session: 8:00-9:30am ET
    -   regular session: 9:30-16:00 ET
    -   after market: 16:00-18:30 ET
-   Some liquid ETFs (e..g, SPY) trades from 4am to 8pm ET

### Missing minutes

-   If a bar is missing it means that there were no trades in that interval
    -   E.g., during pre / after market sessions, or for less liquid stocks

### Historical tick data

-   Only transaction volume is recorded together with best bid / ask prices at
    the moment of the transaction (across multiple exchanges, NBBO)
-   Kibot doesn't record bid / ask volume or multiple bid / ask levels
    -   According to SEC 97% of orders are cancelled, thus bid / ask volume are
        unreliable

### Prices outside bid/ask spread

-   Prices can be outside bid / ask spreads for several reasons

1. For illiquid instruments an order cannot be filled with the first bid / ask
   spread

-   The price registered becomes the average of multiple price levels included
    in the trade

2. A large block trade can be handled by the block desk at a market maker firm
   and reported to the exchange

-   The price can be at a price outside bid / ask spread at the report time

3. Bid / ask are taken across multiple markets

### Historical futures data

-   The continuous contract rolls to the contract
-   No back-adjust previous contract data
    -   TODO(gp): Then there could be a little jump
-   Rollover happens on (or 2-7 days before) the expiration date
    -   TODO(gp): Unclear when exactly
-   The expiration date is available in the metadata
    -   Expiration is 9:30am ET on the third Friday of the delivery month

### Settlement price

-   The settlement price is set by the exchange as weighted average price
    shortly before the close of the market
-   The close value of the last bar in the daily data reflects the settlement
    price

-   For this reason the last minute price might be different from the closing
    price

### Historical forex data

-   Forex market is not organized in a central exchange
-   Historical forex data has no price or volume, but has bid / ask prices only
-   Volume is reported as the number of times the bid / ask has changed in that
    minute

### Indexes

-   Kibot doesn't track changes to indices, like SP500

### Data quality

-   The feed you receive from your broker should have the same data as Kibot

    -   The feed is combined as NBBO

-   Due to lack of liquidity it is possible to have spikes in the pre / after
    sessions

    -   TODO(gp): Compute sensitivity by using all market data vs regular
        session

-   Kibot adjusted stock data should match Yahoo "Adj Close"
-   Kibot unadjusted stock data should match Yahoo "Close"

### Missing bars

-   There are not always 390 1-minute bar per day in a regular session
-   Regular session is from 9:30am to 15:59
    $$(16:00 - 9:30) * 60 = 6.5 * 60 = 390$$

1. Some days are half days:

-   From 9:30am to 12:59pm, which corresponds to 210 minutes

2. Some less liquid instruments can have no trading activity in one minute

3. The open for a particular stock can be delayed so that it starts few minutes
   later than 9:30am

4. Trading during regular hours can be halted

-   One solution is to forward fill the data using 0 volume for the added bars

### Price in bars can be discontinuous

-   The close price from one bar is often different from the open price of the
    next bar
-   The reason is the way prices and volume information are aggregated in bars
-   Consider the case of 5-mins bar, one at 9:30:00am and the second at
    9:35:00am
    -   There can be one trade at 9:30:00am, which goes into 9:30:00am bar both
        for open and close price
    -   Another trade at 9:39:59am, which goes into the open price (!)
    -   The time between the two trades is 10 mins and the prices can be very
        different, reflected in the close vs open price of the consecutive bars

### Closing prices

-   Kibot uses:

    -   the first trade price as the Open price for the first bar of the day
    -   the last trade price as the Close price for the last bar of the day

-   The official closing price is not the last trade price, since it is
    determined during the close auction

-   To get price for "market-on-close" orders you can use free sources like
    Yahoo

### Odd lots

-   Kibot does not include odd lot transactions, for neither price or volume

-   Odd lots cannot set a last price, by exchange rules
-   The official volume from the exchange include transactions smaller than 100
    shares (odd lots)

### Price sources

-   Price and volume data is taken from multiple exchanges (not only the primary
    exchange) and ECNs

### Why adjusted data?

-   A few dozen stocks and ETFs are adjusted for splits and dividends every
    market day
-   When historical data is adjusted, all the values are adjusted retroactively
    (multiplied by a ratio)

-   Without adjustments there will be price gaps before / after split / dividend
-   The real-life price and volume on a certain date is given by the unadjusted
    data

### Adjustment methods for splits and dividends

-   Kibot uses the same procedure as Yahoo (see
    [here](https://help.yahoo.com/kb/adjusted-close-sln28256.html))

-   Kibot adjusted data matches Yahoo historical prices except for the daily
    closing price which is the last transaction during the regular trading
    session, while for Yahoo might be a transaction after the close

### Split multipliers

-   Split multipliers are determined by split ratio
    -   2:1 split (i.e., each share becomes 2 after the split date)
        -   Then the pre-split data is multiplied by 0.5
    -   4:1 split (i.e., each share becomes 4 after the split date)
        -   Then the pre-split data is multiplied by 0.25
    -   1:5 reverse split (i.e., each 5 share becomes 1 share after the split
        date)
        -   Then the pre-split data is multiplied by 5

### Dividend multipliers

-   If a \$0.08 cash dividend is distributed on Feb 19 (ex-date) and the Feb 18
    closing price is \$24.96
    -   Then the pre-dividend data is multiplied by (1 - 0.08 / 24.96) = 0.9968

### Adjustment

-   The value price x volume is independent of adjustment since it represents
    the exact dollar amount exchanged

-   Volume is adjusted in both intraday and daily data
-   Unadjusted volume represents the actual number of traded shares

-   E.g., if there is a 2:1 split, each share becomes 2 shares

    -   a stockholder holding 100 shares, each at \$10, after the split holds
        200 shares each at \$5

    ```
    Date            Unadjusted      Adjusted
    2019/01/04      $10             $10
        <-- 2:1 split
    2019/01/05      $5              $10
    ```

## Comparing to other sources

-   From [here](http://www.kibot.com/comparedata.aspx)

-   Yahoo Finance website has recently changed the meaning of "adj close"
    column, which shows unadjusted prices (instead of adjusted!)
    -   It looks like the [Singapore version](http://sg.finance.yahoo.com/) of
        Yahoo finance is still using the correct definition
