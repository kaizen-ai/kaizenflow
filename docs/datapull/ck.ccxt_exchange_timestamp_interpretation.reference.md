

<!-- toc -->

- [CCXT Timestamp representation](#ccxt-timestamp-representation)
  * [Binance spot and Binance futures](#binance-spot-and-binance-futures)
  * [OKX spot and OKX futures](#okx-spot-and-okx-futures)
  * [Kraken spot and kraken futures](#kraken-spot-and-kraken-futures)

<!-- tocstop -->

### CCXT Timestamp representation

_As of version CCXT version 4.0.53_

There is no precise convention defined for the timestamp interpretation when
receiving data via [CCXT](https://docs.ccxt.com/#/?id=ohlcv-structure). The
problem this brings us to is that we cannot directly answer the following
question: For a given kline (AKA candle) - does the timestamp represent start of
the internal or the end of it?

To avoid confusion and possibly storing incorrect data we check the timestamp
representation manually for each exchange by checking the implementation of
`parse_ohlcv` method in the CCXT library + reviewing the API documentation of a
given exchange.

#### Binance spot and Binance futures

- CCXT returns timestamp which represents the opening time of the bar, reference
  code
  - https://github.com/ccxt/ccxt/blob/master/python/ccxt/binance.py#L3086
- The implementation is a little bit difficult to follow. For the endpoints that
  we use, the raw data is parsed from a list - the 0th position is taken which
  corresponds to ‘openTime’
  https://binance-docs.github.io/apidocs/futures/en/#kline-candlestick-data

#### OKX spot and OKX futures

- CCXT returns timestamp which represents the opening time of the bar
  https://www.okx.com/docs-v5/en/#order-book-trading-market-data-get-candlesticks
- Referential code:
  https://github.com/ccxt/ccxt/blob/master/python/ccxt/okx.py#L1957

#### Kraken spot and kraken futures

- CCXT returns timestamp which represents the opening time of the bar, reference
  code:
  - https://github.com/ccxt/ccxt/blob/master/python/ccxt/kraken.py#L815
  - https://github.com/ccxt/ccxt/blob/master/python/ccxt/krakenfutures.py#L608
- Reference post from reddit comment from official support
  https://www.reddit.com/r/KrakenSupport/comments/yxy8em/kraken_api_ohlc_response_is_not_clear/
  - For futures there is no explicit information about the timestamp
    interpretation but we can hopefully assume it’s the same
