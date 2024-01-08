<!--ts-->

- [OMS](#oms)
  - [High-level invariants](#high-level-invariants)
- [CCXT Broker Overview](#ccxt-broker-overview)
  - [Concepts](#concepts)
  - [Abstract CCXT broker](#abstract-ccxt-broker)
  - [Concepts](#ccxt-broker-v2)
- [CCXT Broker experiment workflow](#ccxt-broker-overview)
  - [Execution experiment flow](#execution-experiment-flow)
  - [Execution quality flow](#execution-quality-flow)

<!--te-->

# OMS

## High-level invariants

- The notion of parent vs child orders is only on our side, CCXT only
  understands orders
- We track data (e.g., orders, fills) into parallel OMS vs CCXT data structures
  - OMS vs CCXT orders
  - OMS vs CCXT fills
  - CCXT trades vs fills

- `Portfolio` only cares about parent orders

  - How orders are executed is an implementation detail
  - Thus, we need to fold all child fills into an equivalent parent fill

## Data structures

- Every time we submit an order to CCXT (parent or child) we get back a
  `ccxt_order` object (aka `ccxt_order_response`)
- It's mainly a confirmation of the order
- The format is https://docs.ccxt.com/#/?id=order-structure
- The most important info is the callback CCXT ID of the order (this is the
  only way to do it)

- `ccxt_trade`
  - For each order (parent or child), we get back from CCXT 0 or more
    `ccxt_trade`, each representing a partial fill (e.g., price, number of
    shares, fees)
  - E.g.,
    - If we walk the book, we obviously get multiple `ccxt_trade`
    - If we match multiple trades even at the same price level, we might get
      different `ccxt_trade`

- `ccxt_fill`
  - When an order closes, we can ask CCXT to summarize the results of that order
    in terms of the composing trades
  - We can't use `ccxt_fill` to create an `oms_fill` because it doesn't contain
    enough info about prices and fees
    - We get some information about quantity, but we don't get fees and other
      info (e.g., prices)
  - We save this info to cross-check `ccxt_fill` vs `ccxt_trade`

- `oms_fill`
  - It represents the fill of a parent order, since outside the execution system
    (e.g., in `Portfolio`) we don't care about tracking individual fills
  - For a parent order we need to convert multiple `ccxt_trades` into a single
    `oms_fill`
  - TODO(gp): Unclear
  - Get all the trades combined to the parent order to get a single OMS fill
    - We use this v1
  - Another way is to query the state of an order
    - We use this in v2 prototype, but it's not sure that it's the final
      approach

## CCXT Broker Overview

### Concepts

- Order (oms.order.Order)
  - An object encapsulating information about an action to buy/sell a certain
    asset
  - It is characterized by asset ID, order type (i.e. limit, market), size (how
    much to buy/sell)

- Portfolio
  - A part of the production system which keeps track of the holdings, executed
    trades and trade statistics. This part of the system is located downstream
    from the Broker and receives trade and holdings information from it.
  - More on the production system:
    [Production system gdoc](https://docs.google.com/document/u/0/d/1EKAyOcx1fv16tt0xCvkIS8htAFY3_fa5wn9UnK_Ovbw/edit)

- CCXT Order Structure
  - A JSON structure is returned from the CCXT API which contains information
    related to the order, e.g. its size, price, type and status. This data type
    is shared by **CCXT child order responses **and **Ccxt Fills.**
  - See [CCXT documentation](https://docs.ccxt.com/#/?id=order-structure)

- Fill (oms.broker.Fill)
  - When we make a trade on an exchange, it can be (partially) successful (AKA
    filled) or unsuccessful
    - We can ask to buy 1 BTC for 20k but we might only obtain 0.5 BTC because
      of various reasons
  - A fill stores information about the originally desired order and the actual
    outcome of a trade

- CCXT Fill
  - Order structure -> {“id”: .. “price” …}
    - ## CCXT order response &lt;- immediately after we submit order
    - CCXT Fill &lt;- we query the order status by an ID, receive the similar
      structure, but
      - We query for it after ~1 min or so to check the `filled` field

- Open positions
  - The amount of an assets/contract currently held by the account in the
    market. Before we start trading, all positions are 0. The position is a
    single number which is changed every time a trade is executed. For example:
    - We start with position of 0 for APE/USDT;
    - We successfully execute a “buy” order for 1000 APE/USDT
    - Then, we successfully execute a “sell” order for 500 APE/USDT
    - Resulting open position on APE/USDT: 500 of APE/USDT
  - See [CCXT documentation](https://docs.ccxt.com/#/?id=positions)
  - [CcxtBroker reference](https://github.com/cryptokaizen/cmamp/blob/master/oms/ccxt/abstract_ccxt_broker.py#L426)

- Child/Parent order
  - Depending on the strategy we want to use when executing an order we can
    split an order into multiple parts, each part then becomes a child order of
    the original order and the original order becomes a parent order to it.
- OMS vs CCXT
  - In the context of a Broker, we use the term OMS (Order Management System)
    when we refer to parts of the system that are specific to our own codebase,
    and CCXT when we refer to data structures, methods and entities specific to
    CCXT API.
  - For example, `oms_order` is an `oms.Order` object; `ccxt_order` is a CCXT
    order structure.

- CCXT Trade
  - There is a “Trade structure” in CCXT documentation
    - In practice, it’s a dictionary template (JSON with particular fields)
    - One order can be filled by multiple trades
      - Order to buy 10k APE not above 1$ can be filled by 2 trades: 5k for 0.98
        and 5k for 0.99
      - Trade contains info on price and fees
      - For oms.Fills, we do a weighted average for total price of the Order and
        sum of fees to calculate the cost
- CCXT response

# Types of orders

- Regular vs. TWAP execution
  - In the context of CcxtBroker, TWAP execution is a type of execution where
    the initial (parent) order is divided into several smaller child orders,
    which are executed over time.
  - Example:
    - With a “buy” order of 1000 APE/USDT, we want to conduct a TWAP execution
      over 5 minutes.
    - We divide the parent order into 5 child orders of 200 APE/USDT to be
      executed each minute for 5 minutes.
    - At the beginning of the minute, we submit a “buy” order for 200 APE/USDT.
    - If at the end of a minute the order is not filled, we cancel it.
    - Repeat for 5 times.
- **Limit vs Market orders (D)**
  - Market orders are executed immediately at the price that the market
    currently offers. Limit orders include a limit price: for “buy” orders the
    “limit price” is a price above which the order will not be executed, and for
    “sell” orders this is a price below which the order will not be executed. It
    is also possible to set a time for which the order will be active, after
    which it will be canceled if it is not filled.
  - More on top of the book and limit orders:
    https://www.machow.ski/posts/2021-07-18-introduction-to-limit-order-books/

### Abstract CCXT Broker

- What functionality is covered in the abstract broker?

### CCXT Broker

- Explain high level how does a process of submitting twap orders work? (Explain
  the concept - if we have a good article link it works and then we can explain
  some implementation details with regards to our code)

## CCXT Broker experiment workflow

### Broker-only execution flow

- By broker-only execution, we mean a trading session (most commonly
  "experiment") in which the orders are generated randomly, without the full
  DataFlow system. Such experiments only test the actual execution irrespective
  of the model, hence the name "broker-only"
  - On how to run the Broker-only experiment and evaluate the quality, see
    `docs/trade_execution/ck.run_broker_only_experiment.how_to_guide.md`.

### Full System execution Flow

- By full system execution, we mean an experiment in which the orders are
  generated by the DataFlow system, based on the predictions generated by one of
  the models.
- A full system experiment is launched via Airflow.
  - On how to run the full system experiment and evaluate the quality, see
    `docs/trade_execution/ck.full_system_execution_experiment.how_to_guide.md`

# Code

- There are multiple loops and distinction
  - Distinction btw market and limit orders
  - Distinction btw different OMS/Exchange backends (CCXT vs DataFrame vs DBs)
  - Loop around the orders
  - Loop around the waves
  - Loop around retry in case of error

Broker has `submit_orders`
- Gets a list of orders to submit during the bar
- Because the orders are implemented differently in different Brokers (e.g.,
  CcxtBroker) it delegates the implementation to the abstract `_submit_orders` in
  derived classes

- AbstractCcxtBroker has a _submit_orders which is not abstract
  - Loop around the orders
    - Based on the order type we need to compute a limit price or not
    - TODO: load_bid_ask_and_calculate_limit_price -> _calculate_limit_price
    - We call _submit_single_order_to_ccxt
      - Just submits the order (market or limit) and retries if there is a
        problem

# OMS

- Expected behaviors from the mock test:
- **_log_into_exchange**:
- Same as in CcxtExtractor, calls CCXT and AWS Secrets
- Correct/incorrect login parameters, spot/futures login

**- _assert_order_methods_presence**
- No output, just a check for presence of these methods in this
instance of a CCXT Exchange object

- e.g. present for binance, not present for bitfinex, i.e. not
implemented yet or have a different name

**- _get_minimal_order_limits:**

- Imitate a call to CCXT; (- `self._exchange.load_markets()`)

- Do we need an example of the Binance API response?

- These methods are called at the `__init__` stage

**- _submit_orders**:

- The main method

- The output is every sent order in DF form (those are also saved as a
`self._sent_orders` attribute, see below the `get_fills` method)
and the response from binance API

- Important to note that a successful execution of
`self._sent_orders` method changes the state of the CcxtBroker class
instance

- mock `self._exchange.createOrder`,
self._force_minimal_order(order), self._check_order_limit(order)

- It’s an async method which can diverge it from methods we mocked in
Extractors

**- Imitate the following behaviors:**

- the order is smaller than the minimal quantity checked via
`self._check_order_limit`;

- In this case the order is modified via the aforementioned method and
successfully placed

- The order meets a liquidity error, see
`_check_binance_code_error(e, -4131)` - this behavior can be
expected from placing an order with one of the lower-volume coins, i.e.
HNT/USDT

- The order is bigger than the minimum and is running in a `local`
environment - in this case the order is forced to the minimum amount via
`self._force_minimal_order`

- The order is bigger than the minimum and running in a `preprod`
environment - in this case the order is submitted as usual

- An example of initialized broker can be found in
get_CcxtBroker_prod_instance1 function;

- the strategy_id used now is SAU1

- The IM client and the market data are initialized via

```

im_client = icdc.get_mock_realtime_client(connection)

market_data =
oomccuti.get_RealTimeImClientMarketData_example2(im_client)

```

- Check the
`amp/im_v2/common/data/client/realtime_clients_example.py` and
`amp/market_data/market_data_example.py` for methods to create
example instances of ImClient and MarketData

- Ideally we would also want to mock those if it's not too much
trouble: Broker requires a MarketData with DB connection to be
initialized, but it is only useful in the context of the System and
Portfolio management and has nothing to do with ability to actually post
orders which we are checking in the tests

**- get_fills:**

- From CCXT, mock `self._exchange.fetch_orders`

- The method gets orders that have been filled out of those that have
been submitted

- What we want to check is the consistency, i.e. if we mock submitting
a test order that has been successfully submitted, we expect the
`get_fills()` to return that order in a `ombroker.Fill` format.

Example of CcxtBroker init:

```

env_file = imvimlita.get_db_env_path("dev")

# Get login info.

connection_params =
hsql.get_connection_info_from_env_file(env_file)

# Login.

connection = hsql.get_connection(*connection_params)

# Remove table if it already existed.

hsql.remove_table(connection, "mock2_marketdata")

# Initialize real-time market data.

im_client = icdc.get_mock_realtime_client(connection)

market_data =
oomccuti.get_RealTimeImClientMarketData_example2(im_client)

broker = get_CcxtBroker_prod_instance1(market_data, "SAU1")

```

## Examples of Orders:

Source:

-   amp/oms/submit_ccxt_orders.py

DEFAULT_ORDERS = """Order: order_id=0 creation_timestamp=2022-08-05
10:36:44.976104-04:00 asset_id=1464553467 type_=price@twap
start_timestamp=2022-08-05 10:36:44.976104-04:00
end_timestamp=2022-08-05 10:38:44.976104-04:00 curr_num_shares=0.0
diff_num_shares=0.121 tz=America/New_York

Order: order_id=1 creation_timestamp=2022-08-05 10:36:44.976104-04:00
asset_id=1467591036 type_=price@twap start_timestamp=2022-08-05
10:36:44.976104-04:00 end_timestamp=2022-08-05 10:38:44.976104-04:00
curr_num_shares=0.0 diff_num_shares=0.011 tz=America/New_York

Order: order_id=2 creation_timestamp=2022-08-05 10:36:44.976104-04:00
asset_id=2061507978 type_=price@twap start_timestamp=2022-08-05
10:36:44.976104-04:00 end_timestamp=2022-08-05 10:38:44.976104-04:00
curr_num_shares=0.0 diff_num_shares=169.063 tz=America/New_York

Order: order_id=3 creation_timestamp=2022-08-05 10:36:44.976104-04:00
asset_id=2237530510 type_=price@twap start_timestamp=2022-08-05
10:36:44.976104-04:00 end_timestamp=2022-08-05 10:38:44.976104-04:00
curr_num_shares=0.0 diff_num_shares=2.828 tz=America/New_York

Order: order_id=4 creation_timestamp=2022-08-05 10:36:44.976104-04:00
asset_id=2601760471 type_=price@twap start_timestamp=2022-08-05
10:36:44.976104-04:00 end_timestamp=2022-08-05 10:38:44.976104-04:00
curr_num_shares=0.0 diff_num_shares=-33.958 tz=America/New_York

Order: order_id=5 creation_timestamp=2022-08-05 10:36:44.976104-04:00
asset_id=3065029174 type_=price@twap start_timestamp=2022-08-05
10:36:44.976104-04:00 end_timestamp=2022-08-05 10:38:44.976104-04:00
curr_num_shares=0.0 diff_num_shares=6052.094 tz=America/New_York

Order: order_id=6 creation_timestamp=2022-08-05 10:36:44.976104-04:00
asset_id=3303714233 type_=price@twap start_timestamp=2022-08-05
10:36:44.976104-04:00 end_timestamp=2022-08-05 10:38:44.976104-04:00
curr_num_shares=0.0 diff_num_shares=-0.07 tz=America/New_York

Order: order_id=7 creation_timestamp=2022-08-05 10:36:44.976104-04:00
asset_id=8717633868 type_=price@twap start_timestamp=2022-08-05
10:36:44.976104-04:00 end_timestamp=2022-08-05 10:38:44.976104-04:00
curr_num_shares=0.0 diff_num_shares=3.885 tz=America/New_York

Order: order_id=8 creation_timestamp=2022-08-05 10:36:44.976104-04:00
asset_id=8968126878 type_=price@twap start_timestamp=2022-08-05
10:36:44.976104-04:00 end_timestamp=2022-08-05 10:38:44.976104-04:00
curr_num_shares=0.0 diff_num_shares=1.384 tz=America/New_York"""

orders = omorder.orders_from_string(DEFAULT_ORDERS)

for order in orders:

# Update order type to one supported by CCXT.

order.type_ = "market"

...

Expected output for these orders:

('filename_0.txt',

order_id creation_timestamp asset_id type_ \

0 0 2022-08-05 10:36:44.976104-04:00 1464553467 market

1 1 2022-08-05 10:36:44.976104-04:00 1467591036 market

2 2 2022-08-05 10:36:44.976104-04:00 2061507978 market

3 3 2022-08-05 10:36:44.976104-04:00 2237530510 market

4 4 2022-08-05 10:36:44.976104-04:00 2601760471 market

5 5 2022-08-05 10:36:44.976104-04:00 3065029174 market

6 6 2022-08-05 10:36:44.976104-04:00 3303714233 market

7 7 2022-08-05 10:36:44.976104-04:00 8717633868 market

8 8 2022-08-05 10:36:44.976104-04:00 8968126878 market

start_timestamp end_timestamp \

0 2022-08-05 10:36:44.976104-04:00 2022-08-05 10:38:44.976104-04:00

1 2022-08-05 10:36:44.976104-04:00 2022-08-05 10:38:44.976104-04:00

2 2022-08-05 10:36:44.976104-04:00 2022-08-05 10:38:44.976104-04:00

3 2022-08-05 10:36:44.976104-04:00 2022-08-05 10:38:44.976104-04:00

4 2022-08-05 10:36:44.976104-04:00 2022-08-05 10:38:44.976104-04:00

5 2022-08-05 10:36:44.976104-04:00 2022-08-05 10:38:44.976104-04:00

6 2022-08-05 10:36:44.976104-04:00 2022-08-05 10:38:44.976104-04:00

7 2022-08-05 10:36:44.976104-04:00 2022-08-05 10:38:44.976104-04:00

8 2022-08-05 10:36:44.976104-04:00 2022-08-05 10:38:44.976104-04:00

curr_num_shares diff_num_shares tz

0 0.0 0.012032 America/New_York

1 0.0 0.001000 America/New_York

2 0.0 11.037528 America/New_York

3 0.0 1.000000 America/New_York

4 0.0 -2.747253 America/New_York

5 0.0 291.205591 America/New_York

6 0.0 -43.215212 America/New_York

7 0.0 1.000000 America/New_York

8 0.0 0.066961 America/New_York )
