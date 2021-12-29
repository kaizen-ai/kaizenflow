<!--ts-->
   * [Invariants and conventions](#invariants-and-conventions)
   * [Implementation](#implementation)
      * [process_forecasts()](#process_forecasts)
      * [AbstractPortfolio](#abstractportfolio)
      * [AbstractBroker](#abstractbroker)
   * [Simulation](#simulation)
      * [SimulatedPortfolio](#simulatedportfolio)
      * [SimulatedBroker](#simulatedbroker)
   * [Implemented system](#implemented-system)
      * [ImplementedPortfolio](#implementedportfolio)
      * [ImplementedBroker](#implementedbroker)
   * [Mocked system](#mocked-system)
      * [MockedPortfolio](#mockedportfolio)
      * [MockedBroker](#mockedbroker)
      * [OmsDb](#omsdb)
      * [OrderProcessor](#orderprocessor)



<!--te-->

# Invariants and conventions

- In this doc we use the new names for concepts and use "aka" to refer to the
  old name, if needed

- We refer to:
  - The as-of-date for a query as `as_of_timestamp`
  - The actual time from `get_wall_clock_time()` as `wall_clock_timestamp`

- Objects need to use `get_wall_clock_time()` to get the "actual" time
  - We don't want to pass `wall_clock_timestamp` because this is dangerous
    - It is difficult to enforce that there is no future peeking when one object
      tells another what time it is, since there is no way for the second object
      to double check that the wall clock time is accurate
  - We pass `wall_clock_timestamp` only when one "action" happens atomically but
    it is split in multiple functions that need to all share this information.
    This approach should be the exception to the rule of calling
    `get_wall_clock_time()`

- It's ok to ask for a view of the world as of `as_of_timestamp`, but then the
  queried object needs to check that there is no future peeking by using
  `get_wall_clock_time()`

- Objects might need to get `event_loop`
  - TODO(gp): Clean it up so that we pass event loop all the times and event
    loop has a reference to the global `get_wall_clock_time()`

- The Optimizer only thinks in terms of dollar

# Implementation

## process_forecasts()

- Aka `place_trades()`
- Act on the forecasts by:
  - Get the state of portfolio (by getting fills from previous clock)
  - Updating the portfolio holdings
  - Computing the optimal positions
  - Submitting the corresponding orders
- `optimize_positions()`
  - Aka `optimize_and_update()`
  - Calls the Optimizer
- `compute_target_positions()`
  - Aka `compute_trades()`
- `submit_orders()`
  - Call `AbstractBroker`
- `get_fills()`
  - Call `AbstractBroker`
  - For IS it is different
- `update_portfolio()`
  - Call `AbstractPortfolio`
  - For IS it is different

- It should not use any concrete implementation but only `Abstract*`

## AbstractPortfolio

- `get_holdings()`
  - Abstract because IS, Mocked, Simulated have a different implementations
- `mark_to_market()`
  - Not abstract
  - -> `get_holdings()`, `PriceInterface`
- `update_state()`
  - Abstract
  - Use abstract but make it NotImplemented (we will get some static checks and
    some other dynamic checks)
    - We are trying not to mix static typing and duck typing

- CASH_ID, `_compute_statistics()` goes in `AbstractPortolio`

## AbstractBroker

- `submit_orders()`
- `get_fills()`

# Simulation

## SimulatedPortfolio

- This is what we call `Portfolio`
  - In RT we can run `SimulatedPortfolio` and `ImplementedPortfolio` in parallel
    to collect real and simulated behavior

- `get_holdings()`
  - Store the holdings in a df
- `update_state()`
  - Update the holdings with fills -> `SimulatedBroker.get_fills()`
  - To make the simulated system closer to the implemented

## SimulatedBroker

- `submit_orders()`
- `get_fills()`

# Implemented system

## ImplementedPortfolio

- `get_holdings()`
  - Check self-consistency and assumptions
    - Check that no order is in flight otherwise we should assert or log an
      error
  - Query the DB and gets you the answer
- `update_state()`
  - No-op since the portfolio is updated automatically

## ImplementedBroker

- `submit_orders()`
  - Save files in the proper location
  - Wait for orders to be accepted
- `get_fills`
  - No-op since the portfolio is updated automatically

# Mocked system

- Our implementation of the implemented system where we replace DB with a mock
  - The mocked DB should be as similar as possible to the implemented DB

## MockedPortfolio

- `get_holdings()`
  - Same behavior of `ImplementedPortfolio` but using `OmsDb`

## MockedBroker

- `submit_orders()`
  - Same behavior of `ImplementedBroker` but using `OmsDb`

## OmsDb

- `submitted_orders` table (mocks S3)
  - Contain the submitted orders
- `accepted_orders` table
- `current_position` table

## OrderProcessor

- Monitor `OmsDb.submitted_orders`
- Update `OmsDb.accepted_orders`
- Update `OmsDb.current_position` using `Fill` and updating the `Portfolio`
