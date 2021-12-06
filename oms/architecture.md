<!--ts-->
   * [Invariants](#invariants)
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

# Invariants

- Use the new names and use "aka" to refer to the old name
- `->` means "calls"
- All objects need to use:
  - `get_wall_clock_time()` to get the actual time
  - `event_loop` to wait on events

# Implementation

## process_forecasts()

- Aka `place_trades()`
- Act on the forecasts by
  - Computing the optimal positions
  - Submitting the corresponding orders
  - Getting fills
  - Updating the portfolio holdings
- `optimize_positions()`
  - Aka `optimize_and_update()`
  - -> Optimizer
- `compute_target_positions()`
  - Aka `compute_trades()`
- `submit_orders()`
  - -> `AbstractBroker`
- `get_fills()`
  - -> `AbstractBroker`
- `update_portfolio()`
  - -> `AbstractPortfolio`

## AbstractPortfolio

- `get_holdings()`
- `mark_to_market()`
  - Not abstract
  - -> `get_holdings()`, `PriceInterface`
- `update_state()`

## AbstractBroker

- `submit_orders()`
- `get_fills()`

# Simulation

## SimulatedPortfolio

- `get_holdings()`
  - Store the holdings
- `update_state()`
  - Update the holdings with fills -> SimulatedBroker.get_fills()

## SimulatedBroker

- `submit_orders()`
- Pass back fills

# Implemented system

## ImplementedPortfolio

- `get_holdings()`
  - Check that no order is in flight
  - Query the DB
- `update_state()`
  - No-op since the portfolio is updated automatically

## ImplementedBroker

- `submit_orders()`
  - Save files in the proper location
  - Wait for orders to be accepted

# Mocked system

## MockedPortfolio

- `get_holdings()`
  - Same behavior of `ImplementedPortfolio` but using `OmsDb`

## MockedBroker

- `submit_orders()`
  - Same behavior of `ImplementedPortfolio` but using `OmsDb`

## OmsDb

- `submitted_orders` table
  - Contain the submitted orders
- `processed_orders` table
- `current_position` table

## OrderProcessor

- Monitor `OmsDb.submitted_orders`
- Update `OmsDb.processed_orders`
- Update `OmsDb.current_position` using `Fill`
