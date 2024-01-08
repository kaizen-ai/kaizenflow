<!-- toc -->

- [Replayed CCXT Exchange](#replayed-ccxt-exchange)
  * [Reliable Unit Testing:](#reliable-unit-testing)
  * [Replaying the Experiment:](#replaying-the-experiment)
  * [Integration with CCXT Broker Script:](#integration-with-ccxt-broker-script)
  * [Data from Log Files:](#data-from-log-files)
  * [Replaying the Simulation:](#replaying-the-simulation)

<!-- tocstop -->

# 

## ReplayedExchange
Goal:
Replay the model (interacting with the exchange) without the exchange
Write unit tests to capture some behavior we have seen in production
Reconcile the model at order level
This version that reads back a log directory and behaves as-if
child order -> fill
The sequence of orders is fixed
We do not need the bid / ask market data inside the CcxtBroker to generate the limit order
Conceptually simple, but the “timing” needs to be perfect!
We allow a little wiggle room in timing
## FakeExchange: synthetic exchange
Goal
Simulate the behavior of the exchange given the order
Perform what-if scenarios (what if we change the passivity factor, what if we submit the orders 2 secs faster)
Pass bid / ask market data
The FakeExchange should match the ReplayedExchange when there is no perturbation
TODO(Paul): come up with the params we want to use

# Replayed CCXT Exchange

The `ReplayedCcxtExchange` class is designed to facilitate testing of a system
using a CCXT broker by replaying logs of events captured from an actual
experiment.

In other words, one can run a system (e.g., a full-system run or a broker only
experiment) using a real `CcxtExchange` which logs all the events (e.g., child
orders and fills). Then the `CcxtExchange` can be replaced with an
`ReplayedCcxtExchange` that loads and replays the events back.

The two main advantage of `ReplayedCcxtExchange` are:
1) Reliable unit testing
2) Replaying an experiment

## Reliable Unit Testing
- `ReplayedCcxtExchange` enables more reliable unit testing of the CCXT broker's
  behavior
- In the absence of such a class, unit testing an entire script run would be
  challenging due to the need for extensive mocking and the use of dummy data
- With this class in place, developers can focus on testing specific actions and
  behaviors of the broker with real log data, making the unit testing process
  more robust and accurate.

## Replaying the Experiment
- Another advantage of the `ReplayedCcxtExchange` is the ability to replay the
  experiment. In this context, "replay" means the ability to recreate and
  simulate the exact sequence of events that occurred during a specific
  actual run
- Essentially, the `ReplayedCcxtExchange` allows developers to take the recorded
  chronological events and execute them as if they were happening in real-time.
- This feature is valuable for understanding and analyzing how the rest of the
  system interacts with the Broker which contains the `CcxtExchange` software
  behaves under various scenarios, as well as for diagnosing issues, fine-tuning
  algorithms, and improving the overall performance of the system.

## Integration with CCXT Broker Script

- `ReplayedCcxtExchange` class is currently integrated with `run_ccxt_broker.py`
  script
- The CCXT broker is responsible for executing orders and managing the
  trading process by interacting with the real CCXT exchange
- For testing, instead of interacting with the real CCXT exchange,
  `ReplayedCcxtExchange` class is injected into the CCXT broker which with the
  help of log files reproduce the behavior of real CCXT exchange. Refer to
  [link](https://github.com/cryptokaizen/cmamp/blob/master/docs/trade_execution/ck.run_broker_only_experiment.how_to_guide.md#example-command)
  for steps to run experiment using `ReplayedCcxtExchange`.
  - TODO(Sameep): I think we have a `run_ccxt_broker.py`

## Data from Log Files

- `ReplayedCcxtExchange` uses data from log files, which contain records of past
  actions such as orders placed, fills of those orders, and actual trades that
  occurred on the exchange
- Following are the different log files used by the `ReplayedCcxtExchange`,
  which are described in https://github.com/cryptokaizen/cmamp/blob/master/docs/trade_execution/ck.ccxt_broker_logs_schema.reference.md
  - Balances
  - Bid ask
  - CCXT child order responses
  - CCXT fills
  - CCXT trades
  - Exchange markets
  - Leverage info
  - Oms child orders
  - Oms fills
  - Oms parent_orders
  - Positions
  - Reduce only CCXT child order responses
  - Reduce only oms child orders

- All logs are recorded at each step to capture crucial information and events.
- The 'balance' and 'position' logs are different in that they are only recorded
  at the very beginning of the CCXT broker process, specifically once during the
  initial setup. Their primary purpose at this point is to capture the initial
  value of the USDT balance in the trading account.
- After this initial logging, any subsequent changes or updates to these 'balance'
  and 'position' values are not explicitly documented in the log files. Instead,
  the responsibility for tracking, managing, and updating these values is internal
  to the `ReplayedCcxtExchange`. This approach is adopted to maintain the clarity
  and brevity of the log files, ensuring that they focus on recording significant
  and noteworthy events, rather than inundating them with numerous entries for
  minor and frequent changes to positions and balances.

## Replaying the Simulation

- With the data from these log files, `ReplayedCcxtBroker` can recreate and
  replay the entire trading simulation in the same sequential order as it
  originally occurred. This allows for detailed and controlled testing of the
  broker's behavior under various conditions.
