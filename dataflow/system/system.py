"""
Import as:

import dataflow.system.system as dtfsyssyst
"""

import abc
import logging
from typing import Any, Callable, Coroutine

import core.config as cconfig
import dataflow.core as dtfcore
import dataflow.system as dtfsys
import helpers.hsql as hsql
import market_data as mdata
import oms as oms

_LOG = logging.getLogger(__name__)


# #############################################################################
# System
# #############################################################################

# The goal of a `System` class is to:
# - create a system config describing the entire system, including the DAG
#   config
# - expose methods to build the various needed objects, e.g.,
#   - `DagRunner`
#   - `Portfolio`

# A `System` is the analogue of `DagBuilder` but for a system
# - They both have functions to:
#   - create configs (e.g., `get_template_config()` vs
#     `get_system_config_template()`)
#   - create objects (e.g., `get_dag()` vs `get_dag_runner()`)

# The lifecycle of `System` is like:
#     ```
#     # Instantiate a System.
#     system = E8_ForecastSystem()
#     # Get the template config.
#     system_config = system.get_system_config_template()
#     # Apply all the changes to the `system_config` to customize the config.
#     system.config[...] = ...
#     ...
#     # Once the system config is complete, build the system.
#     dag_runner = system.get_dag_runner()
#     # Run the system.
#     dag_runner.set_fit_intervals(...)
#     dag_runner.fit()
#     ```

# Invariants:
# - `system_config` should contain all the information needed to build and run
#   a `System`, like a `dag_config` contains all the information to build a `DAG`
# - It's ok to save in the config temporary information (e.g., `dag_builder`)
# - We could add `abc.ABC` to the abstract class definition or not, instead of
#   relying on inspecting the methods
#   - No decision yet
# - We can use stricter or looser types in the interface (e.g.,
#   `DatabasePortfolio` vs `Portfolio`)
#   - We prefer the stricter types unless linter gets upset

# A SystemConfig has multiple parts, conceptually one for each piece of the system
#   - DAG
#     - dag_config
#       - """information to build the DAG"""
#       - Invariant: one key per DAG node
#   - DAG_meta
#     - """information about methods to be called on the DAG"""
#     - debug_mode
#       - save_node_io
#       - profile_execution
#       - dst_dir
#     - force_free_nodes
#   - TODO(gp): -> dag_builder_object
#   - dag_builder_object
#   - dag_builder_meta
#     - """information about methods to be called on the DagBuilder"""
#     - fast_prod_setup
#   - TODO(gp): -> market_data_object
#   - market_data_object
#   - market_data_config
#     - asset_ids
#     - initial_replayed_delay
#   - portfolio
#     - ...
#     ...
#   - forecast_node
#     - ...
#   - dag_runner
#     - TODO(gp): -> dag_runner_object
#     - real_time_loop_time_out_in_secs
#   - backtest
#     - """information about back testing"""
#     - universe_str
#     - trading_period_str
#     - time_interval_str

# Inheritance style conventions:
# - Each class derives only from interfaces (i.e., classes that have all methods
#   abstract)
# - We don't want to use inheritance to share code but we want to explicitly call
#   shared code
#   - Related classes need to specify each abstract method of the base class calling
#     implementations of the methods explicitly, passing `self`, if needed
#   - This makes the code easier to "resolve" for humans since everything is explicit
#     and doesn't rely on the class hierarchy
# - If only one object needs a function we are ok with inlining
#   - As soon as multiple objects need the same code we don't copy-paste or use
#     inheritance, but refactor the common code into a function and call it from
#     everywhere


class System(abc.ABC):
    """
    The simplest possible DataFlow-based system, including:

    - system config
    - `DagRunner`

    A `System` is a DAG that contains various components, such as:
    - a `MarketData`
    - a Forecast pipeline (which is often improperly referred to as DAG)
    - a `Portfolio`
    - a `Broker`
    ...
    """

    def __init__(self) -> None:
        self._config = self._get_system_config_template()
        _LOG.debug("system_config=\n%s", self._config)

    @property
    def config(self) -> cconfig.Config:
        return self._config

    # TODO(gp): -> _get_dag_runner and use a property dag_runner
    # TODO(gp): Now a DagRunner runs a System which is a little weird, but maybe ok.
    @abc.abstractmethod
    def get_dag_runner(
        self,
    ) -> dtfcore.DagRunner:
        """
        Create a DAG runner from a fully specified system config.
        """
        ...

    @abc.abstractmethod
    def _get_system_config_template(
        self,
    ) -> cconfig.Config:
        """
        Create a System config with the basic information (e.g., DAG config and
        builder).

        This is the analogue of `DagBuilder.get_template_config()`.
        """
        ...

    # Caching invariants:
    # - Objects (e.g., DAG, Portfolio) are built as on the first call and cached
    # - To access the objects (e.g., for checking the output of a test) one uses the
    #   public properties

    def _get_cached_value(
        self,
        key: str,
        builder_func: Callable,
    ) -> Any:
        """
        Retrieve the object corresponding to `key` if already built, or call
        `builder_func` to build and cache it.
        """
        _LOG.debug("")
        if key in self.config:
            _LOG.debug("Using cached object for '%s'", key)
            obj = self.config[key]
        else:
            _LOG.debug(
                "No cached object for '%s': calling %s()",
                key,
                builder_func.__name__,
            )
            obj = builder_func()
            # Build.
            self.config[key] = obj
        _LOG.debug("Object for %s=\n%s", key, obj)
        return obj


# #############################################################################
# ForecastSystem
# #############################################################################


class ForecastSystem(System):
    """
    A System producing forecasts and comprised of:

    - a `MarketData` that can be:
        - historical (for back testing)
        - replayed-time (for simulating real time)
        - real-time (for production)
    - a Forecast DAG

    The forecasts can then be processed in terms of a PnL through a notebook or
    other data pipelines.
    """

    @property
    def market_data(
        self,
    ) -> mdata.MarketData:
        market_data: mdata.MarketData = self._get_cached_value(
            "market_object", self._get_market_data
        )
        return market_data

    @property
    def forecast_dag(
        self,
    ) -> dtfcore.DAG:
        forecast_dag: dtfcore.DAG = self._get_cached_value(
            "forecast_dag_object", self._get_forecast_dag
        )
        return forecast_dag

    @property
    def dag(
        self,
    ) -> dtfcore.DAG:
        dag: dtfcore.DAG = self._get_cached_value("dag_object", self._get_dag)
        return dag

    @abc.abstractmethod
    def _get_market_data(
        self,
    ) -> mdata.MarketData:
        ...

    @abc.abstractmethod
    def _get_dag(
        self,
    ) -> dtfcore.DAG:
        """
        Given a completely filled `system_config` build and return the DAG.
        """
        ...

    def _get_forecast_dag(self) -> dtfcore.DAG:
        # TODO(gp): -> dag_builder_object
        dag_builder = self.config["dag_builder"]
        config = self.config["DAG"]
        _LOG.debug("config=\n%s", config)
        dag = dag_builder.get_dag(config)
        return dag


# #############################################################################
# _Time_ForecastSystem_Mixin
# #############################################################################


class _Time_ForecastSystem_Mixin:
    """
    Class adding a time semantic, i.e., an event loop that is not `None`.

    In this set-up, since we await on the `MarketData` to be ready, we need to use:
    - a real-time `MarketData`
    - a `RealTimeDagRunner`
    """

    @abc.abstractmethod
    def get_dag_runner(
        self,
    ) -> dtfsys.RealTimeDagRunner:
        ...

    @abc.abstractmethod
    def _get_market_data(
        self,
    ) -> mdata.RealTimeMarketData:
        ...


# #############################################################################
# _ForecastSystem_with_Portfolio
# #############################################################################


class _ForecastSystem_with_Portfolio(ForecastSystem):
    """
    Create a System composed of:

    - a `MarketData`
      - Historical or replayed
    - a Forecast DAG
      - The Forecast DAG contains a `ProcessForecasts` that creates orders from
        forecasts
    - a `Portfolio`
      - The portfolio is used to store the holdings according to the orders

    This System is used to simulate a forecast system in terms of orders and
    holdings in a portfolio.
    """

    @property
    def portfolio(
        self,
    ) -> oms.Portfolio:
        portfolio: oms.Portfolio = self._get_cached_value(
            "portfolio_object", self._get_portfolio
        )
        return portfolio

    @abc.abstractmethod
    def _get_portfolio(
        self,
    ) -> oms.Portfolio:
        ...


# #############################################################################
# Time_ForecastSystem
# #############################################################################


class Time_ForecastSystem(_Time_ForecastSystem_Mixin, ForecastSystem):
    """
    Like `ForecastSystem` but with a time semantic.
    """

    ...


# #############################################################################
# ForecastSystem_with_DataFramePortfolio
# #############################################################################

# Not all combinations of objects are possible

# Systems without time compute data in one-shot for all the history
# Systems with time compute data as time advances (i.e., clock-by-clock)

# A `ForecastSystem` can have time or not
# A `ForecastSystem_with_DataFramePortfolio` can have time or not

# A `Portfolio` always needs to be fed data in a timed fashion (i.e., clock-by-clock)
# A `ForecastSystem` without time
# - computes the forecasts in one shot
# - scans the forecasts clock-by-clock feeding them to `Portfolio`
# A `Time_ForecastSystem` computes data clock-by-clock and feeds the data in the same
# pattern to `Portfolio`

# A `DataFramePortfolio`
# - requires a `SimulatedBroker`
# - can't work with an `OrderProcessor`
# - can work both with time and without
#
# A `DatabasePortfolio`
# - requires a `DatabaseBroker` and an `OrderProcessor`
# - only works with time

# Testing
# - run without Time with DataFramePortfolio (forecast dag is vectorized, and then
#   df portfolio loops)
# - run with Time (and asyncio) with DataFramePortfolio (forecast dag is
#   clock-by-clock and df portfolio too)
# - run with Time with DatabasePortfolio + DatabaseBroker + OrderProcessor
#   (see the trades going through, the fills coming back, all with a certain timing)


class ForecastSystem_with_DataFramePortfolio(_ForecastSystem_with_Portfolio):
    """
    Same as `_ForecastSystem_with_Portfolio` but with a `DataFramePortfolio`

      - The portfolio is used to store the holdings according to the orders
      - Use a `SimulatedBroker` to fill the orders

    This System is used to simulate a forecast system in terms of orders and
    holdings in a portfolio.
    """

    @abc.abstractmethod
    def _get_portfolio(
        self,
    ) -> oms.DataFramePortfolio:
        ...


# #############################################################################
# Time_ForecastSystem_with_DataFramePortfolio
# #############################################################################


class Time_ForecastSystem_with_DataFramePortfolio(
    _Time_ForecastSystem_Mixin, ForecastSystem_with_DataFramePortfolio
):
    """
    Same as `ForecastSystem_with_DataFramePortfolio`, but with an event loop
    that is not `None`.

    This `System` includes a:
    - a real-time `MarketData`
    - a Forecast DAG
    - a `DataFramePortfolio`
    - a `SimulatedBroker`
    - a `RealTimeDagRunner`
    """


# #############################################################################
# Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor
# #############################################################################


class Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor(
    _Time_ForecastSystem_Mixin, _ForecastSystem_with_Portfolio
):
    """
    Same as `Time_ForecastSystem_with_DatabasePortfolioAndBroker` but with an
    `OrderProcessor` to mock the execution of orders on the market and to
    update the `DatabasePortfolio`. In practice this allows to simulate an IG
    system without interacting with the real OMS / market.

    Create a System composed of:

    - a real-time `MarketData`
    - a Forecast DAG
    - a `DatabasePortfolio`
    - a `DatabaseBroker` (included in the `DatabasePortfolio`)
    - an `OrderProcessor`

    The System has an event loop that is not `None`.

    A system with a `DatabaseBroker` and `OrderProcessor` cannot have a
    `DataFramePortfolio` because a df cannot be updated by an external coroutine such
    as the `OrderProcessor`.
    In practice we use a `DataFramePortfolio` and a `SimulatedBroker` only to
    simulate faster by skipping the interaction with the market through the DB
    interfaces of an OMS system.
    """

    # TODO(gp): Consider moving the db_connection to system_config too.
    def __init__(
        self,
        db_connection: hsql.DbConnection,
    ):
        _Time_ForecastSystem_Mixin.__init__(self)
        _ForecastSystem_with_Portfolio.__init__(self)
        self._db_connection = db_connection
        oms.create_oms_tables(self._db_connection, incremental=False)

    def get_order_processor_coroutine(
        self,
        portfolio: oms.Portfolio,
        real_time_loop_time_out_in_secs: int,
    ) -> Coroutine:
        db_connection = self._db_connection
        timeout_in_secs = 60 * (5 + 15)
        order_processor = oms.get_order_processor_example1(
            db_connection, portfolio, timeout_in_secs=timeout_in_secs
        )
        #
        order_processor_coroutine = oms.get_order_processor_coroutine_example1(
            order_processor, portfolio, real_time_loop_time_out_in_secs
        )
        return order_processor_coroutine
