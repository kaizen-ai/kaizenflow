"""
Import as:

import dataflow.system.system as dtfsyssyst
"""

import abc
import logging
import os
from typing import Any, Callable, Coroutine

import core.config as cconfig
import dataflow.core as dtfcore
import dataflow.system.real_time_dag_runner as dtfsrtdaru
import helpers.hdbg as hdbg
import helpers.hintrospection as hintros
import helpers.hio as hio
import helpers.hprint as hprint
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
#     system = XYZ_ForecastSystem()
#     # Get the template config.
#     system_config = system.get_system_config_template()
#     # Apply all the changes to the `system_config` to customize the config.
#     system.config[...] = ...
#     ...
#     # Once the system config is complete, build the system.
#     dag_runner = system.dag_runner
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
#
# * Invariants:
# - objects have the `_object` suffix
# - the parameters used to build objects have suffix `_config` and should be
#   `Config`

# * Fields:
#   - dag_config
#     - """information to build the DAG"""
#     - Invariant: one key per DAG node
#     - It is created through `dag_builder.get_config_template()` and updated
#   - dag_property_config
#     - """information about methods to be called on the DAG"""
#     - debug_mode_config
#     - save_node_io
#     - profile_execution
#     - dst_dir
#     - force_free_nodes
#
#   - dag_builder_object
#   - dag_builder_config
#     - """information about methods to be called on the DagBuilder"""
#     - fast_prod_setup
#
#   - market_data_object
#   - market_data_config
#     - asset_ids
#     - replayed_delay_in_mins_or_timestamp
#
#   - portfolio_object
#     - ...
#     ...
#
#   - forecast_node
#     - ...
#
#   - dag_runner_object
#     - rt_timeout_in_secs_or_time
#
#   - backtest_config
#     - """information about back testing"""
#     - universe_str
#     - trading_period_str
#     - time_interval_str
#
#   - cf_config

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


# Scattered thoughts:
# Why can't DagBuilder only appear inside of `_get_dag()`?
# - Can we get rid of system_config["dag_builder_object"] and its config?
#   - Claim: we need info from the DagBuilder to tell MarketData how much data to load
# => if market data needs to know about the dag builder, then either we should pass
#    one object to the other (e.g., method in DagBuilder to add a node with market data)
#    or DagBuilder should be a core concept in System
# Maybe the key objects for a system are:
#  - market data
#  - dag builder
#     - dag builder should support methods for adding a market data
#     - dag builder should also have a parameter for the type of data source node
#  - dag runner


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
        self._config["system_class"] = self.__class__.__name__
        _LOG.debug("system_config=\n%s", self._config)
        # Default log dir.
        self._config["system_log_dir"] = "./system_log_dir"

    # TODO(gp): Improve str if needed.
    def __str__(self) -> str:
        txt = []
        txt.append("# %s" % hprint.to_object_str(self))
        txt.append(hprint.indent(str(self._config)))
        txt = "\n".join(txt)
        return txt

    @property
    def config(self) -> cconfig.Config:
        return self._config

    def set_config(self, config: cconfig.Config) -> None:
        """
        Set the config for a System.

        This is used in the tile backtesting flow to create multiple
        configs and then inject one at a time into a `System` in order
        to simulate the `System` for a specific tile.
        """
        self._config = config

    def is_fully_built(self) -> bool:
        key = "dag_runner_object"
        return key in self.config

    @property
    def dag_runner(
        self,
    ) -> dtfcore.DagRunner:
        key = "dag_runner_object"
        if key in self.config:
            _LOG.debug("Using cached object for '%s'", key)
            dag_runner = self.config[key]
            return dag_runner
        _LOG.info(
            "\n"
            + hprint.frame("# Before building dag_runner, config=")
            + "\n"
            + str(self.config)
            + "\n"
            + hprint.frame("End config before dag_runner")
        )
        #
        log_dir = self.config["system_log_dir"]
        hio.create_dir(log_dir, incremental=False)
        #
        tag = "system_config.input"
        self.config.save_to_file(log_dir, tag)
        #
        dag_runner: dtfcore.DagRunner = self._get_cached_value(
            key, self._get_dag_runner
        )
        # After everything is built, mark the config as read-only to avoid
        # further modifications.
        # TODO(gp): Each builder should mark as read-only the piece of the
        #  config that was consumed. For now dag_runner only marks the DAG
        #  config as read-only. OrderProcessor is built after this.
        self._config["dag_config"].mark_read_only()
        #
        _LOG.info(
            "\n"
            + hprint.frame("# After building dag_runner, config=")
            + "\n"
            + str(self.config)
            + "\n"
            + hprint.frame("End config after dag_runner")
        )
        #
        tag = "system_config.output"
        self.config.save_to_file(log_dir, tag)
        return dag_runner

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

    # TODO(gp): Now a DagRunner runs a System which is a little weird, but maybe ok.
    @abc.abstractmethod
    def _get_dag_runner(
        self,
    ) -> dtfcore.DagRunner:
        """
        Create a DAG runner from a fully specified system config.
        """
        ...

    # Caching invariants:
    # - Objects (e.g., DAG, Portfolio) are built as on the first call and cached
    # - To access the objects (e.g., for checking the output of a test) one uses the
    #   public properties

    # TODO(gp): Pass also the expected type so we can check that each function
    #  returns what's expected.
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
            # Build the object.
            hdbg.dassert_not_in(key, self.config)
            self.config[key] = obj
            if False:
                # Add the object representation after it's built.
                key_tmp = ("object.str", key)
                hdbg.dassert_not_in(key_tmp, self.config)
                # Use the unambiguous object representation `__repr__()`.
                self.config[key_tmp] = repr(obj)
            # Add information about who created that object.
            key_tmp = ("object.builder_function", key)
            hdbg.dassert_not_in(key_tmp, self.config)
            self.config[key_tmp] = hintros.get_name_from_function(builder_func)
        _LOG.debug("Object for %s=\n%s", key, obj)
        return obj


# #############################################################################
# ForecastSystem
# #############################################################################


class ForecastSystem(System, abc.ABC):
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


# #############################################################################
# NonTime_ForecastSystem
# #############################################################################


class NonTime_ForecastSystem(ForecastSystem, abc.ABC):
    pass


# #############################################################################
# _Time_ForecastSystem_Mixin
# #############################################################################


class _Time_ForecastSystem_Mixin(abc.ABC):
    """
    Class adding a time semantic, i.e., an event loop that is not `None`.

    In this set-up, since we await on the `MarketData` to be ready, we need to use:
    - a real-time `MarketData`
    - a `RealTimeDagRunner`
    """

    @abc.abstractmethod
    def _get_market_data(
        self,
    ) -> mdata.RealTimeMarketData:
        ...

    @abc.abstractmethod
    def _get_dag_runner(
        self,
    ) -> dtfsrtdaru.RealTimeDagRunner:
        ...


# #############################################################################
# _ForecastSystem_with_Portfolio
# #############################################################################


class _ForecastSystem_with_Portfolio(ForecastSystem, abc.ABC):
    """
    Create a System composed of:

    - a `MarketData`
      - Historical or replayed
    - a Forecast DAG
      - The Forecast DAG contains a `ProcessForecastsNode` that creates orders from
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


class Time_ForecastSystem(_Time_ForecastSystem_Mixin, ForecastSystem, abc.ABC):
    """
    Like `ForecastSystem` but with a time semantic.
    """

    ...


# #############################################################################
# ForecastSystem_with_DataFramePortfolio
# #############################################################################

# Not all combinations of objects are possible:
# - Time_ForecastSystem, NonTime_ForecastSystem
# - MarketData
# - DataFramePortfolio, DatabasePortfolio
# - DataFrameBroker, DatabaseBroker

#  Time vs Non-time system
#
# - A `ForecastSystem` can have time or not
#
# - Systems without time (e.g., `NonTime_ForecastSystem`) compute data in one-shot
#   for all the history, i.e., compute data in a vectorized way
# - Systems with time (e.g., `Time_ForecastSystem`) compute data as time advances
#   (i.e., clock-by-clock, using asyncio)
#
# - A `NonTime_ForecastSystem` computes the forecasts in one shot
#   - It is possible in a set-up to scan the forecasts clock-by-clock and feed them
#     to `Portfolio`
# - A `Time_ForecastSystem` computes data clock-by-clock and feeds the data in the
#   same pattern to `Portfolio`

# # Portfolio
#
# A `Portfolio` always needs to be fed data in a timed fashion (i.e., clock-by-clock)
#
# A `DataFramePortfolio`
# - requires a `DataFrameBroker`
# - can't work with an `OrderProcessor`
# - only works with time
#
# A `DatabasePortfolio`
# - requires a `DatabaseBroker` and an `OrderProcessor`
# - only works with time
#
# The only difference between a System with `DataFramePortfolio` and one with
# `DatabasePortfolio` is the fact that the orders are simulated in terms of their
# timing (e.g., fills)

# Possible set-ups
# - run without Time (forecast DAG is vectorized), save DAG output, and compute
#   PnL with `ForecastEvaluatorFromPrices`
# - run without Time (forecast DAG is vectorized), save DAG output, and apply
#   outputs to a Portfolio using a timed loop
# - run with Time and `DataFramePortfolio` + `DataFrameBroker` (forecast DAG and
#   Portfolio are run clock-by-clock)
# - run with Time with `DatabasePortfolio` + `DatabaseBroker` + `OrderProcessor`
#   (see the trades going through, the fills coming back, all with a certain timing)


# TODO(gp): I don't think this is ever used. Merge it with
#  Time_ForecastSystem_with_DataFramePortfolio
class ForecastSystem_with_DataFramePortfolio(_ForecastSystem_with_Portfolio):
    """
    Same as `_ForecastSystem_with_Portfolio` but with a `DataFramePortfolio`

    - The portfolio is used to store the holdings according to the orders
    - Use a `DataFrameBroker` to fill the orders

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
    - a `DataFrameBroker`
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
    In practice we use a `DataFramePortfolio` and a `DataFrameBroker` only to
    simulate faster by skipping the interaction with the market through the DB
    interfaces of an OMS system.
    """

    def __init__(
        self,
    ) -> None:
        _Time_ForecastSystem_Mixin.__init__(self)
        _ForecastSystem_with_Portfolio.__init__(self)

    @property
    def order_processor(
        self,
    ) -> oms.OrderProcessor:
        """
        OrderProcessor should be built after DAG.
        """
        order_processor: oms.OrderProcessor = self._get_cached_value(
            "order_processor_object", self._get_order_processor
        )
        # TODO(gp): Maybe pass the object type to _get_cached_value() to
        #  centralize the assertion.
        hdbg.dassert_isinstance(order_processor, oms.OrderProcessor)
        return order_processor

    @property
    def order_processor_coroutine(
        self,
    ) -> Coroutine:
        """
        OrderProcessorCoroutine should be built after DAG.
        """
        order_processor_coroutine: Coroutine = self._get_cached_value(
            "order_processor_coroutine", self._get_order_processor_coroutine
        )
        hdbg.dassert_isinstance(order_processor_coroutine, Coroutine)
        return order_processor_coroutine

    @abc.abstractmethod
    def _get_order_processor(self) -> Coroutine:
        """
        Return the OrderProcessor.
        """
        ...

    @abc.abstractmethod
    def _get_order_processor_coroutine(self) -> Coroutine:
        """
        Return the coroutine representing the OrderProcessor.
        """
        ...


# #############################################################################
# Time_ForecastSystem_with_DatabasePortfolio
# #############################################################################


class Time_ForecastSystem_with_DatabasePortfolio(
    _Time_ForecastSystem_Mixin, _ForecastSystem_with_Portfolio
):
    """
    Same as `Time_ForecastSystem_with_DataFramePortfolio` but with Database
    portfolio.

    This configuration corresponds to a production system where we talk
    to a DB to get both current positions updated based on the fills.
    """

    @abc.abstractmethod
    def _get_portfolio(
        self,
    ) -> oms.DatabasePortfolio:
        ...
