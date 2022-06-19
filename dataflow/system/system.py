"""
Import as:

import dataflow.system.system as dtfsyssyst
"""

import abc
import asyncio
import logging
from typing import Any, Coroutine, Optional, Tuple

import core.config as cconfig
import dataflow.core as dtfcore
import dataflow.system as dtfsys
import helpers.hsql as hsql
import market_data as mdata
import oms as oms

_LOG = logging.getLogger(__name__)


# A `System` class allows to build a complete system, which can include:
# - a DAG
# - a `MarketData`
# - a `Portfolio`
# - a `Broker`
# - ...

# The goal of a `System` class is to:
# - create a system config describing the entire system, including the DAG
#   config
# - expose builder methods to build the various needed objects, e.g.,
#   - `DagRunner`
#   - `Portfolio`

# A `System` is the analogue of `DagBuilder`
# - They both have functions to:
#   - create configs (e.g., `get_template_config()`)
#   - create objects (e.g., `get_dag()` vs `get_dag_runner()`)

# Conceptually the lifecycle of `System` is like:
# ```
# # Instantiate a System.
# system = E8_ForecastSystem()
# # Get the template config.
# system_config = system.get_system_config_template()
# # Apply all the changes to the `system_config` to customize the config.
# ...
# # Build the system.
# dag_runner = system_config["dag_runner"]
# # Run the system.
# dag_runner.set_fit_intervals(...)
# dag_runner.fit()
# ```

# Invariants:
# - `system_config` should contain all the information needed to build and run
#   a `System`, like a `dag_config` contains all the information to build a `DAG`
# - It's ok to save in the config temporary information (e.g., `dag_builder`)
# - Every function of a `System` class should take `self` and `system_config`
# - Since a `System` that has a `Portfolio` must also have a `Broker` inside the
#   `Portfolio`, we don't put information about `Broker` in the name unless the
#   `Broker` has some specific characteristics (e.g., `Simulated`).
#   E.g., we use names like `...WithPortfolio` and not `...WithPortfolioAndBroker`
# - We could add `abc.ABC` to the abstract class definition or not, instead of
#   relying on inspecting the methods
#   - No decision yet
# - We can use stricter or looser types in the interface (e.g.,
#   `DatabasePortfolio` vs `Portfolio`)
#   - We prefer the stricter types unless linter gets upset

# A SystemConfig has multiple parts, conceptually one for each piece of the system
# - DAG
#   - dag_config
#     - """information to build the DAG"""
#     - Invariant: one key per DAG node
# - DAG_meta
#   - """information about methods to be called on the DAG"""
#   - debug_mode
#     - save_node_io
#     - profile_execution
#     - dst_dir
#   - force_free_nodes
# - dag_builder
# - dag_builder_meta
#   - """information about methods to be called on the DagBuilder"""
#   - fast_prod_setup
# - market_data
#   - asset_ids
# - portfolio
#   - ...
#   ...
# - forecast_node
#   - ...
# - dag_runner
# - backtest
#   - """information about back testing"""
#   - universe_str
#   - trading_period_str
#   - time_interval_str

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


# #############################################################################
# System
# #############################################################################


class System(abc.ABC):
    """
    The simplest possible DataFlow-based System, i.e. an empty one.
    """


# #############################################################################
# ForecastSystem
# #############################################################################

# TODO(gp): Consider if we should make all methods private, but
#  get_system_config_template() and get_dag_runner()
class ForecastSystem(System):
    """
    A simple System making forecasts and comprised of a:

    - `DAG`
    - `MarketData` that can be:
        - historical (for back testing)
        - replayed-time (for simulating real time)
        - real-time (for production)

    The forecasts can then be processed in terms of a PnL through a notebook or
    other pipelines.
    """

    @abc.abstractmethod
    def get_system_config_template(
        self,
    ) -> cconfig.Config:
        """
        Create a System config with the basic information (e.g., DAG config and
        builder).

        This is the analogue of `DagBuilder.get_template_config()`.
        """
        ...

    # TODO(gp): This method should be called only once and the result saved in the
    #  config. Try to enforce this invariant, e.g., through caching.
    @abc.abstractmethod
    def get_market_data(
        self,
        system_config: cconfig.Config,
    ) -> mdata.MarketData:
        ...

    @abc.abstractmethod
    def get_dag(
        self,
        system_config: cconfig.Config,
    ) -> Tuple[cconfig.Config, dtfcore.DAG]:
        """
        Given a completely filled `system_config` build and return the DAG.

        We return the Config that generated the DAG only for book-
        keeping purposes (e.g., to write the config on file) in case
        there were some updates to the Config.
        """
        ...

    @abc.abstractmethod
    def get_dag_runner(
        self,
        system_config: cconfig.Config,
    ) -> dtfcore.DagRunner:
        """
        Create a DAG runner from a fully specified system config.
        """
        ...


# class ResearchForecastSystem(ForecastSystem):
#     """
#     Create an end-to-end DataFlow-based system that can run a `Dag` in
#     research mode, i.e., running a `Dag` in batch mode and generating the
#     research pnl.
#     """


# #############################################################################
# ForecastSystemWithPortfolio
# #############################################################################


class ForecastSystemWithPortfolio(ForecastSystem):
    """
    Create a System composed of:

    - `MarketData`
    - `Dag`
    - `Portfolio`
    """

    def __init__(self, event_loop: Optional[asyncio.AbstractEventLoop]):
        self._event_loop = event_loop

    @abc.abstractmethod
    def get_portfolio(
        self,
        system_config: cconfig.Config,
    ) -> oms.Portfolio:
        ...


# #############################################################################
# TimeForecastSystemWithPortfolio
# #############################################################################


class TimeForecastSystemWithPortfolio(ForecastSystemWithPortfolio):
    """
    Create a System composed of:

    - `MarketData`
    - `Dag`
    - `Portfolio`

    Time advances clock by clock.
    """

    @abc.abstractmethod
    def get_dag_runner(
        self,
        system_config: cconfig.Config,
    ) -> dtfsys.RealTimeDagRunner:
        ...


# #############################################################################
# TimeForecastSystemWithDatabasePortfolioAndDatabaseBrokerAndOrderProcessor
# #############################################################################


# TODO(gp): Maybe -> ...DatabasePortfolioAndBrokerAnd... or DatabaseOms?
class TimeForecastSystemWithDatabasePortfolioAndDatabaseBrokerAndOrderProcessor(
    TimeForecastSystemWithPortfolio
):
    """
    Create a System composed of:

    - `MarketData`
    - `Dag`
    - `DataBasePortfolio` (which includes a `DatabaseBroker`)
    - `OrderProcessor` (mimicking market execution)

    Time advances clock by clock.
    """

    def __init__(
        self,
        *args: Any,
        db_connection: hsql.DbConnection,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)
        #
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
