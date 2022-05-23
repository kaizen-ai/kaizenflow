"""
Import as:

import dataflow.system.system_runner as dtfsysyrun
"""

import abc
import logging
from typing import Any, Coroutine, Tuple

import pandas as pd

import core.config as cconfig
import dataflow.core as dtfcore
import dataflow.system as dtfsys
import helpers.hasyncio as hasynci
import helpers.hsql as hsql
import market_data as mdata
import oms as oms

_LOG = logging.getLogger(__name__)


# TODO(gp): @all -> system.py

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

# #############################################################################
# System
# #############################################################################


class System(abc.ABC):
    """
    The simplest possible System, i.e. an empty one.
    """


# #############################################################################
# ForecastSystem
# #############################################################################

# TODO(gp): Consider if we should make all methods private, but
#  get_system_config_template() and get_dag_runner()
class ForecastSystem(System):
    """
    The simplest DataFlow-based system comprised of a:

    - `MarketData` that can be:
        - historical (for backtesting)
        - replayed-time (for simulating real time)
        - real-time (for production)
    - `Dag`
    This system allows making forecasts given data.
    The forecasts can then be processed in terms of a PnL through a notebook or
    other pipelines.
    """

    @abc.abstractmethod
    def get_system_config_template(
        self,
    ) -> cconfig.Config:
        """
        Create a Dataflow DAG config with the basic information about this
        method.

        This is the analogue to `get_template_config()` of a
        `DagBuilder`.
        """
        ...

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
    Create an end-to-end DataFlow-based system composed of:

    - `MarketData`
    - `Dag`
    - `DagRunner`
    - `Portfolio`
    """

    def __init__(self, event_loop):
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
    Create an end-to-end DataFlow-based system composed of:

    - `MarketData`
    - `Dag`
    - `DagRunner`
    - `Portfolio`

    where time advances clock by clock.
    """

    # This method should be called only once and the result saved in the config.
    @abc.abstractmethod
    def get_market_data(self, system_config: cconfig.Config) -> mdata.MarketData:
        ...

    # This method should be called only once and the result saved in the config.
    @abc.abstractmethod
    def get_portfolio(
        self,
        system_config: cconfig.Config,
    ) -> oms.Portfolio:
        ...

    @abc.abstractmethod
    def get_dag(
        self,
        system_config: cconfig.Config,
    ) -> Tuple[cconfig.Config, dtfcore.DAG]:
        """
        We need to pass `Portfolio` because ProcessForecast node requires it.
        """
        ...

    # TODO(gp): This could be `get_DagRunner_example()`.
    # TODO(gp): This code should go in a descendant class since it's about RealTime
    #  behavior.
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
    Create an end-to-end DataFlow-based system composed of:

    - `MarketData`
    - `Dag`
    - `DagRunner`
    - `DataBasePortfolio` (which includes a DatabaseBroker)
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

    # TODO(gp): Part of this should become a `get_OrderProcessor_example()`.
    def get_order_processor(
        self,
        portfolio: oms.Portfolio,
        *,
        timeout_in_secs: int = 60 * (5 + 15),
    ) -> oms.OrderProcessor:
        db_connection = self._db_connection
        get_wall_clock_time = portfolio._get_wall_clock_time
        order_processor_poll_kwargs = hasynci.get_poll_kwargs(get_wall_clock_time)
        # order_processor_poll_kwargs["sleep_in_secs"] = 1
        # Since orders should come every 5 mins we give it a buffer of 15 extra
        # mins.
        order_processor_poll_kwargs["timeout_in_secs"] = timeout_in_secs
        delay_to_accept_in_secs = 3
        delay_to_fill_in_secs = 10
        broker = portfolio.broker
        order_processor = oms.OrderProcessor(
            db_connection,
            delay_to_accept_in_secs,
            delay_to_fill_in_secs,
            broker,
            poll_kwargs=order_processor_poll_kwargs,
        )
        return order_processor

    def get_order_processor_coroutine(
        self,
        portfolio: oms.Portfolio,
        real_time_loop_time_out_in_secs: int,
    ) -> Coroutine:
        # Build OrderProcessor.
        order_processor = self.get_order_processor(portfolio)
        get_wall_clock_time = portfolio.broker.market_data.get_wall_clock_time
        initial_timestamp = get_wall_clock_time()
        offset = pd.Timedelta(real_time_loop_time_out_in_secs, unit="seconds")
        termination_condition = initial_timestamp + offset
        order_processor_coroutine = order_processor.run_loop(
            termination_condition
        )
        return order_processor_coroutine
