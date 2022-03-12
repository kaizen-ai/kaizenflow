"""
Import as:

import dataflow.system.system_runner as dtfsysyrun
"""

import abc
import asyncio
import logging
from typing import Any, Coroutine, Optional, Tuple

import pandas as pd

import core.config as cconfig
import dataflow.core.dag_builder as dtfcodabui
import dataflow.system as dtfsys
import helpers.hasyncio as hasynci
import helpers.hdatetime as hdateti
import helpers.hsql as hsql
import market_data as mdata
import oms as oms

_LOG = logging.getLogger(__name__)


# #############################################################################
# SystemRunner
# #############################################################################


# TODO(gp): Consider adding a `SystemRunner` that has the absolute minimum
#  common behavior.
#
# class SystemRunner(abc.ABC):
#     """
#     Create the simplest possible end-to-end DataFlow-based system comprised
#     of a `MarketData` and a `Dag`.
#     """
#
#     @abc.abstractmethod
#     def get_market_data(
#             self, event_loop: asyncio.AbstractEventLoop
#     ) -> mdata.MarketData:
#         ...
#
#     @abc.abstractmethod
#     def get_dag(
#             self, portfolio: oms.AbstractPortfolio
#     ) -> Tuple[cconfig.Config, dtfcodabui.DagBuilder]:
#         ...
#
#
# class ResearchSystemRunner(SystemRunner):
#     """
#     Create an end-to-end DataFlow-based system that can run a `Dag` in
#     research mode, i.e., running a `Dag` in batch mode and generating the
#     research pnl.
#     """


# TODO(gp): This is really a -> RealTimeSystemRunner
class SystemRunner(abc.ABC):
    """
    Create an end-to-end DataFlow-based system composed of:

    - `MarketData`
    - `Portfolio`
    - `Dag`
    - `DagRunner`
    """

    @abc.abstractmethod
    def get_market_data(
        self, event_loop: asyncio.AbstractEventLoop
    ) -> mdata.MarketData:
        ...

    @abc.abstractmethod
    def get_portfolio(
        self,
        event_loop: asyncio.AbstractEventLoop,
        market_data: mdata.MarketData,
    ) -> oms.AbstractPortfolio:
        ...

    @abc.abstractmethod
    def get_dag(
        self, portfolio: oms.AbstractPortfolio
    ) -> Tuple[cconfig.Config, dtfcodabui.DagBuilder]:
        ...

    # TODO(gp): This could be `get_DagRunner_example()`.
    def get_dag_runner(
        self,
        dag_builder: dtfcodabui.DagBuilder,
        config: cconfig.Config,
        get_wall_clock_time: hdateti.GetWallClockTime,
        *,
        sleep_interval_in_secs: int = 60 * 5,
        real_time_loop_time_out_in_secs: Optional[int] = None,
    ):
        _ = self
        # Set up the event loop.
        execute_rt_loop_kwargs = {
            "get_wall_clock_time": get_wall_clock_time,
            "sleep_interval_in_secs": sleep_interval_in_secs,
            "time_out_in_secs": real_time_loop_time_out_in_secs,
        }
        dag_runner_kwargs = {
            "config": config,
            "dag_builder": dag_builder,
            "fit_state": None,
            "execute_rt_loop_kwargs": execute_rt_loop_kwargs,
            "dst_dir": None,
        }
        dag_runner = dtfsys.RealTimeDagRunner(**dag_runner_kwargs)
        return dag_runner


# #############################################################################
# SystemWithOmsRunner
# #############################################################################


class SystemWithSimulatedOmsRunner(SystemRunner, abc.ABC):
    """
    A system with a simulated OMS has always:

    - a `DataFramePortfolio` or a `MockedPortfolio`
    - an `OrderProcessor`
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
        portfolio: oms.AbstractPortfolio,
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
        portfolio: oms.AbstractPortfolio,
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