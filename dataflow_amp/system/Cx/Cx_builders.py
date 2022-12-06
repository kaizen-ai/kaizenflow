"""
Import as:

import dataflow_amp.system.Cx.Cx_builders as dtfasccxbu
"""

import datetime
import logging
from typing import Any, Callable, Dict, List, Union

import pandas as pd

import core.config as cconfig
import dataflow.core as dtfcore
import dataflow.system as dtfsys
import helpers.hdbg as hdbg
import helpers.hprint as hprint
import helpers.hs3 as hs3
import helpers.hsql as hsql
import im_v2.ccxt.data.client.ccxt_clients as imvcdccccl
import im_v2.im_lib_tasks as imvimlita
import market_data as mdata
import oms

_LOG = logging.getLogger(__name__)

# TODO(gp): @all -> Cx_system_builders.py


# #############################################################################
# Market data instances.
# #############################################################################


def get_Cx_HistoricalMarketData_example1(
    system: dtfsys.System,
) -> mdata.ImClientMarketData:
    """
    Build a `MarketData` client backed with the data defined by `ImClient`.
    """
    im_client = system.config["market_data_config", "im_client"]
    asset_ids = system.config["market_data_config", "asset_ids"]
    columns = None
    columns_remap = None
    # TODO(gp): Document why this is the wall clock time.
    wall_clock_time = pd.Timestamp("2100-01-01T00:00:00+00:00")
    market_data = mdata.get_HistoricalImClientMarketData_example1(
        im_client,
        asset_ids,
        columns,
        columns_remap,
        wall_clock_time=wall_clock_time,
    )
    return market_data


def get_Cx_RealTimeMarketData_prod_instance1(
    asset_ids: List[int],
) -> mdata.MarketData:
    """
    Build a `MarketData` backed with `RealTimeImClient`.
    """
    _LOG.debug(hprint.to_str("asset_ids"))
    # TODO(Grisha): @Dan pass as much as possible via `system.config`.
    resample_1min = False
    # Get environment variables with login info.
    env_file = imvimlita.get_db_env_path("dev")
    # Get login info.
    connection_params = hsql.get_connection_info_from_env_file(env_file)
    # Login.
    db_connection = hsql.get_connection(*connection_params)
    # Get the real-time `ImClient`.
    table_name = "ccxt_ohlcv_futures"
    im_client = imvcdccccl.CcxtSqlRealTimeImClient(
        resample_1min, db_connection, table_name
    )
    # Get the real-time `MarketData`.
    market_data, _ = mdata.get_RealTimeImClientMarketData_example1(
        im_client, asset_ids
    )
    return market_data


# TODO(Grisha): @Dan Move to `system_builder_utils.py`.
def get_Cx_ReplayedMarketData_from_file(
    system: dtfsys.System, is_prod: bool
) -> mdata.ReplayedMarketData:
    """
    Build a `ReplayedMarketData` backed with data from the specified file.
    """
    file_path = system.config["market_data_config", "file_path"]
    # TODO(Grisha): ideally we want to pass `aws_profile` via config.
    if hs3.is_s3_path(file_path):
        aws_profile = "ck"
        hs3.dassert_is_valid_aws_profile(file_path, aws_profile)
    else:
        aws_profile = None
    hs3.dassert_is_valid_aws_profile(file_path, aws_profile)
    # TODO(Grisha): @Dan pass `column_remap` and column name parameters via `system.config`.
    # TODO(Grisha): @Dan Refactor default column names in system related functions.
    # TODO(Grisha): @Dan Since remapping is different for prod and simulation,
    # their data scheme is different so we need to fix it.
    # Multiple functions that build the system are looking for "start_datetime"
    # and "end_datetime" columns by default.
    if is_prod:
        # Set remapping for database data used in production.
        column_remap = {
            "start_timestamp": "start_datetime",
            "end_timestamp": "end_datetime",
        }
    else:
        # Set remapping for file system data used in simulation.
        column_remap = {"start_ts": "start_datetime", "end_ts": "end_datetime"}
    timestamp_db_column = "end_datetime"
    datetime_columns = ["start_datetime", "end_datetime", "timestamp_db"]
    # Get market data for replaying.
    market_data_df = mdata.load_market_data(
        file_path,
        aws_profile=aws_profile,
        column_remap=column_remap,
        timestamp_db_column=timestamp_db_column,
        datetime_columns=datetime_columns,
    )
    if not is_prod:
        # Asset ids are passed as params in prod, but for simulation we have to
        # fill system config with asset ids from data for `Portfolio`.
        hdbg.dassert_not_in(("market_data_config", "asset_ids"), system.config)
        # TODO(Grisha): @Dan Add a method to `MarketData.get_asset_ids()` that does
        #  `list(df[asset_id_col_name].unique())`.
        system.config["market_data_config", "asset_ids"] = (
            market_data_df["asset_id"].unique().tolist()
        )
    # Initialize market data client.
    event_loop = system.config["event_loop_object"]
    replayed_delay_in_mins_or_timestamp = system.config[
        "market_data_config", "replayed_delay_in_mins_or_timestamp"
    ]
    delay_in_secs = system.config["market_data_config", "delay_in_secs"]
    market_data, _ = mdata.get_ReplayedTimeMarketData_from_df(
        event_loop,
        replayed_delay_in_mins_or_timestamp,
        market_data_df,
        delay_in_secs=delay_in_secs,
    )
    return market_data


# #############################################################################
# Process forecasts configs.
# #############################################################################


def get_ProcessForecastsNode_dict_instance1(
    system: dtfsys.System, order_duration_in_mins: int, is_prod: bool
) -> Dict[str, Any]:
    """
    Build the `ProcessForecastsNode` dictionary for simulation.
    """
    spread_col = None
    style = "cross_sectional"
    # For prod we use smaller GMV so that we can trade at low capacity while
    # for simulation we do not trade with real money.
    if is_prod:
        compute_target_positions_kwargs = {
            "bulk_frac_to_remove": 0.0,
            "target_gmv": 700.0,
        }
        root_log_dir = system.config.get("system_log_dir")
    else:
        compute_target_positions_kwargs = {
            "bulk_frac_to_remove": 0.0,
            "bulk_fill_method": "zero",
            "target_gmv": 1e5,
        }
        # TODO(Grisha): @Dan CmTask2849 "Pass an actual `system_log_dir` for simulation".
        root_log_dir = None
    dag_builder = system.config["dag_builder_object"]
    volatility_col = dag_builder.get_column_name("volatility")
    prediction_col = dag_builder.get_column_name("prediction")
    process_forecasts_node_dict = dtfsys.get_ProcessForecastsNode_dict_example1(
        system.portfolio,
        prediction_col,
        volatility_col,
        spread_col,
        order_duration_in_mins,
        style,
        compute_target_positions_kwargs,
        root_log_dir,
    )
    if is_prod:
        # Set backend suitable for working with Binance.
        process_forecasts_node_dict["process_forecasts_dict"]["optimizer_config"][
            "backend"
        ] = "cc_pomo"
    return process_forecasts_node_dict


# #############################################################################
# DAG instances.
# #############################################################################


def get_Cx_HistoricalDag_example1(system: dtfsys.System) -> dtfcore.DAG:
    """
    Build a DAG with a historical data source for simulation.
    """
    hdbg.dassert_isinstance(system, dtfsys.System)
    # Create HistoricalDataSource.
    stage = "read_data"
    market_data = system.market_data
    # TODO(gp): This in the original code was
    # ts_col_name = "timestamp_db"
    ts_col_name = "end_ts"
    multiindex_output = True
    # col_names_to_remove = ["start_datetime", "timestamp_db"]
    col_names_to_remove = []
    node = dtfsys.HistoricalDataSource(
        stage,
        market_data,
        ts_col_name,
        multiindex_output,
        col_names_to_remove=col_names_to_remove,
    )
    # Build the DAG.
    dag_builder = system.config["dag_builder_object"]
    dag = dag_builder.get_dag(system.config["dag_config"])
    # Add the data source node.
    dag.insert_at_head(node)
    return dag


def get_Cx_RealTimeDag_example1(system: dtfsys.System) -> dtfcore.DAG:
    """
    Build a DAG with a real time data source.
    """
    hdbg.dassert_isinstance(system, dtfsys.System)
    system = dtfsys.apply_history_lookback(system)
    dag = dtfsys.add_real_time_data_source(system)
    return dag


def get_Cx_RealTimeDag_example2(
    system: dtfsys.System, is_prod: bool
) -> dtfcore.DAG:
    """
    Build a DAG with `RealTimeDataSource` and `ForecastProcessorNode`.
    """
    hdbg.dassert_isinstance(system, dtfsys.System)
    system = dtfsys.apply_history_lookback(system)
    dag = dtfsys.add_real_time_data_source(system)
    # Configure a `ProcessForecastNode`.
    order_duration_in_mins = 5
    process_forecasts_node_dict = get_ProcessForecastsNode_dict_instance1(
        system, order_duration_in_mins, is_prod
    )
    system.config["process_forecasts_node_dict"] = cconfig.Config.from_dict(
        process_forecasts_node_dict
    )
    system = dtfsys.apply_ProcessForecastsNode_config_for_crypto(system, is_prod)
    # Append the `ProcessForecastNode`.
    dag = dtfsys.add_ProcessForecastsNode(system, dag)
    return dag


# TODO(gp): Copied from _get_E1_dag_prod... Try to share code.
def _get_Cx_dag_prod_instance1(
    system: dtfsys.System,
    get_process_forecasts_node_dict_func: Callable,
) -> dtfcore.DAG:
    """
    Build the DAG for a C1b production system from a system config.
    """
    hdbg.dassert_isinstance(system, dtfsys.System)
    # Create the pipeline.
    # TODO(gp): Fast prod system must be set before the DAG is built.
    dag_builder = system.config.get_and_mark_as_used("dag_builder_object")
    dag_config = system.config.get_and_mark_as_used("dag_config")
    # The config must be complete and stable here.
    dag = dag_builder.get_dag(dag_config)
    system = dtfsys.apply_dag_property(dag, system)
    #
    system = dtfsys.apply_DagRunner_config_for_crypto(system)
    # Build Portfolio.
    mark_key_as_used = True
    trading_period_str = dag_builder.get_trading_period(
        dag_config, mark_key_as_used
    )
    # TODO(gp): Add a param to get_trading_period to return the int.
    order_duration_in_mins = int(trading_period_str.replace("T", ""))
    system.config[
        "portfolio_config", "order_duration_in_mins"
    ] = order_duration_in_mins
    # Set market data history lookback in days in to config.
    system = dtfsys.apply_history_lookback(system)
    # Build the process forecast dict.
    is_prod = True
    process_forecasts_node_dict = get_process_forecasts_node_dict_func(
        system, order_duration_in_mins, is_prod
    )
    system.config["process_forecasts_node_dict"] = cconfig.Config.from_dict(
        process_forecasts_node_dict
    )
    is_prod = True
    system = dtfsys.apply_ProcessForecastsNode_config_for_crypto(system, is_prod)
    # Assemble.
    market_data = system.market_data
    market_data_history_lookback = system.config.get_and_mark_as_used(
        ("market_data_config", "history_lookback")
    )
    process_forecasts_node_dict = system.config.get_and_mark_as_used(
        "process_forecasts_node_dict"
    ).to_dict()
    ts_col_name = "timestamp_db"
    # TODO(Grisha): should we use `add_real_time_data_source` and
    # `add_ProcessForecastsNode` from `system_builder_utils.py`?
    dag = dtfsys.adapt_dag_to_real_time(
        dag,
        market_data,
        market_data_history_lookback,
        process_forecasts_node_dict,
        ts_col_name,
    )
    _LOG.debug("dag=\n%s", dag)
    return dag


def get_Cx_dag_prod_instance1(system: dtfsys.System) -> dtfcore.DAG:
    """
    Build the DAG for a production system from a system config.
    """
    # TODO(gp): It seems that we inlined the code somewhere so we should factor it
    #  out.
    # get_process_forecasts_node_dict_func = dtfsys.get_process_forecasts_dict_example3
    get_process_forecasts_node_dict_func = get_ProcessForecastsNode_dict_instance1
    dag = _get_Cx_dag_prod_instance1(system, get_process_forecasts_node_dict_func)
    return dag


# #############################################################################
# Portfolio instances.
# #############################################################################


def get_Cx_portfolio_prod_instance1(system: dtfsys.System) -> oms.Portfolio:
    """
    Build Portfolio instance for production.
    """
    run_mode = system.config["run_mode"]
    market_data = system.market_data
    dag_builder = system.config["dag_builder_object"]
    dag_config = system.config["dag_config"]
    mark_key_as_used = True
    trading_period_str = dag_builder.get_trading_period(
        dag_config, mark_key_as_used
    )
    _LOG.debug(hprint.to_str("trading_period_str"))
    pricing_method = "twap." + trading_period_str
    cf_config_strategy = system.config.get_and_mark_as_used(
        ("cf_config", "strategy")
    )
    market_data_universe_version = system.config.get_and_mark_as_used(
        ("market_data_config", "universe_version")
    )
    market_data_asset_ids = system.config.get_and_mark_as_used(
        ("market_data_config", "asset_ids")
    )
    secret_identifier_config = system.config.get_and_mark_as_used(
        "secret_identifier_config"
    )
    portfolio = oms.get_CcxtPortfolio_prod_instance1(
        run_mode,
        cf_config_strategy,
        market_data,
        market_data_universe_version,
        market_data_asset_ids,
        pricing_method,
        secret_identifier_config,
    )
    return portfolio


# TODO(gp): We should dump the state of the portfolio and load it back.
# TODO(gp): Probably all prod system needs to have use_simulation and trade_date and
#  so we can generalize the class to be not E8 specific.
def get_Cx_portfolio(
    system: dtfsys.System,
) -> oms.Portfolio:
    # We prefer to configure code statically (e.g., without switches) but in this
    # case the prod system vs its simulat-able version are so close (and we want to
    # keep them close) that we use a switch.
    if not system.use_simulation:
        # Prod.
        portfolio = get_Cx_portfolio_prod_instance1(system)
    else:
        # Simulation.
        # TODO(gp): This needs to be fixed before reconciliation.
        # _LOG.warning("Configuring for simulation")
        # portfolio = oms.get_DatabasePortfolio_example3(
        #     system.config["db_connection_object"],
        #     system.config["event_loop_object"],
        #     system.market_data,
        # )
        pass
    return portfolio


# #############################################################################
# Apply config utils.
# #############################################################################


def apply_Cx_MarketData_config(
    system: dtfsys.System,
    replayed_delay_in_mins_or_timestamp: Union[int, pd.Timestamp],
) -> dtfsys.System:
    """
    Extend system config with parameters for `MarketData` init.
    """
    system.config["market_data_config", "asset_id_col_name"] = "asset_id"
    system.config["market_data_config", "delay_in_secs"] = 10
    system.config[
        "market_data_config", "replayed_delay_in_mins_or_timestamp"
    ] = replayed_delay_in_mins_or_timestamp
    return system


def apply_Cx_DagRunner_config(
    system: dtfsys.System,
    rt_timeout_in_secs_or_time: Union[int, datetime.time],
) -> dtfsys.System:
    """
    Extend system config with parameters for `DagRunner` init.
    """
    # TODO(Grisha): infer bar duration from `DagBuilder`.
    system.config["dag_runner_config", "bar_duration_in_secs"] = 60 * 5
    system.config["dag_runner_config", "fit_at_beginning"] = system.config[
        "dag_builder_object"
    ].fit_at_beginning
    system.config[
        "dag_runner_config", "rt_timeout_in_secs_or_time"
    ] = rt_timeout_in_secs_or_time
    return system


def apply_research_pnl_config(system: dtfsys.System) -> dtfsys.System:
    """
    Extend system config with parameters for research PNL computations.
    """
    dag_builder = system.config["dag_builder_object"]
    price_col = dag_builder.get_column_name("price")
    volatility_col = dag_builder.get_column_name("volatility")
    prediction_col = dag_builder.get_column_name("prediction")
    system.config["research_pnl", "price_col"] = price_col
    system.config["research_pnl", "volatility_col"] = volatility_col
    system.config["research_pnl", "prediction_col"] = prediction_col
    return system


def apply_Cx_ForecastEvaluatorFromPrices_config(
    system: dtfsys.System,
) -> dtfsys.System:
    """
    Extend system config with parameters for `ForecastEvaluatorFromPrices`
    init.
    """
    forecast_evaluator_from_prices_dict = {
        "style": "cross_sectional",
        "init": {
            "price_col": system.config["research_pnl", "price_col"],
            "volatility_col": system.config["research_pnl", "volatility_col"],
            "prediction_col": system.config["research_pnl", "prediction_col"],
        },
        "kwargs": {
            "target_gmv": 1e5,
            "liquidate_at_end_of_day": False,
        },
    }
    system.config[
        "research_forecast_evaluator_from_prices"
    ] = cconfig.Config.from_dict(forecast_evaluator_from_prices_dict)
    return system


def apply_Cx_OrderProcessor_config(system: dtfsys.System) -> dtfsys.System:
    """
    Extend system config with parameters for `OrderProcessor` init.
    """
    # If an order is not placed within a bar, then there is a timeout, so
    # we add extra 5 seconds to `bar_duration_in_secs` (which represents
    # the length of a trading bar) to make sure that the `OrderProcessor`
    # waits long enough before timing out.
    max_wait_time_for_order_in_secs = (
        system.config["dag_runner_config", "bar_duration_in_secs"] + 5
    )
    system.config[
        "order_processor_config", "max_wait_time_for_order_in_secs"
    ] = max_wait_time_for_order_in_secs
    system.config["order_processor_config", "duration_in_secs"] = system.config[
        "dag_runner_config", "rt_timeout_in_secs_or_time"
    ]
    return system
