#!/usr/bin/env python
"""
The script runs a simulation equivalent to the production run.
"""
import logging
import os

import dataflow.system as dtfsys
import dataflow_amp.system.common.system_simulation_utils as dtfascssiut
import dataflow_amp.system.Cx.Cx_builders as dtfasccxbu
import dataflow_amp.system.Cx.Cx_prod_system as dtfasccprsy
import dataflow_amp.system.Cx.utils as dtfasycxut
import helpers.hdbg as hdbg
import helpers.hio as hio
import reconciliation as reconcil

if __name__ == "__main__":
    # Set logger.
    hdbg.init_logger(
        verbosity=logging.DEBUG,
        use_exec_path=True,
        report_memory_usage=True,
    )
    # Define params for market data.
    market_data_dst_dir = "./tmp"
    increment = True
    hio.create_dir(market_data_dst_dir, increment)
    market_data_file_path = os.path.join(
        market_data_dst_dir, "market_data.csv.gz"
    )
    start_timestamp_as_str = "20230906_101000"
    end_timestamp_as_str = "20230906_103000"
    db_stage = "preprod"
    universe_version = "v7.4"
    # Dump market data.
    dtfasycxut.dump_market_data_from_db(
        market_data_file_path,
        start_timestamp_as_str,
        end_timestamp_as_str,
        db_stage,
        universe_version,
    )
    # Build the prod system.
    dag_builder_ctor_as_str = (
        "dataflow_orange.pipelines.C1.C1b_pipeline.C1b_DagBuilder"
    )
    system = dtfasccprsy.Cx_ProdSystem_v1_20220727(
        dag_builder_ctor_as_str, run_mode="simulation"
    )
    # Fill the system.
    system = dtfsys.apply_Portfolio_config(system)
    order_config = dtfasccxbu.get_Cx_order_config_instance1()
    optimizer_config = dtfasccxbu.get_Cx_optimizer_config_instance1()
    log_dir = reconcil.get_process_forecasts_dir(system.config["system_log_dir"])
    system = dtfasccxbu.apply_ProcessForecastsNode_config(
        system, order_config, optimizer_config, log_dir
    )
    system.config["trading_period"] = "5T"
    system.config["market_data_config", "days"] = None
    system.config["market_data_config", "universe_version"] = universe_version
    set_config_values = '("process_forecasts_node_dict","process_forecasts_dict","optimizer_config","params","style"),(str("longitudinal"));("process_forecasts_node_dict","process_forecasts_dict","optimizer_config","params","kwargs"),({"target_dollar_risk_per_name": float(1.0)})'
    # Define params.
    dst_dir = "/app/system_log_dir"
    config_tag = "prod_system"
    # Run simulation.
    _ = dtfascssiut.run_simulation(
        system,
        start_timestamp_as_str,
        end_timestamp_as_str,
        market_data_file_path,
        dst_dir,
        set_config_values=set_config_values,
        config_tag=config_tag,
    )
