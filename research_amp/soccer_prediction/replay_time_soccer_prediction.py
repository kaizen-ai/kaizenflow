#!/usr/bin/env python

"""
Compute soccer predictions using ReplayedMarketData.
"""

import logging

import pandas as pd

import core.real_time as creatime
import dataflow.core.dag as dtfcordag
import dataflow.core.dag_runner as dtfcodarun
import dataflow.core.node as dtfcornode
import dataflow.core.nodes.sources as dtfconosou
import dataflow.core.visitors as dtfcorvisi
import dataflow.system.real_time_dag_runner as dtfsrtdaru
import dataflow.system.source_nodes as dtfsysonod
import helpers.hasyncio as hasynci
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import market_data.replayed_market_data as mdremada

hdbg.init_logger(verbosity=logging.DEBUG)

_LOG = logging.getLogger(__name__)

import logging

import pandas as pd

import helpers.hdbg as hdbg
import research_amp.soccer_prediction.models as rasoprmo
import research_amp.soccer_prediction.preprocessing as rasoprpr

if __name__ == "__main__":
    # 1) Fit.
    config = {
        "load_and_preprocess_node": {
            "bucket_name": "cryptokaizen-data-test",
            "dataset_path": "kaizen_ai/soccer_prediction/datasets/OSF_football/ISDBv2.txt",
        }
    }
    node_1 = "load_and_preprocess_node"
    # Initialize the FunctionDataSource with the correct configuration.
    load_and_preprocess_node = dtfconosou.FunctionDataSource(
        node_1, func=rasoprpr.load_and_preprocess_data, func_kwargs=config[node_1]
    )
    fit_dag = dtfcordag.DAG()
    fit_dag.insert_at_head(load_and_preprocess_node)
    node_3 = dtfcornode.NodeId("poisson_regressor")
    # Instantiate the poisson model.
    poisson_model_node = rasoprmo.BivariatePoissonModel(nid=node_3, maxiter=1)
    fit_dag.append_to_tail(poisson_model_node)
    fit_dag_runner = dtfcodarun.FitPredictDagRunner(fit_dag)
    train_intervals = [
        (pd.Timestamp("2000-03-19 00:00:00"), pd.Timestamp("2000-08-26 00:03:20"))
    ]
    fit_dag_runner.set_fit_intervals(train_intervals)
    fit_dag_runner.fit()
    # fit_state = None
    fit_state = dtfcorvisi.get_fit_state(fit_dag)
    print("fit_state = ", fit_state)
    # 2) Preprocess the data.
    load_and_preprocess_node = dtfconosou.FunctionDataSource(
        node_1, func=rasoprpr.load_and_preprocess_data, func_kwargs=config[node_1]
    )
    preprocessed_df_out = load_and_preprocess_node.predict()["df_out"]
    _LOG.info(hpandas.df_to_str(preprocessed_df_out))
    preprocessed_df_out[
        "knowledge_timestamp"
    ] = preprocessed_df_out.index  # + pd.Timedelta("2S")
    preprocessed_df_out = preprocessed_df_out.reset_index().rename(
        columns={"Adjusted_Date": "end_timestamp"}
    )
    preprocessed_df_out["start_timestamp"] = preprocessed_df_out[
        "end_timestamp"
    ] - pd.Timedelta("2H")
    preprocessed_df_out["knowledge_timestamp"] = preprocessed_df_out[
        "knowledge_timestamp"
    ].dt.tz_localize("UTC")
    preprocessed_df_out["start_timestamp"] = preprocessed_df_out[
        "start_timestamp"
    ].dt.tz_localize("UTC")
    preprocessed_df_out["end_timestamp"] = preprocessed_df_out[
        "end_timestamp"
    ].dt.tz_localize("UTC")
    df = preprocessed_df_out
    # 3) Predict using ReplayedTimeMarketData.
    with hasynci.solipsism_context() as event_loop:
        knowledge_datetime_col_name = "knowledge_timestamp"
        delay_in_secs = 0
        asset_id_col_name = "home_team_id"
        asset_ids = preprocessed_df_out["home_team_id"].unique().tolist()
        asset_ids = [int(x) for x in asset_ids]
        start_time_col_name = "start_timestamp"
        end_time_col_name = "end_timestamp"
        columns = None
        tz = "UTC"
        initial_replayed_timestamp = pd.Timestamp("2017-05-01 02:00:00", tz="UTC")
        speed_up_factor = 1.0
        get_wall_clock_time = creatime.get_replayed_wall_clock_time(
            tz,
            initial_replayed_timestamp,
            event_loop=event_loop,
            speed_up_factor=speed_up_factor,
        )
        sleep_in_secs = 1.0
        time_out_in_secs = 2
        market_data = mdremada.ReplayedMarketData(
            df,
            knowledge_datetime_col_name,
            delay_in_secs,
            #
            asset_id_col_name,
            asset_ids,
            start_time_col_name,
            end_time_col_name,
            columns,
            get_wall_clock_time,
            sleep_in_secs=sleep_in_secs,
            time_out_in_secs=time_out_in_secs,
            timezone="UTC",
        )
        multiindex_output = False
        timedelta = pd.Timedelta("8H")
        replayed_market_data_node = dtfsysonod.RealTimeDataSource(
            "replayed_market_data",
            market_data,
            timedelta,
            knowledge_datetime_col_name,
            multiindex_output,
        )
        rt_dag = dtfcordag.DAG()
        rt_dag.insert_at_head(replayed_market_data_node)
        rt_dag.append_to_tail(fit_dag.get_node("poisson_regressor"))
        bar_duration_in_secs = 60 * 60 * 2
        # Run the loop for 10 seconds.
        rt_timeout_in_secs_or_time = 10 * bar_duration_in_secs
        execute_rt_loop_kwargs = {
            # Should be the same clock as in `ReplayedMarketData`.
            "get_wall_clock_time": get_wall_clock_time,
            "bar_duration_in_secs": bar_duration_in_secs,
            "rt_timeout_in_secs_or_time": rt_timeout_in_secs_or_time,
        }
        dag_runner_kwargs = {
            "dag": rt_dag,
            "fit_state": fit_state,
            "execute_rt_loop_kwargs": execute_rt_loop_kwargs,
            "dst_dir": None,
            "get_wall_clock_time": get_wall_clock_time,
            "bar_duration_in_secs": bar_duration_in_secs,
            # We don't want to set the current bar in this test.
            # "set_current_bar_timestamp": False,
            "set_current_bar_timestamp": True,
        }
        # 7) Run the `DAG` using a simulated clock.
        dag_runner = dtfsrtdaru.RealTimeDagRunner(**dag_runner_kwargs)
        result_bundles = hasynci.run(dag_runner.predict(), event_loop=event_loop)
        _ = dag_runner.events
