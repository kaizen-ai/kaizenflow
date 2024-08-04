# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
# %load_ext autoreload
# %autoreload 2
# %matplotlib inline

# %%
import logging
import os
from typing import List

import pandas as pd
import tqdm

import dataflow.core as dtfcore
import dataflow.core.node as dtfcornode
import dataflow.core.nodes.sources as dtfconosou
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hgit as hgit
import helpers.hpandas as hpandas
import helpers.hpickle as hpickle
import helpers.hprint as hprint
import research_amp.soccer_prediction.models as rasoprmo
import research_amp.soccer_prediction.preprocessing as rasoprpr

# %%
hdbg.init_logger(verbosity=logging.DEBUG)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Config

# %%
amp_path = hgit.get_amp_abs_path()
params_file_path = os.path.join(
    amp_path, "research_amp", "soccer_prediction", "params.ENG5.2009_2012.pkl"
)
# Define the necessary preprocessing step configuration.
config = {
    "load_and_preprocess_node": {
        "bucket_name": "cryptokaizen-data-test",
        "dataset_path": "kaizen_ai/soccer_prediction/datasets/OSF_football/ISDBv2.txt",
        "leagues": ["ENG5"],
        "seasons": ["2009", "2010", "2011", "2012"],
    },
    "poisson_regressor": {
        "maxiter": None,
        "col_mode": None,
        "half_life_period": None,
    },
    "model_fit_config": {
        "fit_at_beginning": False,
        "train_intervals": [
            (
                pd.Timestamp("2009-08-08 00:00:00"),
                pd.Timestamp("2012-04-28 00:03:40"),
            )
        ],
        "params_file_path": params_file_path,
    },
    "model_predict_config": {
        "test_intervals": [
            (
                pd.Timestamp("2012-08-10 00:00:00"),
                pd.Timestamp("2013-04-20 00:03:40"),
            )
        ],
        "freq": "1W",
    },
}
display(config)

# %% [markdown]
# # Build the DAG

# %% [markdown]
# ## Source Node

# %% run_control={"marked": true}
node_1 = "load_and_preprocess_node"
# Initialize the FunctionDataSource with the correct configuration.
load_and_preprocess_node = dtfconosou.FunctionDataSource(
    node_1, func=rasoprpr.load_and_preprocess_data, func_kwargs=config[node_1]
)
# Display the data.
soccer_data = load_and_preprocess_node.fit()["df_out"]
_LOG.info(hpandas.df_to_str(soccer_data))


# %% [markdown]
# ## Poisson Node

# %%
# Define node ID and variables.
node_3 = dtfcornode.NodeId("poisson_regressor")
# Instantiate the poisson model.
poisson_model_node = rasoprmo.BivariatePoissonModel(nid=node_3, **config[node_3])

# %% [markdown]
# ## Connect the Nodes

# %%
name = "soccer_prediction"
dag = dtfcore.DAG(name=name, mode="loose")
# Add nodes to the DAG.
dag.insert_at_head(load_and_preprocess_node)
dag.append_to_tail(poisson_model_node)
_LOG.info("DAG=\n%s", repr(dag))

# %%
# Draw the DAG.
dtfcore.draw(dag)

# %% [markdown]
# # Run the DAG

# %% [markdown]
# ## Fit

# %%
if config["model_fit_config"]["fit_at_beginning"]:
    # Estimate the params and save them to disk.
    # Use `FitPredictDagRunner` to learn the parameters.
    fit_predict_dag_runner = dtfcore.FitPredictDagRunner(dag)
    # Define train time interval.
    # train_intervals = [(pd.Timestamp("2000-03-19 00:00:00"), pd.Timestamp("2000-08-26 00:03:20"))]
    fit_predict_dag_runner.set_fit_intervals(
        config["model_fit_config"]["train_intervals"]
    )
    #
    fit_predict_dag_runner.fit()
    fit_state = dtfcore.get_fit_state(dag)
    # Save params to disk.
    hpickle.to_pickle(fit_state, config["model_fit_config"]["params_file_path"])
else:
    # Load params from disk.
    fit_state = hpickle.from_pickle(
        config["model_fit_config"]["params_file_path"]
    )
_LOG.info("Fit_state=\n%s", fit_state)


# %% [markdown]
# ## Predict


# %%
# TODO(Grisha): consider moving to `IncrementalDagRunner`.
def concatenate_results(
    result_bundles: List[dtfcore.ResultBundle],
    start_timestamp: pd.Timestamp,
    end_timestamp: pd.Timestamp,
    freq: str,
) -> pd.DataFrame:
    """
    Concatenate results from multiple iterations into a single dataframe.

    On each iteration we collect only "new" predictions, i.e. the ones
    that were not computed during the previous iteration.
    """
    predictions = []
    for i in tqdm.tqdm(range(len(result_bundles))):
        # For each results bundle collect resulting df.
        df = result_bundles[i].result_df
        # Restrict date range to test start/end timestamps.
        df = df.loc[start_timestamp:end_timestamp]
        # For each iteration keep only the "new" prediction, i.e. the ones that correspond
        # to a given freq.
        iteration_start_timestamp = df.index.max() - pd.Timedelta(freq)
        _LOG.info(
            "Min date=%s, max date=%s", iteration_start_timestamp, df.index.max()
        )
        current_df = df.loc[iteration_start_timestamp:]
        predictions.append(current_df)
        _LOG.info(hpandas.df_to_str(current_df))
    return pd.concat(predictions)


# %%
# Set test intervals.
# test_intervals = [(pd.Timestamp("2000-08-26 00:03:40"), pd.Timestamp("2000-08-31 00:04:00"))]
# Set prediction frequency.
freq = "1W"
incremental_dag_runner = dtfcore.IncrementalDagRunner(
    dag,
    config["model_predict_config"]["test_intervals"][0][0],
    config["model_predict_config"]["test_intervals"][0][1],
    config["model_predict_config"]["freq"],
    fit_state,
)
result_bundles = list(incremental_dag_runner.predict())

# %%
predictions_df = concatenate_results(
    result_bundles,
    config["model_predict_config"]["test_intervals"][0][0],
    config["model_predict_config"]["test_intervals"][0][1],
    config["model_predict_config"]["freq"],
)

# %%
_LOG.info("Resulting df=\n%s", hpandas.df_to_str(predictions_df))

# %%
# # Combine Predictions and calculate match outcomes.
# node_4 = "match_outcomes_node"
# config = {"match_outcomes_node": {"actual_df": df_out_fit, "predictions_df": df_model_predict}}
# # Calculate match outcomes.
# match_outcomes_node = dtfconosou.FunctionDataSource(
#     node_4,
#     func=rasoprut.calculate_match_outcome_probabilities,
#     func_kwargs=config[node_4]
#     )
# match_outcomes_df_out = preprocessing_node.fit()["df_out"]
# _LOG.debug(hpandas.df_to_str(preprocessing_df_out))
