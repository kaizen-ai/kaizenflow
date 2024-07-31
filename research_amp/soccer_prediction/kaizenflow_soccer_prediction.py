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
import logging
from typing import Any, Dict, Optional, List

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from scipy.optimize import minimize
from sklearn.base import BaseEstimator, RegressorMixin

import dataflow.core as dtfcore
import dataflow.core.node as dtfcornode
import dataflow.core.nodes.sklearn_models as dtfcnoskmo
import dataflow.core.nodes.sources as dtfconosou
import dataflow.core.nodes.base as dtfconobas
import dataflow.core.utils as dtfcorutil
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import research_amp.soccer_prediction.preproccesing as rasoprpr
import research_amp.soccer_prediction.utils as rasoprut
import research_amp.soccer_prediction.models as rasoprmo

# %%
hdbg.init_logger(verbosity=logging.DEBUG)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# ### Source Node

# %% run_control={"marked": true}
# Define the necessary preprocessing step configuration.
config = {
            "load_and_preprocess_node": {
                "bucket_name": "cryptokaizen-data-test", 
                "dataset_path": "kaizen_ai/soccer_prediction/datasets/OSF_football/ISDBv2.txt"
            }
         }
node_1 = "load_and_preprocess_node"
# Initialize the FunctionDataSource with the correct configuration.
load_and_preprocess_node = dtfconosou.FunctionDataSource(
    node_1, 
    func=rasoprpr.load_and_preprocess_data, 
    func_kwargs=config[node_1]
    )
# Create train-test split using the source node.
# Define the training and testing intervals.
train_intervals = [(pd.Timestamp("2000-03-19 00:00:00"), pd.Timestamp("2000-08-26 00:03:20"))]
test_intervals = [(pd.Timestamp("2000-08-26 00:03:40"), pd.Timestamp("2000-08-27 00:04:00"))]
# Set the intervals in the source node.
load_and_preprocess_node.set_fit_intervals(train_intervals)
load_and_preprocess_node.set_predict_intervals(test_intervals)
# Generate the training dataset.
train_df_out = load_and_preprocess_node.fit()["df_out"]
# Generate the testing dataset.
test_df_out = load_and_preprocess_node.predict()["df_out"]


# %% [markdown]
# ### Train-Test split

# %%
_LOG.debug(hpandas.df_to_str(train_df_out))
_LOG.debug(hpandas.df_to_str(test_df_out))

# %% [markdown]
# ### Bivariate model Node


# %%
# Define node ID and variables.
node_3 = dtfcornode.NodeId("poisson_regressor")
# Instantiate the poisson model.
poisson_model_node = rasoprmo.BivariatePoissonModel(
    nid=node_3,
    maxiter = 1
)
df_model_fit = poisson_model_node.fit(train_df_out)["df_out"]
_LOG.debug(hpandas.df_to_str(df_model_fit))


# %%
df_model_predict = poisson_model_node.predict(test_df_out)["df_out"]
_LOG.debug(hpandas.df_to_str(df_model_predict))

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

# %% [markdown]
# ### Intiating DAG

# %%
name = "soccer_prediction"
dag = dtfcore.DAG(name=name, mode = "loose")
# Note that DAG objects can print information about their state.
display(dag)

# %%
# Append dag with nodes.
dag.append_to_tail(load_and_preprocess_node)
dag.append_to_tail(poisson_model_node)
#dag.append_to_tail(combine_predictions_node)
display(dag)

# %%
dtfcore.draw(dag)
