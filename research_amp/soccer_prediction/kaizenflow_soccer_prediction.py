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
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from scipy.optimize import minimize
from sklearn.base import BaseEstimator, RegressorMixin

import dataflow.core as dtfcore
import dataflow.core.node as dtfcornode
import dataflow.core.nodes.sklearn_models as dtfcnoskmo
import dataflow.core.nodes.sources as dtfconosou
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

# %% run_control={"marked": true}
# Define the necessary preprocessing step configuration.
config = {"load_and_preprocess_node": {"bucket_name": "cryptokaizen-data-test", "dataset_path": "kaizen_ai/soccer_prediction/datasets/OSF_football/ISDBv2.txt"}}
node_1 = "load_and_preprocess_node"
# Initialize the FunctionDataSource with the correct configuration.
load_and_preprocess_node = dtfconosou.FunctionDataSource(
    node_1, 
    func=rasoprpr.load_and_preprocess_data, 
    func_kwargs=config[node_1]
    )
preprocessed_df_out = load_and_preprocess_node.fit()["df_out"]


# %%
_LOG.debug(hpandas.df_to_str(preprocessed_df_out))

# %% [markdown]
# #### Bivariate model Node


# %%
# This is temporary data for sanity checks.
data = pd.DataFrame(
    {
        "HT_id": np.random.randint(0, 10, 1000),
        "AT_id": np.random.randint(0, 10, 1000),
        "HS": np.random.poisson(1.5, 1000),
        "AS": np.random.poisson(1.5, 1000),
        "Time_Weight": np.random.uniform(0.8, 1.2, 1000),
        "Lge": ["ENG5"] * 1000,
        "Sea": np.random.choice(["07-08", "06-07", "08-09"], 1000),
    }
)

# Select the data for the league and season
final_data = data[
    (data["Lge"] == "ENG5")
    & (
        (data["Sea"] == "07-08")
        | (data["Sea"] == "06-07")
        | (data["Sea"] == "08-09")
    )
]

# Ensure correct column names and types
final_data["HT_id"] = final_data["HT_id"].astype(int)
final_data["AT_id"] = final_data["AT_id"].astype(int)
final_data["HS"] = final_data["HS"].astype(int)
final_data["AS"] = final_data["AS"].astype(int)
# Split into features and target
X = final_data[["HT_id", "AT_id", "Time_Weight"]]
y = final_data[["HS", "AS"]]
df = final_data
# Define the model function.
model_func = lambda: rasoprmo.BivariatePoissonWrapper(maxiter=10)
# Define node ID and variables.
node_3 = dtfcornode.NodeId("poisson_regressor")
x_vars = X.columns.tolist()
y_vars = ["HS", "AS"]
steps_ahead = 1
# Instantiate the ContinuousSkLearnModel with the bivariate Poisson wrapper.
poisson_model_node = dtfcnoskmo.ContinuousSkLearnModel(
    nid=node_3,
    model_func=model_func,
    x_vars=x_vars,
    y_vars=y_vars,
    steps_ahead=steps_ahead,
)
df_model_fit = poisson_model_node.fit(df)["df_out"]
_LOG.debug(hpandas.df_to_str(df_model_fit))


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
