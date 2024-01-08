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

# %% [markdown]
# # Description

# %% [markdown]
# The notebook builds a DAG with a simple risk model and runs it in the historical mode.

# %% [markdown]
# # Imports

# %%
import logging

import numpy as np
import pandas as pd
import sklearn.linear_model as slmode

import core.config as cconfig
import core.features as cofeatur
import core.signal_processing as csigproc
import dataflow.core as dtfcore
import helpers.hdbg as hdbg
import helpers.hprint as hprint

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

hprint.config_notebook()


# %% [markdown]
# # Model configuration

# %% [markdown]
# Build a `DagBuilder` object that defines a model's configuration
# - `get_config_template()`: creates a configuration for each DAG Node
# - `_get_dag()`: specifies all the DAG Nodes and builds a DAG using these Nodes

# %%
class SimpleRiskModel_DagBuilder(dtfcore.DagBuilder):
    """
    A pipeline similar to real feature processing.
    """

    @staticmethod
    def get_column_name(tag: str) -> str:
        """
        See description in the parent class.
        """
        raise NotImplementedError

    def get_trading_period(
        self, config: cconfig.Config, mark_key_as_used: bool
    ) -> str:
        """
        See description in the parent class.
        """
        _ = self
        raise NotImplementedError

    def get_required_lookback_in_effective_days(
        self, config: cconfig.Config, mark_key_as_used: bool
    ) -> str:
        """
        See description in the parent class.
        """
        raise NotImplementedError

    def set_weights(
        self, config: cconfig.Config, weights: pd.Series
    ) -> cconfig.Config:
        """
        See description in the parent class.
        """
        raise NotImplementedError

    def convert_to_fast_prod_setup(
        self, config: cconfig.Config
    ) -> cconfig.Config:
        """
        See description in the parent class.
        """
        raise NotImplementedError

    def get_config_template(self) -> cconfig.Config:
        """
        Return a config template compatible with `self.get_dag()`.
        """
        # Set input and output column names that will be passed to the node.
        columns = ["ret_0", "x_1", "x_2"]
        # X vars stands for the input features column names.
        x_vars = [x for x in columns if x != "ret_0"]
        # Y vars stands for the output column name.
        y_vars = ["ret_0"]
        dict_ = {
            self._get_nid("sklearn"): {
                "in_col_groups": [(x,) for x in columns],
                "out_col_group": (),
                "x_vars": x_vars,
                "y_vars": y_vars,
                "steps_ahead": 1,
                "model_kwargs": {
                    "fit_intercept": False,
                },
                "nan_mode": "drop",
            },
            self._get_nid("residualize"): {
                "in_col_groups": [
                    ("ret_0.shift_-1",),
                    ("ret_0.shift_-1_hat",),
                ],
                "out_col_group": (),
                "transformer_kwargs": {
                    "term1_col": "ret_0.shift_-1",
                    "term2_col": "ret_0.shift_-1_hat",
                    "out_col": "residual.shift_-1",
                    "operation": "sub",
                },
            },
            self._get_nid("compute_rolling_norm"): {
                "in_col_group": ("residual.shift_-1",),
                "out_col_group": ("smoothed_squared_residual.shift_-1",),
                "transformer_kwargs": {
                    "tau": 10,
                },
            },
        }
        config = cconfig.Config.from_dict(dict_)
        return config

    def _get_dag(
        self,
        config: cconfig.Config,
        *,
        mode: str = "strict",
    ) -> dtfcore.DAG:
        """
        Build DAG given a `config`.

        :param config: DAG configuration
        :return: resulting `DAG` that is built from an input config
        """
        dag = dtfcore.DAG(mode=mode)
        _LOG.debug("%s", config)
        #
        stage = "sklearn"
        nid = self._get_nid(stage)
        node = dtfcore.MultiindexSkLearnModel(
            nid,
            model_func=slmode.LinearRegression,
            **config[nid].to_dict(),
        )
        dag.append_to_tail(node)
        #
        stage = "residualize"
        nid = self._get_nid(stage)
        node = dtfcore.GroupedColDfToDfTransformer(
            nid,
            transformer_func=cofeatur.combine_columns,
            **config[nid].to_dict(),
        )
        dag.append_to_tail(node)
        #
        stage = "compute_rolling_norm"
        nid = self._get_nid(stage)
        node = dtfcore.SeriesToSeriesTransformer(
            nid,
            transformer_func=csigproc.compute_rolling_norm,
            **config[nid].to_dict(),
        )
        dag.append_to_tail(node)
        #
        return dag


# %% [markdown]
# # Build a DAG

# %%
dag_builder = SimpleRiskModel_DagBuilder()
dag_config = dag_builder.get_config_template()
print(dag_config)

# %%
dag = dag_builder.get_dag(dag_config)
dtfcore.draw(dag)

# %% [markdown]
# # Generate data and connect data source to the DAG

# %%
n_assets = 4
n_features = 2
n_periods = 10
freq = "B"
period_start = "2023-05-01"
rng_seed = 1


def get_random_data(
    n_assets: int,
    n_features: int,
    n_periods: int,
    period_start: str,
    freq: str,
    seed: int,
) -> pd.DataFrame():
    """
    Generate a dataframe of random returns and random features.
    """
    # Create datetime index for dataframe.
    idx = pd.date_range(start=period_start, periods=n_periods, freq=freq)
    # Create columns names for the X matrix.
    cols = [f"x_{k}" for k in range(1, n_features + 1)]
    cols = cols + ["ret_0"]
    # Instantiate random number generator.
    rng = np.random.default_rng(rng_seed)
    dfs = {}
    for n in range(n_assets):
        asset_id = 100 + n
        dfs[asset_id] = pd.DataFrame(
            rng.standard_normal((n_periods, n_features + 1)), idx, cols
        )
    df = pd.concat(dfs, axis=1).swaplevel(axis=1).sort_index(axis=1)
    return df


data = get_random_data(
    n_assets, n_features, n_periods, period_start, freq, rng_seed
)
display(data.head())

# %% [markdown]
# Wrap the data into a `DfDataSource` that serves as a DAG source Node.

# %%
node = dtfcore.DfDataSource("data", data)
dag.insert_at_head(node)
dtfcore.draw(dag)

# %% [markdown]
# # Run the DAG

# %%
dag_runner = dtfcore.FitPredictDagRunner(dag)
dag_runner.set_fit_intervals(
    [
        (
            data.index.min(),
            data.index.max(),
        )
    ],
)
fit_result_bundle = dag_runner.fit()
#
result_df = fit_result_bundle.result_df
result_df.head()

# %%
