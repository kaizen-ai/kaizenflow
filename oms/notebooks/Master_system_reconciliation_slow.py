# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Description

# %% [markdown]
# The notebook contains full versions of DAG output checks that compare the prod system output vs that of the prod simulation run.

# %% [markdown]
# # Imports

# %%
# %load_ext autoreload
# %autoreload 2
# %matplotlib inline

# %%
import logging

import core.config as cconfig
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import oms as oms

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Build the reconciliation config

# %%
# Get config from env when running the notebook via the `run_notebook.py` script, e.g.,
# in the system reconciliation flow.
config = cconfig.get_config_from_env()
if config:
    _LOG.info("Using config from env vars")
else:
    _LOG.info("Using hardwired config")
    # Specify the config directly when running the notebook manually.
    # Below is just an example.
    dst_root_dir = "/shared_data/ecs/preprod/prod_reconciliation/"
    dag_builder_name = "C3a"
    start_timestamp_as_str = "20230517_131000"
    end_timestamp_as_str = "20230518_130500"
    run_mode = "paper_trading"
    mode = "scheduled"
    config_list = oms.build_reconciliation_configs(
        dst_root_dir,
        dag_builder_name,
        start_timestamp_as_str,
        end_timestamp_as_str,
        run_mode,
        mode,
    )
    config = config_list[0]
print(config)

# %% [markdown]
# # Specify data to load

# %% run_control={"marked": false}
# The dict points to `system_log_dir` for different experiments.
system_log_path_dict = dict(config["system_log_path"].to_dict())

# %%
# This dict points to `system_log_dir/dag/node_io/node_io.data` for different experiments.
data_type = "dag_data"
dag_path_dict = oms.get_system_log_paths(system_log_path_dict, data_type)
dag_path_dict

# %% [markdown]
# # DAG io

# %% [markdown]
# ## Load

# %%
# Get DAG node names.
dag_node_names = oms.get_dag_node_names(dag_path_dict["prod"])
_LOG.info(
    "First node='%s' / Last node='%s'", dag_node_names[0], dag_node_names[-1]
)

# %%
# Get timestamps for the last DAG node.
dag_node_timestamps = oms.get_dag_node_timestamps(
    dag_path_dict["prod"], dag_node_names[-1], as_timestamp=True
)
_LOG.info(
    "First timestamp='%s'/ Last timestamp='%s'",
    dag_node_timestamps[0][0],
    dag_node_timestamps[-1][0],
)

# %%
# Get DAG output for the last node and the last timestamp.
dag_df_prod = oms.load_dag_outputs(dag_path_dict["prod"], dag_node_names[-1])
dag_df_sim = oms.load_dag_outputs(dag_path_dict["sim"], dag_node_names[-1])
_LOG.info("Output of the last node:\n")
hpandas.df_to_str(dag_df_prod, num_rows=5, log_level=logging.INFO)

# %% [markdown]
# ## Check DAG io self-consistency

# %%
# TODO(Grisha): pass the value via config.
diff_threshold = 1e-3
compare_dfs_kwargs = {
    # TODO(Nina): CmTask4387 "DAG self-consistency check fails when
    # `history_lookback=15T` for C3a".
    "compare_nans": False,
    "diff_mode": "pct_change",
    "assert_diff_threshold": None,
}

# %%
# Run for all timestamps.
bar_timestamp = "all"
# Compare DAG output at T with itself at time T-1.
oms.check_dag_output_self_consistency(
    dag_path_dict["prod"],
    dag_node_names[-1],
    bar_timestamp,
    trading_freq=config["meta"]["bar_duration"],
    diff_threshold=diff_threshold,
    **compare_dfs_kwargs,
)

# %% [markdown]
# ## Compare DAG io (prod vs sim)

# %%
# Run for all nodes and all timestamps.
bar_timestamp = "all"
node_name = "all"
oms.compare_dag_outputs(
    dag_path_dict,
    node_name,
    bar_timestamp,
    diff_threshold=diff_threshold,
    **compare_dfs_kwargs,
)
