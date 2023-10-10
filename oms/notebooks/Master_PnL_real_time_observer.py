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

# %%
# %load_ext autoreload
# %autoreload 2
# %matplotlib inline

# %%
import logging
import os
from typing import Optional

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd

import core.config as cconfig
import core.plotting as coplotti
import dataflow.model as dtfmod
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hintrospection as hintros
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import helpers.hs3 as hs3
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
#
if config:
    _LOG.info("Using config from env vars")
else:
    _LOG.info("Using hardwired config")
    # Specify the config directly when running the notebook manually.
    # Below is just an example.
    prod_data_root_dir = "/shared_data/ecs/preprod/system_reconciliation"
    dag_builder_name = "C3a"
    run_mode = "paper_trading"
    start_timestamp_as_str = "20230716_131000"
    end_timestamp_as_str = "20230717_130500"
    mode = "scheduled"
    save_plots_for_investors = True
    html_bucket_path = henv.execute_repo_config_code("get_html_bucket_path()")
    s3_dst_dir = os.path.join(html_bucket_path, "pnl_for_investors")
    config_list = oms.build_prod_pnl_real_time_observer_configs(
        prod_data_root_dir,
        dag_builder_name,
        run_mode,
        start_timestamp_as_str,
        end_timestamp_as_str,
        mode,
        save_plots_for_investors,
        s3_dst_dir=s3_dst_dir,
    )
    config = config_list[0]
print(config)

# %% [markdown]
# # Specify data to load

# %%
# Points to `system_log_dir/dag/node_io/node_io.data`.
data_type = "dag_data"
dag_data_path = oms.get_data_type_system_log_path(
    config["system_log_dir"], data_type
)
_LOG.info("dag_data_path=%s", dag_data_path)
# Points to `system_log_dir/dag/node_io/node_io.prof`.
data_type = "dag_stats"
dag_info_path = oms.get_data_type_system_log_path(
    config["system_log_dir"], data_type
)
_LOG.info("dag_info_path=%s", dag_info_path)
# Points to `system_log_dir/process_forecasts/portfolio`.
data_type = "portfolio"
portfolio_path = oms.get_data_type_system_log_path(
    config["system_log_dir"], data_type
)
_LOG.info("portfolio_path=%s", portfolio_path)
# Points to `system_log_dir/process_forecasts/orders`.
data_type = "orders"
orders_path = oms.get_data_type_system_log_path(
    config["system_log_dir"], data_type
)
_LOG.info("orders_path=%s", orders_path)

# %% [markdown]
# # System config

# %%
# TODO(Grisha): Load the system config as df instead of just printing the DAG config.
system_config = hintros.get_function_from_string(
    config["system_config_func_as_str"]
)
print(system_config)

# %% [markdown]
# # DAG io

# %% [markdown]
# ## Load

# %%
# Get DAG node names.
dag_node_names = oms.get_dag_node_names(dag_data_path)
_LOG.info(
    "First node='%s' / Last node='%s'", dag_node_names[0], dag_node_names[-1]
)

# %%
# Get timestamps for the last DAG node.
dag_node_timestamps = oms.get_dag_node_timestamps(
    dag_data_path, dag_node_names[-1], as_timestamp=True
)
_LOG.info(
    "First timestamp='%s'/ Last timestamp='%s'",
    dag_node_timestamps[0][0],
    dag_node_timestamps[-1][0],
)

# %%
# Get DAG output for the last node and the last timestamp.
dag_df_prod = oms.load_dag_outputs(dag_data_path, dag_node_names[-1])
_LOG.info("Output of last node:")
hpandas.df_to_str(dag_df_prod, num_rows=5, log_level=logging.INFO)

# %% [markdown]
# ## Compute DAG execution time

# %%
df_dag_execution_time = oms.get_execution_time_for_all_dag_nodes(dag_data_path)
_LOG.info("DAG execution time:")
hpandas.df_to_str(df_dag_execution_time, num_rows=5, log_level=logging.INFO)

# %%
oms.plot_dag_execution_stats(df_dag_execution_time, report_stats=False)

# %%
# The time is an approximation of how long it takes to process a bar. Technically the time
# is a distance (in secs) between wall clock time when an order is executed and a bar
# timestamp. The assumption is that order execution is the very last stage.
df_order_execution_time = oms.get_orders_execution_time(orders_path)
# TODO(Grisha): consider adding an assertion that checks that the time does not
# exceed one minute.
_LOG.info(
    "Max order execution time=%s secs",
    df_order_execution_time["execution_time"].max(),
)

# %% [markdown]
# # Portfolio

# %% [markdown]
# ## Compute research portfolio equivalent

# %%
# Set Portofolio start and end timestamps.
start_timestamp = dag_node_timestamps[0][0]
end_timestamp = dag_node_timestamps[-1][0]
_LOG.info("start_timestamp=%s", start_timestamp)
_LOG.info("end_timestamp=%s", end_timestamp)

# %%
fep = dtfmod.ForecastEvaluatorFromPrices(
    **config["research_forecast_evaluator_from_prices"]["init"]
)
annotate_forecasts_kwargs = config["research_forecast_evaluator_from_prices"][
    "annotate_forecasts_kwargs"
].to_dict()
research_portfolio_df, research_portfolio_stats_df = fep.annotate_forecasts(
    dag_df_prod,
    **annotate_forecasts_kwargs,
    compute_extended_stats=True,
)
# TODO(gp): Move it to annotate_forecasts?
research_portfolio_df = research_portfolio_df.sort_index(axis=1)
# Align index with prod portfolio.
research_portfolio_df = research_portfolio_df.loc[start_timestamp:end_timestamp]
research_portfolio_stats_df = research_portfolio_stats_df.loc[
    start_timestamp:end_timestamp
]
#
hpandas.df_to_str(research_portfolio_stats_df, num_rows=5, log_level=logging.INFO)

# %% [markdown]
# ## Load logged portfolios (prod & research)

# %%
portfolio_dfs, portfolio_stats_dfs = oms.load_portfolio_dfs(
    {"prod": portfolio_path},
    config["meta"]["bar_duration"],
)
# Add research portfolio.
portfolio_dfs["research"] = research_portfolio_df
hpandas.df_to_str(portfolio_dfs["prod"], num_rows=5, log_level=logging.INFO)

# %%
# Add research df and combine into a single df.
portfolio_stats_dfs["research"] = research_portfolio_stats_df
portfolio_stats_df = pd.concat(portfolio_stats_dfs, axis=1)
#
hpandas.df_to_str(portfolio_stats_df, num_rows=5, log_level=logging.INFO)

# %% [markdown]
# ## Compute Portfolio statistics (prod vs research)

# %%
bars_to_burn = 1
coplotti.plot_portfolio_stats(portfolio_stats_df.iloc[bars_to_burn:])

# %%
stats_computer = dtfmod.StatsComputer()
stats_sxs, _ = stats_computer.compute_portfolio_stats(
    portfolio_stats_df.iloc[bars_to_burn:], config["meta"]["bar_duration"]
)
display(stats_sxs)


# %% [markdown]
# # PnL for investors

# %%
# TODO(Grisha): move to a lib.
def adjust_matplotlib_settings() -> None:
    """
    Adjust the Matplotlib settings for readability.
    """
    # Matplotlib setting to make the plots readable in presentations.
    matplotlib.rcParams.update({"font.size": 22})
    #
    BIG_SIZE = 22
    #
    plt.rc("font", size=BIG_SIZE)  # controls default text sizes
    plt.rc("axes", titlesize=BIG_SIZE)  # fontsize of the axes title
    plt.rc("axes", labelsize=BIG_SIZE)  # fontsize of the x and y labels
    plt.rc("xtick", labelsize=BIG_SIZE)  # fontsize of the tick labels
    plt.rc("ytick", labelsize=BIG_SIZE)  # fontsize of the tick labels
    plt.rc("legend", fontsize=BIG_SIZE)  # legend fontsize
    plt.rc("figure", titlesize=BIG_SIZE)  # fontsize of the figure title
    #
    matplotlib.rcParams["figure.dpi"] = 300


# TODO(Grisha): move to a lib.
def plot_cumulative_pnl(
    cumul_pnl: pd.Series,
    plot_title: str,
    *,
    save_to_tmp_file: bool = True,
    copy_to_s3: bool = True,
    s3_dst_file_path: Optional[str] = None,
) -> None:
    """
    Plot cumulative PnL.

    :param cumul_pnl: cumulative PnL
    :param plot_title: title for the plot
    :param save_to_tmp_file: save the plot locally to a tmp file if True,
        proceed otherwise
    :param copy_to_s3: copy to the saved plot to S3 if True, proceed otherwise
    :param s3_dst_file_path: path on S3 to copy the saved plot to, e.g.,
        `s3://.../system_reconciliation/test_image.png`
    """
    adjust_matplotlib_settings()
    # TODO(Grisha): set `target_gmv` properly instead.
    cumul_pnl = cumul_pnl * 1000
    ax = cumul_pnl.plot(title=plot_title)
    # Set the labels formatting, e.g., `1000.0` -> `$1,000`.
    ax.yaxis.set_major_formatter("${x:,.0f}")
    if save_to_tmp_file:
        # Save the plot locally to a tmp file.
        tmp_file_path = "/app/tmp.png"
        _LOG.info("Saving the PnL plot to %s", tmp_file_path)
        plt.savefig(tmp_file_path, bbox_inches="tight")
        if copy_to_s3:
            # Copy a tmp file to S3.
            aws_profile = "ck"
            hs3.copy_file_to_s3(tmp_file_path, s3_dst_file_path, aws_profile)
    # Saving must precede `show()`.
    plt.show()


# %%
# Save the plots only for C3a.
if (
    config["meta"]["dag_builder_name"] == "C3a"
    and config["meta"]["save_plots_for_investors"]
):
    pnl_df = portfolio_stats_df["prod"]
    # Get the number of hours for which the system is run.
    bar_duration_in_minutes = int(config["meta"]["bar_duration"].rstrip("T"))
    pnl_num_hours = pnl_df.shape[0] * bar_duration_in_minutes / 60
    # Verify that the number of hours is in [0,24] range.
    hdbg.dassert_lgt(
        0,
        pnl_num_hours,
        24,
        lower_bound_closed=False,
        upper_bound_closed=True,
    )
    # TODO(Grisha): this is a hack. Ideally we should schedule a DAG
    # after the prod run is finished to get a plot for 24 hours.
    current_et_time = hdateti.get_current_time("ET")
    if round(pnl_num_hours) == 24:
        # A 24 hours run is a complete run so save as the last 24 hours results.
        file_name = "cumulative_pnl.last_24hours.png"
        plot_title = f"Cumulative PnL for the last 24 hours, generated at {current_et_time}"
    else:
        # If a run is not complete save as last 5 minutes results.
        file_name = "cumulative_pnl.last_5minutes.png"
        plot_title = f"Cumulative PnL for the last 5 minutes, generated at {current_et_time}"
    cumul_pnl = pnl_df["pnl"].cumsum()
    s3_dst_path = os.path.join(config["s3_dst_dir"], file_name)
    save_to_tmp_file = True
    copy_to_s3 = True
    plot_cumulative_pnl(
        cumul_pnl,
        plot_title,
        save_to_tmp_file=save_to_tmp_file,
        copy_to_s3=copy_to_s3,
        s3_dst_file_path=s3_dst_path,
    )
