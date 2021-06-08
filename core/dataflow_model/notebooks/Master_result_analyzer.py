# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.4.2
#   kernelspec:
#     display_name: Python [conda env:.conda-p1_develop] *
#     language: python
#     name: conda-env-.conda-p1_develop-py
# ---

# %%
# %load_ext autoreload
# %autoreload 2

import collections
import logging
import os
import pathlib

import numpy as np
import pandas as pd

import core.config as cconfig
import core.finance as cfina
import core.model_evaluator as cmodel
import core.model_plotter as modplot
import core.plotting as plot
import core.signal_processing as csigna
import dataflow_lemonade.futures_returns.pipeline as dlfrp
import helpers.dbg as dbg
import helpers.env as henv
import helpers.io_ as hio
import helpers.pickle_ as hpickl
import helpers.printing as hprint

from typing import Any, Dict, Iterable, Optional

from tqdm.auto import tqdm

# %%
dbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()


# %%
def load_files(
    dst_dir: str, file_name: str, idxs: Optional[Iterable[int]] = None
) -> Dict[int, Any]:
    """
    Load according to `file_name` extension.

    Assumes subdirectories withing `dst_dir` have the following structure:
    > /dst_dir/{$RESULT_DIR_NAME}_%i/file_name
    Here `%i` denotes an integer encoded in the subdirectory name.

    The function returns the contents of the files, indexed by the integer
    extracted from the subdirectory index name.

    :param dst_dir: directory containing subdirectories of run results
    :param file_name: the file name within each run results subdirectory to
        load
    :param idxs: specific indices to load; if `None`, loads all indices
    """
    # Retrieve all subdirectories in `dst_dir`.
    p = pathlib.Path(dst_dir)
    subfolders = [f for f in p.iterdir() if f.is_dir()]
    # Order experiment subfolders by number of experiment.
    subfolders_num = {}
    keys = []
    for sf in subfolders:
        key = int(sf.parts[-1].split("_")[-1])
        subfolders_num[key] = sf
        keys.append(key)
    # Ensure there are no duplicate integer keys (e.g., due to an inconsistent
    # subdirectory naming scheme).
    dbg.dassert_no_duplicates(
        keys, "Duplicate keys detected! Check subdirectory names."
    )
    # Specify indices of files to load.
    if idxs is None:
        iter_keys = sorted(keys)
    else:
        idxs_l = set(idxs)
        dbg.dassert_is_subset(idxs_l, set(keys))
        iter_keys = [key for key in sorted(keys) if key in idxs_l]
    # Iterate over experiment subfolders.
    results = collections.OrderedDict()
    for key in tqdm(iter_keys):
        subfolder = subfolders_num[key]
        path_to_file = os.path.join(dst_dir, subfolder, file_name)
        if not os.path.exists(path_to_file):
            _LOG.warning("File `%s` does not exist.", path_to_file)
            continue
        # Load pickle files.
        if file_name.endswith(".pkl"):
            res = hpickl.from_pickle(
                path_to_file, log_level=logging.DEBUG, verbose=False
            )
        # Load json files.
        elif file_name.endswith(".json"):
            with open(path_to_file, "r") as file:
                res = json.load(file)
        # Load txt files.
        elif file_name.endswith(".txt"):
            res = hio.from_file(path_to_file)
        else:
            raise ValueError(f"Unsupported file type='{file_name}'")
        results[key] = res
    return results


def get_config_diffs(
    config_dict: collections.OrderedDict, tag_col: Optional[str] = None
) -> pd.DataFrame:
    """
    Create a dataframe of config diffs.

    :param config_dict: dictionary of configs
    :param tag_col: name of the tag col. If tags are the same for all configs
        and `tag_col` is not None, add tags to config diffs dataframe
    :return: config diffs dataframe
    """
    diffs = cconfig.diff_configs(config_dict.values())
    non_empty_diffs = [diff for diff in diffs if len(diff) > 1]
    if non_empty_diffs:
        config_diffs = cconfig.convert_to_dataframe(diffs).dropna(
            how="all", axis=1
        )
    else:
        config_diffs = pd.DataFrame(index=range(len(diffs)))
    # If tags are the same, still add them to `config_diffs`.
    if tag_col is not None and tag_col not in config_diffs.columns:
        tags = [config[tag_col] for config in config_dict.values()]
        config_diffs[tag_col] = tags
    return config_diffs

# %% [markdown]
# # Load RH2E results

# %%
exp_dir = "/Users/paul/src/alphamatic/lemonade/experiments/RH2E_experiments/run1"

# %%
rbs = load_files(exp_dir, "result_bundle.pkl")

# %%
rbs[0]["result_df"].head()


# %%
def get_rets_and_preds(rbs):
    rets = {}
    preds = {}
    for key, rb in rbs.items():
        df = rb["result_df"]
        rets[key] = df["vwap_ret_0_zscored_2"]
        preds[key] = df["vwap_ret_0_zscored_2_hat"]
    return rets, preds


# %%
rets, preds = get_rets_and_preds(rbs)

# %%
evaluator = cmodel.ModelEvaluator(
    returns=rets,
    predictions=preds,
    target_volatility=0.1,
    oos_start="2017-12-30",
)
plotter = modplot.ModelPlotter(evaluator)

# %%
evaluator.calculate_stats()

# %%
evaluator.aggregate_models(mode="ins", target_volatility=0.05)[2].to_frame()

# %%
evaluator.aggregate_models(mode="oos")[2].to_frame()

# %%
evaluator.aggregate_models(mode="all_available")[2].to_frame()

# %%
plotter.plot_performance(resample_rule="B")

# %%
plotter.plot_performance(
    mode="ins",
    target_volatility=0.05,
    resample_rule="B",
    plot_cumulative_returns_kwargs={"mode": "log"},
)

# %%
plotter.plot_sharpe_ratio_panel(frequencies=["B", "W", "M"])

# %%
plotter.plot_performance(
    mode="all_available",
    target_volatility=0.05,
    resample_rule="B",
    plot_cumulative_returns_kwargs={"mode": "log"},
)

# %%
pnl, pos, stats = evaluator.aggregate_models(mode="all_available", target_volatility=0.05)

# %% [markdown]
# # Calculate ES returns (as a proxy for SPY)

# %%
rb = dlfrp.ReturnsBuilder()

# %%
config = rb.get_config_template()

# %%
config["rets/read_data", "source_node_kwargs", "symbol"] = "ES"

# %%
config["rets/read_data", "source_node_kwargs", "end_date"] = None

# %%
config

# %%
dag = rb.get_dag(config, mode="loose")

# %%
es_rets = dag.run_leq_node("rets/clip", "fit")["df_out"]

# %%
dag.dag.nodes

# %%
es_rets.head()

# %%
es_twap_rets = cfina.resample_time_bars(es_rets, "B", return_cols=["twap_ret_0"])["twap_ret_0"]

# %%
es_twap_rets.head()

# %%
es_twap = es_twap_rets.cumsum()

# %%
es_twap.plot()

# %% [markdown]
# # Calculate correlation of portfolio with ES

# %%
pnl_srs = pnl.resample("B").sum(min_count=1)

# %%
pnl_srs.head()

# %%
{shift: es_twap_rets.corr(pnl_srs.shift(shift)) for shift in list(range(-7, 8, 1))}

# %%
pnl_srs.shift(-2)

# %%
pnl_srs.cumsum().plot()

# %%
es_twap_rets.corr(pnl_srs)

# %%
plot.plot_rolling_correlation(
    srs1=es_twap_rets,
    srs2=pnl_srs.shift(0),
    demean=False,
    tau=365,
    min_periods=90,
    min_depth=2,
    max_depth=3,
    ylim="fixed")

# %%
plot.plot_rolling_correlation(
    srs1=es_twap_rets,
    srs2=pnl_srs.shift(0),
    demean=False,
    tau=365,
    min_periods=90,
    min_depth=2,
    max_depth=3,
)

# %%
plot.plot_rolling_correlation(
    srs1=es_twap_rets,
    srs2=pnl_srs.shift(-2),
    demean=False,
    tau=365/4,
    min_periods=90,
    min_depth=2,
    max_depth=3,
    ylim="fixed"
)

# %%
csigna.compute_rolling_corr(
    es_twap_rets,
    pnl_srs,
    tau=10
).dropna()

# %% [markdown]
# # Analyze portfolio exposure

# %%
pnl, pos, stats = evaluator.aggregate_models(mode="all_available")

# %%
pos.resample("B").sum(min_count=1).plot()

# %%
preds_daily = {k: preds[k].resample("B").sum(min_count=1) for k in preds.keys()}

# %%
df = pd.DataFrame.from_dict(preds_daily)

# %%
## Weights in proportion to signal strength, equally weighted across equities

# %%
df.mean(axis=1).max()

# %%
df.mean(axis=1).abs().mean()

# %%
df.mean(axis=1).mean()

# %%
df.mean(axis=1).min()

# %%
df.mean(axis=1).abs().hist(bins=101)

# %%
df.mean(axis=1).hist(bins=101)

# %%
## Weight equally by equity, but ignore signal strength

# %%
df_pos = (df > 0).astype(int)

# %%
df_neg = -1*(df < 0).astype(int)

# %%
df_preds = df_pos + df_neg

# %%
df_preds.head()

# %%
signed_bets = df_preds.sum(axis=1)

# %%
signed_bets.head()

# %%
bets = df_preds.apply(abs).sum(axis=1)

# %%
bets.head()

# %%
(signed_bets / bets).plot()

# %%
(signed_bets / bets).hist(bins=10)

# %%
