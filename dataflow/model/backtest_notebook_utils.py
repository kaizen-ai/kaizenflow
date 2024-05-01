"""
Contain functions used by Master_research_backtest_analyzer notebook.

Import as:

import dataflow.model.backtest_notebook_utils as dtfmbanout
"""

import logging
from typing import Dict, List

import pandas as pd

import core.config as cconfig
import core.finance as cofinanc
import dataflow.core as dtfcore
import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)


def build_research_backtest_analyzer_config_sweep(
    default_config: cconfig.Config,
) -> Dict[str, cconfig.Config]:
    """
    Build a dict of configs to run a backtest sweep.

    :param default_config: config for the backtest analysis
    :return: dict of configs to run the backtest analysis
    """
    if "sweep_param" in default_config:
        hdbg.dassert_isinstance(default_config["sweep_param"], cconfig.Config)
        # Set param values to sweep and corressponding config keys.
        sweep_param_keys = default_config["sweep_param", "keys"]
        hdbg.dassert_isinstance(sweep_param_keys, tuple)
        sweep_param_values = default_config["sweep_param", "values"]
        hdbg.dassert_isinstance(sweep_param_values, list)
        # Build config dict.
        config_dict = {}
        for val in sweep_param_values:
            # Update new config value.
            config = default_config.copy()
            config.update_mode = "overwrite"
            config[sweep_param_keys] = val
            config.update_mode = "assert_on_overwrite"
            # Set updated config key for config dict.
            config_dict_key = ":".join(sweep_param_keys)
            config_dict_key = " = ".join([config_dict_key, str(val)])
            # Add new config to the config dict.
            config_dict[config_dict_key] = config
    else:
        # Put single input config to a dict.
        config_dict = {"default_config": default_config}
    return config_dict


def resample_with_weights_ohlcv_bars(
    df_ohlcv: pd.DataFrame,
    price_col: str,
    bar_duration: str,
    weights: List[float],
) -> pd.DataFrame:
    """
    Resample 1-minute data to `bar_duration` with weights.

    :param df_ohlcv: input OHLCV data
    :param price_col: price column
    :param bar_duration: bar duration
    :param weights: weights for resampling
    :return: resampled OHLCV data
    """
    resampling_node = dtfcore.GroupedColDfToDfTransformer(
        "resample",
        transformer_func=cofinanc.resample_with_weights,
        **{
            "in_col_groups": [
                (price_col,),
            ],
            "out_col_group": (),
            "transformer_kwargs": {
                "rule": bar_duration,
                "col": price_col,
                "weights": weights,
            },
            "reindex_like_input": False,
            "join_output_with_input": False,
        },
    )
    resampled_ohlcv = resampling_node.fit(df_ohlcv)["df_out"]
    return resampled_ohlcv
