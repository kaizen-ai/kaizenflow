"""
Contain functions used by Master_research_backtest_analyzer notebook.

Import as:

import dataflow.model.backtest_notebook_utils as dtfmbanout
"""

import logging
from typing import Dict

import core.config as cconfig
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
