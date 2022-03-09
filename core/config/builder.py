"""
Import as:

import core.config.builder as cconbuil
"""

import importlib
import itertools
import logging
import os
import re
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, cast

import core.config.config_ as cconconf
import core.config.utils as cconutil
import helpers.hdbg as hdbg
import helpers.hpickle as hpickle

_LOG = logging.getLogger(__name__)


# #############################################################################
# Experiment builders.
# #############################################################################


def get_configs_from_builder(config_builder: str) -> List[cconconf.Config]:
    """
    Execute Python code `config_builder` to build configs.

    :param config_builder: full Python command to create the configs.
        E.g., `nlp.build_configs.build_PTask1088_configs()`
    """
    _LOG.info("Executing function '%s'", config_builder)
    # `config_builder` looks like:
    #   `nlp.build_configs.build_PTask1088_configs()`
    # or
    #   `research.RH8E.RH8Ed_configs.build_rc1_configs("eg_v2_0-all.5T.2015_2022")`
    m = re.match(r"^(\S+)\.(\S+)\((.*)\)$", config_builder)
    hdbg.dassert(m, "config_builder='%s'", config_builder)
    # TODO(gp): Fix this.
    m = cast(re.Match, m)
    import_, function, args = m.groups()
    _LOG.debug("import=%s", import_)
    _LOG.debug("function=%s", function)
    _LOG.debug("args=%s", args)
    # Import the needed module.
    imp = importlib.import_module(import_)
    # Force the linter not to remove this import which is needed in the following
    # eval.
    _ = imp
    python_code = "imp.%s(%s)" % (function, args)
    _LOG.debug("executing '%s'", python_code)
    configs: List[cconconf.Config] = eval(python_code)
    hdbg.dassert_is_not(configs, None)
    # Cast to the right type.
    # TODO(gp): Is this needed?
    # configs = cast(List[cconconf.Config], configs)
    cconutil.validate_configs(configs)
    return configs


# /////////////////////////////////////////////////////////////////////////////////


def patch_configs(
    configs: List[cconconf.Config], experiment_list_params: Dict[str, str]
) -> List[cconconf.Config]:
    """
    Patch the configs with information needed to run.

    This function is used by `run_notebook.py` and `run_experiment.py`
    to pass information through the `Config` to the process running the
    experiment.
    """
    configs_out = []
    for idx, config in enumerate(configs):
        config = config.copy()
        # Add `idx` for book-keeping.
        config[("meta", "id")] = idx
        # Inject all the experiment_list_params in the config.
        for key in sorted(experiment_list_params.keys()):
            config[("meta", key)] = experiment_list_params[key]
        # Inject the dst dir of the entire experiment list.
        hdbg.dassert_in("dst_dir", experiment_list_params)
        dst_dir = experiment_list_params["dst_dir"]
        # Add experiment result dir.
        dst_subdir = f"result_{idx}"
        experiment_result_dir = os.path.join(dst_dir, dst_subdir)
        config[("meta", "experiment_result_dir")] = experiment_result_dir
        #
        configs_out.append(config)
    return configs_out


def get_config_from_experiment_list_params(
    idx: int, experiment_list_params: Dict[str, str]
) -> cconconf.Config:
    """
    Get the `idx`-th config built from the experiment list params, which includes
    `config_builder` (e.g.,
    `research.RH8E.RH8Ed_configs.build_rc1_configs("eg_v2_0-all.5T.2015_2022")`)

    This is used by `run_experiment_stub.py` using the params from command line.

    :param experiment_list_params: parameters from `run_experiment_stub.py`, e.g.,
        `config_builder`, `experiment_builder`, `dst_dir`.
    """
    config_builder = experiment_list_params["config_builder"]
    # Build all the configs.
    configs = get_configs_from_builder(config_builder)
    # Patch the configs with experiment metadata.
    configs = patch_configs(configs, experiment_list_params)
    # Pick the config.
    hdbg.dassert_lte(0, idx)
    hdbg.dassert_lt(idx, len(configs))
    config = configs[idx]
    config = config.copy()
    return config


# /////////////////////////////////////////////////////////////////////////////////


def get_config_from_env() -> Optional[cconconf.Config]:
    """
    Build a config passed through environment vars, if possible, or return
    `None`.
    """
    config_vars = ["__CONFIG_BUILDER__", "__CONFIG_IDX__", "__CONFIG_DST_DIR__"]
    # Check the existence of any config var in env.
    if not any(var in os.environ for var in config_vars):
        _LOG.debug("No CONFIG* env vars for building config: returning")
        config = None
        return config
    _LOG.warning("Found config vars in environment")
    hdbg.dassert(
        all(var in os.environ for var in config_vars),
        "Some config vars '%s' were defined, but not all"
        % (", ".join(config_vars)),
    )
    params = {}
    #
    config_idx = int(os.environ["__CONFIG_IDX__"])
    _LOG.info("config_idx=%s", config_idx)
    #
    config_builder = os.environ["__CONFIG_BUILDER__"]
    _LOG.info("config_builder=%s", config_builder)
    params["config_builder"] = config_builder
    #
    # TODO(gp): -> config_dst_dir?
    dst_dir = os.environ["__CONFIG_DST_DIR__"]
    _LOG.info("dst_dir=%s", dst_dir)
    params["dst_dir"] = dst_dir
    #
    config = get_config_from_experiment_list_params(config_idx, params)
    #
    return config
