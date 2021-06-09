"""
Import as:

import core.config.builder as cfgb
"""

import collections
import importlib
import itertools
import logging
import os
import re
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
    cast,
)

import core.config.config_ as cconfig
import core.config.utils as cfgut
import helpers.dbg as dbg
import helpers.pickle_ as hpickle

_LOG = logging.getLogger(__name__)


# #############################################################################
# Experiment builders.
# #############################################################################


def get_configs_from_builder(config_builder: str) -> List[cconfig.Config]:
    """
    Execute Python code `config_builder` to build configs.

    :param config_builder: full Python command to create the configs.
        E.g., `nlp.build_configs.build_PTask1088_configs()`
    """
    _LOG.info("Executing function '%s'", config_builder)
    # config_builder looks like:
    #   "nlp.build_configs.build_PTask1088_configs()"
    m = re.match(r"^(\S+)\.(\S+)\((.*)\)$", config_builder)
    dbg.dassert(m, "config_builder='%s'", config_builder)
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
    configs: List[cconfig.Config] = eval(python_code)
    dbg.dassert_is_not(configs, None)
    # Cast to the right type.
    # TODO(gp): Is this needed?
    # configs = cast(List[cconfig.Config], configs)
    cfgut.validate_configs(configs)
    return configs


def patch_configs(
    configs: List[cconfig.Config], params: Dict[str, str]
) -> List[cconfig.Config]:
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
        # Inject all the params in the config.
        for key in sorted(params.keys()):
            config[("meta", key)] = params[key]
        # Inject the experiment result dir.
        dbg.dassert_in("dst_dir", params)
        dst_dir = params["dst_dir"]
        # Add experiment result dir.
        dst_subdir = f"result_{idx}"
        experiment_result_dir = os.path.join(dst_dir, dst_subdir)
        config[("meta", "experiment_result_dir")] = experiment_result_dir
        #
        configs_out.append(config)
    return configs_out


def get_config_from_params(idx: int, params: Dict[str, str]) -> cconfig.Config:
    """
    Get the `idx`-th config built from the params, which includes
    `config_builder`.
    """
    config_builder = params["config_builder"]
    # Build all the configs.
    configs = get_configs_from_builder(config_builder)
    # Patch the configs with metadata.
    configs = patch_configs(configs, params)
    # Pick the config.
    dbg.dassert_lte(0, idx)
    dbg.dassert_lt(idx, len(configs))
    config = configs[idx]
    config = config.copy()
    return config


def get_config_from_env() -> Optional[cconfig.Config]:
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
    dbg.dassert(
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
    config = get_config_from_params(config_idx, params)
    #
    return config


# #############################################################################

# TODO(gp): Not clear what this does and if it's needed.


def _generate_template_config(
    config: cconfig.Config,
    params_variants: Dict[Tuple[str, ...], Iterable[Any]],
) -> cconfig.Config:
    """
    Assign `None` to variable parameters in KOTH config.

    A preliminary step required to generate multiple configs.

    :param config: Config to transform into template
    :param params_variants: Config paths to variable parameters and their values
    :return: Template config object
    """
    template_config = config.copy()
    for path in params_variants.keys():
        template_config[path] = None
    return template_config


def generate_default_config_variants(
    template_config_builder: Callable,
    params_variants: Optional[Dict[Tuple[str, ...], Iterable[Any]]] = None,
) -> List[cconfig.Config]:
    """
    Build a list of config files for experiments.

    TODO(*): What experiments? What is a KOTH-generating function?

    This is the base function to be wrapped into specific config-generating functions.
    It is assumed that for each research purpose there will be a KOTH-generating
    function. At the moment, the only such function is `ncfgbld.get_KOTH_config`, which
    accepts no parameters.

    :param template_config_builder: Function used to generate default config.
    :param params_variants: Config paths to variable parameters and their values
    :return: Configs with different parameters.
    """
    config = template_config_builder()
    if params_variants is not None:
        template_config = _generate_template_config(config, params_variants)
        configs = build_multiple_configs(template_config, params_variants)
    else:
        configs = [config]
    return configs


def load_configs(results_dir: str) -> List[cconfig.Config]:
    """
    Load all result pickles and save in order of corresponding configs.

    TODO(*): What results? Also, the function is called `load_configs()` and
        yet the 1-line summary starts by discussing loading results.

    :param results_dir: Directory with results of experiments.
    :return: All result configs and result dataframes.
    """
    # TODO(*): Move function to a different lib.
    configs = []
    result_subfolders = os.listdir(results_dir)
    for subfolder in result_subfolders:
        config_path = os.path.join(results_dir, subfolder, "config.pkl")
        config = hpickle.from_pickle(config_path)
        configs.append(config)
    # Sort configs by order of simulations.
    configs = sorted(configs, key=lambda x: x[("meta", "id")])
    return configs


def build_multiple_configs(
    template_config: cconfig.Config,
    params_variants: Dict[Tuple[str, ...], Iterable[Any]],
) -> List[cconfig.Config]:
    """
    Build configs from a template and the Cartesian product of given keys/vals.

    Create multiple `cconfig.Config` objects using the given config template and
    overwriting `None` or `_DUMMY_` parameter specified through a parameter
    path and several possible elements:
        param_path: Tuple(str) -> param_values: Iterable[Any]
    A parameter path is represented by a tuple of nested names.

    Note that we create a config for each element of the Cartesian product of
    the values to be assigned.

    :param template_config: cconfig.Config object
    :param params_variants: {(param_name_in_the_config_path):
        [param_values]}, e.g. {('read_data', 'symbol'): ['CL', 'QM'],
                                ('resample', 'rule'): ['5T', '10T']}
    :return: a list of configs
    """
    # In the example from above:
    # ```
    # list(params_values) = [('CL', '5T'), ('CL', '10T'), ('QM', '5T'), ('QM', '10T')]
    # ```
    params_values = itertools.product(*params_variants.values())
    param_vars = list(
        dict(zip(params_variants.keys(), values)) for values in params_values
    )
    # In the example above:
    # ```
    # param_vars = [
    #    {('read_data', 'symbol'): 'CL', ('resample', 'rule'): '5T'},
    #    {('read_data', 'symbol'): 'CL', ('resample', 'rule'): '10T'},
    #    {('read_data', 'symbol'): 'QM', ('resample', 'rule'): '5T'},
    #    {('read_data', 'symbol'): 'QM', ('resample', 'rule'): '10T'},
    #  ]
    # ```
    param_configs = []
    for params in param_vars:
        # Create a config for the chosen parameter values.
        config_var = template_config.copy()
        for param_path, param_val in params.items():
            # Select the path for the parameter and set the parameter.
            conf_tmp = config_var
            for pp in param_path[:-1]:
                conf_tmp.check_params([pp])
                conf_tmp = conf_tmp[pp]
            conf_tmp.check_params([param_path[-1]])
            if not (
                conf_tmp[param_path[-1]] is None
                or conf_tmp[param_path[-1]] == "_DUMMY_"
            ):
                raise ValueError(
                    "Trying to change a parameter that is not `None` or "
                    "`'_DUMMY_'`. Parameter path is %s" % str(param_path)
                )
            conf_tmp[param_path[-1]] = param_val
        param_configs.append(config_var)
    return param_configs
