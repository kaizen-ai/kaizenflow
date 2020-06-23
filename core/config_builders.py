"""
Import as:

import core.config_builders as cfgb

# It is?
Tested in: nlp/test_config_builders.py
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

import pandas as pd

import core.config as cfg
import helpers.dbg as dbg
import helpers.dict as dct
import helpers.pickle_ as hpickle

_LOG = logging.getLogger(__name__)


def get_config_from_flattened(flattened: Dict[Tuple[str], Any]) -> cfg.Config:
    """
    Build a config from the flattened config representation.

    :param flattened: flattened config like result from `config.flatten()`
    :return: config object initialized from flattened representation
    """
    dbg.dassert_isinstance(flattened, dict)
    dbg.dassert(flattened)
    config = cfg.Config()
    for k, v in flattened.items():
        config[k] = v
    return config


def get_config_from_nested_dict(nested: Dict[str, Any]) -> cfg.Config:
    """
    Build a config from a nested dict.

    :param nested: nested dict, with certain restrictions:
      - only leaf nodes may not be a dict
      - every nonempty dict must only have keys of type `str`
    """
    dbg.dassert_isinstance(nested, dict)
    dbg.dassert(nested)
    iter_ = dct.get_nested_dict_iterator(nested)
    flattened = collections.OrderedDict(iter_)
    return get_config_from_flattened(flattened)


def get_configs_from_builder(config_builder: str) -> List[cfg.Config]:
    """
    Execute python code to

    :param config_builder: full Python command to create the configs.
        E.g., `nlp.build_configs.build_PartTask1088_configs()`
    """
    # config_builder looks like:
    #   "nlp.build_configs.build_PartTask1088_configs()"
    m = re.match(r"^(\S+)\.(\S+)\((.*)\)$", config_builder)
    dbg.dassert(m, "config_builder='%s'", config_builder)
    import_, function, args = m.groups()
    _LOG.debug("import=%s", import_)
    _LOG.debug("function=%s", function)
    _LOG.debug("args=%s", args)
    #
    imp = importlib.import_module(import_)
    # Force the linter not to remove this import which is needed in the
    # following eval.
    _ = imp
    python_code = "imp.%s(%s)" % (function, args)
    _LOG.debug("executing '%s'", python_code)
    configs: List[cfg.Config] = eval(python_code)
    dbg.dassert_is_not(configs, None)
    # Cast to the right type.
    configs = cast(List[cfg.Config], configs)
    dbg.dassert_isinstance(configs, list)
    for c in configs:
        dbg.dassert_isinstance(c, cfg.Config)
    return configs


def get_config_from_env() -> Optional[cfg.Config]:
    """
    Build a config passed through an environment variable, if possible,
    or return None.
    """
    config_vars = ["__CONFIG_BUILDER__", "__CONFIG_IDX__", "__CONFIG_DST_DIR__"]
    # Check the existence of any config var in env.
    if any(var in os.environ for var in config_vars):
        _LOG.warning("Found some config vars in environment")
        if all(var in os.environ for var in config_vars):
            # Build configs.
            config_builder = os.environ["__CONFIG_BUILDER__"]
            _LOG.info("__CONFIG_BUILDER__=%s", config_builder)
            configs = get_configs_from_builder(config_builder)
            # Add destination directory.
            dst_dir = os.environ["__CONFIG_DST_DIR__"]
            _LOG.info("__DST_DIR__=%s", dst_dir)
            configs = add_result_dir(dst_dir, configs)
            # Pick config with relevant index.
            config_idx = int(os.environ["__CONFIG_IDX__"])
            _LOG.info("__CONFIG_IDX__=%s", config_idx)
            dbg.dassert_lte(0, config_idx)
            dbg.dassert_lt(config_idx, len(configs))
            config = configs[config_idx]
            # Set file path by index.
            config = set_experiment_result_dir(dst_dir, config)
        else:
            msg = "Some config vars '%s' were defined, but not all" % (
                ", ".join(config_vars)
            )
            raise RuntimeError(msg)
    else:
        config = None
    return config


# #############################################################################


# TODO(*): Is this used anywhere?
def assert_on_duplicated_configs(configs: List[cfg.Config]) -> None:
    """
    Assert if the list of configs contains no duplicates.

    :param configs: List of configs to run experiments on.
    """
    configs_as_str = [str(config) for config in configs]
    dbg.dassert_no_duplicates(
        configs_as_str, msg="There are duplicate configs in passed list."
    )


# TODO(*): Deprecate.
def _flatten_config(config: cfg.Config) -> Dict[str, collections.abc.Hashable]:
    """
    Flatten configs, join tuples of strings with "." and make vals hashable.

    Someday you may realize that you want to use "." in the strings of your
    keys. That likely won't be a very fun day.
    """
    flattened = config.flatten()
    normalized = {}
    for k, v in flattened.items():
        val = cfg.make_hashable(v)
        normalized[".".join(k)] = val
    return normalized


# TODO(*): Deprecate.
def _flatten_configs(configs: Iterable[cfg.Config]) -> List[Dict[str, Any]]:
    """
    Flatten configs, squash the str keys, and make vals hashable.

    :param configs: configs
    :return: flattened config dicts
    """
    return list(map(_flatten_config, configs))


# TODO(*): Deprecate.
def get_config_intersection(configs: List[cfg.Config]) -> cfg.Config:
    """
    Compare configs from list to find the common part.

    :param configs: A list of configs
    :return: A config with common part of all input configs.
    """
    return cfg.intersect_configs(configs)


# TODO(*): Are the values of this ever used anywhere?
# TODO(*): Try to deprecate. If needed, compose with `cfg.diff_configs()`.
def get_config_difference(configs: List[cfg.Config]) -> Dict[str, List[Any]]:
    """
    Find parameters in configs that are different and provide the varying values.

    :param configs: A list of configs.
    :return: A dictionary of varying params and lists of their values.
    """
    # Flatten configs into dicts.
    flattened_configs = _flatten_configs(configs)
    # Convert dicts into sets of items for comparison.
    flattened_configs = [set(config.items()) for config in flattened_configs]
    # Build a dictionary of common config values.
    union = set.union(*flattened_configs)
    intersection = set.intersection(*flattened_configs)
    config_varying_params = union - intersection
    # Compute params that vary among different configs.
    config_varying_params = dict(config_varying_params).keys()
    # Remove `meta` params that always vary.
    # TODO(*): Where do these come from?
    redundant_params = ["meta.id", "meta.experiment_result_dir"]
    config_varying_params = [
        param for param in config_varying_params if param not in redundant_params
    ]
    # Build the difference of configs by considering the parts that vary.
    config_difference = dict()
    for param in config_varying_params:
        param_values = []
        for flattened_config in flattened_configs:
            try:
                param_values.append(dict(flattened_config)[param])
            except KeyError:
                param_values.append(None)
        config_difference[param] = param_values
    return config_difference


# TODO(*): Deprecate. Switch to `cfg.convert_to_dataframe()`.
def get_configs_dataframe(
    configs: List[cfg.Config],
    params_subset: Optional[Union[str, List[str]]] = None,
) -> pd.DataFrame:
    """
    Convert the configs into a df with full nested names.

    The column names should correspond to `subconfig1.subconfig2.parameter`
    format, e.g.: `build_targets.target_asset`.

    :param configs: Configs used to run experiments. TODO(*): What experiments?
    :param params_subset: Parameters to include as table columns.
    :return: Table of configs.
    """
    # Convert configs to flattened dicts.
    flattened_configs = _flatten_configs(configs)
    # Convert dicts to pd.Series and create a df.
    config_df = map(pd.Series, flattened_configs)
    config_df = pd.concat(config_df, axis=1).T
    # Process the config_df by keeping only a subset of keys.
    if params_subset is not None:
        if params_subset == "difference":
            config_difference = get_config_difference(configs)
            params_subset = list(config_difference.keys())
        # Filter config_df for the desired columns.
        dbg.dassert_is_subset(params_subset, config_df.columns)
        config_df = config_df[params_subset]
    return config_df


# #############################################################################


def add_result_dir(dst_dir: str, configs: List[cfg.Config]) -> List[cfg.Config]:
    """
    Add a result directory field to all configs in list.

    :param dst_dir: Location of output directory
    :param configs: List of configs for experiments
    :return: List of copied configs with result directories added
    """
    # TODO(*): To be defensive maybe we should assert if the param already exists.
    configs_with_dir = []
    for config in configs:
        config_with_dir = config.copy()
        config_with_dir[("meta", "result_dir")] = dst_dir
        configs_with_dir.append(config_with_dir)
    return configs_with_dir


def set_experiment_result_dir(dst_dir: str, config: cfg.Config) -> cfg.Config:
    """
    Set path to the experiment results file.

    :param dst_dir: Subdirectory with simulation results
    :param config: Config used for simulation
    :return: Config with absolute file path to results
    """
    config_with_filepath = config.copy()
    config_with_filepath[("meta", "experiment_result_dir")] = dst_dir
    return config_with_filepath


def add_config_idx(configs: List[cfg.Config]) -> List[cfg.Config]:
    """
    Add the config id as parameter.

    TODO(*): What is "the config id"? Why does my config have a `meta`? And why
        would this ever depend upon the order in which the configs appear in a
        list?

    :param configs: List of configs for experiments
    :return: List of copied configs with added ids
    """
    configs_idx = []
    for i, config in enumerate(configs):
        config_with_id = config.copy()
        config_with_id[("meta", "id")] = i
        configs_idx.append(config_with_id)
    return configs_idx


# #############################################################################


def _generate_template_config(
    config: cfg.Config, params_variants: Dict[Tuple[str, ...], Iterable[Any]],
) -> cfg.Config:
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
) -> List[cfg.Config]:
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


def load_configs(results_dir: str) -> List[cfg.Config]:
    """
    Load all result pickles and save in order of corresponding configs.

    TODO(*): What results? Also, the function is called `load_configs()` and
        yet the 1-line summary starts by discussing loading results.

    :param results_dir: Directory with results of experiments.
    :return: All result configs and result dataframes.
    """
    # TODO (*) Move function to a different lib.
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
    template_config: cfg.Config,
    params_variants: Dict[Tuple[str, ...], Iterable[Any]],
) -> List[cfg.Config]:
    """
    Build configs from a template and the Cartesian product of given keys/vals.

    Create multiple `cfg.Config` objects using the given config template and
    overwriting `None` or `_DUMMY_` parameter specified through a parameter
    path and several possible elements:
        param_path: Tuple(str) -> param_values: Iterable[Any]
    A parameter path is represented by a tuple of nested names.

    Note that we create a config for each element of the Cartesian product of
    the values to be assigned.

    :param template_config: cfg.Config object
    :param params_variants: {(param_name_in_the_config_path):
        [param_values]}, e.g. {('read_data', 'symbol'): ['CL', 'QM'],
                                ('resample', 'rule'): ['5T', '10T']}
    :return: a list of configs
    """
    # In the example from above, list(params_values) = [('CL', '5T'),
    # ('CL', '10T'), ('QM', '5T'), ('QM', '10T')]
    params_values = itertools.product(*params_variants.values())
    param_vars = list(
        dict(zip(params_variants.keys(), values)) for values in params_values
    )
    # In the example above, param_vars = [
    #    {('read_data', 'symbol'): 'CL', ('resample', 'rule'): '5T'},
    #    {('read_data', 'symbol'): 'CL', ('resample', 'rule'): '10T'},
    #    {('read_data', 'symbol'): 'QM', ('resample', 'rule'): '5T'},
    #    {('read_data', 'symbol'): 'QM', ('resample', 'rule'): '10T'},
    #  ]
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
