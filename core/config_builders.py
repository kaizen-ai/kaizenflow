import itertools
import logging
import os
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd

import core.config as cfg
import helpers.dbg as dbg
import helpers.dict as dct
import helpers.pickle_ as hpickle

_LOG = logging.getLogger(__name__)


def check_same_configs(configs: List[cfg.Config]) -> None:
    """
    Assert whether the list of configs contains no duplicates.
    :param configs: List of configs to run experiments on.
    :return:
    """
    configs_as_str = [str(config) for config in configs]
    dbg.dassert_no_duplicates(
        configs_as_str, msg="There are duplicate configs in passed list."
    )


def get_config_intersection(configs: List[cfg.Config]) -> cfg.Config:
    """
    Compare configs from list to find the common part.

    :param configs: A list of configs
    :return: A config with common part of all input configs.
    """
    flattened_configs = []
    for config in configs:
        flattened_config = config.to_dict()
        flattened_config = dct.flatten_nested_dict(flattened_config)
        flattened_configs.append(set(flattened_config.items()))
    config_intersection = dict(set.intersection(*flattened_configs))
    common_config = cfg.Config()
    for k, v in config_intersection.items():
        common_config[tuple(k.split("."))] = v
    return common_config


def get_config_difference(configs: List[cfg.Config]) -> Dict[str, List[Any]]:
    """
    Find parameters in configs that are different and provide the varying values.

    :param configs: A list of configs.
    :return: A dictionary of varying params and lists of their values.
    """
    flattened_configs = []
    redundant_params = ["meta.id", "meta.result_file_name"]
    for config in configs:
        flattened_config = config.to_dict()
        flattened_config = dct.flatten_nested_dict(flattened_config)
        flattened_configs.append(set(flattened_config.items()))
    config_varying_params = set.union(*flattened_configs) - set.intersection(
        *flattened_configs
    )
    config_varying_params = dict(config_varying_params).keys()
    # Remove `meta` params that always vary.
    config_varying_params = [
        param for param in config_varying_params if param not in redundant_params
    ]
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


def get_configs_dataframe(
    configs: List[cfg.Config],
    params_subset: Optional[Union[str, List[str]]] = None,
) -> pd.DataFrame:
    """
    Convert the configs into a df with full nested names.

    The column names should correspond to `subconfig.subconfig.parameter` format, e.g.:
    `build_targets.target_asset`.
    :param configs: Configs used to run experiments.
    :param params_subset: Parameters to include as table columns.
    :return: Table of configs.
    """
    config_df = []
    for config in configs:
        flattened_config = config.to_dict()
        flattened_config = dct.flatten_nested_dict(flattened_config)
        flattened_config = pd.Series(flattened_config)
        config_df.append(flattened_config)
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


def add_result_dir(dst_dir: str, configs: List[cfg.Config]) -> List[cfg.Config]:
    """
    Add a result directory field to all configs in list.
    :param dst_dir: Location of output directory
    :param configs: List of configs for experiments
    :return: List of copied configs with result directories added
    """
    configs_with_dir = []
    for config in configs:
        config_with_dir = config.copy()
        config_with_dir[("meta", "result_dir")] = dst_dir
        configs_with_dir.append(config_with_dir)
    return configs_with_dir


def set_absolute_result_file_path(sim_dir: str, config: cfg.Config) -> cfg.Config:
    """
    Add absolute path to the simulation results file.
    :param sim_dir: Subdirectory with simulation results
    :param config: Config used for simulation
    :return: Config with absolute file path to results
    """
    config_with_filepath = config.copy()
    config_with_filepath[("meta", "result_file_name")] = os.path.join(
        sim_dir, config_with_filepath[("meta", "result_file_name")]
    )
    return config_with_filepath


def add_config_idx(configs: List[cfg.Config]) -> List[cfg.Config]:
    """
    Add the config id as parameter.
    :param configs: List of configs for experiments
    :return: List of copied configs with added ids
    """
    configs_idx = []
    for i, config in enumerate(configs):
        config_with_id = config.copy()
        config_with_id[("meta", "id")] = i
        configs_idx.append(config_with_id)
    return configs_idx


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

    This is the base function to be wrapped into specific config-generating functions.
    It is assumed that for each research purpose there will be a KOTH-generating
    function. At the moment, the only such function is `ncfgbld.get_KOTH_config`, which
    accepts no additional parameters.
    :param template_config_builder: Function used to generate default config.
    :param params_variants: Config paths to variable parameters and their values
    :return: Configs with different parameters.
    """
    # reference: dev_scripts.notebooks.run_notebook.build_configs
    config = template_config_builder()
    if params_variants is not None:
        template_config = _generate_template_config(config, params_variants)
        configs = build_multiple_configs(template_config, params_variants)
    else:
        configs = [config]
    return configs


def load_results(results_dir: str) -> Tuple[List[cfg.Config], List[pd.DataFrame]]:
    """
    Load all result pickles and save in order of corresponding configs.
    :param results_dir: Directory with results of experiments.
    :return: All result configs and result dataframes.
    """
    # TODO (*) Move function to a different lib.
    result_dfs = []
    configs = []
    result_subfolders = os.listdir(results_dir)
    for subfolder in result_subfolders:
        config_path = os.path.join(results_dir, subfolder, "config.pkl")
        config = hpickle.from_pickle(config_path)
        configs.append(config)
    # Sort configs by order of simulations.
    configs = sorted(configs, key=lambda x: x[("meta", "id")])
    # # Load dataframes with results.
    for config in configs:
        config_result = hpickle.from_pickle(config[("meta", "result_file_name")])
        config_result = config_result["df"]
        result_dfs.append(config_result)
    return configs, result_dfs


def build_multiple_configs(
    template_config: cfg.Config,
    params_variants: Dict[Tuple[str, ...], Iterable[Any]],
) -> List[cfg.Config]:
    """
    Create multiple `cfg.Config` objects using the given config template
    and overwriting a None parameter specified through a parameter path
    and several possible elements:
    param_path: Tuple(str) -> param_values: Iterable[Any]
    A parameter path is represented by a tuple of nested names.

    Note that we create a config for each element of the Cartesian
    product of the values to be assigned.

    :param template_config: cfg.Config object
    :param params_variants: {(param_name_in_the_config_path):
        [param_values]}, e.g. {('read_data', 'symbol'): ['CL', 'QM'],
                                ('resample', 'rule'): ['5T', '10T']}
    :return: a list of configs
    """
    # In the example from above, list(possible_values) = [('CL', '5T'),
    # ('CL', '10T'), ('QM', '5T'), ('QM', '10T')]
    possible_values = list(itertools.product(*params_variants.values()))
    # A dataframe indexed with param_paths and with their possible
    # combinations as columns.
    comb_df = pd.DataFrame(
        possible_values, columns=list(params_variants.keys())
    ).T
    param_vars = list(comb_df.to_dict().values())
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
            if conf_tmp[param_path[-1]] is not None:
                raise ValueError("Trying to change a parameter that is not None.")
            conf_tmp[param_path[-1]] = param_val
        param_configs.append(config_var)
    return param_configs
