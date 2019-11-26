import itertools
import logging
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd

import core.config as cfg

_LOG = logging.getLogger(__name__)


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
