"""
Import as:

import core.config.utils as cfgut
"""

import collections
import logging
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

import core.config.config_ as cconfig
import helpers.dbg as dbg
import helpers.dict as dct

_LOG = logging.getLogger(__name__)


def check_no_dummy_values(config: cconfig.Config) -> bool:
    """
    Assert if there are no `cconfig.DUMMY` values.
    """
    for key, val in dct.get_nested_dict_iterator(config.to_dict()):
        # (k, v) looks like `(('load_prices', 'source_node_name'), 'kibot_equities')`.
        _LOG.debug(hprint.to_str("key val"))
        dbg.dassert_ne(
            val,
            cconfig.DUMMY,
            "DUMMY value %s detected along %s",
            str(val),
            str(key),
        )
    return True


def validate_configs(configs: List[cconfig.Config]) -> None:
    """
    Assert if the list of configs contains duplicates.
    """
    dbg.dassert_container_type(configs, List, cconfig.Config)
    dbg.dassert_no_duplicates(
        list(map(str, configs)), "There are duplicate configs in passed list"
    )


def configs_to_str(configs: List[cconfig.Config]) -> str:
    """
    Print a list of configs into a readable string.
    """
    txt = []
    for i, config in enumerate(configs):
        txt.append("# %s/%s" % (i + 1, len(configs)))
        txt.append(str(config))
    res = "\n".join(txt)
    return res


def get_config_from_flattened_dict(
    flattened: Dict[Tuple[str], Any]
) -> cconfig.Config:
    """
    Build a config from the flattened config representation.

    :param flattened: flattened config like result from `config.flatten()`
    :return: `Config` object initialized from flattened representation
    """
    dbg.dassert_isinstance(flattened, dict)
    dbg.dassert(flattened)
    config = cconfig.Config()
    for k, v in flattened.items():
        config[k] = v
    return config


def get_config_from_nested_dict(nested: Dict[str, Any]) -> cconfig.Config:
    """
    Build a `Config` from a nested dict.

    :param nested: nested dict, with certain restrictions:
      - only leaf nodes may not be a dict
      - every nonempty dict must only have keys of type `str`
    """
    dbg.dassert_isinstance(nested, dict)
    dbg.dassert(nested)
    iter_ = dct.get_nested_dict_iterator(nested)
    flattened = collections.OrderedDict(iter_)
    return get_config_from_flattened_dict(flattened)


# #############################################################################


def make_hashable(obj: Any) -> collections.abc.Hashable:
    """
    Coerce `obj` to a hashable type if not already hashable.
    """
    if isinstance(obj, collections.abc.Hashable):
        return obj
    if isinstance(obj, collections.abc.Iterable):
        return tuple(map(make_hashable, obj))
    return tuple(obj)


def intersect_configs(configs: Iterable[cconfig.Config]) -> cconfig.Config:
    """
    Return a config formed by taking the intersection of configs.

    - Key insertion order is not taken into consideration for the purpose of
      calculating the config intersection
    - The key insertion order of the returned config will respect the key
      insertion order of the first config passed in
    """
    # Flatten configs and convert to sets for intersection.
    # We create a list so that we can reference a flattened config later.
    flattened = [c.flatten() for c in configs]
    dbg.dassert(flattened, "Empty iterable `configs` received.")
    # Obtain a reference config.
    # The purpose of this is to ensure that the config intersection respects a key
    # ordering. We also make this copy so as to maintain the original (not
    # necessarily hashable) values.
    reference_config = flattened[0].copy()
    # Make values hashable.
    for flat in flattened:
        for k, v in flat.items():
            flat[k] = make_hashable(v)
    sets = [set(c.items()) for c in flattened]
    intersection_of_flattened = set.intersection(*sets)
    # Create intersection.
    # Rely on the fact that Config keys are of type `str`.
    intersection = cconfig.Config()
    for k, v in reference_config.items():
        if (k, make_hashable(v)) in intersection_of_flattened:
            intersection[k] = v
    return intersection


def subtract_config(
    minuend: cconfig.Config, subtrahend: cconfig.Config
) -> cconfig.Config:
    """
    Return a `Config` defined via minuend - subtrahend.

    :return: return a `Config` with (path, val pairs) in `minuend` that are not in
        `subtrahend` (like a set difference). Equivalently, return a `Config`-like
        `minuend` but with the intersection of `minuend` and `subtrahend`
        removed.
    """
    dbg.dassert(minuend)
    flat_m = minuend.flatten()
    flat_s = subtrahend.flatten()
    diff = cconfig.Config()
    for k, v in flat_m.items():
        if (k not in flat_s) or (flat_m[k] != flat_s[k]):
            diff[k] = v
    return diff


def diff_configs(configs: Iterable[cconfig.Config]) -> List[cconfig.Config]:
    """
    Diff `Config`s with respect to their common intersection.

    :return: for each config `config` in `configs`, return a new `Config` consisting
        of the part of `config` not in the intersection of the configs
    """
    # Convert the configs to a list for convenience.
    configs = list(configs)
    # Find the intersection of all the configs.
    intersection = intersect_configs(configs)
    # For each config, compute the diff between the config and the intersection.
    config_diffs = []
    for config in configs:
        config_diff = subtract_config(config, intersection)
        config_diffs.append(config_diff)
    dbg.dassert_eq(len(config_diffs), len(configs))
    return config_diffs


# # #############################################################################


def convert_to_series(config: cconfig.Config) -> pd.Series:
    """
    Convert a config into a flattened series representation.

    - This is lossy but useful for comparing multiple configs
    - `str` tuple paths are joined on "."
    - Empty leaf configs are converted to an empty tuple
    """
    dbg.dassert_isinstance(config, cconfig.Config)
    dbg.dassert(config, msg="`config` is empty")
    flat = config.flatten()
    keys: List[str] = []
    vals: List[tuple] = []
    for k, v in flat.items():
        key = ".".join(k)
        keys.append(key)
        if isinstance(v, cconfig.Config):
            vals.append(tuple())
        else:
            vals.append(v)
    dbg.dassert_no_duplicates(keys)
    srs = pd.Series(index=keys, data=vals)
    return srs


def convert_to_dataframe(configs: Iterable[cconfig.Config]) -> pd.DataFrame:
    """
    Convert multiple configs into flattened dataframe representation.
    """
    dbg.dassert_isinstance(configs, Iterable)
    srs = list(map(convert_to_series, configs))
    dbg.dassert(srs)
    df = pd.concat(srs, axis=1).T
    return df


def build_config_diff_dataframe(
    config_dict: collections.OrderedDict, tag_col: Optional[str] = None
) -> pd.DataFrame:
    """
    Create a dataframe of config diffs.

    :param config_dict: dictionary of configs
    :param tag_col: name of the tag col. If tags are the same for all configs
        and `tag_col` is not None, add tags to config diffs dataframe
    :return: config diffs dataframe
    """
    # Convert the dict into a list of tuples (key, value).
    diffs = diff_configs(config_dict.values())
    _LOG.debug("diffs=\n%s", configs_to_str(diffs))
    # Remove empty configs.
    non_empty_diffs = [
        (k, v)
        for (diff, k, v) in zip(diffs, config_dict.keys(), config_dict.values())
        if len(diff) > 0
    ]
    if non_empty_diffs:
        config_diffs = convert_to_dataframe(diffs).dropna(how="all", axis=1)
    else:
        config_diffs = pd.DataFrame(index=range(len(diffs)))
    # If tags are the same, still add them to `config_diffs`.
    if tag_col is not None and tag_col not in config_diffs.columns:
        tags = [config[tag_col] for config in config_dict.values()]
        config_diffs[tag_col] = tags
    return config_diffs
