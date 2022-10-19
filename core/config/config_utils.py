"""
Import as:

import core.config.config_utils as ccocouti
"""

import collections
import copy
import logging
from typing import Any, Iterable, List, Optional

import pandas as pd

import core.config.config_ as cconconf
import helpers.hdbg as hdbg
import helpers.hdict as hdict
import helpers.hprint as hprint

_LOG = logging.getLogger(__name__)


# #############################################################################
# Configs.
# #############################################################################


def validate_configs(configs: List[cconconf.Config]) -> None:
    """
    Assert if the list of configs contains duplicates.
    """
    hdbg.dassert_container_type(configs, List, cconconf.Config)
    hdbg.dassert_no_duplicates(
        list(map(str, configs)), "There are duplicate configs in passed list"
    )


def configs_to_str(configs: List[cconconf.Config]) -> str:
    """
    Print a list of configs into a readable string.
    """
    txt = []
    for i, config in enumerate(configs):
        txt.append("# %s/%s" % (i + 1, len(configs)))
        txt.append(hprint.indent(str(config)))
    res = "\n".join(txt)
    return res


# TODO(gp): Add unit tests.
def sort_config_string(txt: str) -> str:
    """
    Sort a string representing a Config in alphabetical order by the first
    level.

    This function can be used to diff two Configs serialized as strings.
    """
    lines = [line.rstrip("\n") for line in txt]
    # Parse.
    chunks = {}
    state = "look_for_start"
    start_idx = end_idx = None
    for i, line in enumerate(lines):
        _LOG.debug(
            "i=%s state=%s start_idx=%s end_idx=%s line=%s"
            % (i, state, start_idx, end_idx, line)
        )
        if (
            state == "look_for_start"
            and line[0] != " "
            and lines[i + 1][0] != " "
        ):
            _LOG.debug("Found single line")
            # Single line.
            key = lines[i]
            val = " "
            chunks[key] = val
            _LOG.debug("Single line -> %s %s", key, val)
        elif state == "look_for_start" and line[0] != " ":
            _LOG.debug("Found first line")
            start_idx = i
            end_idx = None
            state = "look_for_end"
        elif state == "look_for_end" and line[0] != " ":
            _LOG.debug("Found last line")
            end_idx = i - 1
            hdbg.dassert_lte(start_idx, end_idx)
            key = lines[start_idx]
            _LOG.debug("start_idx=%s end_idx=%s key=%s", start_idx, end_idx, key)
            val = lines[start_idx + 1 : end_idx + 1]
            chunks[key] = val
            _LOG.debug("-> %s %s", key, val)
            #
            state = "look_for_start"
            start_idx = i
            end_idx = None
    # Sort.
    chunks = {k: chunks[k] for k in sorted(chunks.keys())}
    # Assemble with proper indentation.
    chunks = "\n".join(
        [k + hprint.indent("\n".join(chunks[k])) for k in chunks.keys()]
    )
    return chunks


# #############################################################################


# TODO(gp): This could be a method of Config to encapsulate.
def check_no_dummy_values(config: cconconf.Config) -> bool:
    """
    Assert if there are no `cconconf.DUMMY` values.
    """
    dummy_type = type(cconconf.DUMMY)
    _LOG.debug("type(DUMMY)=%s", dummy_type)
    for key, val in hdict.get_nested_dict_iterator(config.to_dict()):
        # (k, v) looks like:
        # ```
        # (('load_prices', 'source_node_name'), 'kibot_equities')
        # ```
        _LOG.debug(hprint.to_str("key val"))
        # Only check for equality if the types agree.
        # Example: if we compare a pd.Series to a built-in type, the comparison
        # is carried out element-wise, which is not what we want in this case.
        if type(val) == dummy_type:
            hdbg.dassert_ne(
                val,
                cconconf.DUMMY,
                "DUMMY value %s detected along %s",
                str(val),
                str(key),
            )
    return True


# #############################################################################


def make_hashable(obj: Any) -> collections.abc.Hashable:
    """
    Coerce `obj` to a hashable type if not already hashable.
    """
    ret = None
    if isinstance(obj, collections.abc.Mapping):
        # Handle dict-like objects.
        new_object = copy.deepcopy(obj)
        for k, v in new_object.items():
            new_object[k] = make_hashable(v)
        ret = tuple(new_object.items())
    # The problem is that `str` is both `Hashable` and `Iterable`, but here
    # we want to treat it like `Hashable`, i.e. return string as it is.
    # Same with `Tuple`, but for `Tuple` we want to apply the function
    # recursively, i.e. make every element `Hashable`.
    elif isinstance(obj, collections.abc.Iterable) and not isinstance(obj, str):
        # Handle iterables, e.g., lists, sets, tuples.
        ret = tuple([make_hashable(element) for element in obj])
    elif isinstance(obj, collections.abc.Hashable):
        # Return the object as is, since it's already hashable.
        ret = obj
    else:
        ret = tuple(obj)
    return ret


def intersect_configs(configs: Iterable[cconconf.Config]) -> cconconf.Config:
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
    hdbg.dassert(flattened, "Empty iterable `configs` received.")
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
    intersection = cconconf.Config()
    for k, v in reference_config.items():
        if (k, make_hashable(v)) in intersection_of_flattened:
            intersection[k] = v
    return intersection


# TODO(gp): This could be a method of Config.
def subtract_config(
    minuend: cconconf.Config, subtrahend: cconconf.Config
) -> cconconf.Config:
    """
    Return a `Config` defined via minuend - subtrahend.

    :return: return a `Config` with (path, val pairs) in `minuend` that are not in
        `subtrahend` (like a set difference). Equivalently, return a `Config`-like
        `minuend` but with the intersection of `minuend` and `subtrahend`
        removed.
    """
    hdbg.dassert(minuend)
    flat_m = minuend.flatten()
    flat_s = subtrahend.flatten()
    diff = cconconf.Config()
    for k, v in flat_m.items():
        if (k not in flat_s) or (flat_m[k] != flat_s[k]):
            # It is not possible to use a dict as a config's value.
            # It should be converted to a config first.
            if isinstance(v, dict):
                if not v:
                    # Replace empty dict with empty config.
                    v = cconconf.Config()
                else:
                    # Get config from a dict.
                    v = cconconf.Config.from_dict(v)
            diff[k] = v
    return diff


def diff_configs(configs: Iterable[cconconf.Config]) -> List[cconconf.Config]:
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
    hdbg.dassert_eq(len(config_diffs), len(configs))
    return config_diffs


# #############################################################################


# TODO(gp): Is this private?
def convert_to_series(config: cconconf.Config) -> pd.Series:
    """
    Convert a config into a flattened series representation.

    - This is lossy but useful for comparing multiple configs
    - `str` tuple paths are joined on "."
    - Empty leaf configs are converted to an empty tuple
    """
    hdbg.dassert_isinstance(config, cconconf.Config)
    hdbg.dassert(config, msg="`config` is empty")
    flat = config.flatten()
    keys: List[str] = []
    vals: List[tuple] = []
    for k, v in flat.items():
        key = ".".join(k)
        keys.append(key)
        if isinstance(v, cconconf.Config):
            vals.append(tuple())
        else:
            vals.append(v)
    hdbg.dassert_no_duplicates(keys)
    srs = pd.Series(index=keys, data=vals)
    return srs


# TODO(gp): Is this private?
def convert_to_dataframe(configs: Iterable[cconconf.Config]) -> pd.DataFrame:
    """
    Convert multiple configs into flattened dataframe representation.
    """
    hdbg.dassert_isinstance(configs, Iterable)
    srs = list(map(convert_to_series, configs))
    hdbg.dassert(srs)
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
