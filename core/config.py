"""
Import as:

import core.config as cfg
"""

import collections
import copy
import logging
import re
from typing import Any, Dict, Iterable, List, Tuple, Union

import pandas as pd

import helpers.dbg as dbg
import helpers.dict as dct
import helpers.introspection as intr
import helpers.printing as pri

_LOG = logging.getLogger(__name__)


class Config:
    """
    A hierarchical ordered dictionary storing configuration information.
    """

    def __init__(
        self,
        array: Union[
            List[Tuple[str, Union[List[int], str]]],
            List[Tuple[str, Union[int, str]]],
            None,
        ] = None,
    ) -> None:
        """
        :param array: array of (key, value), where value can be a python
            type or a Config in case of nested config.
        """
        # pylint: disable=unsubscriptable-object
        self._config: collections.OrderedDict[
            str, Any
        ] = collections.OrderedDict()
        if array is not None:
            for k, v in array:
                self._config[k] = v

    def __setitem__(self, key: Union[str, Iterable[str]], val: Any) -> None:
        """
        Set / update `key` to `val`.

        If `key` is an iterable of keys, then the key hierarchy is navigated /
        created and the leaf value added / updated with `val`.
        """
        if intr.is_iterable(key):
            head_key, tail_key = key[0], key[1:]  # type: ignore
            _LOG.debug(
                "key=%s -> head_key=%s tail_key=%s", key, head_key, tail_key
            )
            dbg.dassert_isinstance(head_key, str, "Keys can only be string")
            if not tail_key:
                # Tuple of a single element, then set the value.
                # Note that the following call is not equivalent to
                # self._config[head_key].
                self.__setitem__(head_key, val)
            else:
                # Recurse.
                subconfig = self.get(head_key, None) or self.add_subconfig(
                    head_key
                )
                dbg.dassert_isinstance(subconfig, Config)
                subconfig.__setitem__(tail_key, val)
            return
        _LOG.debug("key=%s", key)
        dbg.dassert_isinstance(key, str, "Keys can only be string")
        self._config[key] = val  # type: ignore

    def __getitem__(self, key: Union[str, Iterable[str]]) -> Any:
        """
        Get value for `key` or assert, if it doesn't exist.

        If `key` is an iterable of keys (e.g., `("read_data", "file_name")`,
        then the hierarchy is navigated until the corresponding element is found
        or we assert if the element doesn't exist.
        """
        if intr.is_iterable(key):
            head_key, tail_key = key[0], key[1:]  # type: ignore
            _LOG.debug(
                "key=%s -> head_key=%s tail_key=%s", key, head_key, tail_key
            )
            if not tail_key:
                # Tuple of a single element, then return the value.
                # Note that the following call is not equivalent to
                # self._config[head_key].
                ret = self.__getitem__(head_key)
            else:
                # Recurse.
                dbg.dassert_isinstance(head_key, str, "Keys can only be string")
                dbg.dassert_in(head_key, self._config.keys())
                ret = self._config[head_key].__getitem__(tail_key)
            return ret
        _LOG.debug("key=%s", key)
        dbg.dassert_isinstance(key, str, "Keys can only be string")
        dbg.dassert_in(key, self._config.keys())
        ret = self._config[key]  # type: ignore
        return ret

    def __str__(self) -> str:
        """
        Return the string representation.
        """
        txt = []
        for k, v in self._config.items():
            if isinstance(v, Config):
                txt_tmp = str(v)
                txt.append("%s:\n%s" % (k, pri.space(txt_tmp)))
            else:
                txt.append("%s: %s" % (k, v))
        ret = "\n".join(txt)
        # Remove memory locations of functions, if config contains them, e.g.,
        # `<function _filter_relevance at 0x7fe4e35b1a70>`.
        memory_loc_pattern = r"(<function \w+.+) at \dx\w+"
        ret = re.sub(memory_loc_pattern, r"\1", ret)
        # Remove memory locations of objects, if config contains them, e.g.,
        # "<dataflow_p1.task2538_pipeline.ArPredictorBuilder object at 0x7f7c7991d390>"
        memory_loc_pattern = r"(<\w+.+ object) at \dx\w+"
        ret = re.sub(memory_loc_pattern, r"\1", ret)
        return ret

    def __repr__(self) -> str:
        """
        Return as unambiguous representation the same as str().

        This is used by Jupyter notebook when printing.
        """
        return str(self)

    def __len__(self) -> int:
        """
        Return len of underlying dict.

        This enables calculating `len` as with a dict and also enables bool
        evaluation of a Config() object for truth value testing.
        """
        return len(self._config)

    def add_subconfig(self, key: str) -> "Config":
        dbg.dassert_not_in(key, self._config.keys(), "Key already present")
        config = Config()
        self._config[key] = config
        return config

    def update(self, config: "Config") -> None:
        """
        Update `self` with `config`.

        Some features of the update:
        - Updates leaf values in self from values in `config`
        - Recursively creates paths to leaf values if needed
        - `config` values overwrite any existing values
        """
        flattened = config.flatten()
        for path, val in flattened.items():
            self.__setitem__(path, val)

    def get(self, key: str, val: Any) -> Any:
        """
        Implement the same functionality as `__getitem__` but returning `val`
        if the value corresponding to key doesn't exist.
        """
        try:
            ret = self.__getitem__(key)
        except AssertionError:
            ret = val
        return ret

    def pop(self, key: str) -> Any:
        """
        Equivalent to `dict.pop()`.
        """
        return self._config.pop(key)

    def copy(self) -> "Config":
        """
        Create a deep copy of the Config object.
        """
        return copy.deepcopy(self)

    @classmethod
    def from_python(cls, code: str) -> "Config":
        """
        Create an object from the code returned by `to_python()`.
        """
        dbg.dassert_isinstance(code, str)
        val = eval(code)
        dbg.dassert_isinstance(val, Config)
        return val  # type: ignore

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to an ordered dict of ordered dicts, removing the class.
        """
        # pylint: disable=unsubscriptable-object
        dict_: collections.OrderedDict[str, Any] = collections.OrderedDict()
        for k, v in self._config.items():
            if isinstance(v, Config):
                dict_[k] = v.to_dict()
            else:
                dict_[k] = v
        return dict_

    def flatten(self) -> Dict[Tuple[str], Any]:
        """
        Key leaves by tuple representing path to leaf.
        """
        dict_ = self._to_dict_except_for_leaves()
        iter_ = dct.get_nested_dict_iterator(dict_)
        return collections.OrderedDict(iter_)

    def to_python(self, check: bool = True) -> str:
        config_as_str = str(self.to_dict())
        # We don't need 'cfg.' since we are inside the config module.
        config_as_str = config_as_str.replace("OrderedDict", "Config")
        if check:
            # Check that the object can be reconstructed.
            config_tmp = Config.from_python(config_as_str)
            # Compare.
            dbg.dassert_eq(str(self), str(config_tmp))
        return config_as_str

    def check_params(self, keys: Iterable[str]) -> None:
        """
        Check whether all the `keys` are present in the object, otherwise
        raise.
        """
        missing_keys = []
        for key in keys:
            if key not in self._config:
                missing_keys.append(key)
        if missing_keys:
            msg = "Missing %s vars (from %s) in config=\n%s" % (
                ",".join(missing_keys),
                ",".join(keys),
                str(self),
            )
            _LOG.error(msg)
            raise ValueError(msg)

    # TODO(*): Standardize/allow to be configurable what to return if a value is
    #     missing.
    # TODO(gp): return a string
    def print_config(self, keys: Iterable[str]) -> None:
        """
        Return a string representation of a subset of keys, assigning "na" when
        there is no value.
        """
        if isinstance(keys, str):
            keys = [keys]
        for k in keys:
            v = self._config.get(k, "na")
            _LOG.info("%s='%s'", k, v)

    # TODO(gp): Use this everywhere.
    def get_exception(self, key: str) -> None:
        """
        Raise an exception when a key is not present.
        """
        raise ValueError(
            "Invalid %s='%s' in config=\n%s"
            % (key, self._config[key], pri.space(str(self)))
        )

    def _to_dict_except_for_leaves(self) -> Dict[str, Any]:
        """
        Convert as in `to_dict` except for leaf values.
        """
        # pylint: disable=unsubscriptable-object
        dict_: collections.OrderedDict[str, Any] = collections.OrderedDict()
        for k, v in self._config.items():
            if v and isinstance(v, Config):
                dict_[k] = v.to_dict()
            else:
                dict_[k] = v
        return dict_


# #############################################################################
# Config utils
# #############################################################################


def make_hashable(obj: Any) -> collections.abc.Hashable:
    """
    Coerce `obj` to a hashable type if not already hashable.
    """
    if isinstance(obj, collections.abc.Hashable):
        return obj
    if isinstance(obj, collections.abc.Iterable):
        return tuple(map(make_hashable, obj))
    return tuple((obj))


def intersect_configs(configs: Iterable[Config]) -> Config:
    """
    Return a config formed by taking the intersection of configs.

    - Key insertion order is not taken into consideration for the purpose of
      calculating the config intersection.
    - The key insertion order of the returned config will respect the key
      insertion order of the first config passed in.
    """
    # Flatten configs and convert to sets for intersection.
    # We create a list so that we can reference a flattened config later.
    flattened = [c.flatten() for c in configs]
    dbg.dassert(flattened, "Empty iterable `configs` received.")
    # Obtain a reference config. The purpose of this is to ensure that the
    # config intersection respects a key ordering. We also make this copy
    # so as to maintain the original (not necessarily hashable) values.
    reference_config = flattened[0].copy()
    # Make vals hashable.
    for flat in flattened:
        for k, v in flat.items():
            flat[k] = make_hashable(v)
    sets = [set(c.items()) for c in flattened]
    intersection_of_flattened = set.intersection(*sets)
    # Create intersection. Rely on the fact that Config keys are of type `str`.
    intersection = Config()
    for k, v in reference_config.items():
        if (k, make_hashable(v)) in intersection_of_flattened:
            intersection[k] = v
    return intersection


def subtract_config(minuend: Config, subtrahend: Config) -> Config:
    """
    Return a Config() defined via minuend - subtrahend.

    :return: return a Config() with path, val pairs in `minuend` that are not in
        `subtrahend` (like a set difference). Equivalently, return a Config like
        `minuend` but with the intersection of `minuend` and `subtrahend`
        removed.
    """
    dbg.dassert(minuend)
    flat_m = minuend.flatten()
    flat_s = subtrahend.flatten()
    diff = Config()
    for k, v in flat_m.items():
        if (k not in flat_s) or (flat_m[k] != flat_s[k]):
            diff[k] = v
    return diff


def diff_configs(configs: Iterable[Config]) -> List[Config]:
    """
    Diff configs with respect to their common intersection.

    :return: for each config `config` in `configs`, return a new Config()
        consisting of the part of `config` not in the intersection of the
        configs in `configs`
    """
    # Convert `configs` to a list for convenience.
    configs = list(configs)
    intersection = intersect_configs(configs)
    config_diffs = []
    for config in configs:
        config_diff = subtract_config(config, intersection)
        config_diffs.append(config_diff)
    dbg.dassert_eq(len(config_diffs), len(configs))
    return config_diffs


def convert_to_series(config: Config) -> pd.Series:
    """
    Convert config into a flattened series representation.

    - This is lossy but useful for comparing multiple configs
    - `str` tuple paths are joined on "."
    - Empty leaf configs are converted to an empty tuple
    """
    dbg.dassert_isinstance(config, Config)
    dbg.dassert(config, msg="`config` is empty")
    flat = config.flatten()
    keys = []
    vals = []
    for k, v in flat.items():
        key = ".".join(k)
        keys.append(key)
        if isinstance(v, Config):
            vals.append(tuple())
        else:
            vals.append(v)
    dbg.dassert_no_duplicates(keys)
    srs = pd.Series(index=keys, data=vals)
    return srs


def convert_to_dataframe(configs: Iterable[Config]) -> pd.DataFrame:
    """
    Convert multiple configs into flattened dataframe representation.

    E.g., to highlight config differences in a dataframe, for an iterable
    `configs`, do
        ```
        diffs = diff_configs(configs)
        df = convert_to_dataframe(diffs)
        ```
    """
    dbg.dassert_isinstance(configs, Iterable)
    srs = list(map(convert_to_series, configs))
    dbg.dassert(srs)
    df = pd.concat(srs, axis=1).T
    return df
