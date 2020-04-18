"""
Import as:

import core.config as cfg
"""

import collections
import copy
import logging
import re
from typing import Any, Dict, Iterable, List, Tuple, Union

import helpers.dbg as dbg
import helpers.dict as dct
import helpers.introspection as intr
import helpers.printing as pri

_LOG = logging.getLogger(__name__)


class Config:
    """
    A hierarchical ordered dictionary storing configuration informations.
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
            if not tail_key:
                # Tuple of a single element, then set the value.
                # Note that the following call is not equivalent to
                # self._config[head_key].
                self.__setitem__(head_key, val)
            else:
                # Recurse.
                dbg.dassert_isinstance(head_key, str, "Keys can only be string")
                subconfig = self.get(head_key, None) or self.add_subconfig(
                    head_key
                )
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
        # `function _filter_relevance at 0x7fe4e35b1a70`.
        memory_loc_pattern = r"(<function \w+) at \dx\w+"
        ret = re.sub(memory_loc_pattern, r"\1", ret)
        return ret

    def __repr__(self) -> str:
        """
        Return as unambiguous representation the same as str().

        This is used by Jupyter notebook when printing.
        """
        return str(self)

    def __len__(self) -> bool:
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
        dict_ = config._to_dict_for_update()
        for path, val in dct.get_nested_dict_iterator(dict_):
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

    def _to_dict_for_update(self) -> Dict[str, Any]:
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


def flatten_config(config: Config) -> Dict[str, Any]:
    """
    Flatten config by joining nested keys with "." and making val hashable.
    """
    config_as_dict = config.to_dict()
    flattened = {}
    for item in get_nested_dict_iterator(config_as_dict):
        flattened[".".join(item[0])] = item[1]
    flattened_config = dct.flatten_nested_dict(config_as_dict)
    for k, v in flattened_config.items():
        if not isinstance(v, collections.abc.Hashable):
            flattened_config[k] = tuple(v)
    return flattened_config
