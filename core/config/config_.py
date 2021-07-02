# This file is called `config_.py` and not `config.py` to avoid circular
# imports from the fact that also the package `core/config` can be imported as
# `import config`.

import collections
import copy
import logging
import re
from typing import Any, Dict, Iterable, List, Tuple, Union

import numpy as np

import helpers.dbg as dbg
import helpers.dict as dct
import helpers.introspection as intr
import helpers.printing as pri

_LOG = logging.getLogger(__name__)


# Placeholder value used in configs, when configs are built in multiple phases.
DUMMY = "__DUMMY__"


class Config:
    """
    A nested ordered dictionary storing configuration information.

    Keys can only be strings. Values can be strings, ints, or another `Config`.

    We refer to configs as:
    - "flat" when they have a single level
    - "nested" when there are multiple levels
    """

    def __init__(
        self,
        # TODO(gp): Difficult to read and type hints are loose: try to improve.
        array: Union[
            List[Tuple[str, Union[int, str]]],
            List[Tuple[str, Union[List[int], str]]],
            None,
        ] = None,
    ) -> None:
        """
        :param array: array of (key, value), where value can be a Python type or a
            `Config` in case of a nested config.
        """
        # pylint: disable=unsubscriptable-object
        # TODO(gp): MutableMapping instead of disabling the lint?
        self._config: collections.OrderedDict[
            str, Any
        ] = collections.OrderedDict()
        if array is not None:
            for k, v in array:
                self._config[k] = v

    def __setitem__(self, key: Union[str, Iterable[str]], val: Any) -> None:
        """
        Set/update `key` to `val`.

        If `key` is an iterable of keys, then the key hierarchy is
        navigated/created and the leaf value added/updated with `val`.
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
                # `self._config[head_key]`.
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

        If `key` is an iterable of keys (e.g., `("read_data",
        "file_name")`, then the hierarchy is navigated until the
        corresponding element is found or we assert if the element
        doesn't exist.
        """
        if intr.is_iterable(key):
            head_key, tail_key = key[0], key[1:]  # type: ignore
            _LOG.debug(
                "key=%s -> head_key=%s tail_key=%s", key, head_key, tail_key
            )
            if not tail_key:
                # Tuple of a single element, then return the value.
                # Note that the following call is not equivalent to
                # `self._config[head_key]`.
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
                txt.append("%s:\n%s" % (k, pri.indent(txt_tmp)))
            else:
                txt.append("%s: %s" % (k, v))
        ret = "\n".join(txt)
        # Remove memory locations of functions, if config contains them, e.g.,
        #   `<function _filter_relevance at 0x7fe4e35b1a70>`.
        memory_loc_pattern = r"(<function \w+.+) at \dx\w+"
        ret = re.sub(memory_loc_pattern, r"\1", ret)
        # Remove memory locations of objects, if config contains them, e.g.,
        #   `<dataflow.task2538_pipeline.ArPredictor object at 0x7f7c7991d390>`
        memory_loc_pattern = r"(<\w+.+ object) at \dx\w+"
        ret = re.sub(memory_loc_pattern, r"\1", ret)
        return ret

    def __repr__(self) -> str:
        """
        Return an unambiguous representation the same as str().

        This is used by Jupyter notebook when printing.
        """
        return str(self)

    def __len__(self) -> int:
        """
        Return len of underlying dict.

        This enables calculating `len()` as with a dict and also enables
        bool evaluation of a `Config` object for truth value testing.
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
        Equivalent to `dict.get(key, default_val)`.

        It has the same functionality as `__getitem__` but returning
        `val` if the value corresponding to `key` doesn't exist.
        """
        try:
            ret = self.__getitem__(key)
        except AssertionError:
            # TODO(gp): We should throw/catch a KeyError exception, instead of a
            #  generic AssertionError.
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
        # eval function need unknown globals to be set.
        val = eval(code, {"nan": np.nan, "Config": Config})
        dbg.dassert_isinstance(val, Config)
        return val  # type: ignore

    def to_python(self, check: bool = True) -> str:
        """
        Return python code that builds, when executed, the current object.
        """
        config_as_str = str(self.to_dict())
        # We don't need `cconfig.` since we are inside the config module.
        config_as_str = config_as_str.replace("OrderedDict", "Config")
        if check:
            # Check that the object can be reconstructed.
            config_tmp = Config.from_python(config_as_str)
            # Compare.
            dbg.dassert_eq(str(self), str(config_tmp))
        return config_as_str

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the Config to nested ordered dicts.

        In other words, it replaces the `Config` class with simple
        ordered dicts.
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
            % (key, self._config[key], pri.indent(str(self)))
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
