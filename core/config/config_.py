"""
Import as:

import core.config.config_ as cconconf
"""

# This file is called `config_.py` and not `config.py` to avoid circular
# imports from the fact that also the package `core/config` can be imported as
# `import config`.

import collections
import copy
import logging
import os
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import numpy as np

import helpers.dbg as hdbg
import helpers.dict as hdict
import helpers.introspection as hintros
import helpers.printing as hprint

_LOG = logging.getLogger(__name__)

_LOG.debug = lambda *_: 0


# Placeholder value used in configs, when configs are built in multiple phases.
DUMMY = "__DUMMY__"


class Config:
    """
    A nested ordered dictionary storing configuration information.

    Keys can only be strings.
    Values can be a Python type or another `Config`.

    We refer to configs as:
    - "flat" when they have a single level
        - E.g., `config = {"hello": "world"}`
    - "nested" when there are multiple levels
        - E.g., `config = {"hello": {"cruel", "world"}}`
    """

    # A simple or compound key that can be used to access a Config.
    Key = Union[str, Iterable[str]]

    def __init__(
        self,
        array: Optional[List[Tuple[str, Any]]] = None,
    ) -> None:
        """
        Build a config from a list of (key, value).

        :param array: list of (key, value), where value can be a Python type or a
            `Config` in case of a nested config.
        """
        # TODO(gp): MutableMapping instead of disabling the lint?
        # pylint: disable=unsubscriptable-object
        self._config: collections.OrderedDict[
            str, Any
        ] = collections.OrderedDict()
        if array is not None:
            for k, v in array:
                hdbg.dassert_isinstance(k, str)
                self._config[k] = v

    def __setitem__(self, key: Key, val: Any) -> None:
        """
        Set/update `key` to `val`, equivalent to `dict[key] = val`.

        If `key` is an iterable of keys, then the key hierarchy is
        navigated/created and the leaf value added/updated with `val`.
        """
        _LOG.debug("key=%s, config=%s", key, self)
        if hintros.is_iterable(key):
            head_key, tail_key = self._parse_compound_key(key)
            if not tail_key:
                # Tuple of a single element, then set the value.
                self.__setitem__(head_key, val)
            else:
                # Compound key: recurse on the tail of the key.
                _LOG.debug(
                    "head_key='%s', self._config=%s", head_key, self._config
                )
                subconfig = self.get(head_key, None) or self.add_subconfig(
                    head_key
                )
                hdbg.dassert_isinstance(subconfig, Config)
                subconfig.__setitem__(tail_key, val)
            return
        # Base case: key is a string, config is a dict.
        hdbg.dassert(self._check_base_case(key))
        self._config[key] = val  # type: ignore

    def __getitem__(self, key: Key) -> Any:
        """
        Get value for `key` or raise `KeyError` if it doesn't exist.

        If `key` is an iterable of keys (e.g., `("read_data", "file_name")`, then
        the hierarchy is navigated until the corresponding element is found or we
        raise if the element doesn't exist.

        When we report an error about a missing key, we print only the keys of the
        Config at the current level of the recursion and not the original Config
        (which is also not directly accessible inside the recursion), e.g.,
        `key='nrows_tmp' not in ['nrows', 'nrows2']`

        :raises KeyError: if the (nested) key is not found in the `Config`.
        """
        _LOG.debug("key=%s, config=%s", key, self)
        # Check if the key is nested.
        if hintros.is_iterable(key):
            head_key, tail_key = self._parse_compound_key(key)
            if not tail_key:
                # Tuple of a single element, then return the value.
                ret = self.__getitem__(head_key)
            else:
                # Compound key: recurse on the tail of the key.
                if head_key not in self._config:
                    raise KeyError(
                        f"key='{head_key}' not in '{list(self._config.keys())}'"
                    )
                subconfig = self._config[head_key]
                _LOG.debug("subconfig=%s", self._config)
                if isinstance(subconfig, Config):
                    # Recurse.
                    ret = subconfig.__getitem__(tail_key)
                else:
                    # There are more keys to process but we have reached the leaves
                    # of the config, then we assert.
                    raise KeyError(f"tail_key='{tail_key}' not in '{subconfig}'")
            return ret
        # Base case: key is a string, config is a dict.
        hdbg.dassert(self._check_base_case(key))
        if key not in self._config:
            raise KeyError(f"key='{key}' not in '{list(self._config.keys())}'")
        ret = self._config[key]  # type: ignore
        return ret

    def __contains__(self, key: Key) -> bool:
        """
        Implement membership operator like `key in config`.

        If `key` is nested, the hierarchy of Config objects is
        navigated.
        """
        # This is implemented lazily (or Pythonically) with a try-catch around
        # accessing the key.
        _LOG.debug("key=%s, config=\n%s", key, self)
        try:
            val = self.__getitem__(key)
            _LOG.debug("Found val=%s", val)
            found = True
        except KeyError as e:
            _LOG.debug("e=%s", e)
            found = False
        return found

    def __str__(self) -> str:
        """
        Return a short string representation of this `Config`.
        """
        txt = []
        for k, v in self._config.items():
            if isinstance(v, Config):
                txt_tmp = str(v)
                txt.append("%s:\n%s" % (k, hprint.indent(txt_tmp)))
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
        Return an unambiguous representation of this `Config`

        For now it's the same as `str()`. This is used by Jupyter
        notebook when printing.
        """
        return str(self)

    def __len__(self) -> int:
        """
        Return number of keys, i.e., the length of the underlying dict.

        This enables calculating `len()` as with a dict and also enables
        bool evaluation of a `Config` object for truth value testing.
        """
        return len(self._config)

    def add_subconfig(self, key: str) -> "Config":
        hdbg.dassert_not_in(key, self._config.keys(), "Key already present")
        config = Config()
        self._config[key] = config
        return config

    def update(self, config: "Config") -> None:
        """
        Equivalent to `dict.update(config)`.

        Some features of the update:
        - Updates leaf values in self from values in `config`
        - Recursively creates paths to leaf values if needed
        - `config` values overwrite any existing values
        """
        flattened = config.flatten()
        for path, val in flattened.items():
            self.__setitem__(path, val)

    def get(self, key: Key, *args: Any) -> Any:
        """
        Equivalent to `dict.get(key, default_val)`.

        It has the same functionality as `__getitem__()` but returning
        `val` if the value corresponding to `key` doesn't exist.
        """
        try:
            ret = self.__getitem__(key)
        except KeyError as e:
            # No key: use the default val if it was passed or asserts.
            _LOG.debug("e=%s", e)
            if args:
                # There should be only one element.
                hdbg.dassert_eq(
                    len(args),
                    1,
                    "There should be only one parameter passed, instead there is %s",
                    str(args),
                )
                ret = args[0]
            else:
                # No parameter found, then raise.
                raise e
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
    def from_python(cls, code: str) -> Optional["Config"]:
        """
        Create an object from the code returned by `to_python()`.
        """
        hdbg.dassert_isinstance(code, str)
        try:
            # eval function need unknown globals to be set.
            val = eval(code, {"nan": np.nan, "Config": Config})
            hdbg.dassert_isinstance(val, Config)
        except SyntaxError as e:
            _LOG.error("Error deserializing: %s", str(e))
            return None
        return val  # type: ignore

    def to_python(self, check: bool = True) -> str:
        """
        Return python code that builds, when executed, the current object.

        :param check: check that the Config can be serialized/deserialized correctly.
        """
        config_as_str = str(self.to_dict())
        # We don't need `cconfig.` since we are inside the config module.
        config_as_str = config_as_str.replace("OrderedDict", "Config")
        if check:
            # Check that the object can be reconstructed.
            config_tmp = Config.from_python(config_as_str)
            # Compare.
            hdbg.dassert_eq(str(self), str(config_tmp))
        return config_as_str

    @classmethod
    def from_env_var(cls, env_var: str) -> Optional["Config"]:
        if env_var in os.environ:
            python_code = os.environ[env_var]
            config = cls.from_python(python_code)
        else:
            _LOG.warning(
                "Environment variable '%s' not defined: no config retrieved",
                env_var,
            )
            config = None
        return config

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

    def is_serializable(self) -> bool:
        """
        Make sure the config can be serialized and deserialized correctly.
        """
        code = self.to_python(check=False)
        config = self.from_python(code)
        ret = str(config) == str(self)
        return ret

    def flatten(self) -> Dict[Tuple[str], Any]:
        """
        Key leaves by tuple representing path to leaf.
        """
        dict_ = self._to_dict_except_for_leaves()
        iter_ = hdict.get_nested_dict_iterator(dict_)
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
            % (key, self._config[key], hprint.indent(str(self)))
        )

    def dassert_is_serializable(self) -> None:
        """
        Make sure the config can be serialized and deserialized correctly.
        """
        code = self.to_python()
        config = self.from_python(code)
        dbg.dassert_eq(str(config), str(self))
        dbg.dassert_eq(config, self)

    @classmethod
    def from_env_var(cls, env_var: str) -> Optional["Config"]:
        if env_var in os.environ:
            code = os.environ[env_var]
            ret = cls.from_python(code)
        else:
            ret = None
        return ret

    @staticmethod
    def _parse_compound_key(key: Key) -> Tuple[str, Iterable[str]]:
        hdbg.dassert(hintros.is_iterable(key), "Key='%s' is not iterable", key)
        head_key, tail_key = key[0], key[1:]  # type: ignore
        _LOG.debug(
            "key='%s' -> head_key='%s', tail_key='%s'", key, head_key, tail_key
        )
        hdbg.dassert_isinstance(head_key, str, "Keys can only be string")
        return head_key, tail_key

    def _check_base_case(self, key: Key) -> bool:
        _LOG.debug("key=%s", key)
        hdbg.dassert_isinstance(key, str, "Keys can only be string")
        hdbg.dassert_isinstance(self._config, dict)
        return True

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
