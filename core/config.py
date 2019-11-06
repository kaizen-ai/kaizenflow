"""
Import as:

import core.config as cfg
"""

import collections
import logging

# #############################################################################
# Config
# #############################################################################
import os

import helpers.dbg as dbg
import helpers.printing as pri

_LOG = logging.getLogger(__name__)


# TODO(gp): Add mechanism to check if a value was assigned but not used.
class Config:
    def __init__(self, array=None):
        """
        :param array: array of (key, value), where value can be a python
        type or a Config in case of nested config.
        """
        self._config = collections.OrderedDict()
        if array is not None:
            for k, v in array:
                self._config[k] = v

    def __setitem__(self, key, val):
        """
        Set / update `key` to `val`.
        """
        dbg.dassert_isinstance(key, str, "Keys can only be string")
        self._config[key] = val

    def __getitem__(self, key):
        """
        Get value for `key` or assert, if it doesn't exist.
        """
        dbg.dassert_isinstance(key, str, "Keys can only be string")
        dbg.dassert_in(key, self._config)
        return self._config[key]

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
        txt = "\n".join(txt)
        return txt

    def add_subconfig(self, key):
        dbg.dassert_not_in(key, self._config)
        self._config[key] = Config()
        return self._config[key]

    def update(self, dict_: dict):
        """
        Equivalent to `dict.update()`
        """
        self._config.update(dict_)

    def get(self, key, val):
        """
        Equivalent to `dict.get()`
        """
        # TODO(gp): For some reason this doesn't work. It's probably something
        # trivial.
        # self._config.get(key, val)
        res = self._config[key] if key in self._config else val
        return res

    def pop(self, key):
        """
        Equivalent to `dict.pop()`
        """
        return self._config.pop(key)

    def copy(self):
        """
        Equivalent to `dict.copy()`
        """
        return self._config.copy()

    @classmethod
    def from_python(cls, code):
        """
        Create an object from the code returned by `to_python()`.
        """
        val = eval(code)
        dbg.dassert_isinstance(val, Config)
        return val

    def to_dict(self):
        """
        Convert to a ordered dict of order dicts, removing the class.
        """
        dict_ = collections.OrderedDict()
        for k, v in self._config.items():
            if isinstance(v, Config):
                dict_[k] = v.to_dict()
            else:
                dict_[k] = v
        return dict_

    def to_python(self, check=True):
        config_as_str = str(self.to_dict())
        # We don't need 'cfg.' since we are inside the config module.
        config_as_str = config_as_str.replace("OrderedDict", "Config")
        if check:
            # Check that the object can be reconstructed.
            config_tmp = Config.from_python(config_as_str)
            # Compare.
            dbg.dassert_eq(str(self), str(config_tmp))
        return config_as_str

    def check_params(self, keys):
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

    # TODO(gp): return a string
    def print_config(self, keys):
        """
        Return a string representation of a subset of keys, assigning "na" when
        there is no value.
        """
        if isinstance(keys, str):
            keys = [keys]
        for k in keys:
            v = self._config.get(k, "na")
            _LOG.info("%s='%s'", k, v)

    @classmethod
    def from_env(cls):
        """
        Build a config passed through an environment variable, if possible,
        or return None.
        """
        if "__CONFIG__" in os.environ:
            config = os.environ["__CONFIG__"]
            _LOG.info("__CONFIG__=%s", config)
            config = Config.from_python(config)
        else:
            config = None
        return config

    # TODO(gp): Use this everywhere.
    def get_exception(self, key):
        """
        Convenience function to get an exception when a key is not present.
        """
        return ValueError(
            "Invalid %s='%s' in config=\n%s"
            % (key, self._config[key], pri.space(str(self)))
        )
