import collections
import logging

import helpers.dbg as dbg
import helpers.printing as pri

_LOG = logging.getLogger(__name__)


# #############################################################################
# Config
# #############################################################################


class Config:
    def __init__(self):
        self._config = collections.OrderedDict()

    def __setitem__(self, key, val):
        self._config[key] = val

    def __getitem__(self, key):
        dbg.dassert_in(key, self._config)
        return self._config[key]

    def __str__(self) -> str:
        """
        Return the string representation.
        """
        txt = "\n".join(["%s: %s" % (k, v) for (k, v) in self._config.items()])
        return txt

    def update(self, dict_: dict):
        """
        Equivalent to dict.update()
        """
        self._config.update(dict_)

    def get(self, key, val):
        """
        Equivalent to dict.get()
        """
        # TODO(gp): For some reason this doesn't work. It's probably something
        # trivial.
        # self._config.get(key, val)
        res = self._config[key] if key in self._config else val
        return res

    @classmethod
    def from_python(cls, code):
        """
        Create an object from the code returned by `to_python()`.
        """
        val = cls()
        val._config = eval(code)
        return val

    def to_python(self) -> str:
        """
        Return python code as a string that can be evaluated to regenerate the
        object.
        """
        config_as_str = str(self._config)
        code = config_as_str.replace("OrderedDict", "collections.OrderedDict")
        return code

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

    # TODO(gp): Use this everywhere.
    def get_exception(self, key):
        """
        Convenience function to get an exception when a key is not present.
        """
        return ValueError(
            "Invalid %s='%s' in config=\n%s"
            % (key, self._config[key], pri.space(str(self)))
        )
