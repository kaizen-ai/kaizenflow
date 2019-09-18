import logging

import helpers.dbg as dbg
import helpers.printing as pri

_LOG = logging.getLogger(__name__)


# TODO(gp): Move to core.


# #############################################################################
# Config
# #############################################################################

# TODO(gp): Should become a class.


def print_config(config, keys):
    if isinstance(keys, str):
        keys = [keys]
    for k in keys:
        v = config.get(k, "na")
        _LOG.info("%s='%s'", k, v)


def config_to_string(config):
    txt = "\n".join(["%s: %s" % (k, v) for (k, v) in config.items()])
    return txt


def config_to_python(config):
    config_as_str = str(config)
    return config_as_str.replace("OrderedDict", "collections.OrderedDict")


def check_params(config, var_names):
    missing_var_names = []
    for var_name in var_names:
        if var_name not in config:
            missing_var_names.append(var_name)
    if missing_var_names:
        msg = "Missing %s vars (from %s) in config=\n%s" % (
            ",".join(missing_var_names),
            ",".join(var_names),
            config_to_string(config),
        )
        _LOG.error(msg)
        raise ValueError(msg)


def get_param(config, var_name):
    dbg.dassert_in(var_name, config)
    return config[var_name]


# TODO(gp): Use this everywhere.
def get_exception(config, key):
    return ValueError(
        "Invalid %s='%s' in config=\n%s"
        % (key, config[key], pri.space(config_to_string(config)))
    )
