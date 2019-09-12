import logging

import helpers.printing as printing

_LOG = logging.getLogger(__name__)


# TODO(gp): Move to core.


# #############################################################################
# Config
# #############################################################################


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


# TODO(gp): Use this everywhere.
def get_exception(config, key):
    return ValueError(
        "Invalid %s='%s' in config=\n%s"
        % (key, config[key], printing.space(config_to_string(config)))
    )
