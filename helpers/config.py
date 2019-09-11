import logging
import platform

import matplotlib

import helpers.printing as printing

_LOG = logging.getLogger(__name__)


# TODO(gp): Move it somewhere else. Maybe dbg?
def get_system_signature():
    import numpy as np
    import pandas as pd
    import joblib
    import scipy
    import seaborn as sns
    import sklearn
    import statsmodels

    txt = []
    txt.append("python=%s\n" % platform.python_version())
    # TODO(gp): Add date, git commit.
    txt.append("joblib=%s" % joblib.__version__)
    txt.append("matplotlib=%s" % matplotlib.__version__)
    txt.append("numpy=%s" % np.__version__)
    txt.append("pandas=%s" % pd.__version__)
    txt.append("scipy=%s" % scipy.__version__)
    txt.append("seaborn=%s" % sns.__version__)
    txt.append("sklearn=%s" % sklearn.__version__)
    txt.append("statsmodels=%s" % statsmodels.__version__)
    return "\n".join(txt)


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
