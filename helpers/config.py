import logging
import platform

import matplotlib
#import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scipy
import seaborn as sns
import sklearn

_LOG = logging.getLogger(__name__)


def get_system_signature():
    # TODO(gp): Add date, git commit.
    txt = []
    txt.append("python=%s" % platform.python_version())
    txt.append("numpy=%s" % np.__version__)
    txt.append("pandas=%s" % pd.__version__)
    txt.append("seaborn=%s" % sns.__version__)
    txt.append("scipy=%s" % scipy.__version__)
    txt.append("matplotlib=%s" % matplotlib.__version__)
    txt.append("sklearn=%s" % sklearn.__version__)
    #txt.append("joblib=%s" % joblib.__version__)
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