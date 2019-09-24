import functools
import logging

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)

# #############################################################################

# TODO(gp): Use the idiom like kut.read_data().


def read_sentiment_data_from_disk(file_name):
    data = pd.read_csv(file_name)
    _LOG.debug("After reading data=\n%s", data.head(3))
    # Remove unnamed columns.
    # TODO(gp): Fix this by reading this as index.
    data = data.iloc[:, 1:]
    # Sanity check.
    dbg.dassert(np.all(sorted(data["created_at"]) == data["created_at"]))
    _LOG.info(
        "Data is in [%s, %s]",
        data["created_at"].iloc[0],
        data["created_at"].iloc[-1],
    )
    # Format data.
    if "created_at" in data.columns:
        data.set_index("created_at", drop=True, inplace=True)
        data.index.name = "datetime"
        data.index = pd.to_datetime(data.index)
    if "_id" in data.columns:
        data.drop("_id", axis=1, inplace=True)
    _LOG.debug("After reformatting data=\n%s", data.head(3))
    # TODO(gp): Add timezone info. For now we assume ET.
    return data


@functools.lru_cache(maxsize=None)
def read_data_memcached(file_name):
    _LOG.info("Reading file_name='%s'", file_name)
    return read_sentiment_data_from_disk(file_name)


def read_data_from_config(config):
    _LOG.info("Reading data ...")
    return read_data_memcached(config["feature_file_name"])


def get_raw_features():
    return "demand+ demand- inventory+ inventory- supply+ supply-".split()


# #############################################################################


def compute_features_from_config(config, data):
    old_features = data.columns.tolist()
    #
    feat_names = get_raw_features()
    feat_name = "majority1"
    data[feat_name] = data[feat_names].idxmax(axis=1)
    #
    feat_names = "demand inventory supply".split()
    for feat_name in feat_names:
        data[feat_name] = data[feat_name + "+"] - data[feat_name + "-"]
    #
    feat_name = "majority2"
    data[feat_name] = data[feat_names].idxmax(axis=1)
    #
    new_features = [f for f in data.columns if f not in old_features]
    return data, new_features


def sample_features_from_config(config, data):
    # Resample.
    resampler = data.resample(
        config["agg_interval"], closed="left", label="right"
    )
    if config["agg_function"] == "mean":
        data = resampler.mean()
    elif config["agg_function"] == "sum":
        data = resampler.sum()
    else:
        raise config.get_exception("agg_function")
    if config["agg_interval"] in ("1B",):
        data.index.name = "date"
    elif config["agg_interval"] in ("1T", "5T"):
        data.index.name = "datetime"
    else:
        raise config.get_exception("agg_interval")
    _LOG.debug("data=\n%s", data.head(3))
    return data


def plot_raw_data_pdf(data, features):
    for c in features:
        dbg.dassert_in(c, features)
        ax = data[c].plot(kind="hist")
        ax.set_xlabel(c)
        plt.show()
