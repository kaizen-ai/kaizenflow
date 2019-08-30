import collections
import functools
import glob
import logging
import math

#import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import tqdm
from IPython.display import display
from sklearn import linear_model
from sklearn.model_selection import TimeSeriesSplit

import helpers.config as cfg
import helpers.dbg as dbg
import helpers.finance as fin
import helpers.io_ as io_
import helpers.pickle_ as pickle_

_LOG = logging.getLogger(__name__)

# #############################################################################
# Transform.
# #############################################################################

# Design and invariants for a model pipeline
# - config is built by the user and after then it's read-only
# - config is passed to all the pipeline stages
# - each stage transforms data (e.g., the df) in place to avoid making copy of
#   data
#     - it should not violate the idempotency rules, i.e., each stage should
#       always add data but not remove data
# - decouple functions doing the actual work (e.g., zscoring) and functions
#   `*_from_config` driving the operations from the config
# - `result_bundle` stores the information generated / used by stages of
#   pipeline
#     - it follows a "Blackboard" design pattern
#     - the goal is to not keep changing the interfaces of the pipeline stage
#       to account for the evolving interfaces, by letting each stage push /
#       pull information in the `result_bundle`


def filter_by_time_from_config(config,
                               df,
                               result_bundle,
                               dt_col_name="datetime"):
    cfg.print_config(config, ["start_dt", "end_dt"])
    dbg.dassert_lte(1, df.shape[0])
    #
    start_dt = config.get("start_dt", None)
    end_dt = config.get("end_dt", None)
    df = fin.filter_by_time(df, start_dt, end_dt, result_bundle=result_bundle)
    dbg.dassert_lte(1, df.shape[0])
    return df


# TODO(gp): Switch to fin.zscore.
def zscore_from_config(config, df):
    cfg.print_config(config, ["zscore_style", "zscore_com"])
    dbg.dassert_lte(1, df.shape[0])
    #
    zscore_com = config["zscore_com"]
    # TODO(gp): z-score by day and by hour.
    if config["zscore_style"] == "rolling_std":
        std = df["ret_0"].ewm(com=zscore_com).std()
        # To avoid issues with future peeking.
        std = std.shift(1)
        df["zret_0"] = df["ret_0"] / std
    elif config["zscore_style"] == "rolling_mean_std":
        # TODO(gp): Removing the mean seems to create some artifacts.
        mean = df["ret_0"].ewm(com=zscore_com).mean()
        std = df["ret_0"].ewm(com=zscore_com).std()
        df["zret_0"] = (df["ret_0"] - mean) / std
    else:
        raise ValueError("Invalid param'%s' " % config["zscore_style"])
    dbg.dassert_lte(1, df.shape[0])
    return df


def filter_ath_from_config(config, df):
    """
    Filter according to active trading hours.
    """
    cfg.print_config(config, "filter_ath")
    dbg.dassert_lte(1, df.shape[0])
    if config["filter_ath"]:
        df = fin.filter_ath(df)
    _LOG.info("df.shape=%s", df.shape)
    dbg.dassert_lte(1, df.shape[0])
    return df


# #############################################################################
# Features.
# #############################################################################


def compute_features_from_config(config, df, result_bundle):
    """
    Compute features in-place.
    """
    _LOG.debug("df.shape=%s", df.shape)
    # The rest of the flow (e.g., sklearn, to compute train/test intervals)
    # want indices to be integers.
    df = df.copy()
    df.insert(0, "datetime", df.index)
    df.index = range(df.shape[0])
    y_var = config["target_y_var"]
    x_vars = []
    #dbg.dassert_lte(1, config["delay_lag"])
    for i in range(1 + config["delay_lag"],
                   1 + config["delay_lag"] + config["num_lags"]):
        var = y_var.replace("_0", "_%s" % i)
        _LOG.debug("Computing var=%s", var)
        df[var] = df[y_var].shift(i)
        x_vars.append(var)
    #
    _LOG.info("y_var=%s", y_var)
    _LOG.info("x_vars=%s", x_vars)
    # TODO(gp): Not sure if we should replicate this (since it's already in
    # config) just to make things symmetric.
    result_bundle["y_var"] = y_var
    result_bundle["x_vars"] = x_vars
    # TODO(gp): Add dropna stats using exp.dropna().
    df = df.dropna()
    _LOG.info("df.shape=%s", df.shape)
    return df, result_bundle


# #############################################################################
# Learn.
# #############################################################################

# TODO: result_bundle as object? Config also as object?

# TODO: Maybe ResultSplit, FitModelFromConfig
# RsultSplit and RsultBundle can inherit from dict or OrderedDict
# TODO: Add function to present result table as a nice dataframe.

# `result_split` represents statistics for each train / test split.


def _add_split_stats(df, idxs, tag, result_split):
    """
    Update `result_split` with the stats about a split.
    :param idxs: indices to use to filter df
    :param tag: used to distinguish train / test
    """
    df_tmp = df.iloc[idxs]
    result_split["%s.min_datetime" % tag] = min(df_tmp)
    result_split["%s.max_datetime" % tag] = max(df_tmp)
    result_split["%s.count" % tag] = df_tmp.shape[0]
    return result_split


def _add_model_perf(tag, model, df, idxs, x, y, result_split):
    dbg.dassert_eq(len(idxs), x.shape[0])
    dbg.dassert_eq(x.shape[0], y.shape[0])
    # TODO: Not a great abstraction here. It might be better a key
    # "train.perf" with a dict of results instead of flattening the keys.
    result_split[tag + ".hitrate"] = _compute_model_hitrate(model, x, y)
    result_split[tag + ".pnl_rets"] = _compute_model_pnl_rets(
        df.iloc[idxs], model, x, y)
    result_split[tag + ".sr"] = fin.compute_sr(result_split[tag + ".pnl_rets"])
    return result_split


def get_splits(config, df):
    cv_split_style = config["cv_split_style"]
    _LOG.info("min_date=%s, max_date=%s", df.iloc[0]["datetime"],
              df.iloc[-1]["datetime"])
    if cv_split_style == "TimeSeriesSplit":
        n_splits = config["cv_n_splits"]
        dbg.dassert_lte(1, n_splits)
        tscv = TimeSeriesSplit(n_splits=n_splits)
        splits = list(tscv.split(df))
    elif cv_split_style == "TimeSeriesRollingFolds":
        n_splits = config["cv_n_splits"]
        dbg.dassert_lte(1, n_splits)
        idxs = range(df.shape[0])
        # Split in equal chunks.
        chunk_size = int(math.ceil(len(idxs) / n_splits))
        dbg.dassert_lte(1, chunk_size)
        chunks = [
            idxs[i:i + chunk_size] for i in range(0, len(idxs), chunk_size)
        ]
        dbg.dassert_eq(len(chunks), n_splits)
        #
        splits = list(zip(chunks[:-1], chunks[1:]))
    elif cv_split_style == "TrainTest":
        # TODO: Pass this through config
        #cutoff_date = "2016-01-04 09:30:00"
        cutoff_date = "2010-01-12 09:30:00"
        cutoff_date = pd.to_datetime(cutoff_date)
        idx = np.where(df["datetime"] == cutoff_date)
        #print("idx=", idx)
        dbg.dassert_lt(0, len(idx))
        idx = idx[0][0]
        _LOG.debug("idx=%s", idx)
        splits = [(range(idx), range(idx, len(df)))]
    else:
        raise ValueError("Invalid cv_split_style='%s'" % cv_split_style)
    return splits


def splits_to_string(splits, df=None):
    txt = "n_splits=%s\n" % len(splits)
    for train_idxs, test_idxs in splits:
        if df is None:
            txt += "  train=%s [%s, %s]" % (len(train_idxs), min(train_idxs),
                                            max(train_idxs))
            txt += ", test=[%s, %s] %s" % (len(test_idxs), min(test_idxs),
                                           max(test_idxs))
        else:
            txt += "  train=%s [%s, %s]" % (len(train_idxs),
                                            min(df.iloc[train_idxs]),
                                            max(df.iloc[train_idxs]))
            txt += ", test=%s [%s, %s]" % (len(test_idxs),
                                           min(df.iloc[test_idxs]),
                                           max(df.iloc[test_idxs]))
        txt += "\n"
    return txt


def fit_model_from_config(config, df, result_bundle):
    # Compute the splits to be used in the train / test loop.
    splits = get_splits(config, df)
    #
    y_var = result_bundle["y_var"]
    x_vars = result_bundle["x_vars"]
    # TODO: Fix this
    #result_bundle["num_splits"] = len(splits)
    result_bundle["num_splits"] = 0
    #
    result_splits = []
    for i, (train_idxs, test_idxs) in enumerate(splits):
        # Check that there is no intersection between train / test.
        # This is not true when we use the entire dataset for both train / test.
        dbg.dassert_eq(len(set(train_idxs).intersection(set(test_idxs))), 0)
        result_bundle["num_splits"] += 1
        result_split = {}

        # ==== Train ====
        tag = "train"
        x_train = df.iloc[train_idxs][x_vars]
        y_train = df.iloc[train_idxs][y_var]
        #
        reg = linear_model.LinearRegression()
        model = reg.fit(x_train, y_train)
        result_split["idx"] = i
        # TODO: Improve the name of the split.
        result_split["name"] = "split_%s" % i
        result_split["model_coeffs"] = [model.intercept_] + model.coef_.tolist()
        # TODO: Should we pass a 1 column in the dataframe and make all the
        # predictors handled in the same way?
        result_split["model_x_vars"] = ["intercept"] + x_vars
        # Add stats about splits and model.
        result_split = _add_split_stats(df, train_idxs, tag, result_split)
        result_split = _add_model_perf(tag, model, df, train_idxs, x_train,
                                       y_train, result_split)
        perf_as_string = _model_perf_to_string(result_split, tag)

        # ==== Test ====
        tag = "test"
        x_test = df.iloc[test_idxs][x_vars]
        y_test = df.iloc[test_idxs][y_var]
        # Add stats about splits and model.
        result_split = _add_split_stats(df, test_idxs, tag, result_split)
        result_split = _add_model_perf(tag, model, df, test_idxs, x_test,
                                       y_test, result_split)
        perf_as_string += " " + _model_perf_to_string(result_split, tag)

        # Print results for this split.
        result_split["perf_as_string"] = perf_as_string
        print(perf_as_string)

        result_splits.append(result_split)
    #
    result_bundle["result_split"] = result_splits
    dbg.dassert_eq(
        len(result_bundle["result_split"]), result_bundle["num_splits"])
    return result_bundle


# #############################################################################
# Evaluate model.
# #############################################################################


def _compute_model_hitrate(model, x, y):
    hat_y = model.predict(x)
    hr_perf = collections.OrderedDict()
    zero_thr = 1e-6
    #zero_thr = 0.0
    for w in ("pos", "zero", "neg"):
        if w == "pos":
            mask = y >= zero_thr
        elif w == "neg":
            mask = y <= -zero_thr
        else:
            mask = abs(y) < zero_thr
        if w == "zero":
            # We treat the rets close to 0 in a special way.
            # TOOD(gp): Find a better solution. Maybe a confusion matrix.
            y_zero = abs(y[mask]) < zero_thr
            hat_y_zero = abs(hat_y[mask]) < zero_thr
            acc = (y_zero == hat_y_zero).mean()
        else:
            acc = (np.sign(y[mask]) == np.sign(hat_y[mask])).mean()
        #acc = "%.3f" % acc + "(%s)" % len(y[mask])
        hr_perf[w + ".acc"] = acc
        hr_perf[w + ".num"] = len(y[mask])
    return hr_perf


def _hitrate_perf_to_string(hr_perf):
    res = []
    for k, v in hr_perf.items():
        if k.endswith(".acc"):
            fmt = "%.3f"
        elif k.endswith(".num"):
            fmt = "%d"
        else:
            raise ValueError("Invalid key '%s' in hr_perf='%s'" % (k, hr_perf))
        res.append(k + "=" + fmt % v)
    res = " ".join(res)
    return res


def _compute_model_pnl_rets(df, model, x, y):
    hat_y = model.predict(x)
    pnl_rets = y * hat_y
    # Package results into a dataframe.
    pnl_rets = pd.DataFrame(pnl_rets)
    pnl_rets.index = df["datetime"]
    return pnl_rets


# TRAIN: pos= 0.571(1808) zero= 0.000(1165) neg= 0.489(1756) sr=7.0 ||
#   TEST: pos= 0.550(1810) zero= 0.000(1149) neg= 0.483(1770) sr=8.5


def _model_perf_to_string(result_split, tag):
    res = tag + ": "
    res += _hitrate_perf_to_string(result_split[tag + ".hitrate"])
    res += " sr=%.3f" % result_split[tag + ".sr"]
    return res


# #############################################################################
# ResultBundle.
# #############################################################################

# TODO: Method of ResultBundle?


def shorten_for_debug(obj):
    if isinstance(obj, list):
        return [shorten_for_debug(l) for l in obj]
    elif isinstance(obj, tuple):
        return tuple([shorten_for_debug(l) for l in obj])
    elif isinstance(obj, dict):
        return {
            shorten_for_debug(k): shorten_for_debug(v) for k, v in obj.items()
        }
    elif isinstance(obj, pd.DataFrame):
        # Shorten a dataframe to a smaller number of rows.
        return obj.head(3)
    elif isinstance(obj, pd.Series):
        return obj.head(3)
    else:
        return obj


def type_for_debug(obj):
    if isinstance(obj, list):
        return "(%s) list of " % len(obj), type_for_debug(obj[0])
    elif isinstance(obj, tuple):
        return "(%s) tuple of " % len(obj), type_for_debug(obj[0])
    elif isinstance(obj, dict):
        return {str(k): type_for_debug(v) for k, v in obj.items()}
    elif isinstance(obj, pd.DataFrame):
        # Shorten a dataframe to a smaller number of rows.
        return "(%s x %s) dataframe" % obj.shape
    elif isinstance(obj, pd.Series):
        return "(%s) series" % obj.shape[0]
    else:
        val = str(type(obj))
        # <class 'numpy.float64'> -> <numpy.float64>
        val = val.replace("<class '", "<").replace("'>", ">")
        return val


def _accumulate_from_result_split(result_bundle, key):
    """
    Extract and accumualte the field `key` in each `result_split` stored inside
    a `result_bundle`, concatenating into a list.
    """
    res = []
    for result_split in result_bundle["result_split"]:
        dbg.dassert_in(key, result_split)
        res.append(result_split[key])
    return res


def process_model_coeffs(config, result_bundle, report_stats=True):
    """
    Given the data in `result_bundle` for multiple `result_split`, format the
    model coefficients in a dataframe.
    :param report_stats: if True report statistics on the coefficients
    (typically used in notebooks and not in batch mode)
    """
    _ = config
    split_names = _accumulate_from_result_split(result_bundle, "name")
    model_df = _accumulate_from_result_split(result_bundle, "model_coeffs")
    # TODO: Check that they are all the same.
    model_x_vars = _accumulate_from_result_split(result_bundle,
                                                 "model_x_vars")[0]
    #
    model_df = pd.DataFrame(model_df, index=split_names, columns=model_x_vars)
    # TODO: model_coeffs_df?
    result_bundle["model_coeffs"] = model_df
    if report_stats:
        display(model_df)
        model_df.plot(kind="bar")
    return result_bundle


def process_test_pnl(config, result_bundle, report_stats=True):
    """
    Given the data in `result_bundle` for multiple `result_split`, aggregate
    the results into a pnl curve for the model.
    :param report_stats: as `process_model_coeffs.report_stats`
    """
    pnl_rets = _accumulate_from_result_split(result_bundle, "test.pnl_rets")
    pnl_rets = pd.concat(pnl_rets)
    result_bundle["concat_test_pnl_rets"] = pnl_rets
    #
    sr = fin.compute_sr(pnl_rets)
    txt = "sr=%.3f" % sr
    #
    y_var = result_bundle["y_var"]
    kratio = fin.compute_kratio(pnl_rets, y_var)
    txt += "\nkratio=%.1f" % kratio
    #
    txt += "\n\n" + cfg.config_to_string(config)
    #
    if report_stats:
        ax = pnl_rets.resample("1B").sum().cumsum().plot()
        ax.text(
            0.05,
            0.6,
            txt,
            horizontalalignment='left',
            verticalalignment='center',
            transform=ax.transAxes)
    return result_bundle


def to_file(config, result_bundle, file_name):
    io_.create_enclosing_dir(file_name)
    data = {
        "config": config,
        "result_bundle": result_bundle,
    }
    pickle_.to_pickle(data, file_name, backend="pickle_gzip")


def from_file(file_name):
    data = pickle_.from_pickle(file_name, backend="pickle_gzip")
    dbg.dassert_in("config", data)
    dbg.dassert_in("result_bundle", data)
    return data


@functools.lru_cache(maxsize=None)
def load_all_result_bundles(dir_name):
    file_names = glob.glob(dir_name + "/*.result_bundle.pkl.gz")
    print("Found %s file_names" % len(file_names))
    data = []
    for file_name in tqdm.tqdm(file_names):
        _LOG.debug("Loading '%s'", file_name)
        data_tmp = from_file(file_name)
        data.append(data_tmp)
    return data
