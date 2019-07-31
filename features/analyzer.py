import collections
import datetime
import logging

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scipy
import seaborn as sns
import statsmodels.api as sm
from tqdm import tqdm

import helpers.dbg as dbg
import helpers.printing as printing

_LOG = logging.getLogger(__name__)


def _analyze_feature(df, y_var, x_var, use_intercept, nan_mode, x_shift,
                     report_stats):
    _LOG.debug("df=\n%s", df.head(3))
    _LOG.debug("y_var=%s, x_var=%s, use_intercept=%s, nan_mode=%s, x_shift=%s",
               y_var, x_var, use_intercept, nan_mode, x_shift)
    dbg.dassert_isinstance(y_var, str)
    dbg.dassert_isinstance(x_var, str)
    #
    res = collections.OrderedDict()
    res["y_var"] = y_var
    res["x_var"] = x_var
    df_tmp = df[[y_var, x_var]].copy()
    #
    res["x_shift"] = x_shift
    if x_shift != 0:
        df_tmp[x_var] = df_tmp[x_var].shift(x_shift)
    #
    if nan_mode == "drop":
        df_tmp.dropna(inplace=True)
    elif nan_mode == "fill_with_zeros":
        df_tmp.fillna(0.0, inplace=True)
    else:
        raise ValueError("Invalid nan_mode='%s'" % nan_mode)
    res["nan_mode"] = nan_mode
    #
    regr_x_vars = [x_var]
    if use_intercept:
        df_tmp = sm.add_constant(df_tmp)
        regr_x_vars.insert(0, "const")
    res["use_intercept"] = use_intercept
    # Fit.
    reg = sm.OLS(df_tmp[y_var], df_tmp[regr_x_vars])
    model = reg.fit()
    if use_intercept:
        dbg.dassert_eq(len(model.params), 2)
        res["params_const"] = model.params[0]
        res["pvalues_const"] = model.pvalues[0]
        res["params_var"] = model.params[1]
        res["pvalues_var"] = model.pvalues[1]
    else:
        dbg.dassert_eq(len(model.params), 1)
        res["params_var"] = model.params[0]
        res["pvalues_var"] = model.pvalues[0]
    res["nobs"] = model.nobs
    res["condition_number"] = model.condition_number
    res["rsquared"] = model.rsquared
    res["rsquared_adj"] = model.rsquared_adj
    # TODO(gp): Add pnl, correlation, hitrate.
    #
    if report_stats:
        txt = printing.frame(
            "y_var=%s, x_var=%s, use_intercept=%s, nan_mode=%s, x_shift=%s" %
            (y_var, x_var, use_intercept, nan_mode, x_shift))
        _LOG.info("\n%s", txt)
        _LOG.info("model.summary()=\n%s", model.summary())
        sns.regplot(x=df[x_var], y=df[y_var])
        plt.show()
    return res


def analyze_features(df,
                     y_var,
                     x_vars,
                     use_intercept,
                     nan_mode="drop",
                     x_shifts=None,
                     report_stats=False):
    if x_shifts is None:
        x_shifts = [0]
    res_df = []
    for x_var in x_vars:
        _LOG.debug("x_var=%s", x_var)
        for x_shift in x_shifts:
            _LOG.debug("x_shifts=%s", x_shift)
            res_tmp = _analyze_feature(df, y_var, x_var, use_intercept,
                                       nan_mode, x_shift, report_stats)
            res_df.append(res_tmp)
    return pd.DataFrame(res_df)



