import pandas as pd
import scipy as sp
import statsmodels as sm

MEAN_COL = "mean"
STD_COL = "std"
SKEW_COL = "skew"
KURT_COL = "kurt"
PVAL_COL = "pvals"
TVAL_COL = "tvals"
ADJ_PVAL_COL = "adj_pvals"


def moments(df):
    mean = df.mean()
    std = df.std()
    skew = sp.stats.skew(df, nan_policy="omit")
    kurt = sp.stats.kurtosis(df, nan_policy="omit")
    result = pd.DataFrame(
        {MEAN_COL: mean, STD_COL: std, SKEW_COL: skew, KURT_COL: kurt},
        index=df.columns,
    )
    return result


def ttest_1samp(df, popmean=0, nan_policy="omit"):
    """
    Thin wrapper around scipy's ttest.

    WARNING: Passing in df.dropna(how='all') vs df.dropna() (which defaults to
    'any') can yield different results. Safest is to NOT DROP NANs in the input
    and instead use `nan_policy='omit'`.

    :param df: DataFrame with samples along rows, groups along columns.
    :return: DataFrame with t-value and p-value columns, rows like df's columns
    """
    tvals, pvals = sp.stats.ttest_1samp(
        df, popmean=popmean, nan_policy=nan_policy
    )
    result = pd.DataFrame({TVAL_COL: tvals, PVAL_COL: pvals}, index=df.columns)
    return result


def multipletests(srs, method="fdr_bh"):
    """
    Thin wrapper around statsmodel's multipletests.

    Returns results in a series indexed like srs.

    Documentation at
    https://www.statsmodels.org/stable/generated/statsmodels.stats.multitest.multipletests.html

    :param srs: Series with pvalues
    :param method: Passed to statsmodel's
    :return:
    """
    pvals_corrected = sm.stats.multitest.multipletests(srs, method=method)[1]
    return pd.Series(pvals_corrected, index=srs.index, name=ADJ_PVAL_COL)


def multi_ttest(df, popmean=0, nan_policy="omit", method="fdr_bh"):
    """
    Combines ttest and multitest pvalue adjustment.

    :param df: DataFrame with samples along rows, groups along columns.
    :param pop_mean:
    :param nan_policy:
    :param method:
    :return:
    """
    ttest = ttest_1samp(df, popmean=popmean, nan_policy=nan_policy)
    ttest[ADJ_PVAL_COL] = multipletests(ttest[PVAL_COL])
    return ttest
