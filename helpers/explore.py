"""
Utility functions for Jupyter notebook for:
- formatting
- transforming pandas data structures
- computing common stats
"""

import os

import IPython
import pandas as pd
from IPython.display import display
from tqdm import tqdm

import helpers.dbg as dbg
import helpers.printing as print_

# #############################################################################
# Pandas helpers.
# #############################################################################


# TODO(gp): Use logging when appropriate or a wrapper.


def remove_nans(df, *args, **kwargs):
    # TODO(gp): Remove rows completely empty.
    num_rows_before = df.shape[0]
    df = df.dropna(*args, **kwargs)
    num_rows_after = df.shape[0]
    print("removed: %s" % print_.perc(num_rows_before, num_rows_after))
    return df


def get_variable_cols(df, threshold=1):
    """


    :param df:
    :param threshold:  (Default value = 1)

    """
    cols = []
    const_cols = []
    for i in df.columns:
        if len(df[i].unique()) <= threshold:
            const_cols.append(i)
        else:
            cols.append(i)
    return cols, const_cols


def remove_const_columns(df, threshold=1, verb=False):
    """


    :param df:
    :param threshold:  (Default value = 1)
    :param verb:  (Default value = False)

    """
    cols, const_cols = get_variable_cols(df, threshold=threshold)
    if verb:
        print("# Constant cols")
        for c in const_cols:
            print(("  %s: %s" % (c, print_.list_to_string(list(map(str, df[c].unique()))))))
        print("# Var cols")
        print((print_.list_to_string(cols)))
    return df[cols]


def add_count_as_idx(df):
    """


    :param df:

    """
    col = []
    for k in range(df.shape[0]):
        col.append("%2.d / %2.d" % (k + 1, df.shape[0]))
    df.insert(0, "index", col)
    return df


def add_pct(res,
            col_name,
            total,
            dst_col_name,
            num_digits=2,
            use_thousands_separator=True):
    """


    :param res:
    :param col_name:
    :param total:
    :param dst_col_name:
    :param num_digits:  (Default value = 2)
    :param use_thousands_separator:  (Default value = True)

    """
    # Add column with percentage right after col_name.
    pos_col_name = res.columns.tolist().index(col_name)
    res.insert(pos_col_name + 1, dst_col_name, (100.0 * res[col_name]) / total)
    # Format.
    res[col_name] = [
        print_.round_digits(
            v, num_digits=None, use_thousands_separator=use_thousands_separator)
        for v in res[col_name]
    ]
    res[dst_col_name] = [
        print_.round_digits(v, num_digits=num_digits, use_thousands_separator=False)
        for v in res[dst_col_name]
    ]
    return res


# #############################################################################
# Pandas data structure stats.
# #############################################################################


def breakdown_table(df,
                    col_name,
                    num_digits=2,
                    use_thousands_separator=True,
                    verb=False):
    """


    :param df:
    :param col_name:
    :param num_digits:  (Default value = 2)
    :param use_thousands_separator:  (Default value = True)
    :param verb:  (Default value = False)

    """
    if isinstance(col_name, list):
        for c in col_name:
            print(("\n" + print_.frame(c).rstrip("\n")))
            res = breakdown_table(df, c)
            print(res)
        return None
    #
    if verb:
        print(("# col_name=%s" % col_name))
    first_col_name = df.columns[0]
    res = df.groupby(col_name)[first_col_name].count()
    res = pd.DataFrame(res)
    res.columns = ["count"]
    res.sort_values(["count"], ascending=False, inplace=True)
    res = res.append(
        pd.DataFrame([df.shape[0]], index=["Total"], columns=["count"]))
    res["pct"] = (100.0 * res["count"]) / df.shape[0]
    # Format.
    res["count"] = [
        print_.round_digits(
            v, num_digits=None, use_thousands_separator=use_thousands_separator)
        for v in res["count"]
    ]
    res["pct"] = [
        print_.round_digits(v, num_digits=num_digits, use_thousands_separator=False)
        for v in res["pct"]
    ]
    if verb:
        for k, df_tmp in df.groupby(col_name):
            print((print_.frame("%s=%s" % (col_name, k))))
            cols = [col_name, "description"]
            with pd.option_context("display.max_colwidth", 100000,
                                   "display.width", 130):
                print((df_tmp[cols]))
    return res


def print_column_variability(df,
                             max_num_vals=3,
                             num_digits=2,
                             use_thousands_separator=True):
    """
    Print statistics about the values in each column of a data frame.
    This is useful to get a sense of which columns are interesting.

    :param df:
    :param max_num_vals:  (Default value = 3)
    :param num_digits:  (Default value = 2)
    :param use_thousands_separator:  (Default value = True)

    """
    print(("# df.columns=%s" % print_.list_to_string(df.columns)))
    res = []
    for c in tqdm(df.columns):
        vals = df[c].unique()
        min_val = min(vals)
        max_val = max(vals)
        if len(vals) <= max_num_vals:
            txt = ", ".join(map(str, vals))
        else:
            txt = ", ".join(map(str, [min_val, "...", max_val]))
        row = ["%20s" % c, len(vals), txt]
        res.append(row)
    res = pd.DataFrame(res, columns=["col_name", "num", "elems"])
    res.sort_values("num", inplace=True)
    res = add_count_as_idx(res)
    res = add_pct(
        res,
        "num",
        df.shape[0],
        "[diff %]",
        num_digits=num_digits,
        use_thousands_separator=use_thousands_separator)
    res.reset_index(drop=True, inplace=True)
    return res


def find_common_columns(names, dfs):
    df = []
    for i, df1 in enumerate(dfs):
        df1 = dfs[i].columns
        for j in range(i + 1, len(dfs)):
            df2 = dfs[j].columns
            common_cols = [c for c in df1 if c in df2]
            df.append((names[i], len(df1), names[j], len(df2), len(common_cols),
                       ", ".join(common_cols)))
    df = pd.DataFrame(
        df,
        columns=[
            "table1", "num_cols1", "num_cols2", "table2", "num_comm_cols",
            "common_cols"
        ])
    return df


# #############################################################################
# Filter.
# #############################################################################


def remove_columns(df, cols, verb=False):
    """


    :param df:
    :param cols:
    :param verb:  (Default value = False)

    """
    to_remove = set(cols).intersection(set(df.columns))
    if verb:
        print(("to_remove=%s" % print_.list_to_string(to_remove)))
    df.drop(to_remove, axis=1, inplace=True)
    if verb:
        display(df.head(3))
        print((print_.list_to_string(df.columns)))
    return df


def filter_with_df(df, filter_df, verb=True):
    """
    Compute a mask for DataFrame df using common columns and values in
    "filter_df".

    :param df:
    :param filter_df:
    :param verb:   (Default value = True)

    """
    mask = None
    for c in filter_df:
        dbg.dassert_in(c, df.columns)
        vals = filter_df[c].unique()
        if mask is None:
            mask = df[c].isin(vals)
        else:
            mask &= df[c].isin(vals)
    if verb:
        print(("after filter=%s" % perc(mask.sum(), len(mask))))
    return mask


def filter_around_time(df,
                       col_name,
                       timestamp,
                       timedelta_before,
                       timedelta_after=None,
                       verb=False):
    """


    :param df:
    :param col_name:
    :param timestamp:
    :param timedelta_before:
    :param timedelta_after:  (Default value = None)
    :param verb:  (Default value = False)

    """
    dbg.dassert_in(col_name, df)
    dbg.dassert_lte(pd.Timedelta(0), timedelta_before)
    if timedelta_after is None:
        timedelta_after = timedelta_before
    dbg.dassert_lte(pd.Timedelta(0), timedelta_after)
    #
    lower_bound = timestamp - timedelta_before
    upper_bound = timestamp + timedelta_after
    mask = (df[col_name] >= lower_bound) & (df[col_name] <= upper_bound)
    #
    if verb:
        print(("Filtering in [%s, %s] selected rows=%s" % (
            lower_bound, upper_bound, perc(mask.sum(), df.shape[0]))))
    return df[mask]


def filter_by_val(df,
                  col_name,
                  min_val,
                  max_val,
                  use_thousands_separator=True,
                  verb=False):
    """
    Filter out rows of df where df[col_name] is not in [min_val, max_val].

    :param df: param col_name:
    :param min_val: param max_val:
    :param use_thousands_separator: Default value = True)
    :param verb: Default value = False)
    :param col_name:
    :param max_val:

    """
    # TODO(gp): If column is ordered, this can be done more efficiently with
    # binary search.
    num_rows = df.shape[0]
    if min_val is not None and max_val is not None:
        dbg.dassert_lte(min_val, max_val)
    mask = None
    if min_val is not None:
        mask = min_val <= df[col_name]
    if max_val is not None:
        mask2 = df[col_name] <= max_val
        if mask is None:
            mask = mask2
        else:
            mask &= mask2
    res = df[mask]
    dbg.dassert_lt(0, res.shape[0])
    if verb:
        print(("Rows kept %s, removed %s rows" % (
            perc(
                res.shape[0],
                num_rows,
                use_thousands_separator=use_thousands_separator),
            perc(
                num_rows - res.shape[0],
                num_rows,
                use_thousands_separator=use_thousands_separator))))
    return res


# #############################################################################
# Printing
# #############################################################################


def display(df,
               threshold=0,
               remove_index=False,
               head=None,
               max_colwidth=100,
               as_txt=False,
               return_df=False):
    """


    :param df:
    :param threshold:  (Default value = 0)
    :param remove_index:  (Default value = False)
    :param head:  (Default value = None)
    :param max_colwidth:  (Default value = 100)
    :param as_txt:  (Default value = False)
    :param return_df:  (Default value = False)

    """
    df_tmp = df
    if threshold == 0:
        df_tmp = remove_const_columns(df_tmp, threshold=threshold)
    # Head.
    if head is not None:
        df_tmp = df_tmp.head(head)
    # Print.
    with pd.option_context("display.max_colwidth", max_colwidth):
        if as_txt:
            print((df.to_string(index=not remove_index)))
        else:
            IPython.core.display.display(
                IPython.core.display.HTML(df.to_html(index=not remove_index)))
    # pylint: disable=inconsistent-return-statements
    if return_df:
        return df_tmp


def to_csv(df, file_name):
    """


    :param df:
    :param file_name:

    """
    file_name = os.path.abspath(file_name)
    df.to_csv(file_name)
    print(("file_name=%s" % file_name))
