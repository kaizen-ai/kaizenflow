import datetime
import io
import logging
import os
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pytest

import helpers.hpandas as hpandas
import helpers.hprint as hprint
import helpers.hs3 as hs3
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)

_AWS_PROFILE = "am"


class Test_dassert_is_unique1(hunitest.TestCase):
    def get_df1(self) -> pd.DataFrame:
        """
        Return a df without duplicated index.
        """
        num_rows = 5
        idx = [
            pd.Timestamp("2000-01-01 9:00") + pd.Timedelta(minutes=i)
            for i in range(num_rows)
        ]
        values = [[i] for i in range(len(idx))]
        df = pd.DataFrame(values, index=idx)
        _LOG.debug("df=\n%s", df)
        #
        act = hpandas.df_to_str(df)
        exp = r"""
                             0
        2000-01-01 09:00:00  0
        2000-01-01 09:01:00  1
        2000-01-01 09:02:00  2
        2000-01-01 09:03:00  3
        2000-01-01 09:04:00  4"""
        self.assert_equal(act, exp, fuzzy_match=True)
        return df

    def test_dassert_is_unique1(self) -> None:
        df = self.get_df1()
        hpandas.dassert_unique_index(df)

    def get_df2(self) -> pd.DataFrame:
        """
        Return a df with duplicated index.
        """
        num_rows = 4
        idx = [
            pd.Timestamp("2000-01-01 9:00") + pd.Timedelta(minutes=i)
            for i in range(num_rows)
        ]
        idx.append(idx[0])
        values = [[i] for i in range(len(idx))]
        df = pd.DataFrame(values, index=idx)
        _LOG.debug("df=\n%s", df)
        #
        act = hpandas.df_to_str(df)
        exp = r"""
                             0
        2000-01-01 09:00:00  0
        2000-01-01 09:01:00  1
        2000-01-01 09:02:00  2
        2000-01-01 09:03:00  3
        2000-01-01 09:00:00  4"""
        self.assert_equal(act, exp, fuzzy_match=True)
        return df

    def test_dassert_is_unique2(self) -> None:
        df = self.get_df2()
        with self.assertRaises(AssertionError) as cm:
            hpandas.dassert_unique_index(df)
        act = str(cm.exception)
        exp = r"""
        * Failed assertion *
        cond=False
        Duplicated rows are:
                             0
        2000-01-01 09:00:00  0
        2000-01-01 09:00:00  4
        """
        self.assert_equal(act, exp, fuzzy_match=True)


# #############################################################################


class Test_to_series1(hunitest.TestCase):
    def helper(self, n: int, exp: str) -> None:
        vals = list(range(n))
        df = pd.DataFrame([vals], columns=[f"a{i}" for i in vals])
        df = df.T
        _LOG.debug("df=\n%s", df)
        srs = hpandas.to_series(df)
        _LOG.debug("srs=\n%s", srs)
        act = str(srs)
        self.assert_equal(act, exp, dedent=True, fuzzy_match=True)

    def test1(self) -> None:
        n = 0
        exp = r"""
        Series([], dtype: float64)
        """
        self.helper(n, exp)

    def test2(self) -> None:
        n = 1
        exp = r"""
        a0    0
        dtype: int64"""
        self.helper(n, exp)

    def test3(self) -> None:
        n = 5
        exp = r"""
        a0    0
        a1    1
        a2    2
        a3    3
        a4    4
        Name: 0, dtype: int64"""
        self.helper(n, exp)


# #############################################################################


class Test_trim_df1(hunitest.TestCase):
    def get_df(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
        """
        Return a df where the CSV txt is read verbatim without inferring dates.

        The `start_time` column is thus a str.
        """
        txt = """
        ,start_time,egid,close
        4,2022-01-04 21:38:00.000000,13684,1146.48
        8,2022-01-04 21:38:00.000000,17085,179.45
        14,2022-01-04 21:37:00.000000,13684,1146.26
        18,2022-01-04 21:37:00.000000,17085,179.42
        24,2022-01-04 21:36:00.000000,13684,1146.0
        27,2022-01-04 21:36:00.000000,17085,179.46
        34,2022-01-04 21:35:00.000000,13684,1146.0
        38,2022-01-04 21:35:00.000000,17085,179.42
        40,2022-01-04 21:34:00.000000,17085,179.42
        44,2022-01-04 21:34:00.000000,13684,1146.0
        """
        txt = hprint.dedent(txt)
        df = pd.read_csv(io.StringIO(txt), *args, index_col=0, **kwargs)
        df["start_time"] = pd.to_datetime(df["start_time"])
        return df

    def test_types1(self) -> None:
        """
        Check the types of a df coming from `read_csv()`.

        The timestamps in `start_time` are left as strings.
        """
        df = self.get_df()
        #
        act = hpandas.df_to_str(
            df, print_dtypes=True, print_shape_info=True, tag="df"
        )
        exp = r"""# df=
        index=[4, 44]
        columns=start_time,egid,close
        shape=(10, 3)
        * type=
        col_name dtype num_unique num_nans first_elem type(first_elem)
        0 index int64 10 / 10 = 100.00% 0 / 10 = 0.00% 4 <class 'numpy.int64'>
        1 start_time datetime64[ns] 5 / 10 = 50.00% 0 / 10 = 0.00% 2022-01-04T21:38:00.000000000 <class 'numpy.datetime64'>
        2 egid int64 2 / 10 = 20.00% 0 / 10 = 0.00% 13684 <class 'numpy.int64'>
        3 close float64 6 / 10 = 60.00% 0 / 10 = 0.00% 1146.48 <class 'numpy.float64'>
        start_time egid close
        4 2022-01-04 21:38:00 13684 1146.48
        8 2022-01-04 21:38:00 17085 179.45
        14 2022-01-04 21:37:00 13684 1146.26
        ...
        38 2022-01-04 21:35:00 17085 179.42
        40 2022-01-04 21:34:00 17085 179.42
        44 2022-01-04 21:34:00 13684 1146.00"""
        self.assert_equal(act, exp, fuzzy_match=True)

    def get_df_with_parse_dates(self) -> pd.DataFrame:
        """
        Read the CSV parsing `start_time` as timestamps.

        The inferred type is a nasty `datetime64` which is not as well-
        behaved as our beloved `pd.Timestamp`.
        """
        df = self.get_df(parse_dates=["start_time"])
        return df

    def test_types2(self) -> None:
        """
        Check the types of a df coming from `read_csv()` forcing parsing some
        values as dates.
        """
        df = self.get_df_with_parse_dates()
        # Check.
        act = hpandas.df_to_str(
            df, print_dtypes=True, print_shape_info=True, tag="df"
        )
        exp = r"""# df=
        index=[4, 44]
        columns=start_time,egid,close
        shape=(10, 3)
        * type=
             col_name           dtype         num_unique        num_nans                     first_elem            type(first_elem)
        0       index           int64  10 / 10 = 100.00%  0 / 10 = 0.00%                              4       <class 'numpy.int64'>
        1  start_time  datetime64[ns]    5 / 10 = 50.00%  0 / 10 = 0.00%  2022-01-04T21:38:00.000000000  <class 'numpy.datetime64'>
        2        egid           int64    2 / 10 = 20.00%  0 / 10 = 0.00%                          13684       <class 'numpy.int64'>
        3       close         float64    6 / 10 = 60.00%  0 / 10 = 0.00%                        1146.48     <class 'numpy.float64'>
                    start_time   egid    close
        4  2022-01-04 21:38:00  13684  1146.48
        8  2022-01-04 21:38:00  17085   179.45
        14 2022-01-04 21:37:00  13684  1146.26
        ...
        38 2022-01-04 21:35:00  17085   179.42
        40 2022-01-04 21:34:00  17085   179.42
        44 2022-01-04 21:34:00  13684  1146.00"""
        self.assert_equal(act, exp, fuzzy_match=True)

    def get_df_with_tz_timestamp(self) -> pd.DataFrame:
        """
        Force the column parsed as `datetime64` into a tz-aware object.

        The resulting object is a `datetime64[ns, tz]`.
        """
        df = self.get_df_with_parse_dates()
        # Apply the tz.
        col_name = "start_time"
        df[col_name] = (
            df[col_name].dt.tz_localize("UTC").dt.tz_convert("America/New_York")
        )
        df[col_name] = pd.to_datetime(df[col_name])
        return df

    def test_types3(self) -> None:
        """
        Check the types of a df coming from `read_csv()` after conversion to
        tz-aware objects.
        """
        df = self.get_df_with_tz_timestamp()
        # Check.
        act = hpandas.df_to_str(
            df, print_dtypes=True, print_shape_info=True, tag="df"
        )
        exp = r"""# df=
        index=[4, 44]
        columns=start_time,egid,close
        shape=(10, 3)
        * type=
             col_name                             dtype         num_unique        num_nans                     first_elem            type(first_elem)
        0       index                             int64  10 / 10 = 100.00%  0 / 10 = 0.00%                              4       <class 'numpy.int64'>
        1  start_time  datetime64[ns, America/New_York]    5 / 10 = 50.00%  0 / 10 = 0.00%  2022-01-04T21:38:00.000000000  <class 'numpy.datetime64'>
        2        egid                             int64    2 / 10 = 20.00%  0 / 10 = 0.00%                          13684       <class 'numpy.int64'>
        3       close                           float64    6 / 10 = 60.00%  0 / 10 = 0.00%                        1146.48     <class 'numpy.float64'>
                          start_time   egid    close
        4  2022-01-04 16:38:00-05:00  13684  1146.48
        8  2022-01-04 16:38:00-05:00  17085   179.45
        14 2022-01-04 16:37:00-05:00  13684  1146.26
        ...
        38 2022-01-04 16:35:00-05:00  17085   179.42
        40 2022-01-04 16:34:00-05:00  17085   179.42
        44 2022-01-04 16:34:00-05:00  13684  1146.00"""
        self.assert_equal(act, exp, fuzzy_match=True)

    # //////////////////////////////////////////////////////////////////////////////

    def helper(
        self,
        df: pd.DataFrame,
        ts_col_name: Optional[str],
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        left_close: bool,
        right_close: bool,
        expected: str,
    ) -> None:
        """
        Run trimming and check the outcome.

        See param description in `hpandas.trim_df`.

        :param expected: the expected oucome of the trimming
        """
        df_trim = hpandas.trim_df(
            df, ts_col_name, start_ts, end_ts, left_close, right_close
        )
        actual = hpandas.df_to_str(df_trim, print_shape_info=True, tag="df_trim")
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_trim_df1(self) -> None:
        """
        Test trimming: baseline case.
        """
        df = self.get_df()
        # Run.
        ts_col_name = "start_time"
        start_ts = pd.Timestamp("2022-01-04 21:35:00")
        end_ts = pd.Timestamp("2022-01-04 21:38:00")
        left_close = True
        right_close = True
        exp = r"""# df_trim=
        index=[4, 38]
        columns=start_time,egid,close
        shape=(8, 3)
        start_time egid close
        4 2022-01-04 21:38:00 13684 1146.48
        8 2022-01-04 21:38:00 17085 179.45
        14 2022-01-04 21:37:00 13684 1146.26
        ...
        27 2022-01-04 21:36:00 17085 179.46
        34 2022-01-04 21:35:00 13684 1146.00
        38 2022-01-04 21:35:00 17085 179.42"""
        self.helper(
            df, ts_col_name, start_ts, end_ts, left_close, right_close, exp
        )

    def test_trim_df2(self) -> None:
        """
        Trim a df with a column that is `datetime64` without tz using a
        `pd.Timestamp` without tz.

        This operation is valid.
        """
        df = self.get_df_with_parse_dates()
        # Run.
        ts_col_name = "start_time"
        start_ts = pd.Timestamp("2022-01-04 21:35:00")
        end_ts = pd.Timestamp("2022-01-04 21:38:00")
        left_close = True
        right_close = True
        exp = r"""# df_trim=
        index=[4, 38]
        columns=start_time,egid,close
        shape=(8, 3)
                    start_time   egid    close
        4  2022-01-04 21:38:00  13684  1146.48
        8  2022-01-04 21:38:00  17085   179.45
        14 2022-01-04 21:37:00  13684  1146.26
        ...
        27 2022-01-04 21:36:00  17085   179.46
        34 2022-01-04 21:35:00  13684  1146.00
        38 2022-01-04 21:35:00  17085   179.42"""
        self.helper(
            df, ts_col_name, start_ts, end_ts, left_close, right_close, exp
        )

    def test_trim_df3(self) -> None:
        """
        Trim a df with a column that is `datetime64` with tz vs a `pd.Timestamp
        with tz.

        This operation is valid.
        """
        df = self.get_df_with_tz_timestamp()
        # Run.
        ts_col_name = "start_time"
        start_ts = pd.Timestamp("2022-01-04 21:35:00", tz="UTC")
        end_ts = pd.Timestamp("2022-01-04 21:38:00", tz="UTC")
        left_close = True
        right_close = True
        exp = r"""# df_trim=
        index=[4, 38]
        columns=start_time,egid,close
        shape=(8, 3)
                          start_time   egid    close
        4  2022-01-04 16:38:00-05:00  13684  1146.48
        8  2022-01-04 16:38:00-05:00  17085   179.45
        14 2022-01-04 16:37:00-05:00  13684  1146.26
        ...
        27 2022-01-04 16:36:00-05:00  17085   179.46
        34 2022-01-04 16:35:00-05:00  13684  1146.00
        38 2022-01-04 16:35:00-05:00  17085   179.42"""
        self.helper(
            df, ts_col_name, start_ts, end_ts, left_close, right_close, exp
        )

    # pylint: disable=line-too-long
    def test_trim_df4(self) -> None:
        """
        Trim a df with a column that is `datetime64` with tz vs a
        `pd.Timestamp` without tz.

        This operation is invalid and we expect an assertion.
        """
        df = self.get_df_with_tz_timestamp()
        # Run.
        ts_col_name = "start_time"
        start_ts = pd.Timestamp("2022-01-04 21:35:00")
        end_ts = pd.Timestamp("2022-01-04 21:38:00")
        left_close = True
        right_close = True
        with self.assertRaises(TypeError) as cm:
            hpandas.trim_df(
                df, ts_col_name, start_ts, end_ts, left_close, right_close
            )
        # Check.
        act = str(cm.exception)
        exp = r"""
        Invalid comparison between dtype=datetime64[ns, America/New_York] and Timestamp"""
        self.assert_equal(act, exp, fuzzy_match=True)

    def test_trim_df5(self) -> None:
        """
        Test filtering on the index.
        """
        df = self.get_df()
        df = df.set_index("start_time")
        # Run.
        ts_col_name = None
        start_ts = pd.Timestamp("2022-01-04 21:35:00")
        end_ts = pd.Timestamp("2022-01-04 21:38:00")
        left_close = True
        right_close = True
        exp = r"""# df_trim=
        index=[2022-01-04 21:35:00, 2022-01-04 21:38:00]
        columns=egid,close
        shape=(8, 2)
        egid close
        start_time
        2022-01-04 21:38:00 13684 1146.48
        2022-01-04 21:38:00 17085 179.45
        2022-01-04 21:37:00 13684 1146.26
        ...
        2022-01-04 21:36:00 17085 179.46
        2022-01-04 21:35:00 13684 1146.00
        2022-01-04 21:35:00 17085 179.42"""
        self.helper(
            df, ts_col_name, start_ts, end_ts, left_close, right_close, exp
        )

    def test_trim_df6(self) -> None:
        """
        Test excluding the lower boundary.
        """
        df = self.get_df()
        # Run.
        ts_col_name = "start_time"
        start_ts = pd.Timestamp("2022-01-04 21:35:00")
        end_ts = pd.Timestamp("2022-01-04 21:38:00")
        left_close = False
        right_close = True
        exp = r"""# df_trim=
        index=[4, 27]
        columns=start_time,egid,close
        shape=(6, 3)
        start_time egid close
        4 2022-01-04 21:38:00 13684 1146.48
        8 2022-01-04 21:38:00 17085 179.45
        14 2022-01-04 21:37:00 13684 1146.26
        18 2022-01-04 21:37:00 17085 179.42
        24 2022-01-04 21:36:00 13684 1146.00
        27 2022-01-04 21:36:00 17085 179.46"""
        self.helper(
            df, ts_col_name, start_ts, end_ts, left_close, right_close, exp
        )

    def test_trim_df7(self) -> None:
        """
        Test excluding the upper boundary.
        """
        df = self.get_df()
        # Run.
        ts_col_name = "start_time"
        start_ts = pd.Timestamp("2022-01-04 21:35:00")
        end_ts = pd.Timestamp("2022-01-04 21:38:00")
        left_close = True
        right_close = False
        exp = r"""# df_trim=
        index=[14, 38]
        columns=start_time,egid,close
        shape=(6, 3)
        start_time egid close
        14 2022-01-04 21:37:00 13684 1146.26
        18 2022-01-04 21:37:00 17085 179.42
        24 2022-01-04 21:36:00 13684 1146.00
        27 2022-01-04 21:36:00 17085 179.46
        34 2022-01-04 21:35:00 13684 1146.00
        38 2022-01-04 21:35:00 17085 179.42"""
        self.helper(
            df, ts_col_name, start_ts, end_ts, left_close, right_close, exp
        )

    def test_trim_df8(self) -> None:
        """
        Test filtering on a sorted column.
        """
        df = self.get_df()
        # Run.
        ts_col_name = "start_time"
        start_ts = pd.Timestamp("2022-01-04 21:35:00")
        end_ts = pd.Timestamp("2022-01-04 21:38:00")
        left_close = True
        right_close = True
        df = df.sort_values(ts_col_name)
        exp = r"""# df_trim=
        index=[4, 38]
        columns=start_time,egid,close
        shape=(8, 3)
        start_time egid close
        34 2022-01-04 21:35:00 13684 1146.00
        38 2022-01-04 21:35:00 17085 179.42
        24 2022-01-04 21:36:00 13684 1146.00
        ...
        18 2022-01-04 21:37:00 17085 179.42
        4 2022-01-04 21:38:00 13684 1146.48
        8 2022-01-04 21:38:00 17085 179.45"""
        self.helper(
            df, ts_col_name, start_ts, end_ts, left_close, right_close, exp
        )

    def test_trim_df9(self) -> None:
        """
        Test filtering on a sorted index.
        """
        df = self.get_df()
        df = df.set_index("start_time")
        # Run.
        ts_col_name = None
        start_ts = pd.Timestamp("2022-01-04 21:35:00")
        end_ts = pd.Timestamp("2022-01-04 21:38:00")
        left_close = True
        right_close = True
        df = df.sort_index()
        exp = r"""# df_trim=
        index=[2022-01-04 21:35:00, 2022-01-04 21:38:00]
        columns=egid,close
        shape=(8, 2)
        egid close
        start_time
        2022-01-04 21:35:00 13684 1146.00
        2022-01-04 21:35:00 17085 179.42
        2022-01-04 21:36:00 13684 1146.00
        ...
        2022-01-04 21:37:00 17085 179.42
        2022-01-04 21:38:00 13684 1146.48
        2022-01-04 21:38:00 17085 179.45"""
        self.helper(
            df, ts_col_name, start_ts, end_ts, left_close, right_close, exp
        )

    def test_trim_df10(self) -> None:
        """
        Test filtering on a sorted index, excluding lower and upper boundaries.
        """
        df = self.get_df()
        df = df.set_index("start_time")
        # Run.
        ts_col_name = None
        start_ts = pd.Timestamp("2022-01-04 21:35:00")
        end_ts = pd.Timestamp("2022-01-04 21:38:00")
        left_close = False
        right_close = False
        df = df.sort_index()
        exp = r"""# df_trim=
        index=[2022-01-04 21:36:00, 2022-01-04 21:37:00]
        columns=egid,close
        shape=(4, 2)
        egid close
        start_time
        2022-01-04 21:36:00 13684 1146.00
        2022-01-04 21:36:00 17085 179.46
        2022-01-04 21:37:00 13684 1146.26
        2022-01-04 21:37:00 17085 179.42"""
        self.helper(
            df, ts_col_name, start_ts, end_ts, left_close, right_close, exp
        )

    def test_trim_df11(self) -> None:
        """
        Test filtering on a non-sorted column, with `start_ts` being None.
        """
        df = self.get_df()
        # Run.
        ts_col_name = "start_time"
        start_ts = None
        end_ts = pd.Timestamp("2022-01-04 21:37:00")
        left_close = True
        right_close = True
        exp = r"""# df_trim=
        index=[14, 44]
        columns=start_time,egid,close
        shape=(8, 3)
        start_time egid close
        14 2022-01-04 21:37:00 13684 1146.26
        18 2022-01-04 21:37:00 17085 179.42
        24 2022-01-04 21:36:00 13684 1146.00
        ...
        38 2022-01-04 21:35:00 17085 179.42
        40 2022-01-04 21:34:00 17085 179.42
        44 2022-01-04 21:34:00 13684 1146.00"""
        self.helper(
            df, ts_col_name, start_ts, end_ts, left_close, right_close, exp
        )

    def test_trim_df12(self) -> None:
        """
        Test filtering on a sorted index, with `end_ts` being None.
        """
        df = self.get_df()
        df = df.set_index("start_time")
        # Run.
        ts_col_name = None
        start_ts = pd.Timestamp("2022-01-04 21:35:00")
        end_ts = None
        left_close = True
        right_close = True
        df = df.sort_index()
        exp = r"""# df_trim=
        index=[2022-01-04 21:35:00, 2022-01-04 21:38:00]
        columns=egid,close
        shape=(8, 2)
        egid close
        start_time
        2022-01-04 21:35:00 13684 1146.00
        2022-01-04 21:35:00 17085 179.42
        2022-01-04 21:36:00 13684 1146.00
        ...
        2022-01-04 21:37:00 17085 179.42
        2022-01-04 21:38:00 13684 1146.48
        2022-01-04 21:38:00 17085 179.45"""
        self.helper(
            df, ts_col_name, start_ts, end_ts, left_close, right_close, exp
        )


@pytest.mark.skip(
    "Used for comparing speed of different trimming methods (CmTask1404)."
)
class Test_trim_df2(Test_trim_df1):
    """
    Test the speed of different approaches to df trimming.
    """

    def get_data(
        self, set_as_index: bool, sort: bool
    ) -> Tuple[pd.DataFrame, str, pd.Timestamp, pd.Timestamp]:
        """
        Get the data for experiments.

        :param set_as_index: whether to set the filtering values as index
        :param sort: whether to sort the filtering values
        :return: the df to trim, the parameters for trimming
        """
        # Get a large df.
        df = self.get_df()
        df = df.loc[df.index.repeat(100000)].reset_index(drop=True)
        # Define the params.
        ts_col_name = "start_time"
        start_ts = pd.Timestamp("2022-01-04 21:35:00")
        end_ts = pd.Timestamp("2022-01-04 21:38:00")
        # Prepare the data.
        if set_as_index:
            df = df.set_index(ts_col_name, append=True, drop=False)
            if sort:
                df = df.sort_index(level=ts_col_name)
        elif sort:
            df = df.sort_values(ts_col_name)
        return df, ts_col_name, start_ts, end_ts

    def check_trimmed_df(
        self,
        df: pd.DataFrame,
        ts_col_name: str,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
    ) -> None:
        """
        Confirm that the trimmed df matches what is expected.

        The trimmed df is compared to the one produced by `hpandas.trim_df()`
        with lower and upper boundaries included. Thus, it is ensured that all the
        trimming methods produce the same output.

        See param descriptions in `hpandas.trim_df()`.

        :param df: the df trimmed in a test, to compare with the `hpandas.trim_df()` one
        """
        # Clean up the df from the test.
        if df.index.nlevels > 1:
            df = df.droplevel(ts_col_name)
        df = df.reset_index(drop=True)
        df = df.sort_values(by=[ts_col_name, "egid"], ascending=[False, True])
        # Get the reference trimmed df.
        left_close = True
        right_close = True
        df_trim_for_comparison = hpandas.trim_df(
            df, ts_col_name, start_ts, end_ts, left_close, right_close
        )
        assert df.equals(df_trim_for_comparison)

    def test_simple_mask_col(self) -> None:
        """
        Trim with a simple mask; filtering on a column.
        """
        set_as_index = False
        sort = False
        df, ts_col_name, start_ts, end_ts = self.get_data(
            set_as_index=set_as_index, sort=sort
        )
        # Run.
        start_time = time.time()
        mask = df[ts_col_name] >= start_ts
        df = df[mask]
        if not df.empty:
            mask = df[ts_col_name] <= end_ts
            df = df[mask]
        end_time = time.time()
        _LOG.info(
            "Simple mask trim (column): %.2f seconds", (end_time - start_time)
        )
        # Check.
        self.check_trimmed_df(df, ts_col_name, start_ts, end_ts)

    def test_simple_mask_idx(self) -> None:
        """
        Trim with a simple mask; filtering on an index.
        """
        set_as_index = True
        sort = False
        df, ts_col_name, start_ts, end_ts = self.get_data(
            set_as_index=set_as_index, sort=sort
        )
        # Run.
        start_time = time.time()
        mask = df.index.get_level_values(ts_col_name) >= start_ts
        df = df[mask]
        if not df.empty:
            mask = df.index.get_level_values(ts_col_name) <= end_ts
            df = df[mask]
        end_time = time.time()
        _LOG.info(
            "Simple mask trim (index): %.2f seconds", (end_time - start_time)
        )
        # Check.
        self.check_trimmed_df(df, ts_col_name, start_ts, end_ts)

    def test_between_col(self) -> None:
        """
        Trim using `pd.Series.between`; filtering on a column.
        """
        set_as_index = False
        sort = False
        df, ts_col_name, start_ts, end_ts = self.get_data(
            set_as_index=set_as_index, sort=sort
        )
        # Run.
        start_time = time.time()
        df = df[df[ts_col_name].between(start_ts, end_ts, inclusive="both")]
        end_time = time.time()
        _LOG.info(
            "`pd.Series.between` trim (column): %.2f seconds",
            (end_time - start_time),
        )
        # Check.
        self.check_trimmed_df(df, ts_col_name, start_ts, end_ts)

    def test_between_idx(self) -> None:
        """
        Trim using `pd.Series.between`; filtering on an index.
        """
        set_as_index = True
        sort = False
        df, ts_col_name, start_ts, end_ts = self.get_data(
            set_as_index=set_as_index, sort=sort
        )
        # Run.
        start_time = time.time()
        filter_values = pd.Series(df.index.get_level_values(ts_col_name)).between(
            start_ts, end_ts, inclusive="both"
        )
        df = df.droplevel(ts_col_name)
        df = df[filter_values]
        end_time = time.time()
        _LOG.info(
            "`pd.Series.between` trim (index): %.2f seconds",
            (end_time - start_time),
        )
        # Check.
        self.check_trimmed_df(df, ts_col_name, start_ts, end_ts)

    def test_truncate_non_sorted_col(self) -> None:
        """
        Trim using `pd.DataFrame.truncate`; filtering on a non-sorted column.
        """
        set_as_index = False
        sort = False
        df, ts_col_name, start_ts, end_ts = self.get_data(
            set_as_index=set_as_index, sort=sort
        )
        # Run.
        start_time = time.time()
        df = df.set_index(df[ts_col_name], append=True).sort_index(
            level=ts_col_name
        )
        df = df.swaplevel()
        df = df.truncate(before=start_ts, after=end_ts)
        end_time = time.time()
        _LOG.info(
            "`pd.DataFrame.truncate` trim (non-sorted column): %.2f seconds",
            (end_time - start_time),
        )
        # Check.
        self.check_trimmed_df(df, ts_col_name, start_ts, end_ts)

    def test_truncate_non_sorted_idx(self) -> None:
        """
        Trim using `pd.DataFrame.truncate`; filtering on a non-sorted index.
        """
        set_as_index = True
        sort = False
        df, ts_col_name, start_ts, end_ts = self.get_data(
            set_as_index=set_as_index, sort=sort
        )
        df = df.swaplevel()
        # Run.
        start_time = time.time()
        df = df.sort_index(level=ts_col_name)
        df = df.truncate(before=start_ts, after=end_ts)
        end_time = time.time()
        _LOG.info(
            "`pd.DataFrame.truncate` trim (non-sorted index): %.2f seconds",
            (end_time - start_time),
        )
        # Check.
        self.check_trimmed_df(df, ts_col_name, start_ts, end_ts)

    def test_truncate_sorted_col(self) -> None:
        """
        Trim using `pd.DataFrame.truncate`; filtering on a sorted column.
        """
        set_as_index = False
        sort = True
        df, ts_col_name, start_ts, end_ts = self.get_data(
            set_as_index=set_as_index, sort=sort
        )
        # Run.
        start_time = time.time()
        df = df.set_index(ts_col_name, drop=False)
        df = df.truncate(before=start_ts, after=end_ts)
        end_time = time.time()
        _LOG.info(
            "`pd.DataFrame.truncate` trim (sorted column): %.2f seconds",
            (end_time - start_time),
        )
        # Check.
        self.check_trimmed_df(df, ts_col_name, start_ts, end_ts)

    def test_truncate_sorted_idx(self) -> None:
        """
        Trim using `pd.DataFrame.truncate`; filtering on a sorted index.
        """
        set_as_index = True
        sort = True
        df, ts_col_name, start_ts, end_ts = self.get_data(
            set_as_index=set_as_index, sort=sort
        )
        df = df.swaplevel()
        # Run.
        start_time = time.time()
        df = df.truncate(before=start_ts, after=end_ts)
        end_time = time.time()
        _LOG.info(
            "`pd.DataFrame.truncate` trim (sorted index): %.2f seconds",
            (end_time - start_time),
        )
        # Check.
        self.check_trimmed_df(df, ts_col_name, start_ts, end_ts)

    def test_searchsorted_non_sorted_col(self) -> None:
        """
        Trim using `pd.Series.searchsorted`; filtering on a non-sorted column.
        """
        set_as_index = False
        sort = False
        df, ts_col_name, start_ts, end_ts = self.get_data(
            set_as_index=set_as_index, sort=sort
        )
        # Run.
        start_time = time.time()
        df = df.sort_values(ts_col_name, ascending=True)
        left_idx = df[ts_col_name].searchsorted(start_ts, side="left")
        right_idx = df[ts_col_name].searchsorted(end_ts, side="right")
        df = df.iloc[left_idx:right_idx]
        end_time = time.time()
        _LOG.info(
            "`pd.Series.searchsorted` trim (non-sorted column): %.2f seconds",
            (end_time - start_time),
        )
        # Check.
        self.check_trimmed_df(df, ts_col_name, start_ts, end_ts)

    def test_searchsorted_non_sorted_idx(self) -> None:
        """
        Trim using `pd.Series.searchsorted`; filtering on a non-sorted index.
        """
        set_as_index = True
        sort = False
        df, ts_col_name, start_ts, end_ts = self.get_data(
            set_as_index=set_as_index, sort=sort
        )
        # Run.
        start_time = time.time()
        df = df.sort_index(level=ts_col_name)
        left_idx = df.index.get_level_values(ts_col_name).searchsorted(
            start_ts, side="left"
        )
        right_idx = df.index.get_level_values(ts_col_name).searchsorted(
            end_ts, side="right"
        )
        df = df.iloc[left_idx:right_idx]
        end_time = time.time()
        _LOG.info(
            "`pd.Series.searchsorted` trim (non-sorted index): %.2f seconds",
            (end_time - start_time),
        )
        # Check.
        self.check_trimmed_df(df, ts_col_name, start_ts, end_ts)

    def test_searchsorted_sorted_col(self) -> None:
        """
        Trim using `pd.Series.searchsorted`; filtering on a sorted column.
        """
        set_as_index = False
        sort = True
        df, ts_col_name, start_ts, end_ts = self.get_data(
            set_as_index=set_as_index, sort=sort
        )
        # Run.
        start_time = time.time()
        left_idx = df[ts_col_name].searchsorted(start_ts, side="left")
        right_idx = df[ts_col_name].searchsorted(end_ts, side="right")
        df = df.iloc[left_idx:right_idx]
        end_time = time.time()
        _LOG.info(
            "`pd.Series.searchsorted` trim (sorted column): %.2f seconds",
            (end_time - start_time),
        )
        # Check.
        self.check_trimmed_df(df, ts_col_name, start_ts, end_ts)

    def test_searchsorted_sorted_idx(self) -> None:
        """
        Trim using `pd.Series.searchsorted`; filtering on a sorted index.
        """
        set_as_index = True
        sort = True
        df, ts_col_name, start_ts, end_ts = self.get_data(
            set_as_index=set_as_index, sort=sort
        )
        # Run.
        start_time = time.time()
        left_idx = df.index.get_level_values(ts_col_name).searchsorted(
            start_ts, side="left"
        )
        right_idx = df.index.get_level_values(ts_col_name).searchsorted(
            end_ts, side="right"
        )
        df = df.iloc[left_idx:right_idx]
        end_time = time.time()
        _LOG.info(
            "`pd.Series.searchsorted` trim (sorted index): %.2f seconds",
            (end_time - start_time),
        )
        # Check.
        self.check_trimmed_df(df, ts_col_name, start_ts, end_ts)


# #############################################################################


class TestDfToStr(hunitest.TestCase):
    @staticmethod
    def get_test_data() -> pd.DataFrame:
        test_data = {
            "dummy_value_1": [1, 2, 3],
            "dummy_value_2": ["A", "B", "C"],
            "dummy_value_3": [0, 0, 0],
        }
        df = pd.DataFrame(data=test_data)
        return df

    def test_df_to_str1(self) -> None:
        """
        Test common call to `df_to_str` with basic df.
        """
        df = self.get_test_data()
        actual = hpandas.df_to_str(df)
        expected = r"""
            dummy_value_1 dummy_value_2  dummy_value_3
        0              1             A              0
        1              2             B              0
        2              3             C              0"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_df_to_str2(self) -> None:
        """
        Test common call to `df_to_str` with tag.
        """
        df = self.get_test_data()
        actual = hpandas.df_to_str(df, tag="df")
        expected = r"""# df=
           dummy_value_1 dummy_value_2  dummy_value_3
        0              1             A              0
        1              2             B              0
        2              3             C              0"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_df_to_str3(self) -> None:
        """
        Test common call to `df_to_str` with print_shape_info.
        """
        df = self.get_test_data()
        actual = hpandas.df_to_str(df, print_shape_info=True)
        expected = r"""
        index=[0, 2]
        columns=dummy_value_1,dummy_value_2,dummy_value_3
        shape=(3, 3)
           dummy_value_1 dummy_value_2  dummy_value_3
        0              1             A              0
        1              2             B              0
        2              3             C              0"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_df_to_str4(self) -> None:
        """
        Test common call to `df_to_str` with print_dtypes.
        """
        df = self.get_test_data()
        actual = hpandas.df_to_str(df, print_dtypes=True)
        expected = r"""
        * type=
                col_name   dtype       num_unique       num_nans first_elem       type(first_elem)
        0          index   int64  3 / 3 = 100.00%  0 / 3 = 0.00%          0  <class 'numpy.int64'>
        1  dummy_value_1   int64  3 / 3 = 100.00%  0 / 3 = 0.00%          1  <class 'numpy.int64'>
        2  dummy_value_2  object  3 / 3 = 100.00%  0 / 3 = 0.00%          A          <class 'str'>
        3  dummy_value_3   int64   1 / 3 = 33.33%  0 / 3 = 0.00%          0  <class 'numpy.int64'>
           dummy_value_1 dummy_value_2  dummy_value_3
        0              1             A              0
        1              2             B              0
        2              3             C              0"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_df_to_str5(self) -> None:
        """
        Test common call to `df_to_str` with multiple args.
        """
        df = self.get_test_data()
        actual = hpandas.df_to_str(
            df, print_shape_info=True, print_dtypes=True, tag="df"
        )
        expected = r"""
        # df=
        index=[0, 2]
        columns=dummy_value_1,dummy_value_2,dummy_value_3
        shape=(3, 3)
        * type=
                col_name   dtype       num_unique       num_nans first_elem       type(first_elem)
        0          index   int64  3 / 3 = 100.00%  0 / 3 = 0.00%          0  <class 'numpy.int64'>
        1  dummy_value_1   int64  3 / 3 = 100.00%  0 / 3 = 0.00%          1  <class 'numpy.int64'>
        2  dummy_value_2  object  3 / 3 = 100.00%  0 / 3 = 0.00%          A          <class 'str'>
        3  dummy_value_3   int64   1 / 3 = 33.33%  0 / 3 = 0.00%          0  <class 'numpy.int64'>
           dummy_value_1 dummy_value_2  dummy_value_3
        0              1             A              0
        1              2             B              0
        2              3             C              0"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_df_to_str6(self) -> None:
        """
        Test common call to `df_to_str` with `pd.Series`.
        """
        df = self.get_test_data()
        actual = hpandas.df_to_str(df["dummy_value_2"])
        expected = r"""
            dummy_value_2
        0             A
        1             B
        2             C
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_df_to_str7(self) -> None:
        """
        Test common call to `df_to_str` with `pd.Index`.
        """
        df = self.get_test_data()
        index = df.index
        index.name = "index_name"
        actual = hpandas.df_to_str(index)
        expected = r"""
        index_name
        0  0
        1  1
        2  2
        """
        self.assert_equal(actual, expected, fuzzy_match=True)


# #############################################################################


class TestDataframeToJson(hunitest.TestCase):
    def test_dataframe_to_json(self) -> None:
        """
        Verify correctness of dataframe to JSON transformation.
        """
        # Initialize a dataframe.
        test_dataframe = pd.DataFrame(
            {
                "col_1": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
                "col_2": [1, 2, 3, 4, 5, 6, 7],
            }
        )
        # Convert dataframe to JSON.
        output_str = hpandas.convert_df_to_json_string(
            test_dataframe, n_head=3, n_tail=3
        )
        self.check_string(output_str)

    def test_dataframe_to_json_uuid(self) -> None:
        """
        Verify correctness of UUID-containing dataframe transformation.
        """
        # Initialize a dataframe.
        test_dataframe = pd.DataFrame(
            {
                "col_1": [
                    uuid.UUID("421470c7-7797-4a94-b584-eb83ff2de88a"),
                    uuid.UUID("22cde381-1782-43dc-8c7a-8712cbdf5ee1"),
                ],
                "col_2": [1, 2],
            }
        )
        # Convert dataframe to JSON.
        output_str = hpandas.convert_df_to_json_string(
            test_dataframe, n_head=None, n_tail=None
        )
        self.check_string(output_str)

    def test_dataframe_to_json_timestamp(self) -> None:
        """
        Verify correctness of transformation of a dataframe with Timestamps.
        """
        # Initialize a dataframe.
        test_dataframe = pd.DataFrame(
            {
                "col_1": [pd.Timestamp("2020-01-01"), pd.Timestamp("2020-05-12")],
                "col_2": [1.0, 2.0],
            }
        )
        # Convert dataframe to JSON.
        output_str = hpandas.convert_df_to_json_string(
            test_dataframe, n_head=None, n_tail=None
        )
        self.check_string(output_str)

    def test_dataframe_to_json_datetime(self) -> None:
        """
        Verify correctness of transformation of a dataframe with datetime.
        """
        # Initialize a dataframe.
        test_dataframe = pd.DataFrame(
            {
                "col_1": [
                    datetime.datetime(2020, 1, 1),
                    datetime.datetime(2020, 5, 12),
                ],
                "col_2": [1.0, 2.0],
            }
        )
        # Convert dataframe to JSON.
        output_str = hpandas.convert_df_to_json_string(
            test_dataframe, n_head=None, n_tail=None
        )
        self.check_string(output_str)


# #############################################################################


class TestFindGapsInDataframes(hunitest.TestCase):
    def test_find_gaps_in_dataframes(self) -> None:
        """
        Verify that gaps are caught.
        """
        # Prepare inputs.
        test_data = pd.DataFrame(
            data={
                "dummy_value_1": [1, 2, 3],
                "dummy_value_2": ["A", "B", "C"],
                "dummy_value_3": [0, 0, 0],
            }
        )
        # Run.
        missing_data = hpandas.find_gaps_in_dataframes(
            test_data.head(2), test_data.tail(2)
        )
        # Check output.
        actual = pd.concat(missing_data)
        actual = hpandas.df_to_str(actual)
        expected = r"""   dummy_value_1 dummy_value_2  dummy_value_3
        2              3             C              0
        0              1             A              0"""
        self.assert_equal(actual, expected, fuzzy_match=True)


# #############################################################################


class TestCompareDataframeRows(hunitest.TestCase):
    def get_test_data(self) -> pd.DataFrame:
        test_data = {
            "dummy_value_1": [0, 1, 3, 2, 0],
            "dummy_value_2": ["0", "A", "C", "B", "D"],
            "dummy_value_3": [0, 0, 0, 0, 0],
        }
        df = pd.DataFrame(data=test_data)
        df.index.name = "test"
        return df

    def test_compare_dataframe_rows1(self) -> None:
        """
        Verify that differences are caught and displayed properly.
        """
        # Prepare inputs.
        test_data = self.get_test_data()
        edited_test_data = test_data.copy()[1:-1]
        edited_test_data.loc[1, "dummy_value_2"] = "W"
        edited_test_data.loc[2, "dummy_value_2"] = "Q"
        edited_test_data.loc[2, "dummy_value_3"] = "1"
        # Run.
        data_difference = hpandas.compare_dataframe_rows(
            test_data, edited_test_data
        )
        # Check output.
        actual = hpandas.df_to_str(data_difference)
        expected = r"""  dummy_value_2       dummy_value_3       test
                   self other          self other
        0             W     A          <NA>  <NA>    1
        1             Q     C             1     0    2"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test_compare_dataframe_rows2(self) -> None:
        """
        Verify that differences are caught and displayed properly without
        original index.
        """
        # Prepare inputs.
        test_data = self.get_test_data()
        test_data.index.name = None
        edited_test_data = test_data.copy()[1:-1]
        edited_test_data.loc[1, "dummy_value_2"] = "W"
        edited_test_data.loc[2, "dummy_value_2"] = "Q"
        edited_test_data.loc[2, "dummy_value_3"] = "1"
        # Run.
        data_difference = hpandas.compare_dataframe_rows(
            test_data, edited_test_data
        )
        # Check output.
        actual = hpandas.df_to_str(data_difference)
        expected = r"""  dummy_value_2       dummy_value_3
                   self other          self other
        0             W     A           NaN   NaN
        1             Q     C             1   0.0"""
        self.assert_equal(actual, expected, fuzzy_match=True)


# #############################################################################


class TestReadDataFromS3(hunitest.TestCase):
    def test_read_csv1(self) -> None:
        s3fs = hs3.get_s3fs(_AWS_PROFILE)
        file_name = os.path.join(
            hs3.get_s3_bucket_path(_AWS_PROFILE),
            "data/kibot/all_stocks_1min/RIMG.csv.gz",
        )
        hs3.dassert_path_exists(file_name, s3fs)
        stream, kwargs = hs3.get_local_or_s3_stream(file_name, s3fs=s3fs)
        hpandas.read_csv_to_df(stream, **kwargs)

    def test_read_parquet1(self) -> None:
        s3fs = hs3.get_s3fs(_AWS_PROFILE)
        file_name = os.path.join(
            hs3.get_s3_bucket_path(_AWS_PROFILE),
            "data/kibot/pq/sp_500_1min/AAPL.pq",
        )
        hs3.dassert_path_exists(file_name, s3fs)
        stream, kwargs = hs3.get_local_or_s3_stream(file_name, s3fs=s3fs)
        hpandas.read_parquet_to_df(stream, **kwargs)


class TestSubsetDf1(hunitest.TestCase):
    def test1(self) -> None:
        # Generate some random data.
        np.random.seed(42)
        df = pd.DataFrame(
            np.random.randint(0, 100, size=(20, 4)), columns=list("ABCD")
        )
        # Subset.
        df2 = hpandas.subset_df(df, nrows=5, seed=43)
        # Check.
        actual = hpandas.df_to_str(df2)
        expected = r"""
           A   B   C   D
        0  51  92  14  71
        1  60  20  82  86
        3  23   2  21  52
        ...
        17  80  35  49   3
        18   1   5  53   3
        19  53  92  62  17
        """
        self.assert_equal(actual, expected, fuzzy_match=True)


class TestDropNa(hunitest.TestCase):
    def test_dropna1(self) -> None:
        """
        Test if all types of NaNs are dropped.
        """
        # Prepare actual result.
        test_data = {
            "dummy_value_1": [np.nan, 1, 3, 2, 0],
            "dummy_value_2": ["0", "A", "B", None, "D"],
            "dummy_value_3": [0, 0, pd.NA, 0, 0],
        }
        test_df = pd.DataFrame(data=test_data)
        # Drop NA.
        actual = hpandas.dropna(test_df, drop_infs=False)
        # Prepare expected result.
        expected = {
            "dummy_value_1": [1, 0],
            "dummy_value_2": ["A", "D"],
            "dummy_value_3": [0, 0],
        }
        # Set the dtype of numeral columns to float to match the dataframe after NA dropping.
        expected = pd.DataFrame(data=expected).astype(
            {"dummy_value_1": "float64", "dummy_value_3": "object"}
        )
        # Set the index of the rows that remained.
        expected = expected.set_index(pd.Index([1, 4]))
        # Check.
        hunitest.compare_df(actual, expected)

    def test_dropna2(self) -> None:
        """
        Test if infs are dropped.
        """
        # Prepare actual result.
        test_data = {
            "dummy_value_1": [-np.inf, 1, 3, 2, 0],
            "dummy_value_2": ["0", "A", "B", "C", "D"],
            "dummy_value_3": [0, 0, np.inf, 0, 0],
        }
        test_df = pd.DataFrame(data=test_data)
        # Drop NA.
        actual = hpandas.dropna(test_df, drop_infs=True)
        # Prepare expected result.
        expected = {
            "dummy_value_1": [1, 2, 0],
            "dummy_value_2": ["A", "C", "D"],
            "dummy_value_3": [0, 0, 0],
        }
        # Set the dtype of numeral columns to float to match the dataframe after NA dropping.
        expected = pd.DataFrame(data=expected).astype(
            {"dummy_value_1": "float64", "dummy_value_3": "float64"}
        )
        # Set the index of the rows that remained.
        expected = expected.set_index(pd.Index([1, 3, 4]))
        # Check.
        hunitest.compare_df(actual, expected)


class TestDropAxisWithAllNans(hunitest.TestCase):
    def test_drop_rows1(self) -> None:
        """
        Test if row full of nans is dropped.
        """
        # Prepare actual result.
        test_data = {
            "dummy_value_1": [np.nan, 2, 3],
            "dummy_value_2": [pd.NA, "B", "C"],  # type: ignore
            "dummy_value_3": [None, 1.0, 1.0],
        }
        test_df = pd.DataFrame(data=test_data)
        # Drop NA.
        actual = hpandas.drop_axis_with_all_nans(test_df, drop_rows=True)
        # Prepare expected result.
        expected = {
            "dummy_value_1": [2, 3],
            "dummy_value_2": ["B", "C"],
            "dummy_value_3": [1.0, 1.0],
        }
        # Set the dtype of numeral columns to float to match the dataframe after NA dropping.
        expected = pd.DataFrame(data=expected).astype(
            {"dummy_value_1": "float64"}
        )
        # Set the index of the rows that remained.
        expected = expected.set_index(pd.Index([1, 2]))
        # Check.
        hunitest.compare_df(actual, expected)

    def test_drop_rows2(self) -> None:
        """
        Test if non fully nan row is not dropped.
        """
        # Prepare actual result.
        test_data = {
            "dummy_value_1": [np.nan, 2, 3],
            "dummy_value_2": ["A", "B", "C"],  # type: ignore
            "dummy_value_3": [None, 1.0, 1.0],
        }
        test_df = pd.DataFrame(data=test_data)
        # Drop NA.
        actual = hpandas.drop_axis_with_all_nans(test_df, drop_rows=True)
        # Prepare expected result.
        expected = {
            "dummy_value_1": [np.nan, 2, 3],
            "dummy_value_2": ["A", "B", "C"],  # type: ignore
            "dummy_value_3": [None, 1.0, 1.0],
        }
        # Set the dtype of numeral columns to float to match the dataframe after NA dropping.
        expected = pd.DataFrame(data=expected).astype(
            {"dummy_value_1": "float64"}
        )
        # Set the index of the rows that remained.
        expected = expected.set_index(pd.Index([0, 1, 2]))
        # Check.
        hunitest.compare_df(actual, expected)

    def test_drop_columns1(self) -> None:
        """
        Test if column full of nans is dropped.
        """
        # Prepare actual result.
        test_data = {
            "dummy_value_1": [np.nan, pd.NA, None],
            "dummy_value_2": ["A", "B", "C"],
            "dummy_value_3": [1.0, 1.0, 1.0],
        }
        test_df = pd.DataFrame(data=test_data)
        # Drop NA.
        actual = hpandas.drop_axis_with_all_nans(test_df, drop_columns=True)
        # Prepare expected result.
        expected = {
            "dummy_value_2": ["A", "B", "C"],
            "dummy_value_3": [1.0, 1.0, 1.0],
        }
        expected = pd.DataFrame(data=expected)
        # Check.
        hunitest.compare_df(actual, expected)

    def test_drop_columns2(self) -> None:
        """
        Test if column that is not full of nans is not dropped.
        """
        # Prepare actual result.
        test_data = {
            "dummy_value_1": [np.nan, 2, None],
            "dummy_value_2": ["A", "B", "C"],
            "dummy_value_3": [1.0, 1.0, 1.0],
        }
        test_df = pd.DataFrame(data=test_data)
        # Drop NA.
        actual = hpandas.drop_axis_with_all_nans(test_df, drop_columns=True)
        # Prepare expected result.
        expected = {
            "dummy_value_1": [np.nan, 2, None],
            "dummy_value_2": ["A", "B", "C"],
            "dummy_value_3": [1.0, 1.0, 1.0],
        }
        expected = pd.DataFrame(data=expected)
        # Check.
        hunitest.compare_df(actual, expected)


class TestDropDuplicates(hunitest.TestCase):
    """
    Test that duplicates are dropped correctly.
    """

    @staticmethod
    def get_test_data() -> pd.DataFrame:
        test_data = [
            (1, "A", 3.2),
            (1, "A", 3.2),
            (10, "B", 3.2),
            (8, "A", 3.2),
            (4, "B", 8.2),
            (10, "B", 3.2),
        ]
        index = [
            "dummy_value1",
            "dummy_value3",
            "dummy_value2",
            "dummy_value1",
            "dummy_value1",
            "dummy_value2",
        ]
        columns = ["int", "letter", "float"]
        df = pd.DataFrame(data=test_data, index=index, columns=columns)
        return df

    def test_drop_duplicates1(self) -> None:
        """
        - use_index = True
        - subset is not None
        """
        # Prepare test data.
        df = self.get_test_data()
        use_index = True
        subset = ["float"]
        no_duplicates_df = hpandas.drop_duplicates(df, use_index, subset=subset)
        no_duplicates_df = hpandas.df_to_str(no_duplicates_df)
        # Prepare expected result.
        expected_signature = r"""
                      int letter  float
        dummy_value1    1      A    3.2
        dummy_value3    1      A    3.2
        dummy_value2   10      B    3.2
        dummy_value1    4      B    8.2
        """
        # Check.
        self.assert_equal(no_duplicates_df, expected_signature, fuzzy_match=True)

    def test_drop_duplicates2(self) -> None:
        """
        - use_index = True
        - subset = None
        """
        # Prepare test data.
        df = self.get_test_data()
        use_index = True
        no_duplicates_df = hpandas.drop_duplicates(df, use_index)
        no_duplicates_df = hpandas.df_to_str(no_duplicates_df)
        # Prepare expected result.
        expected_signature = r"""
                      int letter  float
        dummy_value1    1      A    3.2
        dummy_value3    1      A    3.2
        dummy_value2   10      B    3.2
        dummy_value1    8      A    3.2
        dummy_value1    4      B    8.2
        """
        # Check.
        self.assert_equal(no_duplicates_df, expected_signature, fuzzy_match=True)

    def test_drop_duplicates3(self) -> None:
        """
        - use_index = False
        - subset = None
        """
        # Prepare test data.
        df = self.get_test_data()
        use_index = False
        no_duplicates_df = hpandas.drop_duplicates(df, use_index)
        no_duplicates_df = hpandas.df_to_str(no_duplicates_df)
        # Prepare expected result.
        expected_signature = r"""
                      int letter  float
        dummy_value1    1      A    3.2
        dummy_value2   10      B    3.2
        dummy_value1    8      A    3.2
        dummy_value1    4      B    8.2
        """
        # Check.
        self.assert_equal(no_duplicates_df, expected_signature, fuzzy_match=True)

    def test_drop_duplicates4(self) -> None:
        """
        - use_index = False
        - subset is not None
        """
        # Prepare test data.
        df = self.get_test_data()
        use_index = False
        subset = ["letter", "float"]
        no_duplicates_df = hpandas.drop_duplicates(df, use_index, subset)
        no_duplicates_df = hpandas.df_to_str(no_duplicates_df)
        # Prepare expected result.
        expected_signature = r"""
                      int letter  float
        dummy_value1    1      A    3.2
        dummy_value2   10      B    3.2
        dummy_value1    4      B    8.2
        """
        # Check.
        self.assert_equal(no_duplicates_df, expected_signature, fuzzy_match=True)


class TestCheckAndFilterMatchingColumns(hunitest.TestCase):
    """
    Test that matching columns are filtered correctly.
    """

    @staticmethod
    def get_test_data() -> pd.DataFrame:
        df = pd.DataFrame(
            data=[[3, 4, 5]] * 3,
            columns=["col1", "col2", "col3"],
        )
        return df

    def test_check_and_filter_matching_columns1(self) -> None:
        """
        - required columns = received columns
        - `filter_data_mode` = "assert"
        """
        df = self.get_test_data()
        columns = ["col1", "col2", "col3"]
        filter_data_mode = "assert"
        df = hpandas.check_and_filter_matching_columns(
            df, columns, filter_data_mode
        )
        actual_columns = df.columns.to_list()
        self.assert_equal(str(actual_columns), str(columns))

    def test_check_and_filter_matching_columns2(self) -> None:
        """
        -  received columns contain some columns apart from required ones
        - `filter_data_mode` = "assert"
        """
        df = self.get_test_data()
        columns = ["col1", "col3"]
        filter_data_mode = "assert"
        with self.assertRaises(AssertionError):
            hpandas.check_and_filter_matching_columns(
                df, columns, filter_data_mode
            )

    def test_check_and_filter_matching_columns3(self) -> None:
        """
        - received columns do not contain some of required columns
        - `filter_data_mode` = "assert"
        """
        df = self.get_test_data()
        columns = ["col1", "col4"]
        filter_data_mode = "assert"
        with self.assertRaises(AssertionError):
            hpandas.check_and_filter_matching_columns(
                df, columns, filter_data_mode
            )

    def test_check_and_filter_matching_columns4(self) -> None:
        """
        - received columns contain some columns apart from required ones
        - `filter_data_mode` = "warn_and_trim"
        """
        df = self.get_test_data()
        columns = ["col1", "col3"]
        filter_data_mode = "warn_and_trim"
        df = hpandas.check_and_filter_matching_columns(
            df, columns, filter_data_mode
        )
        actual_columns = df.columns.to_list()
        self.assert_equal(str(actual_columns), str(columns))

    def test_check_and_filter_matching_columns5(self) -> None:
        """
        - received columns do not contain some of required columns
        - `filter_data_mode` = "warn_and_trim"
        """
        df = self.get_test_data()
        columns = ["col1", "col2", "col4"]
        filter_data_mode = "warn_and_trim"
        df = hpandas.check_and_filter_matching_columns(
            df, columns, filter_data_mode
        )
        actual_columns = df.columns.to_list()
        expected_columns = ["col1", "col2"]
        self.assert_equal(str(actual_columns), str(expected_columns))


# #############################################################################


class Test_merge_dfs1(hunitest.TestCase):
    """
    Test that 2 dataframes are merged correctly.
    """

    @staticmethod
    def get_dataframe(data: Dict, index: List[int]) -> pd.DataFrame:
        df = pd.DataFrame.from_dict(data)
        index = pd.Index(index)
        df = df.set_index(index, drop=True)
        return df

    def test1(self) -> None:
        """
        Overlap of `threshold_col` values is 100%.
        """
        # Create test data.
        data1 = {
            "col1": [1, 10, 100],
            "col2": [2, np.nan, 200],
            "col3": [3, 30, 300],
            "threshold_col": [7, 70, 700],
        }
        index1 = [1, 2, 3]
        df1 = self.get_dataframe(data1, index1)
        #
        data2 = {
            "col3": [3, 30, 300],
            "col4": [4, 40, 400],
            "col5": [5, np.nan, 500],
            "threshold_col": [7, 70, 700],
        }
        index2 = [3, 4, 5]
        df2 = self.get_dataframe(data2, index2)
        #
        threshold_col_name = "threshold_col"
        cols_to_merge_on = ["col3", "threshold_col"]
        merged_df = hpandas.merge_dfs(
            df1,
            df2,
            threshold_col_name,
            how="outer",
            on=cols_to_merge_on,
        )
        # Set expected values.
        expected_length = 3
        expected_column_names = [
            "col1",
            "col2",
            "col3",
            "col4",
            "col5",
            "threshold_col",
        ]
        expected_column_unique_values = None
        expected_signature = r"""
        # df=
        index=[0, 2]
        columns=col1,col2,col3,threshold_col,col4,col5
        shape=(3, 6)
            col1   col2  col3  threshold_col  col4   col5
        0     1    2.0     3              7     4    5.0
        1    10    NaN    30             70    40    NaN
        2   100  200.0   300            700   400  500.0
        """
        # Check.
        self.check_df_output(
            merged_df,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    def test2(self) -> None:
        """
        Overlap of `threshold_col` values is below the threshold.
        """
        # Create test data.
        data1 = {
            "col1": [1, 10, 100],
            "col2": [2, np.nan, 200],
            "col3": [3, 30, 300],
            "threshold_col": [7, 70, 700],
        }
        index1 = [1, 2, 3]
        df1 = self.get_dataframe(data1, index1)
        #
        data2 = {
            "col3": [3, 30, 300],
            "col4": [4, 40, 400],
            "col5": [5, np.nan, 500],
            "threshold_col": [7, 60, 600],
        }
        index2 = [3, 4, 5]
        df2 = self.get_dataframe(data2, index2)
        #
        threshold_col_name = "threshold_col"
        cols_to_merge_on = ["col3", "threshold_col"]
        # Check.
        with self.assertRaises(AssertionError):
            hpandas.merge_dfs(
                df1,
                df2,
                threshold_col_name,
                how="outer",
                on=cols_to_merge_on,
            )

    def test3(self) -> None:
        """
        Overlap of `threshold_col` values is above the threshold.
        """
        # Create test data.
        data1 = {
            "col1": [1, 3, 5, 7, 10, 100, 100, 100, 100, 10, 10],
            "col2": [2, 4, 6, 8, np.nan, 200, 200, np.nan, 10, 10, 100],
            "col3": [1, 2, 3, 4, 30, 300, 300, np.nan, 300, 300, 30],
            "threshold_col": [0, 1, 3, 5, 7, 9, 11, 13, 15, 70, 700],
        }
        index1 = range(0, 11)
        df1 = self.get_dataframe(data1, index1)
        #
        data2 = {
            "col3": [3, 30, 300, 1, 2, 3, 4, 30, 300, 300, np.nan],
            "col4": [4, 40, 400, 2, 4, 6, 8, 11, 13, 15, 70],
            "col5": [5, np.nan, 500, 5, 7, 10, 1, 2, 3, 4, 30],
            "threshold_col": [1, 2, 3, 5, 7, 9, 11, 13, 15, 70, 700],
        }
        index2 = range(9, 20)
        df2 = self.get_dataframe(data2, index2)
        #
        threshold_col_name = "threshold_col"
        cols_to_merge_on = ["col3", "threshold_col"]
        merged_df = hpandas.merge_dfs(
            df1,
            df2,
            threshold_col_name,
            how="outer",
            on=cols_to_merge_on,
        )
        # Set expected values.
        expected_length = 20
        expected_column_names = [
            "col1",
            "col2",
            "col3",
            "col4",
            "col5",
            "threshold_col",
        ]
        expected_column_unique_values = None
        expected_signature = r"""
        # df=
        index=[0, 19]
        columns=col1,col2,col3,threshold_col,col4,col5
        shape=(20, 6)
        col1  col2  col3  threshold_col  col4  col5
        0   1.0   2.0   1.0              0   NaN   NaN
        1   3.0   4.0   2.0              1   NaN   NaN
        2   5.0   6.0   3.0              3   NaN   NaN
        ...
        17   NaN   NaN   4.0             11   8.0   1.0
        18   NaN   NaN  30.0             13  11.0   2.0
        19   NaN   NaN   NaN            700  70.0  30.0
        """
        # Check.
        self.check_df_output(
            merged_df,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    def test4(self) -> None:
        """
        There are common columns (besides columns to merge on) in dataframes.
        """
        # Create test data.
        data1 = {
            "col1": [1, 10, 100],
            "col5": [2, np.nan, 200],
            "col3": [3, 30, 300],
            "threshold_col": [7, 70, 700],
        }
        index1 = [1, 2, 3]
        df1 = self.get_dataframe(data1, index1)
        #
        data2 = {
            "col3": [3, 30, 300],
            "col4": [4, 40, 400],
            "col5": [5, np.nan, 500],
            "threshold_col": [7, 70, 700],
        }
        index2 = [3, 4, 5]
        df2 = self.get_dataframe(data2, index2)
        #
        threshold_col_name = "threshold_col"
        cols_to_merge_on = ["col3", "threshold_col"]
        # Check.
        with self.assertRaises(AssertionError):
            hpandas.merge_dfs(
                df1,
                df2,
                threshold_col_name,
                how="outer",
                on=cols_to_merge_on,
            )


# #############################################################################


class Test_compare_visually_dataframes(hunitest.TestCase):
    """
    - Define two DataFrames that can be either equal or different in terms of columns or rows
    - Compare its values by calculating the difference
    """

    def get_test_dfs_equal(self) -> pd.DataFrame:
        """
        Both DataFrames have only equal rows and columns names.
        """
        timestamp_index1 = [
            pd.Timestamp("2022-01-01 21:01:00+00:00"),
            pd.Timestamp("2022-01-01 21:02:00+00:00"),
            pd.Timestamp("2022-01-01 21:03:00+00:00"),
        ]
        values1 = {
            "tsA": pd.Series([1, 2, 3]),
            "tsB": pd.Series([4, 5, 6]),
            "tsC": pd.Series([7, 8, 9]),
            "timestamp": timestamp_index1,
        }
        df1 = pd.DataFrame(data=values1)
        df1 = df1.set_index("timestamp")
        #
        timestamp_index2 = [
            pd.Timestamp("2022-01-01 21:01:00+00:00"),
            pd.Timestamp("2022-01-01 21:02:00+00:00"),
            pd.Timestamp("2022-01-01 21:03:00+00:00"),
        ]
        values2 = {
            "tsA": pd.Series([1.1, 1.9, 3.15]),
            "tsB": pd.Series([4.2, 5, 5.8]),
            "tsC": pd.Series([6.5, 8.6, 9.07]),
            "timestamp": timestamp_index2,
        }
        df2 = pd.DataFrame(data=values2)
        df2 = df2.set_index("timestamp")
        return df1, df2

    def get_test_dfs_different(self) -> pd.DataFrame:
        """
        DataFrames have both unique and equal rows and columns.
        """
        df1, df2 = self.get_test_dfs_equal()
        df2 = df2.rename(
            columns={"tsC": "extra_col"},
            index={pd.Timestamp("2022-01-01 21:03:00+00:00"): "extra_row"},
        )
        return df1, df2

    def test1(self) -> None:
        """
        - DataFrames are equal
        - Column and row modes are `equal`
        - diff_mode = "diff"
        """
        df1, df2 = self.get_test_dfs_equal()
        df_diff = hpandas.compare_visually_dataframes(
            df1,
            df2,
            row_mode="equal",
            column_mode="equal",
            diff_mode="diff",
            background_gradient=False,
        )
        expected_length = 3
        expected_column_names = ["tsA_diff", "tsB_diff", "tsC_diff"]
        expected_column_unique_values = None
        expected_signature = r"""# df=
        index=[2022-01-01 21:01:00+00:00, 2022-01-01 21:03:00+00:00]
        columns=tsA_diff,tsB_diff,tsC_diff
        shape=(3, 3)
                                tsA_diff  tsB_diff  tsC_diff
        timestamp
        2022-01-01 21:01:00+00:00     -0.10      -0.2      0.50
        2022-01-01 21:02:00+00:00      0.10       0.0     -0.60
        2022-01-01 21:03:00+00:00     -0.15       0.2     -0.07
        """
        self.check_df_output(
            df_diff,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    def test2(self) -> None:
        """
        - DataFrames are equal
        - Column and row modes are `equal`
        - diff_mode = "pct_change"
        """
        df1, df2 = self.get_test_dfs_equal()
        df_diff = hpandas.compare_visually_dataframes(
            df1,
            df2,
            row_mode="equal",
            column_mode="equal",
            diff_mode="pct_change",
            background_gradient=False,
        )
        expected_length = 3
        expected_column_names = [
            "tsA_pct_change",
            "tsB_pct_change",
            "tsC_pct_change",
        ]
        expected_column_unique_values = None
        expected_signature = r"""# df=
        index=[2022-01-01 21:01:00+00:00, 2022-01-01 21:03:00+00:00]
        columns=tsA_pct_change,tsB_pct_change,tsC_pct_change
        shape=(3, 3)
                                tsA_pct_change  tsB_pct_change  tsC_pct_change
        timestamp
        2022-01-01 21:01:00+00:00     -9.090909       -4.761905        7.692308
        2022-01-01 21:02:00+00:00      5.263158        0.000000       -6.976744
        2022-01-01 21:03:00+00:00     -4.761905        3.448276       -0.771775
        """
        self.check_df_output(
            df_diff,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    def test3(self) -> None:
        """
        - DataFrames are not equal
        - Column and row modes are `inner`
        - diff_mode = "diff"
        """
        df1, df2 = self.get_test_dfs_different()
        df_diff = hpandas.compare_visually_dataframes(
            df1,
            df2,
            row_mode="inner",
            column_mode="inner",
            diff_mode="diff",
            background_gradient=False,
        )
        expected_length = 2
        expected_column_names = ["tsA_diff", "tsB_diff"]
        expected_column_unique_values = None
        expected_signature = r"""# df=
        index=[2022-01-01 21:01:00+00:00, 2022-01-01 21:02:00+00:00]
        columns=tsA_diff,tsB_diff
        shape=(2, 2)
                                tsA_diff  tsB_diff
        timestamp
        2022-01-01 21:01:00+00:00      -0.1      -0.2
        2022-01-01 21:02:00+00:00       0.1       0.0
        """
        self.check_df_output(
            df_diff,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    def test4(self) -> None:
        """
        - DataFrames are not equal
        - Column and row modes are `inner`
        - diff_mode = "pct_change"
        """
        df1, df2 = self.get_test_dfs_different()
        df_diff = hpandas.compare_visually_dataframes(
            df1,
            df2,
            row_mode="inner",
            column_mode="inner",
            diff_mode="pct_change",
            background_gradient=False,
        )
        expected_length = 2
        expected_column_names = ["tsA_pct_change", "tsB_pct_change"]
        expected_column_unique_values = None
        expected_signature = r"""# df=
        index=[2022-01-01 21:01:00+00:00, 2022-01-01 21:02:00+00:00]
        columns=tsA_pct_change,tsB_pct_change
        shape=(2, 2)
                                tsA_pct_change  tsB_pct_change
        timestamp
        2022-01-01 21:01:00+00:00       -9.090909       -4.761905
        2022-01-01 21:02:00+00:00        5.263158        0.000000
        """
        self.check_df_output(
            df_diff,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    def test5(self) -> None:
        """
        - DataFrames are equal
        - Column and row modes are `equal`
        - diff_mode = "diff"
        - All values of the second DataFrame are zeros

        Check that if the second DataFrame consists of zeros,
        the function will perform comparison to the initial DataFrame.
        """
        df1, df2 = self.get_test_dfs_different()
        # Create DataFrame with zeros.
        df2 = df1 * 0
        # Compare.
        df_diff = hpandas.compare_visually_dataframes(
            df1,
            df2,
            row_mode="equal",
            column_mode="equal",
            diff_mode="diff",
            background_gradient=False,
        )
        expected_length = 3
        expected_column_names = ["tsA_diff", "tsB_diff", "tsC_diff"]
        expected_column_unique_values = None
        expected_signature = r"""# df=
        index=[2022-01-01 21:01:00+00:00, 2022-01-01 21:03:00+00:00]
        columns=tsA_diff,tsB_diff,tsC_diff
        shape=(3, 3)
                                tsA_diff  tsB_diff  tsC_diff
        timestamp
        2022-01-01 21:01:00+00:00         1         4         7
        2022-01-01 21:02:00+00:00         2         5         8
        2022-01-01 21:03:00+00:00         3         6         9
        """
        self.check_df_output(
            df_diff,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    def test_invalid_input(self) -> None:
        """
        Put two different DataFrames with `equal` mode.
        """
        df1, df2 = self.get_test_dfs_different()
        with self.assertRaises(AssertionError):
            hpandas.compare_visually_dataframes(
                df1,
                df2,
                row_mode="equal",
                column_mode="equal",
                diff_mode="pct_change",
                background_gradient=False,
            )


# #############################################################################


class Test_subset_multiindex_df(hunitest.TestCase):
    """
    Filter Multiindex DataFrame with 2 column levels.
    """

    @staticmethod
    def get_multiindex_df() -> pd.DataFrame:
        timestamp_index = [
            pd.Timestamp("2022-01-01 21:01:00+00:00"),
            pd.Timestamp("2022-01-01 21:02:00+00:00"),
            pd.Timestamp("2022-01-01 21:03:00+00:00"),
            pd.Timestamp("2022-01-01 21:04:00+00:00"),
            pd.Timestamp("2022-01-01 21:05:00+00:00"),
        ]
        iterables = [["asset1", "asset2"], ["open", "high", "low", "close"]]
        index = pd.MultiIndex.from_product(iterables, names=[None, "timestamp"])
        nums = np.array(
            [
                [
                    0.77650806,
                    0.12492164,
                    -0.35929232,
                    1.04137784,
                    0.20099949,
                    1.4078602,
                    -0.1317103,
                    0.10023361,
                ],
                [
                    -0.56299812,
                    0.79105046,
                    0.76612895,
                    -1.49935339,
                    -1.05923797,
                    0.06039862,
                    -0.77652117,
                    2.04578691,
                ],
                [
                    0.77348467,
                    0.45237724,
                    1.61051308,
                    0.41800008,
                    0.20838053,
                    -0.48289112,
                    1.03015762,
                    0.17123323,
                ],
                [
                    0.40486053,
                    0.88037142,
                    -1.94567068,
                    -1.51714645,
                    -0.52759748,
                    -0.31592803,
                    1.50826723,
                    -0.50215196,
                ],
                [
                    0.17409714,
                    -2.13997243,
                    -0.18530403,
                    -0.48807381,
                    0.5621593,
                    0.25899393,
                    1.14069646,
                    2.07721856,
                ],
            ]
        )
        df = pd.DataFrame(nums, index=timestamp_index, columns=index)
        return df

    def test1(self) -> None:
        """
        Filter by:

        - Timestamp index range
        - Level 1 columns
        - Level 2 columns
        """
        df = self.get_multiindex_df()
        df_filtered = hpandas.subset_multiindex_df(
            df,
            start_timestamp=pd.Timestamp("2022-01-01 21:01:00+00:00"),
            end_timestamp=pd.Timestamp("2022-01-01 21:03:00+00:00"),
            columns_level0=["asset1"],
            columns_level1=["high", "low"],
        )
        expected_length = 3
        expected_column_names = [("asset1", "high"), ("asset1", "low")]
        expected_column_unique_values = None
        expected_signature = r"""# df=
        index=[2022-01-01 21:01:00+00:00, 2022-01-01 21:03:00+00:00]
        columns=('asset1', 'high'),('asset1', 'low')
        shape=(3, 2)
                                    asset1
        timestamp                      high       low
        2022-01-01 21:01:00+00:00  0.124922 -0.359292
        2022-01-01 21:02:00+00:00  0.791050  0.766129
        2022-01-01 21:03:00+00:00  0.452377  1.610513
        """
        self.check_df_output(
            df_filtered,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    def test2(self) -> None:
        """
        Filter by:

        - Timestamp index range
        - Level 1 columns
        """
        df = self.get_multiindex_df()
        df_filtered = hpandas.subset_multiindex_df(
            df,
            start_timestamp=pd.Timestamp("2022-01-01 21:01:00+00:00"),
            end_timestamp=pd.Timestamp("2022-01-01 21:02:00+00:00"),
            columns_level1=["close"],
        )
        expected_length = 2
        expected_column_names = [("asset1", "close"), ("asset2", "close")]
        expected_column_unique_values = None
        expected_signature = r"""# df=
        index=[2022-01-01 21:01:00+00:00, 2022-01-01 21:02:00+00:00]
        columns=('asset1', 'close'),('asset2', 'close')
        shape=(2, 2)
                                    asset1    asset2
        timestamp                     close     close
        2022-01-01 21:01:00+00:00  1.041378  0.100234
        2022-01-01 21:02:00+00:00 -1.499353  2.045787
        """
        self.check_df_output(
            df_filtered,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    def test3(self) -> None:
        """
        Filter by:

        - Timestamp index range
        - Level 2 columns
        """
        df = self.get_multiindex_df()
        df_filtered = hpandas.subset_multiindex_df(
            df,
            start_timestamp=pd.Timestamp("2022-01-01 21:01:00+00:00"),
            end_timestamp=pd.Timestamp("2022-01-01 21:02:00+00:00"),
            columns_level0=["asset2"],
        )
        expected_length = 2
        expected_column_names = [
            ("asset2", "close"),
            ("asset2", "high"),
            ("asset2", "low"),
            ("asset2", "open"),
        ]
        expected_column_unique_values = None
        expected_signature = r"""# df=
        index=[2022-01-01 21:01:00+00:00, 2022-01-01 21:02:00+00:00]
        columns=('asset2', 'open'),('asset2', 'high'),('asset2', 'low'),('asset2', 'close')
        shape=(2, 4)
                                    asset2
        timestamp                      open      high       low     close
        2022-01-01 21:01:00+00:00  0.200999  1.407860 -0.131710  0.100234
        2022-01-01 21:02:00+00:00 -1.059238  0.060399 -0.776521  2.045787
        """
        self.check_df_output(
            df_filtered,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    def test4(self) -> None:
        """
        Filter by:

        - Level 1 columns
        - Level 2 columns
        """
        df = self.get_multiindex_df()
        df_filtered = hpandas.subset_multiindex_df(
            df,
            columns_level0=["asset2"],
            columns_level1=["low"],
        )
        expected_length = 5
        expected_column_names = [("asset2", "low")]
        expected_column_unique_values = None
        expected_signature = r"""# df=
        index=[2022-01-01 21:01:00+00:00, 2022-01-01 21:05:00+00:00]
        columns=('asset2', 'low')
        shape=(5, 1)
                                    asset2
        timestamp                       low
        2022-01-01 21:01:00+00:00 -0.131710
        2022-01-01 21:02:00+00:00 -0.776521
        2022-01-01 21:03:00+00:00  1.030158
        2022-01-01 21:04:00+00:00  1.508267
        2022-01-01 21:05:00+00:00  1.140696
        """
        self.check_df_output(
            df_filtered,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    def test_columns_level0_invalid_input(self) -> None:
        df = self.get_multiindex_df()
        with self.assertRaises(AssertionError):
            hpandas.subset_multiindex_df(
                df,
                columns_level0=["invalid_input"],
            )

    def test_columns_level1_invalid_input(self) -> None:
        df = self.get_multiindex_df()
        with self.assertRaises(AssertionError):
            hpandas.subset_multiindex_df(
                df,
                columns_level1=["invalid_input"],
            )


# #############################################################################


class Test_compare_multiindex_dfs(hunitest.TestCase):
    """
    Subset Multiindex DataFrames with 2 column levels and compare its values.
    """

    @staticmethod
    def get_multiindex_dfs() -> pd.DataFrame:
        timestamp_index1 = [
            pd.Timestamp("2022-01-01 21:01:00+00:00"),
            pd.Timestamp("2022-01-01 21:02:00+00:00"),
            pd.Timestamp("2022-01-01 21:03:00+00:00"),
            pd.Timestamp("2022-01-01 21:04:00+00:00"),
            pd.Timestamp("2022-01-01 21:05:00+00:00"),
        ]
        iterables1 = [["asset1", "asset2"], ["open", "high", "low", "close"]]
        index1 = pd.MultiIndex.from_product(iterables1, names=[None, "timestamp"])
        nums1 = np.array(
            [
                [
                    0.77650806,
                    0.12492164,
                    -0.35929232,
                    1.04137784,
                    0.20099949,
                    1.4078602,
                    -0.1317103,
                    0.10023361,
                ],
                [
                    -0.56299812,
                    0.79105046,
                    0.76612895,
                    -1.49935339,
                    -1.05923797,
                    0.06039862,
                    -0.77652117,
                    2.04578691,
                ],
                [
                    0.77348467,
                    0.45237724,
                    1.61051308,
                    0.41800008,
                    0.20838053,
                    -0.48289112,
                    1.03015762,
                    0.17123323,
                ],
                [
                    0.40486053,
                    0.88037142,
                    -1.94567068,
                    -1.51714645,
                    -0.52759748,
                    -0.31592803,
                    1.50826723,
                    -0.50215196,
                ],
                [
                    0.17409714,
                    -2.13997243,
                    -0.18530403,
                    -0.48807381,
                    0.5621593,
                    0.25899393,
                    1.14069646,
                    2.07721856,
                ],
            ]
        )
        df1 = pd.DataFrame(nums1, index=timestamp_index1, columns=index1)
        #
        timestamp_index2 = [
            pd.Timestamp("2022-01-01 21:00:00+00:00"),
            pd.Timestamp("2022-01-01 21:01:00+00:00"),
            pd.Timestamp("2022-01-01 21:02:00+00:00"),
            pd.Timestamp("2022-01-01 21:03:00+00:00"),
            pd.Timestamp("2022-01-01 21:04:00+00:00"),
            pd.Timestamp("2022-01-01 21:05:00+00:00"),
            pd.Timestamp("2022-01-01 21:06:00+00:00"),
            pd.Timestamp("2022-01-01 21:06:00+00:00"),
        ]
        iterables2 = [
            ["asset1", "asset2", "asset3"],
            ["open", "high", "low", "close", "volume"],
        ]
        index2 = pd.MultiIndex.from_product(iterables2, names=[None, "timestamp"])
        nums2 = [
            [
                0.79095104,
                -0.10304008,
                -0.69848962,
                0.50078409,
                0.41756371,
                -1.33487885,
                1.04546138,
                0.191062,
                0.08841533,
                0.61717725,
                -2.15558483,
                1.21036169,
                2.60355386,
                0.07508052,
                1.00702849,
            ],
            [
                0.56223723,
                0.97433151,
                -1.40471182,
                0.53292355,
                0.24381913,
                0.64343069,
                -0.46733655,
                -1.20471491,
                -0.08347491,
                0.33365524,
                0.04370572,
                -0.53547653,
                -1.07622168,
                0.7318155,
                -0.47146482,
            ],
            [
                -0.48272741,
                1.17859032,
                -0.40816664,
                0.46684297,
                0.42518077,
                -1.52913855,
                1.09925095,
                0.48817537,
                1.2662552,
                -0.59757824,
                0.23724902,
                -0.00660826,
                0.09780482,
                -0.17166633,
                -0.54515917,
            ],
            [
                -0.37618442,
                -0.3086281,
                1.09168123,
                -1.1751162,
                0.38291194,
                1.80830268,
                1.28318855,
                0.75696503,
                -1.04042572,
                0.06493231,
                -0.10392893,
                1.89053412,
                -0.21200498,
                1.61212857,
                -2.00765278,
            ],
            [
                -0.19674075,
                -1.02532132,
                -0.22486018,
                0.37664998,
                0.35619408,
                -0.77304675,
                0.59053699,
                -1.53249898,
                0.57548424,
                -0.32093537,
                -0.52109972,
                1.70938034,
                -0.55419632,
                0.45531674,
                0.66878119,
            ],
            [
                0.05903553,
                1.2040308,
                0.62323671,
                -0.23639535,
                0.87270792,
                2.60253287,
                -0.77788842,
                0.80645833,
                1.85438743,
                -1.77561587,
                0.41469478,
                -0.29791883,
                0.75140743,
                0.50389702,
                0.55311024,
            ],
            [
                -0.97820763,
                -1.32155197,
                -0.6143911,
                0.01473404,
                0.87798665,
                0.1701048,
                -0.75376376,
                0.72503616,
                0.5791076,
                0.43942739,
                0.62505817,
                0.44998739,
                0.37350664,
                -0.73485633,
                -0.70406184,
            ],
            [
                -1.35719477,
                -1.82401288,
                0.77263763,
                2.36399552,
                -0.45353019,
                0.33983713,
                -0.62895329,
                1.34256611,
                0.2207564,
                0.24146184,
                0.90769186,
                0.57426869,
                -0.04587782,
                -1.6319128,
                0.38094798,
            ],
        ]
        df2 = pd.DataFrame(nums2, index=timestamp_index2, columns=index2)
        return df1, df2

    def test1(self) -> None:
        """
        - Subset by both columns and index
        - Make inner intersection and compute pct_change
        """
        df1, df2 = self.get_multiindex_dfs()
        subset_multiindex_df_kwargs = {
            "start_timestamp": pd.Timestamp("2022-01-01 21:02:00+00:00"),
            "end_timestamp": pd.Timestamp("2022-01-01 21:04:00+00:00"),
            "columns_level0": ["asset1", "asset2"],
            "columns_level1": ["low", "high"],
        }
        compare_visually_dataframes_kwargs = {
            "column_mode": "inner",
            "row_mode": "inner",
            "diff_mode": "pct_change",
            "background_gradient": False,
        }
        df_diff = hpandas.compare_multiindex_dfs(
            df1,
            df2,
            subset_multiindex_df_kwargs,
            compare_visually_dataframes_kwargs,
        )
        expected_length = 3
        expected_column_names = [
            ("asset1_pct_change", "high_pct_change"),
            ("asset1_pct_change", "low_pct_change"),
            ("asset2_pct_change", "high_pct_change"),
            ("asset2_pct_change", "low_pct_change"),
        ]
        expected_column_unique_values = None
        expected_signature = r"""# df=
        index=[2022-01-01 21:02:00+00:00, 2022-01-01 21:04:00+00:00]
        columns=('asset1_pct_change', 'high_pct_change'),('asset1_pct_change', 'low_pct_change'),('asset2_pct_change', 'high_pct_change'),('asset2_pct_change', 'low_pct_change')
        shape=(3, 4)
                                asset1_pct_change                asset2_pct_change
        timestamp                   high_pct_change low_pct_change   high_pct_change low_pct_change
        2022-01-01 21:02:00+00:00        -32.881643    -287.700041        -94.505475    -259.066028
        2022-01-01 21:03:00+00:00       -246.576815      47.525948       -137.632125      36.090517
        2022-01-01 21:04:00+00:00       -185.862978     765.280229       -153.498432    -198.418808
        """
        self.check_df_output(
            df_diff,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )
