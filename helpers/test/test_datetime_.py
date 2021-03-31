import logging

import pandas as pd

import helpers.datetime_ as hdatet
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class Test_to_datetime(hut.TestCase):
    def test_srs1(self) -> None:
        srs = pd.Series(["2010-01-01", "2010-01-02"])
        actual = hdatet.to_datetime(srs)
        expected = pd.Series(
            [pd.Timestamp("2010-01-01"), pd.Timestamp("2010-01-02")]
        )
        pd.testing.assert_series_equal(actual, expected)

    def test_index1(self) -> None:
        idx = pd.Index(["2010-01-01", "2010-01-02"])
        actual = hdatet.to_datetime(idx)
        expected = pd.Index(
            [pd.Timestamp("2010-01-01"), pd.Timestamp("2010-01-02")]
        )
        pd.testing.assert_index_equal(actual, expected)

    def test_daily1(self) -> None:
        srs = pd.Series(["1 Jan 2010", "2 Jan 2010"])
        actual = hdatet.to_datetime(srs)
        expected = pd.Series(
            [pd.Timestamp("2010-01-01"), pd.Timestamp("2010-01-02")]
        )
        pd.testing.assert_series_equal(actual, expected)

    def test_weekly1(self) -> None:
        srs = pd.Series(["2021-W14", "2021-W15"])
        actual = hdatet.to_datetime(srs)
        expected = pd.Series(
            [pd.Timestamp("2021-04-10"), pd.Timestamp("2021-04-17")]
        )
        pd.testing.assert_series_equal(actual, expected)

    def test_semiannual1(self) -> None:
        srs = pd.Series(["2021-S1", "2021-S2"])
        actual = hdatet.to_datetime(srs)
        expected = pd.Series(
            [pd.Timestamp("2021-06-30"), pd.Timestamp("2021-12-31")]
        )
        pd.testing.assert_series_equal(actual, expected)

    def test_bimonthly1(self) -> None:
        srs = pd.Series(["2021-B1", "2021-B2"])
        actual = hdatet.to_datetime(srs)
        expected = pd.Series(
            [pd.Timestamp("2021-01-01"), pd.Timestamp("2021-03-01")]
        )
        pd.testing.assert_series_equal(actual, expected)

    def test_monthly1(self) -> None:
        srs = pd.Series(["2020-M1", "2020-M2"])
        actual = hdatet.to_datetime(srs)
        expected = pd.Series(
            [pd.Timestamp("2020-01-31"), pd.Timestamp("2020-02-29")]
        )
        pd.testing.assert_series_equal(actual, expected)

    def test_monthly2(self) -> None:
        srs = pd.Series(["2020M01", "2020M02"])
        actual = hdatet.to_datetime(srs)
        expected = pd.Series(
            [pd.Timestamp("2020-01-31"), pd.Timestamp("2020-02-29")]
        )
        pd.testing.assert_series_equal(actual, expected)

    def test_monthly3(self) -> None:
        srs = pd.Series(["2020-01", "2020-02"])
        actual = hdatet.to_datetime(srs)
        expected = pd.Series(
            [pd.Timestamp("2020-01-31"), pd.Timestamp("2020-02-29")]
        )
        pd.testing.assert_series_equal(actual, expected)

    def test_monthly4(self) -> None:
        srs = pd.Series(["2020 Jan", "2020 Feb"])
        actual = hdatet.to_datetime(srs)
        expected = pd.Series(
            [pd.Timestamp("2020-01-31"), pd.Timestamp("2020-02-29")]
        )
        pd.testing.assert_series_equal(actual, expected)

    def test_monthly5(self) -> None:
        srs = pd.Series(["January 2020", "February 2020"])
        actual = hdatet.to_datetime(srs)
        expected = pd.Series(
            [pd.Timestamp("2020-01-31"), pd.Timestamp("2020-02-29")]
        )
        pd.testing.assert_series_equal(actual, expected)

    def test_quarterly1(self) -> None:
        srs = pd.Series(["2020-Q1", "2020-Q2"])
        actual = hdatet.to_datetime(srs)
        expected = pd.Series(
            [pd.Timestamp("2020-03-31"), pd.Timestamp("2020-06-30")]
        )
        pd.testing.assert_series_equal(actual, expected)

    def test_quarterly2(self) -> None:
        srs = pd.Series(["2020Q1", "2020Q2"])
        actual = hdatet.to_datetime(srs)
        expected = pd.Series(
            [pd.Timestamp("2020-03-31"), pd.Timestamp("2020-06-30")]
        )
        pd.testing.assert_series_equal(actual, expected)

    def test_quarterly3(self) -> None:
        srs = pd.Series(["Q1 2020", "Q2 2020"])
        actual = hdatet.to_datetime(srs)
        expected = pd.Series(
            [pd.Timestamp("2020-03-31"), pd.Timestamp("2020-06-30")]
        )
        pd.testing.assert_series_equal(actual, expected)

    def test_annual1(self) -> None:
        srs = pd.Series(["2021", "2022"])
        actual = hdatet.to_datetime(srs)
        expected = pd.Series(
            [pd.Timestamp("2021-12-31"), pd.Timestamp("2022-12-31")]
        )
        pd.testing.assert_series_equal(actual, expected)
