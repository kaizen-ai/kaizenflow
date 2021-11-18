import datetime
import logging

import pandas as pd
import pytz

import helpers.datetime_ as hdatetim
import helpers.unit_test as huntes

_LOG = logging.getLogger(__name__)


# #############################################################################

_STR_TS_NAIVE = "2021-01-04 09:30:00"
_STR_TS_UTC = "2021-01-04 09:30:00-00:00"
_STR_TS_ET = "2021-01-04 09:30:00-05:00"

_PD_TS_NAIVE = pd.Timestamp("2021-01-04 09:30:00")
_PD_TS_UTC = pd.Timestamp("2021-01-04 09:30:00-00:00")
_PD_TS_ET = pd.Timestamp("2021-01-04 09:30:00-05:00")

_DT_DT_NAIVE = datetime.datetime(2021, 1, 4, 9, 30, 0)
_DT_DT_UTC = pytz.timezone("UTC").localize(_DT_DT_NAIVE)
_DT_DT_ET = pytz.timezone("US/Eastern").localize(_DT_DT_NAIVE)


# #############################################################################


class Test_dassert_is_datetime1(huntes.TestCase):
    def test_is_datetime1(self) -> None:
        """
        Test valid datetime objects.
        """
        objs = [
            _STR_TS_NAIVE,
            _STR_TS_UTC,
            _STR_TS_ET,
            _PD_TS_NAIVE,
            _PD_TS_UTC,
            _PD_TS_ET,
            _DT_DT_NAIVE,
            _DT_DT_UTC,
            _DT_DT_ET,
        ]
        for obj in objs:
            _LOG.debug("obj='%s', type='%s'", str(obj), str(type(obj)))
            hdatetim.dassert_is_datetime(obj)

    def test_is_datetime_fail1(self) -> None:
        """
        Test invalid datetime objects.
        """
        objs = [0, 0.0]
        for obj in objs:
            _LOG.debug("obj='%s', type='%s'", str(obj), str(type(obj)))
            with self.assertRaises(AssertionError):
                hdatetim.dassert_is_datetime(obj)

    def test_is_strict_datetime1(self) -> None:
        """
        Test valid datetime objects.
        """
        objs = [
            _PD_TS_NAIVE,
            _PD_TS_UTC,
            _PD_TS_ET,
            _DT_DT_NAIVE,
            _DT_DT_UTC,
            _DT_DT_ET,
        ]
        for obj in objs:
            _LOG.debug("obj='%s', type='%s'", str(obj), str(type(obj)))
            hdatetim.dassert_is_strict_datetime(obj)

    def test_is_strict_datetime_fail1(self) -> None:
        """
        Test invalid datetime objects.
        """
        objs = [0, _STR_TS_NAIVE, _STR_TS_UTC, _STR_TS_ET, "hello"]
        for obj in objs:
            _LOG.debug("obj='%s', type='%s'", str(obj), str(type(obj)))
            with self.assertRaises(AssertionError):
                hdatetim.dassert_is_strict_datetime(obj)


# #############################################################################


class Test_dassert_tz1(huntes.TestCase):
    def test_datetime_conversions(self) -> None:
        # Get a tz-naive datetime.
        dt = datetime.datetime(2020, 1, 5, 9, 30, 0)
        hdatetim.dassert_is_tz_naive(dt)
        # Localize it to UTC.
        dt_utc = pytz.timezone("UTC").localize(dt)
        hdatetim.dassert_has_tz(dt_utc)
        hdatetim.dassert_has_UTC_tz(dt_utc)
        # Convert to ET.
        dt_et = dt_utc.astimezone(pytz.timezone("US/Eastern"))
        hdatetim.dassert_has_tz(dt_et)
        hdatetim.dassert_has_ET_tz(dt_et)
        # Convert it back to UTC.
        dt_utc2 = dt_et.astimezone(pytz.timezone("UTC"))
        hdatetim.dassert_has_tz(dt_utc2)
        hdatetim.dassert_has_UTC_tz(dt_utc2)
        self.assertEqual(dt_utc, dt_utc2)
        # Make it naive.
        dt2 = dt_utc2.replace(tzinfo=None)
        hdatetim.dassert_is_tz_naive(dt2)
        self.assertEqual(dt, dt2)

    def test_dassert_is_datetime1(self) -> None:
        for obj in [
            _STR_TS_NAIVE,
            _STR_TS_UTC,
            _STR_TS_ET,
            _PD_TS_NAIVE,
            _PD_TS_UTC,
            _PD_TS_ET,
            _DT_DT_NAIVE,
            _DT_DT_UTC,
            _DT_DT_ET,
        ]:
            hdatetim.dassert_is_datetime(obj)

    def test_dassert_is_datetime_assert1(self) -> None:
        datetime_ = 5
        with self.assertRaises(AssertionError) as cm:
            hdatetim.dassert_is_datetime(datetime_)
        act = str(cm.exception)
        # pylint: disable=line-too-long
        exp = r"""
        * Failed assertion *
        Instance of '5' is '<class 'int'>' instead of '(<class 'str'>, <class 'pandas._libs.tslibs.timestamps.Timestamp'>, <class 'datetime.datetime'>)'
        datetime_='5' of type '<class 'int'>' is not a DateTimeType
        """
        # pylint: enable=line-too-long
        self.assert_equal(act, exp, fuzzy_match=True)

    def test_to_datetime1(self) -> None:
        """
        Apply `to_datetime` to a naive datetime.
        """
        for obj in [
            _STR_TS_NAIVE,
            _PD_TS_NAIVE,
            _DT_DT_NAIVE,
        ]:
            _LOG.debug("obj='%s' type='%s'", obj, type(obj))
            act = hdatetim.to_datetime(obj)
            exp = _DT_DT_NAIVE
            self.assertEqual(act, exp)
            # Check the tz info.
            hdatetim.dassert_is_tz_naive(act)
            with self.assertRaises(AssertionError):
                hdatetim.dassert_has_tz(act)
                hdatetim.dassert_has_UTC_tz(act)
                hdatetim.dassert_has_ET_tz(act)

    def test_to_datetime2(self) -> None:
        """
        Apply `to_datetime` to a UTC datetime.
        """
        for obj in [
            _STR_TS_UTC,
            _PD_TS_UTC,
            _DT_DT_UTC,
        ]:
            _LOG.debug("obj='%s' type='%s'", obj, type(obj))
            act = hdatetim.to_datetime(obj)
            exp = _DT_DT_UTC
            self.assertEqual(act, exp)
            # Check the tz info.
            hdatetim.dassert_has_tz(act)
            hdatetim.dassert_has_UTC_tz(act)
            with self.assertRaises(AssertionError):
                hdatetim.dassert_is_tz_naive(act)
                hdatetim.dassert_has_ET_tz(act)

    def test_to_datetime3(self) -> None:
        """
        Apply `to_datetime` to an ET datetime.
        """
        for obj in [
            _STR_TS_ET,
            _PD_TS_ET,
            _DT_DT_ET,
        ]:
            _LOG.debug("obj='%s' type='%s'", obj, type(obj))
            act = hdatetim.to_datetime(obj)
            exp = _DT_DT_ET
            self.assertEqual(str(act), str(exp))


# #############################################################################


class Test_dassert_tz_compatible1(huntes.TestCase):
    def test_dassert_compatible_timestamp1(self) -> None:
        """
        Both datetimes are naive.
        """
        for datetime1 in [_PD_TS_NAIVE, _DT_DT_NAIVE]:
            for datetime2 in [_PD_TS_NAIVE, _DT_DT_NAIVE]:
                hdatetim.dassert_tz_compatible(datetime1, datetime2)

    def test_dassert_compatible_timestamp2(self) -> None:
        """
        Both datetimes have tz info.
        """
        for datetime1 in [_PD_TS_UTC, _PD_TS_ET]:
            for datetime2 in [_DT_DT_UTC, _DT_DT_ET]:
                hdatetim.dassert_tz_compatible(datetime1, datetime2)

    def test_dassert_compatible_timestamp_assert1(self) -> None:
        """
        Test a single not compatible pair of datetimes and check the raised
        exception.
        """
        with self.assertRaises(AssertionError) as cm:
            hdatetim.dassert_tz_compatible(_PD_TS_NAIVE, _DT_DT_UTC)
        act = str(cm.exception)
        # pylint: disable=line-too-long
        exp = """
        * Failed assertion *
        'False'
        ==
        'True'
        datetime1='2021-01-04 09:30:00' and datetime2='2021-01-04 09:30:00+00:00' are not compatible
        """
        # pylint: enable=line-too-long
        self.assert_equal(act, exp, fuzzy_match=True)

    def test_dassert_compatible_timestamp_assert2(self) -> None:
        """
        Test a pairs of non-compatible datetimes making sure the assertion is
        raised.
        """
        for datetime1 in [_PD_TS_NAIVE, _DT_DT_NAIVE, _PD_TS_NAIVE, _DT_DT_NAIVE]:
            for datetime2 in [_PD_TS_UTC, _PD_TS_ET, _DT_DT_UTC, _DT_DT_ET]:
                with self.assertRaises(AssertionError):
                    hdatetim.dassert_tz_compatible(datetime1, datetime2)


# #############################################################################


class Test_get_current_time1(huntes.TestCase):
    def test_get_current_time_UTC(self) -> None:
        tz = "UTC"
        dt = hdatetim.get_current_time(tz)
        _LOG.debug("tz=%s -> dt=%s", tz, dt)
        hdatetim.dassert_has_UTC_tz(dt)

    def test_get_current_time_ET(self) -> None:
        tz = "ET"
        dt = hdatetim.get_current_time(tz)
        _LOG.debug("tz=%s -> dt=%s", tz, dt)
        hdatetim.dassert_has_ET_tz(dt)

    def test_get_current_time_naive_UTC(self) -> None:
        tz = "naive_UTC"
        dt = hdatetim.get_current_time(tz)
        _LOG.debug("tz=%s -> dt=%s", tz, dt)
        hdatetim.dassert_is_tz_naive(dt)

    def test_get_current_time_naive_ET(self) -> None:
        tz = "naive_ET"
        dt = hdatetim.get_current_time(tz)
        _LOG.debug("tz=%s -> dt=%s", tz, dt)
        hdatetim.dassert_is_tz_naive(dt)


# #############################################################################


class Test_to_generalized_datetime(huntes.TestCase):
    def test_srs1(self) -> None:
        srs = pd.Series(["2010-01-01", "2010-01-02"])
        actual = hdatetim.to_generalized_datetime(srs)
        expected = pd.Series(
            [pd.Timestamp("2010-01-01"), pd.Timestamp("2010-01-02")]
        )
        pd.testing.assert_series_equal(actual, expected)

    def test_index1(self) -> None:
        idx = pd.Index(["2010-01-01", "2010-01-02"])
        actual = hdatetim.to_generalized_datetime(idx)
        expected = pd.Index(
            [pd.Timestamp("2010-01-01"), pd.Timestamp("2010-01-02")]
        )
        pd.testing.assert_index_equal(actual, expected)

    def test_daily1(self) -> None:
        srs = pd.Series(["1 Jan 2010", "2 Jan 2010"])
        actual = hdatetim.to_generalized_datetime(srs)
        expected = pd.Series(
            [pd.Timestamp("2010-01-01"), pd.Timestamp("2010-01-02")]
        )
        pd.testing.assert_series_equal(actual, expected)

    def test_weekly1(self) -> None:
        srs = pd.Series(["2021-W14", "2021-W15"])
        actual = hdatetim.to_generalized_datetime(srs)
        expected = pd.Series(
            [pd.Timestamp("2021-04-10"), pd.Timestamp("2021-04-17")]
        )
        pd.testing.assert_series_equal(actual, expected)

    def test_semiannual1(self) -> None:
        srs = pd.Series(["2021-S1", "2021-S2"])
        actual = hdatetim.to_generalized_datetime(srs)
        expected = pd.Series(
            [pd.Timestamp("2021-06-30"), pd.Timestamp("2021-12-31")]
        )
        pd.testing.assert_series_equal(actual, expected)

    def test_semiannual2(self) -> None:
        srs = pd.Series(["2021/S1", "2021/S2"])
        actual = hdatetim.to_generalized_datetime(srs)
        expected = pd.Series(
            [pd.Timestamp("2021-06-30"), pd.Timestamp("2021-12-31")]
        )
        pd.testing.assert_series_equal(actual, expected)

    def test_bimonthly1(self) -> None:
        srs = pd.Series(["2021-B1", "2021-B2"])
        actual = hdatetim.to_generalized_datetime(srs)
        expected = pd.Series(
            [pd.Timestamp("2021-01-01"), pd.Timestamp("2021-03-01")]
        )
        pd.testing.assert_series_equal(actual, expected)

    def test_monthly1(self) -> None:
        srs = pd.Series(["2020-M1", "2020-M2"])
        actual = hdatetim.to_generalized_datetime(srs)
        expected = pd.Series(
            [pd.Timestamp("2020-01-31"), pd.Timestamp("2020-02-29")]
        )
        pd.testing.assert_series_equal(actual, expected)

    def test_monthly2(self) -> None:
        srs = pd.Series(["2020M01", "2020M02"])
        actual = hdatetim.to_generalized_datetime(srs)
        expected = pd.Series(
            [pd.Timestamp("2020-01-31"), pd.Timestamp("2020-02-29")]
        )
        pd.testing.assert_series_equal(actual, expected)

    def test_monthly3(self) -> None:
        srs = pd.Series(["2020-01", "2020-02"])
        actual = hdatetim.to_generalized_datetime(srs)
        expected = pd.Series(
            [pd.Timestamp("2020-01-31"), pd.Timestamp("2020-02-29")]
        )
        pd.testing.assert_series_equal(actual, expected)

    def test_monthly4(self) -> None:
        srs = pd.Series(["2020 Jan", "2020 Feb"])
        actual = hdatetim.to_generalized_datetime(srs)
        expected = pd.Series(
            [pd.Timestamp("2020-01-31"), pd.Timestamp("2020-02-29")]
        )
        pd.testing.assert_series_equal(actual, expected)

    def test_monthly5(self) -> None:
        srs = pd.Series(["January 2020", "February 2020"])
        actual = hdatetim.to_generalized_datetime(srs)
        expected = pd.Series(
            [pd.Timestamp("2020-01-31"), pd.Timestamp("2020-02-29")]
        )
        pd.testing.assert_series_equal(actual, expected)

    def test_quarterly1(self) -> None:
        srs = pd.Series(["2020-Q1", "2020-Q2"])
        actual = hdatetim.to_generalized_datetime(srs)
        expected = pd.Series(
            [pd.Timestamp("2020-03-31"), pd.Timestamp("2020-06-30")]
        )
        pd.testing.assert_series_equal(actual, expected)

    def test_quarterly2(self) -> None:
        srs = pd.Series(["2020Q1", "2020Q2"])
        actual = hdatetim.to_generalized_datetime(srs)
        expected = pd.Series(
            [pd.Timestamp("2020-03-31"), pd.Timestamp("2020-06-30")]
        )
        pd.testing.assert_series_equal(actual, expected)

    def test_quarterly3(self) -> None:
        srs = pd.Series(["Q1 2020", "Q2 2020"])
        actual = hdatetim.to_generalized_datetime(srs)
        expected = pd.Series(
            [pd.Timestamp("2020-03-31"), pd.Timestamp("2020-06-30")]
        )
        pd.testing.assert_series_equal(actual, expected)

    def test_annual1(self) -> None:
        srs = pd.Series(["2021", "2022"])
        actual = hdatetim.to_generalized_datetime(srs)
        expected = pd.Series(
            [pd.Timestamp("2021-12-31"), pd.Timestamp("2022-12-31")]
        )
        pd.testing.assert_series_equal(actual, expected)


# #############################################################################


class Test_convert_unix_epoch_to_timestamp(huntes.TestCase):
    def test1(self) -> None:
        """
        Test with default parameter values.
        """
        epoch = 1631145600000
        actual = hdatetim.convert_unix_epoch_to_timestamp(epoch=epoch)
        expected = pd.Timestamp("2021-09-09T00:00:00", tz="UTC")
        self.assert_equal(str(actual), str(expected))

    def test2(self) -> None:
        """
        Test with specified unit.
        """
        epoch = 1631145600
        unit = "s"
        actual = hdatetim.convert_unix_epoch_to_timestamp(epoch=epoch, unit=unit)
        expected = pd.Timestamp("2021-09-09T00:00:00", tz="UTC")
        self.assert_equal(str(actual), str(expected))

    def test3(self) -> None:
        """
        Test with specified timezone.
        """
        epoch = 1631145600000
        tz = "US/Pacific"
        actual = hdatetim.convert_unix_epoch_to_timestamp(epoch=epoch, tz=tz)
        expected = pd.Timestamp("2021-09-08T17:00:00", tz="US/Pacific")
        self.assert_equal(str(actual), str(expected))


class Test_convert_timestamp_to_unix_epoch(huntes.TestCase):
    def test1(self) -> None:
        """
        Test with default parameter values.
        """
        timestamp = pd.Timestamp("2021-09-09")
        actual = hdatetim.convert_timestamp_to_unix_epoch(timestamp=timestamp)
        expected = 1631145600000
        self.assert_equal(str(actual), str(expected))

    def test2(self) -> None:
        """
        Test with specified unit.
        """
        timestamp = pd.Timestamp("2021-09-09")
        unit = "s"
        actual = hdatetim.convert_timestamp_to_unix_epoch(
            timestamp=timestamp, unit=unit
        )
        expected = 1631145600
        self.assert_equal(str(actual), str(expected))

    def test3(self) -> None:
        """
        Test for a timestamp with specified timezone.
        """
        timestamp = pd.Timestamp("2021-09-08T17:00:00", tz="US/Pacific")
        actual = hdatetim.convert_timestamp_to_unix_epoch(timestamp=timestamp)
        expected = 1631145600000
        self.assert_equal(str(actual), str(expected))
