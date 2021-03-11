import logging
import os
from typing import Tuple

try:
    import ib_insync
except ModuleNotFoundError:
    print("Can't find ib_insync")
import pandas as pd
import pytest

import helpers.dbg as dbg
import helpers.unit_test as hut
import vendors_amp.ib.extract.utils as vieuti

_LOG = logging.getLogger(__name__)


@pytest.mark.skipif(
    not (
        (
            os.environ.get("STAGE") == "TEST"
            and os.environ.get("IB_GW_CONNECTION_HOST") == "ib_connect_test"
        )
        or (
            os.environ.get("STAGE") == "LOCAL"
            and os.environ.get("IB_GW_CONNECTION_HOST") == "ib_connect_local"
        )
    ),
    reason="Testable only inside IB container",
)
class Test_get_historical_data(hut.TestCase):
    @classmethod
    def setUpClass(cls):
        dbg.shutup_chatty_modules()

    def setUp(self):
        super().setUp()
        self.ib = vieuti.ib_connect(
            sum(bytes(self._get_test_name(), encoding="UTF-8")), is_notebook=False
        )

    def tearDown(self):
        self.ib.disconnect()
        super().tearDown()

    # #########################################################################

    def test_get_end_timestamp1(self) -> None:
        """
        Test get_end_timestamp() for ES in and outside regular trading hours
        (RTH).
        """
        contract = ib_insync.ContFuture("ES", "GLOBEX", currency="USD")
        what_to_show = "TRADES"
        use_rth = True
        ts1 = vieuti.get_end_timestamp(self.ib, contract, what_to_show, use_rth)
        _LOG.debug("ts1=%s", ts1)
        #
        use_rth = False
        ts2 = vieuti.get_end_timestamp(self.ib, contract, what_to_show, use_rth)
        _LOG.debug("ts2=%s", ts2)

    def test_req_historical_data1(self) -> None:
        """
        Test req_historical_data() on a single day in trading hours.

        Requesting data for a day ending at 18:00 gets the entire
        trading day.
        """
        # 2021-02-18 is a Thursday and it's full day.
        end_ts = pd.Timestamp("2021-02-18 18:00:00")
        use_rth = True
        short_signature, long_signature = self._req_historical_data_helper(
            end_ts, use_rth
        )
        exp = """
        signature=len=9 [2021-02-18 09:30:00-05:00, 2021-02-18 16:30:00-05:00]
        min_max_df=
                         min       max
        2021-02-18  09:30:00  16:30:00
        """
        self.assert_equal(short_signature, exp, fuzzy_match=True)
        self.check_string(long_signature)

    def test_req_historical_data2(self) -> None:
        """
        Test req_historical_data() on a single day outside trading hours.

        Requesting data for a day ending at 18:00 gets data for a 24 hr
        period.
        """
        # 2021-02-18 is a Thursday and it's full day.
        end_ts = pd.Timestamp("2021-02-18 18:00:00")
        use_rth = False
        short_signature, long_signature = self._req_historical_data_helper(
            end_ts, use_rth
        )
        exp = """
        signature=len=24 [2021-02-17 18:00:00-05:00, 2021-02-18 16:30:00-05:00]
        min_max_df=
                         min       max
        2021-02-17  18:00:00  23:00:00
        2021-02-18  00:00:00  16:30:00
        """
        self.assert_equal(short_signature, exp, fuzzy_match=True)
        self.check_string(long_signature)

    def test_req_historical_data3(self) -> None:
        """
        Test req_historical_data() on a single day outside trading hours.

        Requesting data for a day ending at midnight gets data after
        18:00.
        """
        # 2021-02-18 is a Thursday and it's full day.
        end_ts = pd.Timestamp("2021-02-18 00:00:00")
        use_rth = False
        short_signature, long_signature = self._req_historical_data_helper(
            end_ts, use_rth
        )
        exp = """
        signature=len=6 [2021-02-17 18:00:00-05:00, 2021-02-17 23:00:00-05:00]
        min_max_df=
                         min       max
        2021-02-17  18:00:00  23:00:00
        """
        self.assert_equal(short_signature, exp, fuzzy_match=True)
        self.check_string(long_signature)

    def test_req_historical_data4(self) -> None:
        """
        Test req_historical_data() on a single day outside trading hours.

        Requesting data for a day ending at noon gets data after 18:00
        of the day before.
        """
        # 2021-02-18 is a Thursday and it's full day.
        end_ts = pd.Timestamp("2021-02-18 12:00:00")
        use_rth = False
        short_signature, long_signature = self._req_historical_data_helper(
            end_ts, use_rth
        )
        exp = """
        signature=len=18 [2021-02-17 18:00:00-05:00, 2021-02-18 11:00:00-05:00]
        min_max_df=
                         min       max
        2021-02-17  18:00:00  23:00:00
        2021-02-18  00:00:00  11:00:00
        """
        self.assert_equal(short_signature, exp, fuzzy_match=True)
        self.check_string(long_signature)

    def test_req_historical_data5(self) -> None:
        """
        Test req_historical_data() on a non-existing day.
        """
        # 2018-02-29 doesn't exist, since 2018 is not a leap year.
        end_ts = pd.Timestamp("2018-01-29 14:00:00-05:00")
        use_rth = False
        short_signature, long_signature = self._req_historical_data_helper(
            end_ts, use_rth
        )
        exp = """
        """
        self.assert_equal(short_signature, exp, fuzzy_match=True)
        self.check_string(long_signature)

    def test_req_historical_data6(self) -> None:
        """
        Test req_historical_data() on a day when the market is closed.
        """
        # 2018-02-19 is a Thursday and it's president day.
        end_ts = pd.Timestamp("2018-02-19 14:00:00-05:00")
        use_rth = False
        short_signature, long_signature = self._req_historical_data_helper(
            end_ts, use_rth
        )
        exp = """
        """
        self.assert_equal(short_signature, exp, fuzzy_match=True)
        self.check_string(long_signature)

    def test_get_historical_data_with_IB_loop1(self) -> None:
        """
        Test getting 1 hr data for 1 full day.
        """
        # 2021-02-17 is a Wednesday and it's full day.
        start_ts = pd.Timestamp("2021-02-17 00:00:00")
        end_ts = start_ts + pd.DateOffset(days=1)
        bar_size_setting = "1 hour"
        use_rth = False
        (
            df,
            short_signature,
            long_signature,
        ) = self._get_historical_data_with_IB_loop_helper(
            start_ts, end_ts, bar_size_setting, use_rth
        )
        #
        act = "\n".join(map(str, df.index))
        # NOTE: IB returns also a bar at close 16:30 even if the frequency is hourly.
        exp = """
        2021-02-17 00:00:00-05:00
        2021-02-17 01:00:00-05:00
        2021-02-17 02:00:00-05:00
        2021-02-17 03:00:00-05:00
        2021-02-17 04:00:00-05:00
        2021-02-17 05:00:00-05:00
        2021-02-17 06:00:00-05:00
        2021-02-17 07:00:00-05:00
        2021-02-17 08:00:00-05:00
        2021-02-17 09:00:00-05:00
        2021-02-17 10:00:00-05:00
        2021-02-17 11:00:00-05:00
        2021-02-17 12:00:00-05:00
        2021-02-17 13:00:00-05:00
        2021-02-17 14:00:00-05:00
        2021-02-17 15:00:00-05:00
        2021-02-17 16:00:00-05:00
        2021-02-17 16:30:00-05:00
        2021-02-17 18:00:00-05:00
        2021-02-17 19:00:00-05:00
        2021-02-17 20:00:00-05:00
        2021-02-17 21:00:00-05:00
        2021-02-17 22:00:00-05:00
        2021-02-17 23:00:00-05:00
        """
        self.assert_equal(act, exp, fuzzy_match=True)
        #
        exp_short_signature = """
        signature=len=24 [2021-02-17 00:00:00-05:00, 2021-02-17 23:00:00-05:00]
        min_max_df=
                        min       max
        2021-02-17  00:00:00  23:00:00
        """
        self.assert_equal(short_signature, exp_short_signature, fuzzy_match=True)
        #
        self.check_string(long_signature)

    def test_get_historical_data_with_IB_loop2(self) -> None:
        """
        Like test_get_historical_data_with_IB_loop1() but for 1 regular trading
        day.
        """
        # 2021-02-17 is a Wednesday and it's full day.
        start_ts = pd.Timestamp("2021-02-17 00:00:00")
        end_ts = start_ts + pd.DateOffset(days=1)
        bar_size_setting = "1 hour"
        use_rth = True
        (
            df,
            short_signature,
            long_signature,
        ) = self._get_historical_data_with_IB_loop_helper(
            start_ts, end_ts, bar_size_setting, use_rth
        )
        #
        act = "\n".join(map(str, df.index))
        exp = r"""
        2021-02-17 09:30:00-05:00
        2021-02-17 10:00:00-05:00
        2021-02-17 11:00:00-05:00
        2021-02-17 12:00:00-05:00
        2021-02-17 13:00:00-05:00
        2021-02-17 14:00:00-05:00
        2021-02-17 15:00:00-05:00
        2021-02-17 16:00:00-05:00
        2021-02-17 16:30:00-05:00
        """
        self.assert_equal(act, exp, fuzzy_match=True)
        #
        exp_short_signature = """
        signature=len=9 [2021-02-17 09:30:00-05:00, 2021-02-17 16:30:00-05:00]
        min_max_df=
                        min      max
        2021-02-17 09:30:00 16:30:00
        """
        self.assert_equal(short_signature, exp_short_signature, fuzzy_match=True)
        #
        self.check_string(long_signature)

    def test_get_historical_data_with_IB_loop3(self) -> None:
        """
        Test getting 1 hr data for 1 full days.

        Data is returned for the entire day for both days.
        """
        # 2021-02-18 is a Thursday and it's full day.
        start_ts = pd.Timestamp("2021-02-17 00:00:00").tz_localize(
            tz="America/New_York"
        )
        end_ts = start_ts + pd.DateOffset(days=1)
        bar_size_setting = "1 hour"
        use_rth = False
        (
            _,
            short_signature,
            long_signature,
        ) = self._get_historical_data_with_IB_loop_helper(
            start_ts, end_ts, bar_size_setting, use_rth
        )
        #
        exp_short_signature = """
        signature=len=24 [2021-02-17 00:00:00-05:00, 2021-02-17 23:00:00-05:00]
        min_max_df=
                         min       max
        2021-02-17  00:00:00  23:00:00
        """
        self.assert_equal(short_signature, exp_short_signature, fuzzy_match=True)
        #
        self.check_string(long_signature)

    def test_get_historical_data_with_IB_loop4(self) -> None:
        """
        Test getting 1 hr data for 1 RTH days.

        Data is returned for the regular trading session for both days.
        """
        # 2021-02-18 is a Thursday and it's full day.
        start_ts = pd.Timestamp("2021-02-17 00:00:00")
        end_ts = start_ts + pd.DateOffset(days=1)
        bar_size_setting = "1 hour"
        use_rth = True
        (
            df,
            short_signature,
            long_signature,
        ) = self._get_historical_data_with_IB_loop_helper(
            start_ts, end_ts, bar_size_setting, use_rth
        )
        #
        exp_short_signature = """
        signature=len=9 [2021-02-17 09:30:00-05:00, 2021-02-17 16:30:00-05:00]
        min_max_df=
                         min       max
        2021-02-17  09:30:00  16:30:00
        """
        self.assert_equal(short_signature, exp_short_signature, fuzzy_match=True)
        #
        self.check_string(long_signature)

    def test_get_historical_data_with_IB_loop5(self) -> None:
        """
        Test getting 1 hr data for a day when the market is closed.
        """
        # 2021-02-15 is a Monday and there is no trading activity since it's MLK day.
        start_ts = pd.Timestamp("2021-02-15 00:00:00")
        end_ts = start_ts + pd.DateOffset(days=1)
        bar_size_setting = "1 hour"
        use_rth = False
        (
            df,
            short_signature,
            long_signature,
        ) = self._get_historical_data_with_IB_loop_helper(
            start_ts, end_ts, bar_size_setting, use_rth
        )
        #
        # act = ("\n".join(map(str, df.index)))
        # exp = r"""
        # """
        # self.assert_equal(act, exp, fuzzy_match=True)
        #
        exp_short_signature = """
        signature=len=19 [2021-02-15 00:00:00-05:00, 2021-02-15 23:00:00-05:00]
        min_max_df=
                         min       max
        2021-02-15  00:00:00  23:00:00
        """
        self.assert_equal(short_signature, exp_short_signature, fuzzy_match=True)
        #
        self.check_string(long_signature)

    def test_get_historical_data_with_IB_loop6(self) -> None:
        """
        Test getting 1 hr data when the market is open.
        """
        # 2021-02-07 is a Sunday.
        start_ts = pd.Timestamp("2021-02-07 00:00:00")
        end_ts = start_ts + pd.DateOffset(days=2)
        bar_size_setting = "1 hour"
        use_rth = False
        (
            df,
            short_signature,
            long_signature,
        ) = self._get_historical_data_with_IB_loop_helper(
            start_ts, end_ts, bar_size_setting, use_rth
        )
        #
        act = "\n".join(map(str, df.index))
        exp = r"""
        2021-02-07 18:00:00-05:00
        2021-02-07 19:00:00-05:00
        2021-02-07 20:00:00-05:00
        2021-02-07 21:00:00-05:00
        2021-02-07 22:00:00-05:00
        2021-02-07 23:00:00-05:00
        2021-02-08 00:00:00-05:00
        2021-02-08 01:00:00-05:00
        2021-02-08 02:00:00-05:00
        2021-02-08 03:00:00-05:00
        2021-02-08 04:00:00-05:00
        2021-02-08 05:00:00-05:00
        2021-02-08 06:00:00-05:00
        2021-02-08 07:00:00-05:00
        2021-02-08 08:00:00-05:00
        2021-02-08 09:00:00-05:00
        2021-02-08 10:00:00-05:00
        2021-02-08 11:00:00-05:00
        2021-02-08 12:00:00-05:00
        2021-02-08 13:00:00-05:00
        2021-02-08 14:00:00-05:00
        2021-02-08 15:00:00-05:00
        2021-02-08 16:00:00-05:00
        2021-02-08 16:30:00-05:00
        2021-02-08 18:00:00-05:00
        2021-02-08 19:00:00-05:00
        2021-02-08 20:00:00-05:00
        2021-02-08 21:00:00-05:00
        2021-02-08 22:00:00-05:00
        2021-02-08 23:00:00-05:00
        """
        self.assert_equal(act, exp, fuzzy_match=True)
        #
        exp_short_signature = """
        signature=len=30 [2021-02-07 18:00:00-05:00, 2021-02-08 23:00:00-05:00]
        min_max_df=
                         min       max
        2021-02-07  18:00:00  23:00:00
        2021-02-08  00:00:00  23:00:00
        """
        self.assert_equal(short_signature, exp_short_signature, fuzzy_match=True)
        #
        self.check_string(long_signature)

    def test_get_historical_data_with_IB_loop7(self) -> None:
        """
        Test getting 1hr data on Sunday when the market is open half day.
        """
        # 2021-02-07 is Sunday.
        start_ts = pd.Timestamp("2021-02-07 00:00:00")
        end_ts = start_ts + pd.DateOffset(days=1)
        bar_size_setting = "1 min"
        use_rth = False
        (
            df,
            short_signature,
            long_signature,
        ) = self._get_historical_data_with_IB_loop_helper(
            start_ts, end_ts, bar_size_setting, use_rth
        )
        #
        exp_short_signature = """
        signature=len=360 [2021-02-07 18:00:00-05:00, 2021-02-07 23:59:00-05:00]
        min_max_df=
                        min      max
        2021-02-07 18:00:00 23:59:00
        """
        self.assert_equal(short_signature, exp_short_signature, fuzzy_match=True)
        #
        self.check_string(long_signature)

    def test_save_historical_data_with_IB_loop1(self) -> None:
        """
        
        """
        # 2021-02-17 is a Wednesday and it's full day.
        start_ts = pd.Timestamp("2021-02-17 00:00:00")
        end_ts = start_ts + pd.DateOffset(days=1)
        bar_size_setting = "1 hour"
        use_rth = False
        (
            df,
            short_signature,
            long_signature,
        ) = self._save_historical_data_with_IB_loop_helper(
            start_ts, end_ts, bar_size_setting, use_rth
        )
        #
        act = "\n".join(map(str, df.index))
        # NOTE: IB returns also a bar at close 16:30 even if the frequency is hourly.
        exp = """
        2021-02-17 00:00:00-05:00
        2021-02-17 01:00:00-05:00
        2021-02-17 02:00:00-05:00
        2021-02-17 03:00:00-05:00
        2021-02-17 04:00:00-05:00
        2021-02-17 05:00:00-05:00
        2021-02-17 06:00:00-05:00
        2021-02-17 07:00:00-05:00
        2021-02-17 08:00:00-05:00
        2021-02-17 09:00:00-05:00
        2021-02-17 10:00:00-05:00
        2021-02-17 11:00:00-05:00
        2021-02-17 12:00:00-05:00
        2021-02-17 13:00:00-05:00
        2021-02-17 14:00:00-05:00
        2021-02-17 15:00:00-05:00
        2021-02-17 16:00:00-05:00
        2021-02-17 16:30:00-05:00
        2021-02-17 18:00:00-05:00
        2021-02-17 19:00:00-05:00
        2021-02-17 20:00:00-05:00
        2021-02-17 21:00:00-05:00
        2021-02-17 22:00:00-05:00
        2021-02-17 23:00:00-05:00
        """
        self.assert_equal(act, exp, fuzzy_match=True)
        #
        exp_short_signature = """
        signature=len=24 [2021-02-17 00:00:00-05:00, 2021-02-17 23:00:00-05:00]
        min_max_df=
                         min       max
        2021-02-17  00:00:00  23:00:00
        """
        self.assert_equal(short_signature, exp_short_signature, fuzzy_match=True)
        #
        self.check_string(long_signature)

    # #########################################################################

    def test_download_ib_data1(self) -> None:
        target = "continuous_futures"
        frequency = "hour"
        use_rth = False
        symbols = "ES".split()
        start_ts = pd.Timestamp("2020-12-09 18:00:00-05:00")
        end_ts = pd.Timestamp("2020-12-13 18:00:00-05:00")
        tasks = vieuti.get_tasks(
            self.ib, target, frequency, symbols, start_ts, end_ts, use_rth
        )
        #
        client_id_base = 5
        #
        num_threads = "serial"
        dst_dir = self.get_scratch_space()
        incremental = False
        file_names = vieuti.download_ib_data(
            client_id_base, tasks, incremental, dst_dir, num_threads
        )
        dbg.dassert_eq(len(file_names), 1)
        _LOG.debug("file_names=%s", file_names)
        # Load the data.
        df = vieuti.load_historical_data(file_names[0])
        short_signature, long_signature = self._get_df_signatures(df)
        exp_short_signature = """
        signature=len=48 [2020-12-09 18:00:00-05:00, 2020-12-11 16:30:00-05:00]
        min_max_df=
                        min      max
        2020-12-09 18:00:00 23:00:00
        2020-12-10 00:00:00 23:00:00
        2020-12-11 00:00:00 16:30:00
        """
        self.assert_equal(short_signature, exp_short_signature, fuzzy_match=True)
        #
        self.check_string(long_signature)

    # #########################################################################

    def test_ib_date_range1(self):
        start_ts = pd.Timestamp("2018-01-26 15:00").tz_localize(
            tz="America/New_York"
        )
        end_ts = pd.Timestamp("2018-02-03 15:00").tz_localize(
            tz="America/New_York"
        )
        vieuti._ib_date_range(start_ts, end_ts)
        bar_size_setting = "1 hour"
        use_rth = False
        (
            df,
            short_signature,
            long_signature,
        ) = self._get_historical_data_with_IB_loop_helper(
            start_ts, end_ts, bar_size_setting, use_rth
        )
        #
        act = "\n".join(map(str, df.index))
        # NOTE: IB returns also a bar at close 16:30 even if the frequency is hourly.
        exp = """
        2021-02-17 00:00:00-05:00
        2021-02-17 01:00:00-05:00
        2021-02-17 02:00:00-05:00
        2021-02-17 03:00:00-05:00
        2021-02-17 04:00:00-05:00
        2021-02-17 05:00:00-05:00
        2021-02-17 06:00:00-05:00
        2021-02-17 07:00:00-05:00
        2021-02-17 08:00:00-05:00
        2021-02-17 09:00:00-05:00
        2021-02-17 10:00:00-05:00
        2021-02-17 11:00:00-05:00
        2021-02-17 12:00:00-05:00
        2021-02-17 13:00:00-05:00
        2021-02-17 14:00:00-05:00
        2021-02-17 15:00:00-05:00
        2021-02-17 16:00:00-05:00
        2021-02-17 16:30:00-05:00
        2021-02-17 18:00:00-05:00
        2021-02-17 19:00:00-05:00
        2021-02-17 20:00:00-05:00
        2021-02-17 21:00:00-05:00
        2021-02-17 22:00:00-05:00
        2021-02-17 23:00:00-05:00
        """
        self.assert_equal(act, exp, fuzzy_match=True)
        #
        exp_short_signature = """
        signature=len=24 [2021-02-17 00:00:00-05:00, 2021-02-17 23:00:00-05:00]
        min_max_df=
                        min       max
        2021-02-17  00:00:00  23:00:00
        """
        self.assert_equal(short_signature, exp_short_signature, fuzzy_match=True)
        #
        self.check_string(long_signature)

    # #########################################################################

    def test_ib_date_range2(self) -> None:
        start_ts = pd.Timestamp("2018-02-07 00:00").tz_localize(
            tz="America/New_York"
        )
        end_ts = start_ts + pd.DateOffset(days=3)
        dates = vieuti._ib_date_range(start_ts, end_ts)
        #
        act = "\n".join(map(str, dates))
        exp = """
        2018-02-08 18:00:00-05:00
        2018-02-10 00:00:00-05:00
        """
        self.assert_equal(act, exp, fuzzy_match=True)

    def test_ib_date_range3(self) -> None:
        start_ts = pd.Timestamp("2018-02-07 18:00").tz_localize(
            tz="America/New_York"
        )
        end_ts = start_ts + pd.DateOffset(days=1)
        dates = vieuti._ib_date_range(start_ts, end_ts)
        #
        act = "\n".join(map(str, dates))
        exp = """
        2018-02-07 18:00:00-05:00
        2018-02-08 18:00:00-05:00
        """
        self.assert_equal(act, exp, fuzzy_match=True)

    def test_ib_date_range4(self) -> None:
        start_ts = pd.Timestamp("2018-02-07 17:59").tz_localize(
            tz="America/New_York"
        )
        end_ts = start_ts + pd.DateOffset(days=2)
        dates = vieuti._ib_date_range(start_ts, end_ts)
        #
        act = "\n".join(map(str, dates))
        exp = """
        2018-02-08 18:00:00-05:00
        2018-02-09 17:59:00-05:00
        """
        self.assert_equal(act, exp, fuzzy_match=True)

    def test_ib_date_range5(self) -> None:
        start_ts = pd.Timestamp("2018-02-07 17:59").tz_localize(
            tz="America/New_York"
        )
        end_ts = start_ts + pd.DateOffset(days=3)
        dates = vieuti._ib_date_range(start_ts, end_ts)
        #
        act = "\n".join(map(str, dates))
        exp = """
        2018-02-08 18:00:00-05:00
        2018-02-10 17:59:00-05:00
        """
        self.assert_equal(act, exp, fuzzy_match=True)

    def test_ib_date_range6(self) -> None:
        start_ts = pd.Timestamp("2018-02-07 00:00:00").tz_localize(
            tz="America/New_York"
        )
        end_ts = start_ts + pd.DateOffset(days=3)
        dates = vieuti._ib_date_range(start_ts, end_ts)
        #
        act = "\n".join(map(str, dates))
        exp = """
        2018-02-08 18:00:00-05:00
        2018-02-10 00:00:00-05:00
        """
        self.assert_equal(act, exp, fuzzy_match=True)

    def test_get_historical_data1(self) -> None:
        """
        Test getting 1 hr data for 3 days.
        """
        start_ts = pd.Timestamp("2021-02-07 00:00:00")
        end_ts = start_ts + pd.DateOffset(days=3)
        bar_size_setting = "1 hour"
        use_rth = False
        #
        self._compare_historical_data(bar_size_setting, start_ts, end_ts, use_rth)

    def test_get_historical_data2(self) -> None:
        """
        Test getting 1 hr data for 4 days.
        """
        start_ts = pd.Timestamp("2021-02-07 18:00:00")
        end_ts = start_ts + pd.DateOffset(days=4)
        bar_size_setting = "1 hour"
        use_rth = True
        #
        self._compare_historical_data(bar_size_setting, start_ts, end_ts, use_rth)

    def test_get_historical_data3(self) -> None:
        """
        Test getting 1 hr data for 3 days.
        """
        start_ts = pd.Timestamp("2021-02-08 09:00:00")
        end_ts = start_ts + pd.DateOffset(days=3)
        bar_size_setting = "1 hour"
        use_rth = True
        #
        self._compare_historical_data(bar_size_setting, start_ts, end_ts, use_rth)

    def test_get_historical_data4(self) -> None:
        """
        Test getting 1 minute data for 3 days.
        """
        start_ts = pd.Timestamp("2021-02-07 18:00:00")
        end_ts = start_ts + pd.DateOffset(days=3)
        bar_size_setting = "1 min"
        use_rth = True
        #
        self._compare_historical_data(bar_size_setting, start_ts, end_ts, use_rth)

    # #########################################################################

    @staticmethod
    def _get_df_signatures(df: pd.DataFrame) -> Tuple[str, str]:
        """
        Compute a short and a long signature for the df.

        The short signature is suitable to the self.assertEqual()
        approach. The long signature contains the data and it's suitable
        to the self.check_string() approach.
        """
        txt = []
        #
        act = vieuti.get_df_signature(df)
        txt.append("signature=%s" % act)
        #
        if not df.empty:
            df_tmp = df.copy()
            _LOG.debug("df_tmp=\n%s", df_tmp.head())
            dbg.dassert_isinstance(df_tmp.index[0], pd.Timestamp)
            df_tmp["time"] = df_tmp.index.time
            min_max_df = (
                df_tmp["time"].groupby(lambda x: x.date()).agg([min, max])
            )
            txt.append("min_max_df=\n%s" % min_max_df)
            short_signature = "\n".join(txt)
        else:
            short_signature = ""
        #
        txt.append("df=\n%s" % df.to_csv())
        long_signature = "\n".join(txt)
        return short_signature, long_signature

    def _req_historical_data_helper(self, end_ts, use_rth) -> Tuple[str, str]:
        """
        Run vieuti.req_historical_data() with some fixed params and return
        short and long signature.
        """
        contract = ib_insync.ContFuture("ES", "GLOBEX", currency="USD")
        what_to_show = "TRADES"
        duration_str = "1 D"
        bar_size_setting = "1 hour"
        df = vieuti.req_historical_data(
            self.ib,
            contract,
            end_ts,
            duration_str,
            bar_size_setting,
            what_to_show,
            use_rth,
        )
        short_signature, long_signature = self._get_df_signatures(df)
        return short_signature, long_signature

    # #########################################################################

    def _get_historical_data_with_IB_loop_helper(
        self,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        bar_size_setting: str,
        use_rth: bool,
    ) -> Tuple[pd.DataFrame, str, str]:
        _LOG.debug("start_ts='%s' end_ts='%s'", start_ts, end_ts)
        contract = ib_insync.ContFuture("ES", "GLOBEX", currency="USD")
        what_to_show = "TRADES"
        duration_str = "1 D"
        df, ts_seq = vieuti.get_historical_data_with_IB_loop(
            self.ib,
            contract,
            start_ts,
            end_ts,
            duration_str,
            bar_size_setting,
            what_to_show,
            use_rth,
            return_ts_seq=True,
        )
        short_signature, long_signature = self._get_df_signatures(df)
        long_signature += "ts_seq=\n" + "\n".join(map(str, ts_seq))
        return df, short_signature, long_signature

    # #########################################################################

    def _save_historical_data_with_IB_loop_helper(
        self,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        bar_size_setting: str,
        use_rth: bool,
    ) -> Tuple[pd.DataFrame, str, str]:
        _LOG.debug("start_ts='%s' end_ts='%s'", start_ts, end_ts)
        contract = ib_insync.ContFuture("ES", "GLOBEX", currency="USD")
        what_to_show = "TRADES"
        duration_str = "1 D"
        file_name = os.path.join(self.get_scratch_space(), "output.csv")
        incremental = False
        vieuti.save_historical_data_with_IB_loop(
            self.ib,
            contract,
            start_ts,
            end_ts,
            duration_str,
            bar_size_setting,
            what_to_show,
            use_rth,
            file_name,
            incremental,
        )
        # Load the data generated.
        df = vieuti.load_historical_data(file_name)
        # Check.
        short_signature, long_signature = self._get_df_signatures(df)
        return df, short_signature, long_signature

    # #########################################################################

    def _get_historical_data_helper(
        self,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        bar_size_setting: str,
        use_rth: bool,
    ) -> Tuple[pd.DataFrame, str, str]:
        _LOG.debug("start_ts='%s' end_ts='%s'", start_ts, end_ts)
        contract = ib_insync.ContFuture("ES", "GLOBEX", currency="USD")
        what_to_show = "TRADES"
        mode = "in_memory"
        client_id = 2
        df, ts_seq = vieuti.get_historical_data(
            client_id,
            contract,
            start_ts,
            end_ts,
            bar_size_setting,
            what_to_show,
            use_rth,
            mode,
            return_ts_seq=True,
        )
        short_signature, long_signature = self._get_df_signatures(df)
        long_signature += "ts_seq=\n" + "\n".join(map(str, ts_seq))
        return df, short_signature, long_signature

    def _compare_historical_data(
        self,
        bar_size_setting: str,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        use_rth: bool,
    ) -> None:
        """
        Retrieve historical data with `get_historical_data` and
        `get_historical_data_with_IB_loop` and compare it.
        """
        # Get the data in two different ways.
        df, short_signature, long_signature = self._get_historical_data_helper(
            start_ts, end_ts, bar_size_setting, use_rth
        )
        (
            df2,
            short_signature2,
            long_signature2,
        ) = self._get_historical_data_with_IB_loop_helper(
            start_ts, end_ts, bar_size_setting, use_rth
        )
        # Compare the outcomes.
        self.assert_equal(df.to_csv(), df2.to_csv(), fuzzy_match=True)
        self.assert_equal(short_signature, short_signature2)
        #
        self.check_string(long_signature)
