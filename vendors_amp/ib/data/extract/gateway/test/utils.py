import logging
import os
from typing import Tuple

try:
    import ib_insync
except ModuleNotFoundError:
    print("Can't find ib_insync")
import pandas as pd

import helpers.dbg as dbg
import helpers.unit_test as hut
import vendors_amp.ib.extract.download_data_ib_loop as viedow
import vendors_amp.ib.extract.save_historical_data_with_IB_loop as viesav
import vendors_amp.ib.extract.unrolling_download_data_ib_loop as vieunr
import vendors_amp.ib.extract.utils as vieuti

_LOG = logging.getLogger(__name__)


IS_TWS_ENABLED = (
    os.environ.get("STAGE") == "TEST"
    and os.environ.get("IB_GW_CONNECTION_HOST") == "ib_connect_test"
) or (
    os.environ.get("STAGE") == "LOCAL"
    and os.environ.get("IB_GW_CONNECTION_HOST") == "ib_connect_local"
)


class IbExtractionTest(hut.TestCase):
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

    @staticmethod
    def get_df_signatures(df: pd.DataFrame) -> Tuple[str, str]:
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
        short_signature, long_signature = self.get_df_signatures(df)
        return short_signature, long_signature

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
        df, ts_seq = viedow.get_historical_data_with_IB_loop(
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
        short_signature, long_signature = self.get_df_signatures(df)
        long_signature += "ts_seq=\n" + "\n".join(map(str, ts_seq))
        return df, short_signature, long_signature

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
        df, ts_seq = vieunr.get_historical_data(
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
        short_signature, long_signature = self.get_df_signatures(df)
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
        viesav.save_historical_data_with_IB_loop(
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
        df = viedow.load_historical_data(file_name)
        # Check.
        short_signature, long_signature = self.get_df_signatures(df)
        return df, short_signature, long_signature
