import datetime
import io
import logging

import pandas as pd

import core.finance.prediction_processing as cfiprpro
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


class TestStackPredictionDf(hunitest.TestCase):
    def test1(self) -> None:
        df = self._get_data()
        actual = cfiprpro.stack_prediction_df(
            df,
            id_col="amid",
            close_price_col="close",
            vwap_col="vwap",
            ret_col="ret",
            prediction_col="pred",
            ath_start=datetime.time(9, 30),
            ath_end=datetime.time(16, 0),
        )
        expected_txt = """
,amid,eob_close,eob_vwap,eob_ret,eopb_close,alpha,minute_index,start_bar_et_ts
0,MN0,100.0,100.0,,,,24198330,2016-01-04T09:30:00
1,MN1,73.25,73.25,,,,24198330,2016-01-04T09:30:00
2,MN0,100.05,100.04,0.0005,100.0,,24198331,2016-01-04T09:31:00
3,MN1,73.22,73.24,-0.00041,73.25,,24198331,2016-01-04T09:31:00
4,MN0,100.07,100.05,0.0002,100.05,1e-05,24198332,2016-01-04T09:32:00
5,MN1,73.22,73.23,0.0,73.22,-3e-05,24198332,2016-01-04T09:32:00
"""
        expected = pd.read_csv(
            io.StringIO(expected_txt),
            index_col=0,
        )
        # NOTE: If the test fails, converting to json strings may make
        #       debugging easier.
        # import helpers.hpandas as hpandas
        # actual_str = hpandas.convert_df_to_json_string(actual)
        # expected_str = hpandas.convert_df_to_json_string(expected)
        # self.assert_equal(actual_str, expected_str)
        hunitest.compare_df(actual, expected)

    @staticmethod
    def _get_data() -> pd.DataFrame:
        txt = """
,close,close,vwap,vwap,ret,ret,pred,pred
datetime,MN0,MN1,MN0,MN1,MN0,MN1,MN0,MN1
2016-01-04 09:31:00,100.0,73.25,100.0,73.25,NaN,NaN,NaN,NaN
2016-01-04 09:32:00,100.05,73.22,100.04,73.24,0.0005,-0.00041,0.00001,-0.00003
2016-01-04 09:33:00,100.07,73.22,100.05,73.23,0.0002,0.0,0.00001,0.00001
"""
        df = pd.read_csv(
            io.StringIO(txt), index_col=0, parse_dates=True, header=[0, 1]
        )
        df.index.freq = "T"
        return df


# ########################################################################


class TestComputeEpoch(hunitest.TestCase):
    """
    Test the computation of epoch time series with different units.
    """

    def get_series_data(self):
        """
        Generate series input data for test.
        """

    def get_dataframe_data(self):
        """
        Generateb dataframe input data for test.
        """

    def test1(self):
        """
        Test series input data with unit - minutes.
        """

    def test2(self):
        """
        Test series input data with unit - seconds.
        """

    def test3(self):
        """
        Test series input data with unit - nanoseconds.
        """

    def test4(self):
        """
        Test dataframe input data with any unit.
        """