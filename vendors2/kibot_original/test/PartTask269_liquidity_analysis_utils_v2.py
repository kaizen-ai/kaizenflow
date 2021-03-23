import numpy as np
import pandas as pd
import pytest

import helpers.unit_test as ut
import vendors2.kibot_original.PartTask269_liquidity_analysis_utils as klau


class TestTimeSeriesDailyStudy(ut.TestCase):
    def test_usual_case(self):
        idx = pd.date_range("2018-12-31", "2019-01-31")
        vals = np.random.randn(len(idx))
        ts = pd.Series(vals, index=idx)
        tsds = klau.TimeSeriesDailyStudy(ts)
        tsds.execute()


@pytest.mark.skip
class TestTimeSeriesMinuteStudy(ut.TestCase):
    def test_usual_case(self):
        idx = pd.date_range("2018-12-31", "2019-01-31", freq="5T")
        vals = np.random.randn(len(idx))
        ts = pd.Series(vals, index=idx)
        tsms = klau.TimeSeriesMinuteStudy(ts)
        tsms.execute()
