import numpy as np
import pandas as pd
import pytest

import core.timeseries_study as tss
import helpers.unit_test as ut


class TestTimeSeriesDailyStudy(ut.TestCase):
    def test_usual_case(self):
        idx = pd.date_range("2018-12-31", "2019-01-31")
        vals = np.random.randn(len(idx))
        ts = pd.Series(vals, index=idx)
        tsds = tss.TimeSeriesDailyStudy(ts)
        tsds.execute()


class TestTimeSeriesMinuteStudy(ut.TestCase):
    def test_usual_case(self):
        idx = pd.date_range("2018-12-31", "2019-01-31", freq="5T")
        vals = np.random.randn(len(idx))
        ts = pd.Series(vals, index=idx)
        tsms = tss.TimeSeriesMinuteStudy(ts)
        tsms.execute()
