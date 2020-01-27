import logging
from typing import List, Tuple

import gluonts

# TODO(*): gluon needs this import to work properly.
import gluonts.model.forecast as gmf  # isort: skip # noqa: F401 # pylint: disable=unused-import
import numpy as np
import pandas as pd

import core.data_adapters as adpt
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class _TestAdapter:
    def __init__(self, n_rows: int = 100, n_cols: int = 5):
        self._frequency = "T"
        self._n_rows = n_rows
        self._n_cols = n_cols
        self._df = self._get_test_df()
        self._x_vars = self._df.columns[:-2].tolist()
        self._y_vars = self._df.columns[-2:].tolist()

    def _get_test_df(self) -> pd.DataFrame:
        np.random.seed(42)
        idx = pd.Series(
            pd.date_range(
                "2010-01-01 00:00", "2010-01-01 03:00", freq=self._frequency
            )
        ).sample(self._n_rows)
        df = pd.DataFrame(np.random.randn(self._n_rows, self._n_cols), index=idx)
        df.index.name = "timestamp"
        df.columns = [f"col_{j}" for j in range(0, self._n_cols)]
        df.sort_index(inplace=True)
        df = df.asfreq(self._frequency)
        return df


class TestTransformToGluon(hut.TestCase):
    def test_transform(self) -> None:
        ta = _TestAdapter()
        gluon_ts = adpt.transform_to_gluon(
            ta._df, ta._x_vars, ta._y_vars, ta._frequency
        )
        self.check_string(str(list(gluon_ts)))

    def test_transform_local_ts(self) -> None:
        ta = _TestAdapter()
        local_ts = pd.concat([ta._df, ta._df + 1], keys=[0, 1])
        gluon_ts = adpt.transform_to_gluon(
            local_ts, ta._x_vars, ta._y_vars, ta._frequency
        )
        self.check_string(str(list(gluon_ts)))

    def test_transform_series_target(self) -> None:
        ta = _TestAdapter()
        y_vars = ta._y_vars[-1:]
        gluon_ts = adpt.transform_to_gluon(
            ta._df, ta._x_vars, y_vars, ta._frequency
        )
        self.check_string(str(list(gluon_ts)))


class TestTransformFromGluon(hut.TestCase):
    def test_transform(self) -> None:
        ta = _TestAdapter()
        gluon_ts = adpt.transform_to_gluon(
            ta._df, ta._x_vars, ta._y_vars, ta._frequency
        )
        df = adpt.transform_from_gluon(
            gluon_ts, ta._x_vars, ta._y_vars, index_name=ta._df.index.name,
        )
        self.check_string(df.to_string())

    def test_correctness(self) -> None:
        ta = _TestAdapter()
        gluon_ts = adpt.transform_to_gluon(
            ta._df, ta._x_vars, ta._y_vars, ta._frequency
        )
        inverted_df = adpt.transform_from_gluon(
            gluon_ts, ta._x_vars, ta._y_vars, index_name=ta._df.index.name,
        )
        inverted_df = inverted_df.astype(np.float64)
        pd.testing.assert_frame_equal(ta._df, inverted_df)

    def test_correctness_local_ts(self) -> None:
        ta = _TestAdapter()
        local_ts = pd.concat([ta._df, ta._df + 1], keys=[0, 1])
        gluon_ts = adpt.transform_to_gluon(
            local_ts, ta._x_vars, ta._y_vars, ta._frequency
        )
        inverted_df = adpt.transform_from_gluon(
            gluon_ts, ta._x_vars, ta._y_vars, index_name=ta._df.index.name,
        )
        inverted_df = inverted_df.astype(np.float64)
        pd.testing.assert_frame_equal(local_ts, inverted_df)


class TestTransformFromGluonForecasts(hut.TestCase):
    @staticmethod
    def _get_mock_forecasts(
        n_traces: int = 3,
        n_offsets: int = 50,
        n_forecasts: int = 2,
        frequency: str = "T",
    ) -> List[gluonts.model.forecast.SampleForecast]:
        np.random.seed(42)
        all_samples = np.random.randn(n_traces, n_offsets, n_forecasts)
        start_dates = pd.date_range(
            pd.Timestamp("2010-01-01"), freq="D", periods=n_forecasts
        )
        forecasts = [
            gluonts.model.forecast.SampleForecast(
                all_samples[:, :, i], start_date, frequency
            )
            for i, start_date in enumerate(start_dates)
        ]
        return forecasts

    def test_transform1(self) -> None:
        forecasts = TestTransformFromGluonForecasts._get_mock_forecasts()
        df = adpt.transform_from_gluon_forecasts(forecasts)
        self.check_string(df.to_string())


class TestTransformToSklean(hut.TestCase):
    def test_transform1(self) -> None:
        ta = _TestAdapter()
        df = ta._df.dropna()
        sklearn_input = adpt.transform_to_sklearn(df, ta._x_vars, ta._y_vars)
        self.check_string("x_vals:\n{}\ny_vals:\n{}".format(*sklearn_input))


class TestTransformFromSklean(hut.TestCase):
    @staticmethod
    def _get_sklearn_data() -> Tuple[pd.Index, pd.DataFrame, pd.DataFrame]:
        np.random.seed(42)
        ta = _TestAdapter()
        df = ta._df.dropna()
        idx = df.index
        x_vars = ta._x_vars
        x_vals = df[x_vars]
        return idx, x_vars, x_vals

    def test_transform1(self) -> None:
        sklearn_data = TestTransformFromSklean._get_sklearn_data()
        transformed_df = adpt.transform_from_sklearn(*sklearn_data)
        self.check_string(transformed_df.to_string())
