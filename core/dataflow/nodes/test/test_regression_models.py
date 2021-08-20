import io
import logging

import numpy as np
import pandas as pd

import core.artificial_signal_generators as casgen
import core.config as cconfig
import core.dataflow.nodes.regression_models as cdnrm
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestLinearRegression(hut.TestCase):

    def test0(self) -> None:
        """
        Check that the randomly generated data is stable. See AmpTask1649.
        """
        _LOG.debug("Current seed=%s", np.random.get_state()[1][0])
        _LOG.debug("Generating data")
        data = self._get_data(seed=1)
        _LOG.debug("Current seed=%s", np.random.get_state()[1][0])
        _LOG.debug("data=\n%s", str(data))
        _LOG.debug("Checking against golden")
        df_str = hut.convert_df_to_string(data, index=True, decimals=3)
        self.check_string(df_str)

    def test1(self) -> None:
        # Load test data.
        data = self._get_data(seed=1)
        # Generate node config.
        config = cconfig.get_config_from_nested_dict(
            {
                "x_vars": ["x1", "x2", "x3", "x4"],
                "y_vars": ["y"],
                "steps_ahead": 1,
                "col_mode": "merge_all",
            }
        )
        node = cdnrm.LinearRegression(
            "linear_regression",
            **config.to_dict(),
        )
        #
        df_out = node.fit(data)["df_out"]
        df_str = hut.convert_df_to_string(df_out.round(3), index=True, decimals=3)
        self.check_string(df_str)

    def test2(self) -> None:
        # Load test data.
        data = self._get_data(seed=1)
        # Generate node config.
        config = cconfig.get_config_from_nested_dict(
            {
                "x_vars": ["x1", "x2", "x3", "x4"],
                "y_vars": ["y"],
                "steps_ahead": 1,
                "smoothing": 2,
                "col_mode": "merge_all",
            }
        )
        node = cdnrm.LinearRegression(
            "linear_regression",
            **config.to_dict(),
        )
        #
        df_out = node.fit(data)["df_out"]
        df_str = hut.convert_df_to_string(df_out.round(3), index=True, decimals=3)
        self.check_string(df_str)

    def test3(self) -> None:
        # Load test data.
        data = self._get_data(seed=1)
        data_fit = data.loc[:"2000-01-10"]
        data_predict = data.loc["2000-01-10":]
        # Generate node config.
        config = cconfig.get_config_from_nested_dict(
            {
                "x_vars": ["x1", "x2", "x3", "x4"],
                "y_vars": ["y"],
                "steps_ahead": 1,
                "smoothing": 2,
                "col_mode": "merge_all",
            }
        )
        node = cdnrm.LinearRegression(
            "linear_regression",
            **config.to_dict(),
        )
        #
        node.fit(data_fit)
        df_out = node.predict(data_predict)["df_out"]
        df_str = hut.convert_df_to_string(df_out.round(3), index=True, decimals=3)
        self.check_string(df_str)

    def _get_data(self, seed) -> pd.DataFrame:
        """
        Generate multivariate normal returns.
        """
        # cov = pd.DataFrame(
        #     np.array(
        #         [
        #             [1, 0.05, 0.3, -0.2, 0.1],
        #             [0.05, 1, 0, 0, 0],
        #             [0.3, 0, 1, 0, 0],
        #             [-0.2, 0, 0, 1, 0],
        #             [0.1, 0, 0, 0, 1],
        #         ],
        #     )
        # )
        # mn_process = casgen.MultivariateNormalProcess(cov=cov)
        # data = mn_process.generate_sample(
        #     {"start": "2000-01-01", "periods": 40, "freq": "B"}, seed=seed
        # )
        #
        # First 10 lines of output frozen for reproducibility.
        # TODO(Paul): Auto-save to a file instead when plumbing is in place.
        # TODO(Paul): Add shift to correlated y column before test.
        csv_data = """
,y,x1,x2,x3,x4
2000-01-03,-0.865,1.074,-1.604,0.966,-0.705
2000-01-04,1.771,-0.705,1.262,-2.374,-0.447
2000-01-05,-0.581,1.238,-0.811,2.345,-0.465
2000-01-06,1.238,0.368,0.713,-0.275,-0.654
2000-01-07,1.416,-1.19,-0.044,-0.78,0.719
2000-01-10,0.863,0.562,0.317,-0.267,-0.741
2000-01-11,0.2,1.19,0.655,-0.604,-0.187
2000-01-12,0.424,-0.843,0.785,1.7,-0.057
2000-01-13,0.188,-0.659,1.321,1.135,-0.871
2000-01-14,0.873,-0.729,-0.262,0.064,1.973
"""
        data = pd.read_csv(io.StringIO(csv_data), index_col=0, parse_dates=True)
        data = data.resample("B").sum()
        return data
