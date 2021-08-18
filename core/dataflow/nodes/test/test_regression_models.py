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
                "x_vars": [1, 2, 3, 4],
                "y_vars": [0],
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
                "x_vars": [1, 2, 3, 4],
                "y_vars": [0],
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
        data_fit = data.loc[:"2000-01-31"]
        data_predict = data.loc["2000-01-31":]
        # Generate node config.
        config = cconfig.get_config_from_nested_dict(
            {
                "x_vars": [1, 2, 3, 4],
                "y_vars": [0],
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
        cov = pd.DataFrame(
            np.array(
                [
                    [1, 0.05, 0.3, -0.2, 0.1],
                    [0.05, 1, 0, 0, 0],
                    [0.3, 0, 1, 0, 0],
                    [-0.2, 0, 0, 1, 0],
                    [0.1, 0, 0, 0, 1],
                ],
            )
        )
        mn_process = casgen.MultivariateNormalProcess(cov=cov)
        data = mn_process.generate_sample(
            {"start": "2000-01-01", "periods": 40, "freq": "B"}, seed=seed
        )
        return data
