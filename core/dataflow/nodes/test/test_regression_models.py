import logging
import os

import numpy as np
import pandas as pd
import pytest

import core.artificial_signal_generators as casgen
import core.config as cconfig
import core.dataflow.nodes.regression_models as cdnrm
import helpers.io_ as hio
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestLinearRegression(hut.TestCase):
    @pytest.mark.skip(reason="This test generates the input data")
    def test_generate_input_data(self) -> None:
        """
        Uncomment the skip tag and run this test to update the input data.
        """
        file_name = self._get_test_data_file_name()
        # Generate the data.
        data = self._get_data(seed=1)
        # Save the data to the proper location.
        hio.create_enclosing_dir(file_name, incremental=True)
        data.to_csv(file_name)
        _LOG.info("Generated file '%s'", file_name)
        # Read the data back and make sure it is the same.
        data2 = self._get_frozen_input()
        #
        hut.compare_df(data, data2)

    @pytest.mark.skip(
        reason="This test fails on some computers due to AmpTask1649"
    )
    def test0(self) -> None:
        """
        Check that the randomly generated data is the same as our development
        computers.
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
        """
        Test `fit()`.
        """
        # Load test data.
        data = self._get_frozen_input()
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
        """
        Test `fit()` with nonzero smoothing.
        """
        # Load test data.
        data = self._get_frozen_input()
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
        """
        Test `predict()` after `fit()`.
        """
        # Load test data.
        data = self._get_frozen_input()
        data_fit = data.loc[:"2000-01-10"]  # type: error[misc]
        data_predict = data.loc["2000-01-10":]  # type: error[misc]
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

    def test4(self) -> None:
        """
        Test `fit()` with weights.
        """
        # Load test data.
        data = self._get_frozen_input()
        data["weight"] = pd.Series(
            index=data.index, data=range(1, data.shape[0] + 1)
        )
        # Generate node config.
        config = cconfig.get_config_from_nested_dict(
            {
                "x_vars": ["x1", "x2", "x3", "x4"],
                "y_vars": ["y"],
                "sample_weight_col": "weight",
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

    def test5(self) -> None:
        """
        Test `predict()` after `fit()`, both with weights.
        """
        # Load test data.
        data = self._get_frozen_input()
        data["weight"] = pd.Series(
            index=data.index, data=range(1, data.shape[0] + 1)
        )
        data_fit = data.loc[:"2000-01-10"]  # type: error[misc]
        data_predict = data.loc["2000-01-10":]  # type: error[misc]
        # Generate node config.
        config = cconfig.get_config_from_nested_dict(
            {
                "x_vars": ["x1", "x2", "x3", "x4"],
                "y_vars": ["y"],
                "sample_weight_col": "weight",
                "steps_ahead": 1,
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

    def _get_test_data_file_name(self) -> str:
        """
        Return the name of the file containing the data for testing this class.
        """
        dir_name = self.get_input_dir(use_only_test_class=True)
        file_name = os.path.join(dir_name, "data.csv")
        _LOG.debug("file_name=%s", file_name)
        return file_name

    def _get_frozen_input(self) -> pd.DataFrame:
        """
        Read the data generated by `test_generate_input_data()` through
        `_get_data()`.
        """
        file_name = self._get_test_data_file_name()
        # Since pickle is not portable, we use CSV to serialize the data.
        # Unfortunately CSV is a lousy serialization format and loses metadata so
        # we need to patch it up to make it look exactly the original one.
        df = pd.read_csv(file_name, index_col=0, parse_dates=True)
        df = df.asfreq("B")
        return df

    def _get_data(self, seed: int) -> pd.DataFrame:
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
            {"start": "2000-01-01", "periods": 10, "freq": "B"}, seed=seed
        )
        data.columns = ["y", "x1", "x2", "x3", "x4"]
        return data
