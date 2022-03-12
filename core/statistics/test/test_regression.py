import logging
import os

import numpy as np
import pandas as pd
import pytest

import core.artificial_signal_generators as carsigen
import core.statistics.regression as cstaregr
import helpers.hio as hio
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


class TestComputeRegressionCoefficients1(hunitest.TestCase):
    @pytest.mark.skip(reason="This test generates the input data")
    def test_generate_input_data(self) -> None:
        """
        Uncomment the skip tag and run this test to update the input data.
        """
        file_name = self._get_test_data_file_name()
        # Generate the data.
        data = self._get_data()
        # Save the data to the proper location.
        hio.create_enclosing_dir(file_name, incremental=True)
        data.to_csv(file_name)
        _LOG.info("Generated file '%s'", file_name)
        # Read the data back and make sure it is the same.
        data2 = self._get_data_from_disk()
        if data.equals(data2):
            print(data.compare(data2))
            raise ValueError("Dfs are different")

        def _compute_df_signature(df: pd.DataFrame) -> str:
            txt = []
            txt.append("df=\n%s" % str(df))
            txt.append("df.dtypes=\n%s" % str(df.dtypes))
            txt.append("df.index.freq=\n%s" % str(df.index.freq))
            return "\n".join(txt)

        self.assert_equal(
            _compute_df_signature(data), _compute_df_signature(data2)
        )

    @pytest.mark.skip(
        reason="This test fails on some computers due to AmpTask1649"
    )
    def test0(self) -> None:
        """
        Check that the randomly generated data is reproducible.
        """
        _LOG.debug("Current seed=%s", np.random.get_state()[1][0])
        _LOG.debug("Generating data")
        data = self._get_data()
        _LOG.debug("Current seed=%s", np.random.get_state()[1][0])
        _LOG.debug("data=\n%s", str(data))
        _LOG.debug("Checking against golden")
        df_str = hunitest.convert_df_to_string(data, index=True, decimals=3)
        self.check_string(df_str)

    def test1(self) -> None:
        # Load test data.
        df = self._get_data_from_disk()
        actual = cstaregr.compute_regression_coefficients(
            df,
            x_cols=list(range(0, 9)),
            y_col=9,
        )
        actual_string = hunitest.convert_df_to_string(
            actual.round(3), index=True, decimals=3
        )
        self.check_string(actual_string, fuzzy_match=True)

    def _get_test_data_file_name(self) -> str:
        """
        Return the name of the file containing the data for testing this class.
        """
        dir_name = self.get_input_dir(use_only_test_class=True)
        file_name = os.path.join(dir_name, "data.csv")
        _LOG.debug("file_name=%s", file_name)
        return file_name

    def _get_data_from_disk(self) -> pd.DataFrame:
        """
        Read the data generated by `test_generate_input_data()`.
        """
        file_name = self._get_test_data_file_name()
        df = pd.read_csv(file_name, index_col=0, parse_dates=True)
        df = df.asfreq("B")
        df.columns = df.columns.astype(int)
        return df

    def _get_data(self) -> pd.DataFrame:
        """
        Generate multivariate normal returns.
        """
        mvnp = carsigen.MultivariateNormalProcess()
        mvnp.set_cov_from_inv_wishart_draw(dim=10, seed=1)
        df = mvnp.generate_sample(
            date_range_kwargs={"start": "2010-01-04", "periods": 40, "freq": "B"},
            seed=20,
        )
        return df


class TestComputeRegressionCoefficients2(hunitest.TestCase):
    @pytest.mark.skip(reason="This test generates the input data")
    def test_generate_input_data(self) -> None:
        """
        Uncomment the skip tag and run this test to update the input data.
        """
        file_name = self._get_test_data_file_name()
        # Generate the data.
        data = self._get_data()
        # Save the data to the proper location.
        hio.create_enclosing_dir(file_name, incremental=True)
        data.to_csv(file_name)
        _LOG.info("Generated file '%s'", file_name)
        # Read the data back and make sure it is the same.
        data2 = self._get_data_from_disk()
        if data.equals(data2):
            print(data.compare(data2))
            raise ValueError("Dfs are different")

        def _compute_df_signature(df: pd.DataFrame) -> str:
            txt = []
            txt.append("df=\n%s" % str(df))
            txt.append("df.dtypes=\n%s" % str(df.dtypes))
            txt.append("df.index.freq=\n%s" % str(df.index.freq))
            return "\n".join(txt)

        self.assert_equal(
            _compute_df_signature(data), _compute_df_signature(data2)
        )

    @pytest.mark.skip(
        reason="This test fails on some computers due to AmpTask1649"
    )
    def test0(self) -> None:
        """
        Check that the randomly generated data is reproducible.
        """
        _LOG.debug("Current seed=%s", np.random.get_state()[1][0])
        _LOG.debug("Generating data")
        data = self._get_data()
        _LOG.debug("Current seed=%s", np.random.get_state()[1][0])
        _LOG.debug("data=\n%s", str(data))
        _LOG.debug("Checking against golden")
        df_str = hunitest.convert_df_to_string(data, index=True, decimals=3)
        self.check_string(df_str)

    def test1(self) -> None:
        # Load test data.
        df = self._get_data_from_disk()
        actual = cstaregr.compute_regression_coefficients(
            df,
            x_cols=list(range(1, 5)),
            y_col=0,
        )
        actual_string = hunitest.convert_df_to_string(
            actual.round(3), index=True, decimals=3
        )
        self.check_string(actual_string, fuzzy_match=True)

    def test2(self) -> None:
        """
        Ensure that uniform weights give the same result as no weights.
        """
        df_unweighted = self._get_data_from_disk()
        unweighted_actual = cstaregr.compute_regression_coefficients(
            df_unweighted,
            x_cols=list(range(1, 5)),
            y_col=0,
        )
        df_uniform_weights = self._get_data_from_disk()
        df_uniform_weights["weight"] = 1
        weighted_actual = cstaregr.compute_regression_coefficients(
            df_uniform_weights,
            x_cols=list(range(1, 5)),
            y_col=0,
            sample_weight_col="weight",
        )
        hunitest.compare_df(unweighted_actual, weighted_actual)

    def test3(self) -> None:
        """
        Ensure that rescaling weights does not affect the result.
        """
        df_weights1 = self._get_data_from_disk()
        weights = pd.Series(
            index=df_weights1.index, data=list(range(1, df_weights1.shape[0] + 1))
        )
        df_weights1["weight"] = weights
        weights1_actual = cstaregr.compute_regression_coefficients(
            df_weights1,
            x_cols=list(range(1, 5)),
            y_col=0,
            sample_weight_col="weight",
        )
        df_weights2 = self._get_data_from_disk()
        # Multiply the weights by a constant factor.
        df_weights2["weight"] = 7.6 * weights
        weights2_actual = cstaregr.compute_regression_coefficients(
            df_weights2,
            x_cols=list(range(1, 5)),
            y_col=0,
            sample_weight_col="weight",
        )
        # This fails, though the values agree to at least six decimal places.
        # The failure appears to be due to floating point error.
        # hunitest.compare_df(weights1_actual, weights2_actual)
        np.testing.assert_allclose(
            weights1_actual, weights2_actual, equal_nan=True
        )

    def _get_test_data_file_name(self) -> str:
        """
        Return the name of the file containing the data for testing this class.
        """
        dir_name = self.get_input_dir(use_only_test_class=True)
        file_name = os.path.join(dir_name, "data.csv")
        _LOG.debug("file_name=%s", file_name)
        return file_name

    def _get_data_from_disk(self) -> pd.DataFrame:
        """
        Read the data generated by `test_generate_input_data()`.
        """
        file_name = self._get_test_data_file_name()
        # Since pickle is not portable, we use CSV to serialize the data.
        # Unfortunately CSV is a lousy serialization format and loses metadata so
        # we need to patch it up to make it look exactly the original one.
        df = pd.read_csv(file_name, index_col=0, parse_dates=True)
        df = df.asfreq("B")
        df.columns = df.columns.astype(int)
        return df

    def _get_data(self) -> pd.DataFrame:
        """
        Generate multivariate normal returns.
        """
        mvnp = carsigen.MultivariateNormalProcess()
        mvnp.set_cov_from_inv_wishart_draw(dim=10, seed=1)
        df = mvnp.generate_sample(
            date_range_kwargs={"start": "2010-01-04", "periods": 40, "freq": "B"},
            seed=20,
        )
        df.columns = df.columns.astype(int)
        return df