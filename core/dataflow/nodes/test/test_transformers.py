import logging

import numpy as np
import pandas as pd

import core.artificial_signal_generators as casgen
import core.config_builders as ccbuild
import helpers.unit_test as hut
import core.dataflow.nodes.transformers as cdnt
import core.dataflow.nodes.test.helpers as cdnth

_LOG = logging.getLogger(__name__)


class TestSeriesToSeriesTransformer(hut.TestCase):
    def test1(self) -> None:
        """
        Test `fit()` call.
        """
        data = self._get_data()
        config = ccbuild.get_config_from_nested_dict(
            {
                "in_col_group": ("close",),
                "out_col_group": ("ret_0",),
                "transformer_func": lambda x: x.pct_change(),
            }
        )
        node = cdnt.SeriesToSeriesTransformer("sklearn", **config.to_dict())
        df_out = node.fit(data)["df_out"]
        df_str = hut.convert_df_to_string(df_out.round(3), index=True, decimals=3)
        self.check_string(df_str)

    def test2(self) -> None:
        """
        Test `predict()` call.
        """
        data = self._get_data()
        config = ccbuild.get_config_from_nested_dict(
            {
                "in_col_group": ("close",),
                "out_col_group": ("ret_0",),
                "transformer_func": lambda x: x.pct_change(),
            }
        )
        node = cdnt.SeriesToSeriesTransformer("sklearn", **config.to_dict())
        expected, actual = cdnth.get_fit_predict_outputs(data, node)
        self.assert_equal(actual, expected)

    def _get_data(self) -> pd.DataFrame:
        """
        Generate multivariate normal returns.
        """
        mn_process = casgen.MultivariateNormalProcess()
        mn_process.set_cov_from_inv_wishart_draw(dim=4, seed=342)
        realization = mn_process.generate_sample(
            {"start": "2000-01-01", "periods": 40, "freq": "B"}, seed=134
        )
        realization = realization.rename(columns=lambda x: "MN" + str(x))
        realization = np.exp(0.1 * realization.cumsum())
        volume = pd.DataFrame(
            index=realization.index, columns=realization.columns, data=100
        )
        data = pd.concat([realization, volume], axis=1, keys=["close", "volume"])
        return data


class TestTwapVwapComputer(hut.TestCase):
    def test1(self) -> None:
        """

        :return:
        """
        data = self._get_data()
        config = ccbuild.get_config_from_nested_dict(
            {
                "rule": "5T",
                "price_col": "close",
                "volume_col": "volume",
            }
        )
        node = cdnt.TwapVwapComputer("twapvwap", **config.to_dict())
        df_out = node.fit(data)["df_out"]
        df_str = hut.convert_df_to_string(df_out.round(3), index=True, decimals=3)
        self.check_string(df_str)


    def _get_data(self) -> pd.DataFrame:
        """

        :return:
        """
        date_range_kwargs = {
            "start": "2001-01-04 09:30:00",
            "end": "2001-01-04 10:00:00",
            "freq": "T",
        }
        ar_params = [0.5]
        arma_process = casgen.ArmaProcess(ar_params, [])
        rets = arma_process.generate_sample(
            date_range_kwargs=date_range_kwargs,
            scale=1,
            burnin=0,
            seed=100,
        )
        prices = np.exp(0.25 * rets.cumsum())
        prices.name = "close"
        poisson_process = casgen.PoissonProcess(mu=100)
        volume = poisson_process.generate_sample(
            date_range_kwargs=date_range_kwargs,
            seed=100,
        )
        volume.name ="volume"
        df = pd.concat([prices, volume], axis=1)
        return df