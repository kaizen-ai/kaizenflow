import logging

import pandas as pd

import core.artificial_signal_generators as sig_gen
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestArmaProcess(hut.TestCase):
    def test1(self) -> None:
        ar_params = [0.75, -0.25]
        ma_params = [0.65, 0.35]
        arma_process = sig_gen.ArmaProcess(ar_params, ma_params)
        realization = arma_process.generate_sample(
            {"start": "2000-01-01", "periods": 40, "freq": "B",},
            scale=1,
            burnin=10,
        )
        self.check_string(
            hut.convert_df_to_string(
                realization, title=realization.name, index=True
            )
        )

    def test2(self) -> None:
        ar_params = [0.5]
        ma_params = [-0.5]
        arma_process = sig_gen.ArmaProcess(ar_params, ma_params)
        realization = arma_process.generate_sample(
            {"start": "2000-01-01", "periods": 40, "freq": "B",},
            scale=1,
            burnin=5,
        )
        self.check_string(
            hut.convert_df_to_string(
                realization, title=realization.name, index=True
            )
        )

    def test3(self) -> None:
        ar_params = []
        ma_params = []
        arma_process = sig_gen.ArmaProcess(ar_params, ma_params)
        realization = arma_process.generate_sample(
            {"start": "2000-01-01", "periods": 40, "freq": "B",},
            scale=1,
            burnin=5,
        )
        self.check_string(
            hut.convert_df_to_string(
                realization, title=realization.name, index=True
            )
        )


class TestMultivariateNormalProcess(hut.TestCase):
    def test1(self) -> None:
        mn_process = sig_gen.MultivariateNormalProcess()
        mn_process.set_cov_from_inv_wishart_draw(dim=4, seed=0)
        realization = mn_process.generate_sample(
            {"start": "2000-01-01", "periods": 40, "freq": "B",}, seed=0
        )
        self.check_string(hut.convert_df_to_string(realization, index=True))

    def test2(self) -> None:
        mean = pd.Series([1, 2])
        cov = pd.DataFrame([[0.5, 0.2], [0.2, 0.3]])
        mn_process = sig_gen.MultivariateNormalProcess(mean=mean, cov=cov)
        realization = mn_process.generate_sample(
            {"start": "2000-01-01", "periods": 40, "freq": "B",}, seed=0
        )
        self.check_string(hut.convert_df_to_string(realization, index=True))


# TODO(*): Disabled because of PartTask186.
# import gluonts
# import gluonts.dataset.artificial.recipe as gdar  # isort: skip # noqa: F401 # pylint: disable=unused-import
# import mxnet
# import pandas as pd
#
#
#
#
# class TestGenerateRecipeDataset(hut.TestCase):
#    def test1(self) -> None:
#        mxnet.random.seed(0, ctx="all")
#        recipe = [
#            (
#                "feat_dynamic_real",
#                gluonts.dataset.artificial.recipe.SmoothSeasonality(
#                    period=288, phase=-72
#                ),
#            ),
#            (
#                "noise",
#                gluonts.dataset.artificial.recipe.RandomGaussian(
#                    shape=[0], stddev=0.1
#                ),
#            ),
#            (
#                "target",
#                gluonts.dataset.artificial.recipe.Add(
#                    inputs=["feat_dynamic_real", "noise"]
#                ),
#            ),
#            (
#                "feat_dynamic_real",
#                gluonts.dataset.artificial.recipe.RandomGaussian(shape=(1, 0)),
#            ),
#            # What GluonTS does is find zeros in shape and replace them
#            # with length.
#        ]
#        train_dataset = sig_gen.generate_recipe_dataset(
#            recipe, "D", pd.Timestamp("2010-01-01"), 100, 10, 1
#        )
#        train_str = str(list(train_dataset.train))
#        test_str = str(list(train_dataset.test))
#        self.check_string(
#            f"{prnt.frame('train')}{train_str}\n"
#            f"{prnt.frame('test')}{test_str}"
#        )
