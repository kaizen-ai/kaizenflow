import logging

import pandas as pd

import core.artificial_signal_generators as sig_gen
import core.dataflow as dtf
import core.finance as fin
import core.signal_processing as sigp
import helpers.printing as prnt
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


# #############################################################################
# Results processing
# #############################################################################


class TestVolatilityNormalizer(hut.TestCase):
    def test_fit1(self) -> None:
        y = TestVolatilityNormalizer._get_series(42).rename("ret_0")
        y_hat = sigp.compute_smooth_moving_average(y, 28).rename("ret_0_hat")
        df_in = pd.concat([y, y_hat], axis=1)
        #
        vn = dtf.VolatilityNormalizer("normalize_volatility", "ret_0_hat", 0.1)
        df_out = vn.fit(df_in)["df_out"]
        #
        volatility = 100 * df_out.apply(fin.compute_annualized_volatility)
        output_str = (
            f"{prnt.frame('df_out')}\n"
            f"{hut.convert_df_to_string(df_out, index=True)}\n"
            f"{prnt.frame('df_out annualized volatility')}\n"
            f"{volatility}"
        )
        self.check_string(output_str)

    def test_fit2(self) -> None:
        """
        Test with `col_mode`="replace_all".
        """
        y = TestVolatilityNormalizer._get_series(42).rename("ret_0")
        y_hat = sigp.compute_smooth_moving_average(y, 28).rename("ret_0_hat")
        df_in = pd.concat([y, y_hat], axis=1)
        #
        vn = dtf.VolatilityNormalizer(
            "normalize_volatility",
            "ret_0_hat",
            0.1,
            col_mode="replace_all",
        )
        df_out = vn.fit(df_in)["df_out"]
        #
        volatility = 100 * df_out.apply(fin.compute_annualized_volatility)
        output_str = (
            f"{prnt.frame('df_in')}\n"
            f"{hut.convert_df_to_string(df_in, index=True)}\n"
            f"{prnt.frame('df_out')}\n"
            f"{hut.convert_df_to_string(df_out, index=True)}\n"
            f"{prnt.frame('df_out annualized volatility')}\n"
            f"{volatility}"
        )
        self.check_string(output_str)

    def test_predict1(self) -> None:
        y = TestVolatilityNormalizer._get_series(42).rename("ret_0")
        y_hat = sigp.compute_smooth_moving_average(y, 28).rename("ret_0_hat")
        fit_df_in = pd.concat([y, y_hat], axis=1)
        predict_df_in = (
            TestVolatilityNormalizer._get_series(0).rename("ret_0_hat").to_frame()
        )
        predict_df_in = sigp.compute_smooth_moving_average(predict_df_in, 18)
        # Fit normalizer.
        vn = dtf.VolatilityNormalizer("normalize_volatility", "ret_0_hat", 0.1)
        fit_df_out = vn.fit(fit_df_in)["df_out"]
        # Predict.
        predict_df_out = vn.predict(predict_df_in)["df_out"]
        #
        fit_df_out_volatility = 100 * fit_df_out.apply(
            fin.compute_annualized_volatility
        )
        predict_df_out_volatility = 100 * predict_df_out.apply(
            fin.compute_annualized_volatility
        )
        output_str = (
            # Fit outputs.
            f"{prnt.frame('fit_df_out')}\n"
            f"{hut.convert_df_to_string(fit_df_out, index=True)}\n"
            f"{prnt.frame('fit_df_out annualized volatility')}\n"
            f"{fit_df_out_volatility}"
            # Predict outputs.
            f"{prnt.frame('predict_df_out')}\n"
            f"{hut.convert_df_to_string(predict_df_out, index=True)}\n"
            f"{prnt.frame('predict_df_out annualized volatility')}\n"
            f"{predict_df_out_volatility}"
        )
        self.check_string(output_str)

    @staticmethod
    def _get_series(seed: int, periods: int = 44) -> pd.Series:
        arma_process = sig_gen.ArmaProcess([0], [0])
        date_range = {"start": "2010-01-01", "periods": periods, "freq": "B"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, scale=0.1, seed=seed
        )
        return series


class Test_get_df_info_as_string(hut.TestCase):
    def test1(self):
        df = pd.DataFrame({"col_1": [1, 2], "col_2": [3, 4]})
        info = dtf.get_df_info_as_string(df, exclude_memory_usage=False)
        self.check_string(info)

    def test2(self):
        df = pd.DataFrame({"col_1": [1, 2], "col_2": [3, 4]})
        info = dtf.get_df_info_as_string(df)
        self.check_string(info)
