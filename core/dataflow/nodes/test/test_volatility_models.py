import collections
import logging
from typing import Any, Dict, List

import numpy as np
import pandas as pd

import core.artificial_signal_generators as sig_gen
import core.artificial_signal_generators as casgen
import core.config as ccfg
import core.config_builders as ccbuild
import core.finance as fin
import core.signal_processing as sigp
import core.signal_processing as csproc
import helpers.dbg as dbg
import helpers.printing as prnt
import helpers.printing as hprint
import helpers.unit_test as hut
from core.dataflow.nodes.volatility_models import (
    SmaModel,
    VolatilityModel,
    VolatilityModulator,
    VolatilityNormalizer,
)

_LOG = logging.getLogger(__name__)


class TestSmaModel(hut.TestCase):
    def test1(self) -> None:
        # Load test data.
        data = self._get_data()
        config = ccbuild.get_config_from_nested_dict(
            {
                "col": ["vol_sq"],
                "steps_ahead": 2,
                "nan_mode": "drop",
            }
        )
        # Specify config and create modeling node.
        node = SmaModel("sma", **config.to_dict())
        # Run `fit()` and get output dataframe.
        df_out = node.fit(data)["df_out"]
        info = node.get_info("fit")
        # Package results.
        self._check_results(config, info, df_out)

    def test2(self) -> None:
        """
        Specify `tau` parameter.
        """
        data = self._get_data()
        config = ccbuild.get_config_from_nested_dict(
            {
                "col": ["vol_sq"],
                "steps_ahead": 2,
                "tau": 8,
                "nan_mode": "drop",
            }
        )
        node = SmaModel("sma", **config.to_dict())
        df_out = node.fit(data)["df_out"]
        info = node.get_info("fit")
        self._check_results(config, info, df_out)

    def test3(self) -> None:
        """
        Specify `col_mode=='merge_all'`.
        """
        data = self._get_data()
        config = ccbuild.get_config_from_nested_dict(
            {
                "col": ["vol_sq"],
                "steps_ahead": 2,
                "col_mode": "merge_all",
                "nan_mode": "drop",
            }
        )
        node = SmaModel("sma", **config.to_dict())
        df_out = node.fit(data)["df_out"]
        info = node.get_info("fit")
        self._check_results(config, info, df_out)

    def test4(self) -> None:
        """
        Run `predict()` after `fit()`.
        """
        data = self._get_data()
        config = ccbuild.get_config_from_nested_dict(
            {
                "col": ["vol_sq"],
                "steps_ahead": 2,
                "nan_mode": "drop",
            }
        )
        node = SmaModel("sma", **config.to_dict())
        # Run `fit()`, then `predict()`.
        node.fit(data.loc["2000-01-01":"2000-02-10"])
        df_out = node.predict(data.loc["2000-01-20":"2000-02-23"])["df_out"]
        # Extract info for both `fit()` and `predict()` stages.
        info = collections.OrderedDict()
        info["fit"] = node.get_info("fit")
        info["predict"] = node.get_info("predict")
        # Package results.
        self._check_results(config, info, df_out)

    def _check_results(
        self,
        config: ccfg.Config,
        info: collections.OrderedDict,
        df_out: pd.DataFrame,
    ) -> None:
        """
        Convert inputs to a string and check against golden reference.
        """
        act: List[str] = []
        act.append(hprint.frame("config"))
        act.append(str(config))
        act.append(str(ccbuild.get_config_from_nested_dict(info)))
        act.append(hprint.frame("df_out"))
        act.append(hut.convert_df_to_string(df_out, index=True, decimals=2))
        act = "\n".join(act)
        self.check_string(act)

    @staticmethod
    def _get_data() -> pd.DataFrame:
        """
        Generate "random returns".
        """
        arma_process = casgen.ArmaProcess([0.45], [0])
        date_range_kwargs = {"start": "2000-01-01", "periods": 40, "freq": "B"}
        date_range = pd.date_range(**date_range_kwargs)
        realization = arma_process.generate_sample(
            date_range_kwargs=date_range_kwargs, seed=0
        )
        vol_sq = np.abs(realization) ** 2
        vol_sq.name = "vol_sq"
        df = pd.DataFrame(index=date_range, data=vol_sq)
        return df


class TestVolatilityModel(hut.TestCase):
    def test_fit_dag1(self) -> None:
        """
        Perform a typical `fit()` call.
        """
        # Load test data.
        data = self._get_data()
        config = ccbuild.get_config_from_nested_dict(
            {
                "cols": ["ret_0"],
                "steps_ahead": 2,
                "nan_mode": "leave_unchanged",
            }
        )
        node = VolatilityModel("vol_model", **config.to_dict())
        df_out = node.fit(data)["df_out"]
        info = node.get_info("fit")
        # Package results.
        act = self._package_results1(config, info, df_out)
        self.check_string(act)

    def test_fit_dag_correctness1(self) -> None:
        """
        Check that the volatility adjustment can be inverted.
        """
        data = self._get_data()
        config = ccbuild.get_config_from_nested_dict(
            {
                "cols": ["ret_0"],
                "steps_ahead": 2,
                "nan_mode": "leave_unchanged",
            }
        )
        node = VolatilityModel("vol_model", **config.to_dict())
        vol_adj_df = node.fit(data)["df_out"]
        # Invert volatility adjustment.
        ret_0_vol_0_hat = vol_adj_df["ret_0_vol_2_hat"].shift(2)
        inverted_rets = (ret_0_vol_0_hat * vol_adj_df["ret_0_zscored"]).rename(
            "ret_0_inverted"
        )
        # Compare results.
        df_out = vol_adj_df.join(inverted_rets)
        info = node.get_info("fit")
        # Package results.
        act = self._package_results1(config, info, df_out)
        self.check_string(act)

    def test_predict_dag1(self) -> None:
        """
        Perform a typical `predict()` call.
        """
        # Load test data.
        data = self._get_data()
        # Specify config and create modeling node.
        config = ccbuild.get_config_from_nested_dict(
            {
                "cols": ["ret_0"],
                "steps_ahead": 2,
                "nan_mode": "leave_unchanged",
            }
        )
        node = VolatilityModel("vol_model", **config.to_dict())
        node.fit(data.loc["2000-01-01":"2000-02-10"])
        # TODO(*): Update the `predict()` interval.
        df_out = node.predict(data.loc["2000-01-20":"2000-02-23"])["df_out"]
        # TODO(*): Propagate `fit()` and `predict()` info.
        info = node.get_info("fit")
        # Package results.
        act = self._package_results1(config, info, df_out)
        self.check_string(act)

    def test_predict_dag_correctness1(self) -> None:
        """
        Check that the `predict()` volatility adjustment can be inverted.
        """
        data = self._get_data()
        config = ccbuild.get_config_from_nested_dict(
            {
                "cols": ["ret_0"],
                "steps_ahead": 2,
                "nan_mode": "leave_unchanged",
            }
        )
        node = VolatilityModel("vol_model", **config.to_dict())
        node.fit(data.loc["2000-01-01":"2000-02-10"])
        vol_adj_df = node.predict(data.loc["2000-01-20":"2000-02-23"])["df_out"]
        # Invert volatility adjustment.
        ret_0_vol_0_hat = vol_adj_df["ret_0_vol_2_hat"].shift(2)
        inverted_rets = (ret_0_vol_0_hat * vol_adj_df["ret_0_zscored"]).rename(
            "ret_0_inverted"
        )
        # Compare results.
        df_out = vol_adj_df.join(inverted_rets)
        # TODO(*): Propagate `fit()` and `predict()` info.
        info = node.get_info("fit")
        # Package results.
        act = self._package_results1(config, info, df_out)
        self.check_string(act)

    def test_col_mode1(self) -> None:
        """
        Use "replace_all" column mode.
        """
        data = self._get_data()
        config = ccbuild.get_config_from_nested_dict(
            {
                "cols": ["ret_0"],
                "steps_ahead": 2,
                "col_mode": "replace_all",
                "nan_mode": "leave_unchanged",
            }
        )
        node = VolatilityModel("vol_model", **config.to_dict())
        #
        df_out = node.fit(data)["df_out"]
        info = node.get_info("fit")
        #
        act = self._package_results1(config, info, df_out)
        self.check_string(act)

    def test_col_mode2(self) -> None:
        """
        Use "replace_selected" column mode.
        """
        data = self._get_data()
        config = ccbuild.get_config_from_nested_dict(
            {
                "cols": ["ret_0"],
                "steps_ahead": 2,
                "col_mode": "replace_selected",
                "nan_mode": "leave_unchanged",
            }
        )
        node = VolatilityModel("vol_model", **config.to_dict())
        #
        df_out = node.fit(data)["df_out"]
        info = node.get_info("fit")
        #
        act = self._package_results1(config, info, df_out)
        self.check_string(act)

    def test_fit_multiple_columns(self) -> None:
        """
        Model volatility for multiple columns (independently).
        """
        # Load test data.
        data = self._get_data()
        # TODO(*): Rename this column
        data["ret_0_2"] = data.ret_0 + np.random.normal(size=len(data))
        # Specify config and create modeling node.
        config = ccbuild.get_config_from_nested_dict(
            {
                "cols": ["ret_0", "ret_0_2"],
                "steps_ahead": 2,
                "nan_mode": "leave_unchanged",
            }
        )
        node = VolatilityModel("vol_model", **config.to_dict())
        #
        df_out = node.fit(data)["df_out"]
        info = node.get_info("fit")
        # Package results.
        act = self._package_results1(config, info, df_out)
        self.check_string(act)

    def test_multiple_columns_with_specified_tau(self) -> None:
        """
        Ensure that explicit `tau` is used post-`fit()`.
        """
        # Load test data.
        data = self._get_data()
        data["ret_0_2"] = data.ret_0 + np.random.normal(size=len(data))
        # Specify config.
        config = ccbuild.get_config_from_nested_dict(
            {
                "cols": ["ret_0", "ret_0_2"],
                "steps_ahead": 2,
                "nan_mode": "drop",
                "tau": 10,
            }
        )
        # Check if specified tau is used for all columns via learned taus property.
        node = VolatilityModel("vol_model", **config.to_dict())
        node.fit(data)
        dbg.dassert_set_eq(node.taus.values(), [10])

    def test_fit_none_columns(self) -> None:
        """
        Ensure equivalence of explicit and implicit column specification.
        """
        # Load test data.
        data = self._get_data()
        data["ret_0_2"] = data.ret_0 + np.random.normal(size=len(data))
        # Specify config with columns implicit.
        config1 = ccbuild.get_config_from_nested_dict(
            {
                "steps_ahead": 2,
                "nan_mode": "leave_unchanged",
            }
        )
        node1 = VolatilityModel("vol_model", **config1.to_dict())
        df_out1 = node1.fit(data)["df_out"]
        # Specify config with explicit column names.
        config2 = ccbuild.get_config_from_nested_dict(
            {
                "cols": ["ret_0", "ret_0_2"],
                "steps_ahead": 2,
                "nan_mode": "leave_unchanged",
            }
        )
        node2 = VolatilityModel("vol_model", **config2.to_dict())
        df_out2 = node2.fit(data)["df_out"]
        # TODO(*): Improve the string conversion.
        np.testing.assert_equal(
            df_out1.to_string(),
            df_out2.to_string(),
        )

    def test_fit_int_columns(self) -> None:
        """
        Ensure that `int` columns are supported.
        """
        # Load test data.
        data = self._get_data()
        data[10] = data.ret_0 + np.random.normal(size=len(data))
        # Specify config.
        config = ccbuild.get_config_from_nested_dict(
            {
                "cols": [10],
                "steps_ahead": 2,
                "nan_mode": "leave_unchanged",
            }
        )
        # Get output with integer column names.
        node = VolatilityModel("vol_model", **config.to_dict())
        df_out = node.fit(data)["df_out"]
        self.check_string(df_out.to_string())

    def test_get_fit_state1(self) -> None:
        data = self._get_data()
        config = ccfg.Config()
        config["cols"] = ["ret_0"]
        config["steps_ahead"] = 2
        config["nan_mode"] = "leave_unchanged"
        node = VolatilityModel("vol_model", **config.to_dict())
        df_out = node.fit(data)["df_out"]
        # Package results.
        state = node.get_fit_state()
        act = self._package_results2(config, state, df_out)
        self.check_string(act)

    def test_get_fit_state2(self) -> None:
        data = self._get_data()
        config = ccfg.Config()
        config["cols"] = ["ret_0"]
        config["steps_ahead"] = 2
        config["nan_mode"] = "leave_unchanged"
        state = {
            "_fit_cols": ["ret_0"],
            "_vol_cols": {"ret_0": "ret_0_vol"},
            "_fwd_vol_cols": {"ret_0": "ret_0_vol_2"},
            "_fwd_vol_cols_hat": {"ret_0": "ret_0_vol_2_hat"},
            "_taus": {"ret_0": 10},
            "_info['fit']": None,
        }
        node = VolatilityModel("vol_model", **config.to_dict())
        node.set_fit_state(state)
        df_out = node.predict(data)["df_out"]
        # Package results.
        act = self._package_results2(config, state, df_out)
        self.check_string(act)

    def test_predict_with_predefined_state(self) -> None:
        data = self._get_data()
        config = ccfg.Config()
        config["cols"] = ["ret_0"]
        config["steps_ahead"] = 2
        config["nan_mode"] = "leave_unchanged"
        node_fit = VolatilityModel("vol_model", **config.to_dict())
        node_fit.fit(data)
        output_fit = node_fit.predict(data)["df_out"]
        node_predefined = VolatilityModel("vol_model", **config.to_dict())
        node_predefined.set_fit_state(node_fit.get_fit_state())
        output_predefined = node_predefined.predict(data)["df_out"]
        pd.testing.assert_frame_equal(output_fit, output_predefined)

    @staticmethod
    def _package_results1(
        config: ccfg.Config,
        info: collections.OrderedDict,
        df_out: pd.DataFrame,
    ) -> str:
        act: List[str] = []
        act.append(hprint.frame("config"))
        act.append(str(config))
        act.append(hprint.frame("info"))
        act.append(str(ccbuild.get_config_from_nested_dict(info)))
        act.append(hprint.frame("df_out"))
        act.append(hut.convert_df_to_string(df_out, index=True))
        act = "\n".join(act)
        return act

    @staticmethod
    def _package_results2(
        config: ccfg.Config, state: Dict[str, Any], df_out: pd.DataFrame
    ) -> str:
        act: List[str] = []
        act.append(hprint.frame("config"))
        act.append(str(config))
        act.append(hprint.frame("state"))
        act.append(str(state))
        act.append(hprint.frame("df_out"))
        act.append(hut.convert_df_to_string(df_out, index=True))
        act = "\n".join(act)
        return act

    @staticmethod
    def _get_data() -> pd.DataFrame:
        """
        Generate "random returns".

        Use lag + noise as predictor.
        """
        arma_process = casgen.ArmaProcess([0.45], [0])
        date_range_kwargs = {"start": "2000-01-01", "periods": 40, "freq": "B"}
        date_range = pd.date_range(**date_range_kwargs)
        realization = arma_process.generate_sample(
            date_range_kwargs=date_range_kwargs, seed=0
        )
        realization.name = "ret_0"
        df = pd.DataFrame(index=date_range, data=realization)
        return df


class TestVolatilityModulator(hut.TestCase):
    def test_modulate1(self) -> None:
        steps_ahead = 2
        df_in = self._get_signal_and_fwd_vol(steps_ahead)
        # Get mock returns prediction 1 step ahead indexed by knowledge time.
        y_hat = csproc.compute_smooth_moving_average(df_in["ret_0"], 4).shift(-1)
        df_in["ret_1_hat"] = y_hat
        config = ccbuild.get_config_from_nested_dict(
            {
                "signal_cols": ["ret_1_hat"],
                "volatility_col": "vol_2_hat",
                "signal_steps_ahead": 1,
                "volatility_steps_ahead": 2,
                "mode": "modulate",
            }
        )
        node = VolatilityModulator("modulate", **config.to_dict())
        df_out = node.fit(df_in)["df_out"]
        # Check results.
        self._check_results(config, df_in, df_out)

    def test_demodulate1(self) -> None:
        steps_ahead = 2
        df_in = self._get_signal_and_fwd_vol(steps_ahead)
        config = ccbuild.get_config_from_nested_dict(
            {
                "signal_cols": ["ret_0"],
                "volatility_col": "vol_2_hat",
                "signal_steps_ahead": 0,
                "volatility_steps_ahead": 2,
                "mode": "demodulate",
            }
        )
        node = VolatilityModulator("demodulate", **config.to_dict())
        df_out = node.fit(df_in)["df_out"]
        # Check results.
        self._check_results(config, df_in, df_out)

    def test_col_mode1(self) -> None:
        steps_ahead = 2
        df_in = self._get_signal_and_fwd_vol(steps_ahead)
        config = ccbuild.get_config_from_nested_dict(
            {
                "signal_cols": ["ret_0"],
                "volatility_col": "vol_2_hat",
                "signal_steps_ahead": 0,
                "volatility_steps_ahead": 2,
                "mode": "demodulate",
                "col_rename_func": lambda x: f"{x}_zscored",
                "col_mode": "merge_all",
            }
        )
        node = VolatilityModulator("demodulate", **config.to_dict())
        df_out = node.fit(df_in)["df_out"]
        # Check results.
        self._check_results(config, df_in, df_out)

    def test_col_mode2(self) -> None:
        steps_ahead = 2
        df_in = self._get_signal_and_fwd_vol(steps_ahead)
        config = ccbuild.get_config_from_nested_dict(
            {
                "signal_cols": ["ret_0"],
                "volatility_col": "vol_2_hat",
                "signal_steps_ahead": 0,
                "volatility_steps_ahead": 2,
                "mode": "demodulate",
                "col_rename_func": lambda x: f"{x}_zscored",
                "col_mode": "replace_selected",
            }
        )
        node = VolatilityModulator("demodulate", **config.to_dict())
        df_out = node.fit(df_in)["df_out"]
        # Check results.
        self._check_results(config, df_in, df_out)

    def _check_results(
        self, config: ccfg.Config, df_in: pd.DataFrame, df_out: pd.DataFrame
    ) -> None:
        act: List[str] = []
        act.append(hprint.frame("config"))
        act.append(str(config))
        act = "\n".join(act)
        self.check_string(act)
        self.check_dataframe(df_in, tag="df_in", err_threshold=0.01)
        self.check_dataframe(df_out, tag="df_out", err_threshold=0.01)

    @staticmethod
    def _get_signal_and_fwd_vol(
        steps_ahead: int,
    ) -> pd.DataFrame:
        arma_process = casgen.ArmaProcess([0.45], [0])
        date_range_kwargs = {"start": "2010-01-01", "periods": 40, "freq": "B"}
        signal = arma_process.generate_sample(
            date_range_kwargs=date_range_kwargs, scale=0.1, seed=42
        )
        vol = csproc.compute_smooth_moving_average(signal, 16)
        fwd_vol = vol.shift(steps_ahead)
        return pd.concat(
            [signal.rename("ret_0"), fwd_vol.rename("vol_2_hat")], axis=1
        )


class TestVolatilityNormalizer(hut.TestCase):
    def test_fit1(self) -> None:
        y = TestVolatilityNormalizer._get_series(42).rename("ret_0")
        y_hat = sigp.compute_smooth_moving_average(y, 28).rename("ret_0_hat")
        df_in = pd.concat([y, y_hat], axis=1)
        #
        vn = VolatilityNormalizer("normalize_volatility", "ret_0_hat", 0.1)
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
        vn = VolatilityNormalizer(
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
        vn = VolatilityNormalizer("normalize_volatility", "ret_0_hat", 0.1)
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
