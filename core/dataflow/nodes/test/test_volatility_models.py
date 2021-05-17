import logging

import pandas as pd

import core.artificial_signal_generators as sig_gen
import core.finance as fin
import core.signal_processing as sigp
import helpers.printing as prnt
import helpers.unit_test as hut
from core.dataflow.nodes.volatility_models import VolatilityNormalizer

import collections
import logging
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import sklearn.decomposition as sdecom
import sklearn.linear_model as slmode

import core.artificial_signal_generators as casgen
import core.config as ccfg
import core.config_builders as ccbuild
import core.dataflow as cdataf
import core.signal_processing as csproc
import helpers.dbg as dbg
import helpers.printing as hprint
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestSmaModel(hut.TestCase):
    def test_fit_dag1(self) -> None:
        # Load test data.
        data = self._get_data()
        data_source_node = cdataf.ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Specify config and create modeling node.
        config = ccfg.Config()
        config["col"] = ["vol"]
        config["steps_ahead"] = 2
        config["nan_mode"] = "drop"
        node = cdataf.SmaModel("sma", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "sma")
        #
        df_out = dag.run_leq_node("sma", "fit")["df_out"]
        info = cdataf.extract_info(dag, ["fit"])
        # Package results.
        self._check_results(config, info, df_out)

    def test_fit_dag2(self) -> None:
        """
        Specify `tau` parameter.
        """
        # Load test data.
        data = self._get_data()
        data_source_node = cdataf.ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Specify config and create modeling node.
        config = ccfg.Config()
        config["col"] = ["vol"]
        config["steps_ahead"] = 2
        config["tau"] = 8
        config["nan_mode"] = "drop"
        node = cdataf.SmaModel("sma", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "sma")
        #
        df_out = dag.run_leq_node("sma", "fit")["df_out"]
        info = cdataf.extract_info(dag, ["fit"])
        # Package results.
        self._check_results(config, info, df_out)

    def test_fit_dag3(self) -> None:
        """
        Specify `col_mode=='merge_all'`.
        """
        # Load test data.
        data = self._get_data()
        data_source_node = cdataf.ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Specify config and create modeling node.
        config = ccfg.Config()
        config["col"] = ["vol"]
        config["steps_ahead"] = 2
        config["col_mode"] = "merge_all"
        config["nan_mode"] = "drop"
        node = cdataf.SmaModel("sma", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "sma")
        #
        df_out = dag.run_leq_node("sma", "fit")["df_out"]
        info = cdataf.extract_info(dag, ["fit"])
        # Package results.
        self._check_results(config, info, df_out)

    def test_predict_dag1(self) -> None:
        # Load test data.
        data = self._get_data()
        fit_interval = ("2000-01-01", "2000-02-10")
        predict_interval = ("2000-01-20", "2000-02-23")
        data_source_node = cdataf.ReadDataFromDf("data", data)
        data_source_node.set_fit_intervals([fit_interval])
        data_source_node.set_predict_intervals([predict_interval])
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Specify config and create modeling node.
        config = ccfg.Config()
        config["col"] = ["vol"]
        config["steps_ahead"] = 2
        config["nan_mode"] = "drop"
        node = cdataf.SmaModel("sma", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "sma")
        #
        dag.run_leq_node("sma", "fit")
        df_out = dag.run_leq_node("sma", "predict")["df_out"]
        info = collections.OrderedDict()
        info["fit"] = cdataf.extract_info(dag, ["fit"])
        info["predict"] = cdataf.extract_info(dag, ["predict"])
        # Package results.
        self._check_results(config, info, df_out)

    def _check_results(
            self,
            config: ccfg.Config,
            info: collections.OrderedDict,
            df_out: pd.DataFrame,
    ) -> None:
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
        vol = np.abs(realization) ** 2
        vol.name = "vol"
        df = pd.DataFrame(index=date_range, data=vol)
        return df


class TestVolatilityModel(hut.TestCase):
    def test_fit_dag1(self) -> None:
        # Load test data.
        data = self._get_data()
        data_source_node = cdataf.ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Specify config and create modeling node.
        config = ccfg.Config()
        config["cols"] = ["ret_0"]
        config["steps_ahead"] = 2
        config["nan_mode"] = "leave_unchanged"
        node = cdataf.VolatilityModel("vol_model", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "vol_model")
        #
        df_out = dag.run_leq_node("vol_model", "fit")["df_out"]
        info = cdataf.extract_info(dag, ["fit"])
        # Package results.
        act = self._package_results1(config, info, df_out)
        self.check_string(act)

    def test_fit_dag_correctness1(self) -> None:
        # Load test data.
        data = self._get_data()
        data_source_node = cdataf.ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Specify config and create modeling node.
        config = ccfg.Config()
        config["cols"] = ["ret_0"]
        config["steps_ahead"] = 2
        config["nan_mode"] = "leave_unchanged"
        node = cdataf.VolatilityModel("vol_model", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "vol_model")
        # Z-score.
        zscore_df = dag.run_leq_node("vol_model", "fit")["df_out"]
        # Invert z-scoring.
        ret_0_vol_0_hat = zscore_df["ret_0_vol_2_hat"].shift(2)
        inverted_rets = (ret_0_vol_0_hat * zscore_df["ret_0_zscored"]).rename(
            "ret_0_inverted"
        )
        # Compare results.
        df_out = zscore_df.join(inverted_rets)
        info = cdataf.extract_info(dag, ["fit"])
        # Package results.
        act = self._package_results1(config, info, df_out)
        self.check_string(act)

    def test_predict_dag1(self) -> None:
        # Load test data.
        data = self._get_data()
        fit_interval = ("2000-01-01", "2000-02-10")
        predict_interval = ("2000-01-20", "2000-02-23")
        data_source_node = cdataf.ReadDataFromDf("data", data)
        data_source_node.set_fit_intervals([fit_interval])
        data_source_node.set_predict_intervals([predict_interval])
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Specify config and create modeling node.
        config = ccfg.Config()
        config["cols"] = ["ret_0"]
        config["steps_ahead"] = 2
        config["nan_mode"] = "leave_unchanged"
        node = cdataf.VolatilityModel("vol_model", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "vol_model")
        #
        dag.run_leq_node("vol_model", "fit")
        df_out = dag.run_leq_node("vol_model", "predict")["df_out"]
        info = cdataf.extract_info(dag, ["fit"])
        # Package results.
        act = self._package_results1(config, info, df_out)
        self.check_string(act)

    def test_predict_dag_correctness1(self) -> None:
        # Load test data.
        data = self._get_data()
        fit_interval = ("2000-01-01", "2000-02-10")
        predict_interval = ("2000-01-20", "2000-02-23")
        data_source_node = cdataf.ReadDataFromDf("data", data)
        data_source_node.set_fit_intervals([fit_interval])
        data_source_node.set_predict_intervals([predict_interval])
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Specify config and create modeling node.
        config = ccfg.Config()
        config["cols"] = ["ret_0"]
        config["steps_ahead"] = 2
        config["nan_mode"] = "leave_unchanged"
        node = cdataf.VolatilityModel("vol_model", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "vol_model")
        #
        dag.run_leq_node("vol_model", "fit")
        zscore_df = dag.run_leq_node("vol_model", "predict")["df_out"]
        # Invert z-scoring.
        ret_0_vol_0_hat = zscore_df["ret_0_vol_2_hat"].shift(2)
        inverted_rets = (ret_0_vol_0_hat * zscore_df["ret_0_zscored"]).rename(
            "ret_0_inverted"
        )
        # Compare results.
        df_out = zscore_df.join(inverted_rets)
        info = cdataf.extract_info(dag, ["fit"])
        # Package results.
        act = self._package_results1(config, info, df_out)
        self.check_string(act)

    def test_col_mode1(self) -> None:
        # Load test data.
        data = self._get_data()
        data_source_node = cdataf.ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Specify config and create modeling node.
        config = ccfg.Config()
        config["cols"] = ["ret_0"]
        config["steps_ahead"] = 2
        config["col_mode"] = "replace_all"
        config["nan_mode"] = "leave_unchanged"
        node = cdataf.VolatilityModel("vol_model", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "vol_model")
        #
        df_out = dag.run_leq_node("vol_model", "fit")["df_out"]
        info = cdataf.extract_info(dag, ["fit"])
        # Package results.
        act = self._package_results1(config, info, df_out)
        self.check_string(act)

    def test_col_mode2(self) -> None:
        # Load test data.
        data = self._get_data()
        data_source_node = cdataf.ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Specify config and create modeling node.
        config = ccfg.Config()
        config["cols"] = ["ret_0"]
        config["steps_ahead"] = 2
        config["col_mode"] = "replace_selected"
        config["nan_mode"] = "leave_unchanged"
        node = cdataf.VolatilityModel("vol_model", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "vol_model")
        #
        df_out = dag.run_leq_node("vol_model", "fit")["df_out"]
        info = cdataf.extract_info(dag, ["fit"])
        # Package results.
        act = self._package_results1(config, info, df_out)
        self.check_string(act)

    def test_fit_multiple_columns(self) -> None:
        # Load test data.
        data = self._get_data()
        data["ret_0_2"] = data.ret_0 + np.random.normal(size=len(data))
        data_source_node = cdataf.ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Specify config and create modeling node.
        config = ccfg.Config()
        config["cols"] = ["ret_0", "ret_0_2"]
        config["steps_ahead"] = 2
        config["nan_mode"] = "leave_unchanged"
        node = cdataf.VolatilityModel("vol_model", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "vol_model")
        #
        df_out = dag.run_leq_node("vol_model", "fit")["df_out"]
        info = cdataf.extract_info(dag, ["fit"])
        # Package results.
        act = self._package_results1(config, info, df_out)
        self.check_string(act)

    def test_multiple_columns_with_specified_tau(self) -> None:
        # Load test data.
        data = self._get_data()
        data["ret_0_2"] = data.ret_0 + np.random.normal(size=len(data))
        # Specify config.
        config = ccfg.Config()
        config["cols"] = ["ret_0", "ret_0_2"]
        config["steps_ahead"] = 2
        config["nan_mode"] = "drop"
        config["tau"] = 10
        # Check if specified tau is used for all columns via learned taus property.
        node = cdataf.VolatilityModel("vol_model", **config.to_dict())
        node.fit(data)
        dbg.dassert_set_eq(node.taus.values(), [10])

    def test_fit_none_columns(self) -> None:
        # Load test data.
        data = self._get_data()
        data["ret_0_2"] = data.ret_0 + np.random.normal(size=len(data))
        # Specify config.
        config = ccfg.Config()
        config["steps_ahead"] = 2
        config["nan_mode"] = "leave_unchanged"
        # Get outputs with `cols`=None and all specified.
        output_cols_none = self._run_volatility_model(data, config)
        config["cols"] = ["ret_0", "ret_0_2"]
        output_cols_specified = self._run_volatility_model(data, config)
        np.testing.assert_equal(
            output_cols_none.to_string(),
            output_cols_specified.to_string(),
        )

    def test_fit_int_columns(self) -> None:
        # Load test data.
        data = self._get_data()
        data[10] = data.ret_0 + np.random.normal(size=len(data))
        # Specify config.
        config = ccfg.Config()
        config["cols"] = [10]
        config["steps_ahead"] = 2
        config["nan_mode"] = "leave_unchanged"
        # Get output with integer column names.
        output = self._run_volatility_model(data, config)
        self.check_string(output.to_string())

    def test_get_fit_state1(self) -> None:
        data = self._get_data()
        config = ccfg.Config()
        config["cols"] = ["ret_0"]
        config["steps_ahead"] = 2
        config["nan_mode"] = "leave_unchanged"
        node = cdataf.VolatilityModel("vol_model", **config.to_dict())
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
        node = cdataf.VolatilityModel("vol_model", **config.to_dict())
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
        node_fit = cdataf.VolatilityModel("vol_model", **config.to_dict())
        node_fit.fit(data)
        output_fit = node_fit.predict(data)["df_out"]
        node_predefined = cdataf.VolatilityModel("vol_model", **config.to_dict())
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

    @staticmethod
    def _run_volatility_model(data: pd.DataFrame, config: ccfg.Config) -> str:
        data_source_node = cdataf.ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Create modeling node.
        node = cdataf.VolatilityModel("vol_model", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "vol_model")
        output = dag.run_leq_node("vol_model", "fit")["df_out"]
        return output


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
        node = cdataf.VolatilityModulator("modulate", **config.to_dict())
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
        node = cdataf.VolatilityModulator("demodulate", **config.to_dict())
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
        node = cdataf.VolatilityModulator("demodulate", **config.to_dict())
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
        node = cdataf.VolatilityModulator("demodulate", **config.to_dict())
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
