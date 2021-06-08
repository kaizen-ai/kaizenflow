import collections
import logging

import pandas as pd
import sklearn.linear_model as slm

import core.artificial_signal_generators as sig_gen
import core.config as cconfig
import core.dataflow as dtf
import core.dataframe_modeler as dfmod
import helpers.printing as prnt
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestDataFrameModeler(hut.TestCase):
    def test_dump_json1(self) -> None:
        df = pd.DataFrame(
            {"col0": [1, 2, 3], "col1": [4, 5, 6]},
            index=pd.date_range("2010-01-01", periods=3),
        )
        oos_start = pd.Timestamp("2010-01-01")
        info = collections.OrderedDict({"df_info": dtf.get_df_info_as_string(df)})
        df_modeler = dfmod.DataFrameModeler(df, oos_start=oos_start, info=info)
        output = df_modeler.dump_json()
        self.check_string(output)

    def test_load_json1(self) -> None:
        """
        Test by dumping json and loading it again.
        """
        df = pd.DataFrame(
            {"col0": [1, 2, 3], "col1": [4, 5, 6]},
            index=pd.date_range("2010-01-01", periods=3),
        )
        oos_start = pd.Timestamp("2010-01-01")
        info = collections.OrderedDict({"df_info": dtf.get_df_info_as_string(df)})
        df_modeler = dfmod.DataFrameModeler(df, oos_start=oos_start, info=info)
        json_str = df_modeler.dump_json()
        df_modeler_loaded = dfmod.DataFrameModeler.load_json(json_str)
        pd.testing.assert_frame_equal(df_modeler.df, df_modeler_loaded.df)
        self.assertEqual(df_modeler.oos_start, df_modeler_loaded.oos_start)
        self.assertDictEqual(df_modeler.info, df_modeler_loaded.info)

    def test_load_json2(self) -> None:
        """
        Test by dumping json and loading it again with `oos_start=None`.
        """
        df = pd.DataFrame(
            {"col0": [1, 2, 3], "col1": [4, 5, 6]},
            index=pd.date_range("2010-01-01", periods=3),
        )
        oos_start = None
        info = collections.OrderedDict({"df_info": dtf.get_df_info_as_string(df)})
        df_modeler = dfmod.DataFrameModeler(df, oos_start=oos_start, info=info)
        json_str = df_modeler.dump_json()
        df_modeler_loaded = dfmod.DataFrameModeler.load_json(json_str)
        pd.testing.assert_frame_equal(df_modeler.df, df_modeler_loaded.df)
        self.assertEqual(df_modeler.oos_start, df_modeler_loaded.oos_start)
        self.assertDictEqual(df_modeler.info, df_modeler_loaded.info)

    def test_load_json3(self) -> None:
        """
        Test by dumping json and loading it again with `info=None`.
        """
        df = pd.DataFrame(
            {"col0": [1, 2, 3], "col1": [4, 5, 6]},
            index=pd.date_range("2010-01-01", periods=3),
        )
        oos_start = pd.Timestamp("2010-01-01")
        info = None
        df_modeler = dfmod.DataFrameModeler(df, oos_start=oos_start, info=info)
        json_str = df_modeler.dump_json()
        df_modeler_loaded = dfmod.DataFrameModeler.load_json(json_str)
        pd.testing.assert_frame_equal(df_modeler.df, df_modeler_loaded.df)
        self.assertEqual(df_modeler.oos_start, df_modeler_loaded.oos_start)
        self.assertEqual(df_modeler.info, df_modeler_loaded.info)

    def test_apply_sklearn_model_fit_with_oos(self) -> None:
        pred_lag = 2
        data = self._get_data(pred_lag)
        config = self._get_config(pred_lag)
        df_modeler = dfmod.DataFrameModeler(df=data, oos_start="2010-03-01")
        output = df_modeler.apply_sklearn_model(**config.to_dict())
        output_df = output.df
        info_fit = output.info["fit"]
        str_output = (
            f"{prnt.frame('config')}\n{config}\n"
            f"{prnt.frame('df_out')}\n{hut.convert_df_to_string(output_df, index=True)}\n"
            f"{hut.convert_info_to_string(info_fit)}"
        )
        self.check_string(str_output)

    def test_apply_sklearn_model_predict_with_oos(self) -> None:
        pred_lag = 2
        data = self._get_data(pred_lag)
        config = self._get_config(pred_lag)
        df_modeler = dfmod.DataFrameModeler(df=data, oos_start="2010-03-01")
        output = df_modeler.apply_sklearn_model(
            **config.to_dict(), method="predict"
        )
        output_df = output.df
        info_fit = output.info["fit"]
        info_predict = output.info["predict"]
        str_output = (
            f"{prnt.frame('config')}\n{config}\n"
            f"{prnt.frame('df_out')}\n{hut.convert_df_to_string(output_df, index=True)}\n"
            f"{hut.convert_info_to_string(info_fit)}\n"
            f"{hut.convert_info_to_string(info_predict)}"
        )
        self.check_string(str_output)

    def test_apply_sklearn_model_fit_without_oos(self) -> None:
        pred_lag = 2
        data = self._get_data(pred_lag)
        config = self._get_config(pred_lag)
        df_modeler = dfmod.DataFrameModeler(df=data)
        output = df_modeler.apply_sklearn_model(**config.to_dict())
        output_df = output.df
        info_fit = output.info["fit"]
        str_output = (
            f"{prnt.frame('config')}\n{config}\n"
            f"{prnt.frame('df_out')}\n{hut.convert_df_to_string(output_df, index=True)}\n"
            f"{hut.convert_info_to_string(info_fit)}"
        )
        self.check_string(str_output)

    def test_apply_sklearn_model_predict_without_oos(self) -> None:
        pred_lag = 2
        data = self._get_data(pred_lag)
        config = self._get_config(pred_lag)
        df_modeler = dfmod.DataFrameModeler(df=data)
        with self.assertRaises(AssertionError):
            df_modeler.apply_sklearn_model(**config.to_dict(), method="predict")

    def test_merge(self) -> None:
        df1 = pd.DataFrame(
            {"col0": [1, 2, 3], "col1": [4, 5, 6]},
            index=pd.date_range("2010-01-01", periods=3),
        )
        df2 = pd.DataFrame(
            {"col2": [7, 8, 9, 10, 11]},
            index=pd.date_range("2010-01-01", periods=5),
        )
        dfm = dfmod.DataFrameModeler(df1, oos_start="2010-01-01")
        dfm = dfm.merge(dfmod.DataFrameModeler(df2, oos_start="2010-01-02"))
        df = pd.merge(df1, df2, left_index=True, right_index=True)
        pd.testing.assert_frame_equal(dfm.df, df)
        self.assertEqual(dfm.oos_start, pd.Timestamp("2010-01-01"))
        str_output = hut.convert_df_to_string(dfm.df, index=True)
        self.check_string(str_output)

    def _get_config(self, steps_ahead: int) -> cconfig.Config:
        config = cconfig.Config()
        config["x_vars"] = ["x"]
        config["y_vars"] = ["y"]
        config["steps_ahead"] = steps_ahead
        config["model_func"] = slm.LinearRegression
        return config

    def _get_data(self, lag: int) -> pd.DataFrame:
        """
        Generate "random returns".

        Use lag + noise as predictor.
        """
        num_periods = 100
        total_steps = num_periods + lag + 1
        rets = sig_gen.get_gaussian_walk(0, 0.2, total_steps, seed=10).diff()
        noise = sig_gen.get_gaussian_walk(0, 0.02, total_steps, seed=1).diff()
        pred = rets.shift(-lag).loc[1:num_periods] + noise.loc[1:num_periods]
        resp = rets.loc[1:num_periods]
        idx = pd.date_range("2010-01-01", periods=num_periods, freq="D")
        df = pd.DataFrame.from_dict({"x": pred, "y": resp}).set_index(idx)
        return df
