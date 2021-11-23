import logging

import core.dataflow_model.model_plotter as cdtfmomopl
import core.dataflow_model.test.test_model_evaluator as cdmttme
import helpers.printing as hprint
import helpers.unit_test as hunitest

_LOG = logging.getLogger(__name__)


# TODO(gp): For now we just test that code executes, without checking the output.
#  When we add DataFrame output to the ModelPlotter functions so we can check that.


class TestModelPlotter1(hunitest.TestCase):
    def test_plot_multiple_tests_adjustment1(self) -> None:
        plotter, _, eval_config = self._get_example_model_plotter()
        #
        plotter.plot_multiple_tests_adjustment(
            threshold=eval_config["bh_adj_threshold"], mode=eval_config["mode"]
        )

    def test_model_selection1(self) -> None:
        plotter, evaluator, eval_config = self._get_example_model_plotter()
        # Calculate stats.
        pnl_stats = evaluator.calculate_stats(
            mode=eval_config["mode"],
            target_volatility=eval_config["target_volatility"],
        )
        # TODO(gp): Move this chunk of code in a function and call that.
        col_mask = (
            pnl_stats.loc["signal_quality"].loc["sr.adj_pval"]
            < eval_config["bh_adj_threshold"]
        )
        selected = pnl_stats.loc[:, col_mask].columns.to_list()
        not_selected = pnl_stats.loc[:, ~col_mask].columns.to_list()
        #
        print(
            "num model selected=%s"
            % hprint.perc(len(selected), pnl_stats.shape[1])
        )
        print("model selected=%s" % selected)
        print("model not selected=%s" % not_selected)
        #
        plotter.plot_multiple_pnls(
            keys=selected,
            resample_rule=eval_config["resample_rule"],
            mode=eval_config["mode"],
        )

    def test_plot_return_correlation1(self) -> None:
        plotter, _, eval_config = self._get_example_model_plotter()
        #
        plotter.plot_correlation_matrix(
            series="returns",
            resample_rule=eval_config["resample_rule"],
            mode=eval_config["mode"],
        )

    def test_plot_model_return_correlation1(self) -> None:
        plotter, _, eval_config = self._get_example_model_plotter()
        #
        plotter.plot_correlation_matrix(
            series="pnl",
            resample_rule=eval_config["resample_rule"],
            mode=eval_config["mode"],
        )

    def test_plot_sharpe_ratio_panel1(self) -> None:
        plotter, _, eval_config = self._get_example_model_plotter()
        # Use all the models.
        keys = None
        plotter.plot_sharpe_ratio_panel(keys=keys, mode=eval_config["mode"])

    def test_plot_rets_signal_analysis1(self) -> None:
        plotter, _, eval_config = self._get_example_model_plotter()
        # Use all the models.
        keys = None
        plotter.plot_rets_signal_analysis(
            keys=keys,
            resample_rule=eval_config["resample_rule"],
            mode=eval_config["mode"],
            target_volatility=eval_config["target_volatility"],
        )

    def test_plot_rets_signal_analysis1(self) -> None:
        plotter, _, eval_config = self._get_example_model_plotter()
        # Use all the models.
        keys = None
        plotter.plot_performance(
            keys=keys,
            resample_rule=eval_config["resample_rule"],
            mode=eval_config["mode"],
            target_volatility=eval_config["target_volatility"],
        )

    def test_plot_rets_and_vol1(self) -> None:
        plotter, _, eval_config = self._get_example_model_plotter()
        # Use all the models.
        keys = None
        plotter.plot_rets_and_vol(
            keys=keys,
            resample_rule=eval_config["resample_rule"],
            mode=eval_config["mode"],
            target_volatility=eval_config["target_volatility"],
        )

    def test_plot_positions1(self) -> None:
        plotter, _, eval_config = self._get_example_model_plotter()
        # Use all the models.
        keys = None
        plotter.plot_positions(
            keys=keys,
            mode=eval_config["mode"],
            target_volatility=eval_config["target_volatility"],
        )

    def test_plot_returns_and_predictions1(self) -> None:
        plotter, _, eval_config = self._get_example_model_plotter()
        # Use all the models.
        keys = None
        plotter.plot_returns_and_predictions(
            keys=keys,
            resample_rule=eval_config["resample_rule"],
            mode=eval_config["mode"],
        )

    def _get_example_model_plotter(self):
        evaluator, eval_config = cdmttme.get_example_model_evaluator()
        # Build the ModelPlotter.
        plotter = cdtfmomopl.ModelPlotter(evaluator)
        return plotter, evaluator, eval_config
