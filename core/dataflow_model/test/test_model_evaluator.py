import logging
from typing import Any, Dict, List, Tuple

import pytest

import helpers.dbg as dbg
import helpers.printing as hprint
import helpers.system_interaction as hsinte
import helpers.unit_test as hut

import numpy as np
import pandas as pd

import core.artificial_signal_generators as sig_gen
import core.config as cconfig
import core.dataflow_model.model_evaluator as modeval
import core.dataflow_model.model_plotter as modplot
import core.dataflow_model.utils as cdmu
import core.statistics as stats
import helpers.dbg as dbg


_LOG = logging.getLogger(__name__)


def generate_synthetic_rets_and_preds(n_assets: int, seed: int = 0):
    """
    Generate synthetic returns and predictions for the passed number of assets.
    """
    # Generate synthetic returns.
    mn_process = sig_gen.MultivariateNormalProcess()
    mn_process.set_cov_from_inv_wishart_draw(dim=n_assets, seed=seed)
    realization = mn_process.generate_sample(
        {"start": "2000-01-01", "end": "2010-01-01", "freq": "B"}, seed=seed
    )
    rets = realization.to_dict(orient="series")
    _LOG.debug("rets=\n%s", rets)
    # Generate fake predictions.
    noise = sig_gen.MultivariateNormalProcess(
        pd.Series(data=[0] * n_assets), pd.DataFrame(np.identity(n_assets))
    )
    noise_draw = noise.generate_sample(
        {"start": "2000-01-01", "end": "2010-01-01", "freq": "B"}, seed=seed
    )
    pred_df = 0.01 * realization + 0.1 * noise_draw
    # Adjust so that all models have positive SR.
    pred_df = (
        stats.compute_annualized_sharpe_ratio(pred_df.multiply(realization))
            .apply(np.sign)
            .multiply(pred_df)
    )
    _LOG.debug("pred_df=\n%s", pred_df)
    # Assemble the synthetic data.
    data_dict = {}
    for k in pred_df.columns:
        data_dict[k] = pd.concat(
            [rets[k].rename("returns"), pred_df[k].rename("predictions")], axis=1
        )
    _LOG.debug("data_dict=\n%s", str(data_dict))
    return data_dict


def get_example_model_evaluator():
    n_assets = 8
    data_dict = generate_synthetic_rets_and_preds(n_assets)
    # Build the config.
    eval_config = cconfig.get_config_from_nested_dict(
        {
            "model_evaluator_kwargs": {
                "oos_start": "2017-01-01",
            },
            "bh_adj_threshold": 0.1,
            "resample_rule": "W",
            "mode": "ins",
            "target_volatility": 0.1,
        }
    )
    # Build the ModelEvaluator.
    evaluator = modeval.ModelEvaluator(
        data=data_dict,
        target_col="returns",
        prediction_col="predictions",
        oos_start=eval_config["model_evaluator_kwargs", "oos_start"],
    )
    return evaluator, eval_config


class TestModelEvaluator1(hut.TestCase):

    def test_calculate_stats1(self) -> None:
        evaluator, eval_config = get_example_model_evaluator()
        # Calculate stats.
        pnl_stats = evaluator.calculate_stats(
            mode=eval_config["mode"], target_volatility=eval_config["target_volatility"]
        )
        # Check.
        #stats_df = pnl_stats.loc[["signal_quality", "correlation"]]
        actual = hut.convert_df_to_string(pnl_stats, index=True)
        self.check_string(actual)

    def test_aggregate_models1(self) -> None:
        evaluator, eval_config = get_example_model_evaluator()
        # Use all the models.
        keys = None
        pnl_srs, pos_srs, aggregate_stats = evaluator.aggregate_models(
            keys=keys,
            mode=eval_config["mode"],
            target_volatility=eval_config["target_volatility"],
        )
        aggregate_stats_df = aggregate_stats.to_frame()
        # Check.
        actual = hut.convert_df_to_string(aggregate_stats_df, index=True)
        self.check_string(actual)