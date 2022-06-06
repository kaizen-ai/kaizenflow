"""
Import as:

import core.finance.target_position_generation as cftapoge
"""

import logging
from typing import Any, Optional, Union

import numpy as np
import pandas as pd

import core.config as cconfig
import core.signal_processing as sigproc
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas

_LOG = logging.getLogger(__name__)


def compute_target_positions_cross_sectionally(
    prediction: pd.DataFrame,
    volatility: pd.DataFrame,
    config: cconfig.Config,
) -> pd.DataFrame:
    """
    Compute target dollar positions based on forecasts, basic constraints.

    :param bulk_frac_to_remove: applied to predictions; as in
        `csigproc.gaussian_rank()`
    :param bulk_fill_method: applied to predictions; as in
        `csigproc.gaussian_rank()`
    :param target_gmv: a float (constant target GMV) or else a
        `datetime.time` indexed `pd.Series` of GMVs (e.g., to simulate
        intraday ramp-up/ramp-down)
    :param volatility_lower_bound: threshold for volatility clipping
    """
    _validate_compatibility(prediction, volatility)
    bulk_frac_to_remove = _get_object_from_config(
        config, "bulk_frac_to_remove", float, 0.0
    )
    bulk_fill_method = _get_object_from_config(
        config, "bulk_fill_method", str, "zero"
    )
    target_gmv = _get_object_from_config(
        config, "target_gmv", (float, pd.Series), 1e6
    )
    volatility_lower_bound = _get_object_from_config(
        config, "volatility_lower_bound", float, 1e-5
    )
    #
    if prediction.columns.size > 1:
        gaussian_ranked = sigproc.gaussian_rank(
            prediction,
            bulk_frac_to_remove=bulk_frac_to_remove,
            bulk_fill_method=bulk_fill_method,
        )
    else:
        _LOG.info(
            "Predictions provided for one asset; skipping Gaussian ranking."
        )
        gaussian_ranked = prediction
    _LOG.debug(
        "gaussian_ranked=\n%s",
        hpandas.df_to_str(gaussian_ranked, num_rows=10),
    )
    target_position_signs = np.sign(gaussian_ranked)
    _LOG.debug(
        "target_position_signs=\n%s",
        hpandas.df_to_str(target_position_signs, num_rows=10),
    )
    _LOG.debug(
        "position count=\n%s",
        hpandas.df_to_str(target_position_signs.abs().sum(axis=1)),
    )
    _LOG.debug(
        "position sign imbalance=\n%s",
        hpandas.df_to_str(target_position_signs.sum(axis=1)),
    )
    volatility = volatility.clip(lower=volatility_lower_bound)
    target_positions = target_position_signs.divide(volatility ** 2)
    _LOG.debug(
        "target_positions prior to gmv scaling=\n%s",
        hpandas.df_to_str(target_positions, num_rows=10),
    )
    target_positions = _apply_gmv_scaling(target_positions, target_gmv)
    _LOG.debug(
        "gmv-scaled target_positions=\n%s",
        hpandas.df_to_str(target_positions, num_rows=10),
    )
    hdbg.dassert_isinstance(target_positions, pd.DataFrame)
    return target_positions


# TODO(Paul): Rename with cross-sectional.
def _apply_gmv_scaling(
    target_positions: pd.DataFrame,
    target_gmv: Union[float, pd.Series],
) -> pd.DataFrame:
    l1_norm = target_positions.abs().sum(axis=1, min_count=1)
    if isinstance(target_gmv, float):
        hdbg.dassert_lt(0, target_gmv)
        scale_factors = l1_norm / target_gmv
    elif isinstance(target_gmv, pd.Series):
        hdbg.dassert_lte(0, target_gmv.min())
        # TODO(Paul): Perform an index comparison.
        scale_factors = l1_norm.divide(target_gmv, axis=0).replace(
            [-np.inf, np.inf], 0.0
        )
    else:
        raise ValueError("`target_gmv` type=%s not supported", type(target_gmv))
    _LOG.debug("`scale_factors`=\n%s", hpandas.df_to_str(scale_factors))
    target_positions = target_positions.divide(scale_factors, axis=0).replace(
        [-np.inf, np.inf], 0.0
    )
    return target_positions


def compute_target_positions_longitudinally(
    prediction: pd.DataFrame,
    volatility: pd.DataFrame,
    config: cconfig.Config,
    spread: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """
    Compute target dollar positions based on forecasts, basic constraints.

    :prediction_scaling_factor: a multiplicative scaling factor to
        pre-apply to each column, e.g., to set to a unit scale
    :prediction_abs_threshold: absolute value threshold below which
        predictions are
    """
    hpandas.dassert_time_indexed_df(
        prediction, allow_empty=True, strictly_increasing=True
    )
    hpandas.dassert_time_indexed_df(
        volatility, allow_empty=True, strictly_increasing=True
    )
    _validate_compatibility(prediction, volatility)
    prediction_abs_threshold = _get_object_from_config(
        config, "prediction_abs_threshold", float, 0.0
    )
    volatility_to_spread_threshold = _get_object_from_config(
        config, "volatility_to_spread_threshold", float, 0.0
    )
    gamma = _get_object_from_config(
        config, "gamma", float, 0.0
    )
    target_dollar_risk_per_name = _get_object_from_config(
        config, "target_dollar_risk_per_name", float, 1e2
    )
    volatility_lower_bound = _get_object_from_config(
        config, "volatility_lower_bound", float, 1e-4
    )
    #
    hdbg.dassert_lte(0, prediction_abs_threshold)
    hdbg.dassert_lt(0, target_dollar_risk_per_name)
    hdbg.dassert_lte(0, volatility_lower_bound)
    #
    spread_lower_bound = _get_object_from_config(
        config, "spread_lower_bound", float, 1e-4
    )
    if spread is None:
        _LOG.info("spread is `None`; imputing spread_lower_bound=%f", spread_lower_bound)
        spread = pd.DataFrame(spread_lower_bound, prediction.index, prediction.columns)
    _validate_compatibility(prediction, spread)
    _LOG.debug(
        "spread=\n%s",
        hpandas.df_to_str(spread),
    )
    #
    idx = prediction.index
    prediction = prediction.dropna(how="all")
    non_nan_idx = prediction.index
    volatility_to_spread = volatility.divide(spread)
    _LOG.debug(
        "volatility_to_spread=\n%s",
        hpandas.df_to_str(volatility_to_spread),
    )
    pred_term = (prediction.abs() - prediction_abs_threshold).clip(lower=0.0)
    vol_to_spread_term = (volatility_to_spread - volatility_to_spread_threshold).clip(lower=0.0)
    mask = pred_term.multiply(vol_to_spread_term) <= gamma
    prediction = prediction[~mask]
    prediction = prediction.reindex(index=non_nan_idx)
    prediction[mask] = 0.0
    # Add back the all-NaN rows.
    prediction = prediction.reindex(index=idx)
    #
    _LOG.debug(
        "prediction=\n%s",
        hpandas.df_to_str(prediction),
    )
    #
    target_position_signs = np.sign(prediction)
    _LOG.debug(
        "target_position_signs=\n%s",
        hpandas.df_to_str(target_position_signs),
    )
    _LOG.debug(
        "position count=\n%s",
        hpandas.df_to_str(target_position_signs.abs().sum(axis=1)),
    )
    _LOG.debug(
        "position sign imbalance=\n%s",
        hpandas.df_to_str(target_position_signs.sum(axis=1)),
    )
    #
    volatility = volatility.clip(lower=volatility_lower_bound)
    target_capital = target_dollar_risk_per_name / volatility
    _LOG.debug(
        "target_capital=\n%s",
        hpandas.df_to_str(target_capital),
    )
    target_positions = target_position_signs.multiply(target_capital)
    _LOG.debug("target_positions=\n%s", hpandas.df_to_str(target_positions))
    #
    hdbg.dassert_isinstance(target_positions, pd.DataFrame)
    return target_positions


# TODO(Paul): This also exists in `process_forecasts_.py`. Factor it out.
def _validate_compatibility(df1: pd.DataFrame, df2: pd.DataFrame) -> None:
    hpandas.dassert_indices_equal(df1, df2)
    hpandas.dassert_columns_equal(df1, df2)


# TODO(Paul): This also exists in `process_forecasts_.py`. Factor it out.
# Extract the objects from the config.
def _get_object_from_config(
    config: cconfig.Config,
    key: str,
    expected_type: type,
    default_value: Any,
) -> Any:
    hdbg.dassert_isinstance(config, cconfig.Config),
    hdbg.dassert_isinstance(key, str)
    hdbg.dassert_issubclass(default_value, expected_type)
    if key in config:
        obj = config[key]
        hdbg.dassert_issubclass(obj, expected_type)
    else:
        obj = default_value
    return obj
