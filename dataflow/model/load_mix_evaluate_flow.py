import datetime
import logging

import pandas as pd

_LOG = logging.getLogger(__name__)

from typing import List, Optional, Union

import dataflow.model.forecast_mixer as dtfmofomix
import dataflow.model.parquet_utils as dtfmopauti
import helpers.hdbg as hdbg
import helpers.hparquet as hparque


def load_mix_evaluate(
    file_name: str,
    start_date: datetime.time,
    end_date: datetime.time,
    asset_id_col: str,
    returns_col: str,
    volatility_col: str,
    feature_cols: List[Union[int, str]],
    weights: pd.DataFrame,
    target_gmv: Optional[float] = None,
    dollar_neutrality: str = "no_constraint",
) -> pd.DataFrame:
    """
    Load a tiled backtest, mix features, and evaluate the portfolio.

    :param file_name: as in hparque.yield_parquet_tiles_by_year()
    :param start_date: as in hparque.yield_parquet_tiles_by_year()
    :param end_date: hparque.yield_parquet_tiles_by_year()
    :param asset_id_col: name of asset id column
    :param returns_col: name of realize returns column
    :param volatility_col: name of volatility forecast column
    :param feature_cols: names of predictive feature columns
    :param weights: feature weights, indexed by feature column name; one
        set of weights per column
    :param target_gmv: target gmv for forecast evaluation
    :param dollar_neutrality: dollar neutrality constraint for forecast
        evaluation, e.g.,
    :return: a portfolio bar metrics dataframe (see
        dtfmofomix.get_portfolio_bar_metrics_dataframe() for an example).
    """
    hdbg.dassert_isinstance(weights, pd.DataFrame)
    hdbg.dassert_set_eq(weights.index, feature_cols)
    # Load parquet tile.
    columns = [asset_id_col, returns_col, volatility_col] + feature_cols
    #
    tiles = hparque.yield_parquet_tiles_by_year(
        file_name,
        start_date,
        end_date,
        columns,
    )
    results = []
    for tile in tiles:
        # Convert the `from_parquet()` dataframe to a dataflow-style dataframe.
        df = dtfmopauti.process_parquet_read_df(
            tile,
            asset_id_col,
        )
        bar_metrics = mix_and_evaluate(
            df,
            returns_col,
            volatility_col,
            feature_cols,
            weights,
            target_gmv,
            dollar_neutrality,
        )
        results.append(bar_metrics)
    return pd.concat(results)


def mix_and_evaluate(
    df: pd.DataFrame,
    returns_col: str,
    volatility_col: str,
    feature_cols: list,
    weights: pd.DataFrame,
    target_gmv: Optional[float] = None,
    dollar_neutrality: str = "no_constraint",
) -> pd.DataFrame:
    # For each weight column, mix the features using those weights into a
    # single forecast and generate the corresponding portfolio.
    fm = dtfmofomix.ForecastMixer(
        returns_col=returns_col,
        volatility_col=volatility_col,
        prediction_cols=feature_cols,
    )
    bar_metrics = fm.generate_portfolio_bar_metrics_df(
        df,
        weights,
        target_gmv=target_gmv,
        dollar_neutrality=dollar_neutrality,
    )
    return bar_metrics
