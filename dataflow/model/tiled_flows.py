"""
Import as:

import dataflow.model.tiled_flows as dtfmotiflo
"""

import datetime
import logging

import pandas as pd

_LOG = logging.getLogger(__name__)

from typing import List, Optional, Union

from tqdm.autonotebook import tqdm

import dataflow.model.forecast_evaluator as dtfmofoeva
import dataflow.model.forecast_mixer as dtfmofomix
import dataflow.model.parquet_tile_analyzer as dtfmpatian
import dataflow.model.regression_analyzer as dtfmoreana
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hparquet as hparque


def generate_bar_metrics(
    file_name: str,
    start_date: datetime.date,
    end_date: datetime.date,
    asset_id_col: str,
    returns_col: str,
    volatility_col: str,
    prediction_col: str,
    target_gmv: Optional[float] = None,
    dollar_neutrality: str = "no_constraint",
    overnight_returns: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Generate "research" portfolio bar metrics over a tiled backtest.
    """
    columns = [asset_id_col, returns_col, volatility_col, prediction_col]
    tiles = hparque.yield_parquet_tiles_by_year(
        file_name,
        start_date,
        end_date,
        columns,
    )
    forecast_evaluator = dtfmofoeva.ForecastEvaluator(
        returns_col=returns_col,
        volatility_col=volatility_col,
        prediction_col=prediction_col,
    )
    results = []
    num_years = end_date.year - start_date.year + 1
    for tile in tqdm(tiles, total=num_years):
        # Convert the `from_parquet()` dataframe to a dataflow-style dataframe.
        df = process_parquet_read_df(
            tile,
            asset_id_col,
        )
        _, bar_metrics = forecast_evaluator.annotate_forecasts(
            df,
            target_gmv=target_gmv,
            dollar_neutrality=dollar_neutrality,
        )
        results.append(bar_metrics)
        if overnight_returns is not None:
            _, overnight_bar_metrics = forecast_evaluator.compute_overnight_pnl(
                df,
                overnight_returns=overnight_returns,
                target_gmv=target_gmv,
                dollar_neutrality=dollar_neutrality,
            )
            results.append(overnight_bar_metrics)
    df = pd.concat(results)
    # TODO(Paul): Handle the duplicates from the overnight returns.
    hdbg.dassert(not df.index.has_duplicates)
    df.sort_index(inplace=True)
    return df


def load_mix_evaluate(
    file_name: str,
    start_date: datetime.date,
    end_date: datetime.date,
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
    columns = [asset_id_col, returns_col, volatility_col] + feature_cols
    tiles = hparque.yield_parquet_tiles_by_year(
        file_name,
        start_date,
        end_date,
        columns,
    )
    fm = dtfmofomix.ForecastMixer(
        returns_col=returns_col,
        volatility_col=volatility_col,
        prediction_cols=feature_cols,
    )
    results = []
    num_years = end_date.year - start_date.year + 1
    for tile in tqdm(tiles, total=num_years):
        # Convert the `from_parquet()` dataframe to a dataflow-style dataframe.
        df = process_parquet_read_df(
            tile,
            asset_id_col,
        )
        bar_metrics = fm.generate_portfolio_bar_metrics_df(
            df,
            weights,
            target_gmv=target_gmv,
            dollar_neutrality=dollar_neutrality,
        )
        results.append(bar_metrics)
    df = pd.concat(results)
    df.sort_index(inplace=True)
    return df


def regress(
    file_name: str,
    asset_id_col: str,
    target_col: str,
    feature_cols: List[Union[int, str]],
    feature_lag: int,
    batch_size: int,
) -> pd.DataFrame:
    """
    Perform per-asset regressions over a tiled backtest.

    For each asset, the regression is performed over the entire time window.
    """
    cols = [asset_id_col, target_col] + feature_cols
    parquet_tile_analyzer = dtfmpatian.ParquetTileAnalyzer()
    parquet_tile_metadata = parquet_tile_analyzer.collate_parquet_tile_metadata(
        file_name
    )
    asset_ids = parquet_tile_metadata.index.levels[0].to_list()
    _LOG.debug("Num assets=%d", len(asset_ids))
    ra = dtfmoreana.RegressionAnalyzer(
        target_col=target_col,
        feature_cols=feature_cols,
        feature_lag=feature_lag,
    )
    results = []
    tile_iter = hparque.yield_parquet_tiles_by_assets(
        file_name,
        asset_ids,
        asset_id_col,
        batch_size,
        cols,
    )
    for tile in tile_iter:
        df = process_parquet_read_df(
            tile,
            asset_id_col,
        )
        coeffs = ra.compute_regression_coefficients(
            df,
        )
        results.append(coeffs)
    df = pd.concat(results)
    df.sort_index(inplace=True)
    return df


def process_parquet_read_df(
    df: pd.DataFrame,
    asset_id_col: str,
) -> pd.DataFrame:
    """
    Post-process a multiindex dataflow result dataframe re-read from parquet.

    :param df: dataframe in "long" format
    :param asset_id_col: asset id column to pivot on
    :return: multiindexed dataframe with asset id's at the inner column level
    """
    # Convert the asset it column to an integer column.
    df = hpandas.convert_col_to_int(df, asset_id_col)
    # If a (non-asset id) column can be represented as an int, then do so.
    df = df.rename(columns=hparque.maybe_cast_to_int)
    # Convert from long format to column-multiindexed format.
    df = df.pivot(columns=asset_id_col)
    # NOTE: the asset ids may already be sorted and so this may not be needed.
    df.sort_index(axis=1, level=-2, inplace=True)
    return df
