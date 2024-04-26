"""
Import as:

import dataflow.model.abstract_forecast_evaluator as dtfmabfoev
"""

import logging
import os
from typing import Dict, Optional, Tuple

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hparquet as hparque

_LOG = logging.getLogger(__name__)


class AbstractForecastEvaluator:
    def load_portfolio(
        self,
        log_dir: str,
        *,
        file_name: Optional[str] = None,
        tz: str = "America/New_York",
        cast_asset_ids_to_int: bool = True,
    ) -> pd.DataFrame:
        """
        Load and process saved portfolio.

        :param log_dir: directory for loading log files of portfolio state
        :param file_name: if `None`, find and use the latest
        :param tz: timezone to apply to timestamps (this information is lost in
            the logging/reading round trip)
        :param cast_asset_ids_to_int: cast asset ids from integers-as-strings
            to integers (the data type is lost in the logging/reading round
            trip)
        """
        if file_name is None:
            file_name = self._get_latest_file_in_log_dir(log_dir)
        price = self._read_df(log_dir, "price", file_name, tz)
        volatility = self._read_df(log_dir, "volatility", file_name, tz)
        predictions = self._read_df(log_dir, "prediction", file_name, tz)
        holdings_shares = self._read_df(log_dir, "holdings_shares", file_name, tz)
        holdings_notional = self._read_df(
            log_dir, "holdings_notional", file_name, tz
        )
        executed_trades_shares = self._read_df(
            log_dir, "executed_trades_shares", file_name, tz
        )
        executed_trades_notional = self._read_df(
            log_dir, "executed_trades_notional", file_name, tz
        )
        pnl = self._read_df(log_dir, "pnl", file_name, tz)
        if cast_asset_ids_to_int:
            for df in [
                price,
                volatility,
                predictions,
                holdings_shares,
                holdings_notional,
                executed_trades_shares,
                executed_trades_notional,
                pnl,
            ]:
                self._cast_cols_to_int(df)
        dfs = {
            "price": price,
            "volatility": volatility,
            "prediction": predictions,
            "holdings_shares": holdings_shares,
            "holdings_notional": holdings_notional,
            "executed_trades_shares": executed_trades_shares,
            "executed_trades_notional": executed_trades_notional,
            "pnl": pnl,
        }
        portfolio_df = self._build_multiindex_df(dfs)
        return portfolio_df

    def load_portfolio_stats(
        self,
        log_dir: str,
        *,
        file_name: Optional[str] = None,
        tz: str = "America/New_York",
    ) -> pd.DataFrame:
        """
        Load portfolio stats.

        Used when portfolio is too large to load alongside the stats DataFrame.

        :param log_dir: directory for loading log files of portfolio state
        :param file_name: if `None`, find and use the latest
        :param tz: timezone to apply to timestamps (this information is lost in
            the logging/reading round trip)
        """
        if file_name is None:
            file_name = self._get_latest_file_in_log_dir(log_dir)
        #
        statistics_df = self._read_df(log_dir, "statistics", file_name, tz)
        return statistics_df

    def load_portfolio_and_stats(
        self,
        log_dir: str,
        *,
        file_name: Optional[str] = None,
        tz: str = "America/New_York",
        cast_asset_ids_to_int: bool = True,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Load and process saved portfolio together with stats.
        """
        if file_name is None:
            file_name = self._get_latest_file_in_log_dir(log_dir)
        portfolio_df = self.load_portfolio(
            log_dir,
            file_name=file_name,
            tz=tz,
            cast_asset_ids_to_int=cast_asset_ids_to_int,
        )
        statistics_df = self.load_portfolio_stats(
            log_dir, file_name=file_name, tz=tz
        )
        return portfolio_df, statistics_df

    # /////////////////////////////////////////////////////////////////////////////

    @staticmethod
    def _get_latest_file_in_log_dir(log_dir: str) -> str:
        """
        Get the latest file in the log directory.
        """
        dir_name = os.path.join(log_dir, "price")
        pattern = "*"
        only_files = True
        use_relative_paths = True
        files = hio.listdir(dir_name, pattern, only_files, use_relative_paths)
        hdbg.dassert_lte(1, len(files), "No files in %s", dir_name)
        files.sort()
        file_name = files[-1]
        _LOG.info("file_name=%s", file_name)
        return file_name

    @staticmethod
    def _read_df(
        log_dir: str,
        name: str,
        file_name: str,
        tz: str,
    ) -> pd.DataFrame:
        path = os.path.join(log_dir, name, file_name)
        df = hparque.from_parquet(path)
        df.index = df.index.tz_convert(tz)
        return df

    @staticmethod
    def _cast_cols_to_int(
        df: pd.DataFrame,
    ) -> None:
        # If integers are converted to floats and then strings, then upon
        # being read they must be cast to floats before being cast to ints.
        df.columns = df.columns.astype("float64").astype("int64")

    @staticmethod
    def _build_multiindex_df(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        portfolio_df = pd.concat(dfs.values(), axis=1, keys=dfs.keys())
        return portfolio_df
