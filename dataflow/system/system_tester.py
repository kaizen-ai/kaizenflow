"""
Import as:

import dataflow.system.system_tester as dtfsysytes
"""

import logging
from typing import List, Tuple, Union

import pandas as pd

import dataflow.model as dtfmod
import helpers.hpandas as hpandas

_LOG = logging.getLogger(__name__)


# TODO(gp): -> System_TestCase for symmetry with the rest of the objects.
# TODO(gp): system_tester.py -> system_test_case.py
class SystemTester:
    """
    Test a System.
    """

    def get_events_signature(self, events) -> str:
        # TODO(gp): Use events.to_str()
        actual = ["# event signature=\n"]
        events_as_str = "\n".join(
            [
                event.to_str(
                    include_tenths_of_secs=False,
                    include_wall_clock_time=False,
                )
                for event in events
            ]
        )
        actual.append("events_as_str=\n%s" % events_as_str)
        actual = "\n".join(actual)
        return actual

    def get_portfolio_signature(self, portfolio) -> Tuple[str, pd.Series]:
        actual = ["\n# portfolio signature=\n"]
        actual.append(str(portfolio))
        actual = "\n".join(actual)
        statistics = portfolio.get_historical_statistics()
        pnl = statistics["pnl"]
        _LOG.debug("pnl=\n%s", pnl)
        return actual, pnl

    def compute_run_signature(
        self,
        dag_runner,
        portfolio,
        result_bundle,
        *,
        price_col: str,
        volatility_col: str,
        prediction_col: str,
    ) -> str:
        # Check output.
        actual = []
        #
        events = dag_runner.events
        actual.append(self.get_events_signature(events))
        signature, pnl = self.get_portfolio_signature(portfolio)
        actual.append(signature)
        signature, research_pnl = self.get_research_pnl_signature(
            result_bundle,
            price_col=price_col,
            volatility_col=volatility_col,
            prediction_col=prediction_col,
        )
        actual.append(signature)
        if min(pnl.count(), research_pnl.count()) > 1:
            # Drop leading NaNs and burn the first PnL entry.
            research_pnl = research_pnl.dropna().iloc[1:]
            tail = research_pnl.size
            # We create new series because the portfolio times may be
            # disaligned from the research bar times.
            pnl1 = pd.Series(pnl.tail(tail).values)
            _LOG.debug("portfolio pnl=\n%s", pnl1)
            pnl2 = pd.Series(research_pnl.tail(min(tail, pnl1.size)).values)
            _LOG.debug("research pnl=\n%s", pnl2)
            correlation = pnl1.corr(pnl2)
            actual.append("\n# pnl agreement with research pnl\n")
            actual.append(f"corr = {correlation:.3f}")
        actual = "\n".join(map(str, actual))
        return actual

    def get_research_pnl_signature(
        self,
        result_bundle,
        *,
        price_col: str,
        volatility_col: str,
        prediction_col: str,
        bulk_frac_to_remove: float = 0.0,
        bulk_fill_method: str = "zero",
        target_gmv: float = 1e5,
        liquidate_at_end_of_day: bool = False,
    ) -> Tuple[str, pd.Series]:
        # TODO(gp): @all use actual.append(hprint.frame("system_config"))
        #  to separate the sections of the output.
        actual = ["\n# forecast_evaluator_from_prices signature=\n"]
        forecast_evaluator = dtfmod.ForecastEvaluatorFromPrices(
            price_col=price_col,
            volatility_col=volatility_col,
            prediction_col=prediction_col,
        )
        result_df = result_bundle.result_df
        _LOG.debug("result_df=\n%s", hpandas.df_to_str(result_df))
        signature = forecast_evaluator.to_str(
            result_df,
            bulk_frac_to_remove=bulk_frac_to_remove,
            bulk_fill_method=bulk_fill_method,
            liquidate_at_end_of_day=liquidate_at_end_of_day,
            target_gmv=target_gmv,
        )
        _LOG.debug("signature=\n%s", signature)
        actual.append(signature)
        _, _, _, _, stats = forecast_evaluator.compute_portfolio(
            result_df,
            bulk_frac_to_remove=bulk_frac_to_remove,
            bulk_fill_method=bulk_fill_method,
            target_gmv=target_gmv,
            liquidate_at_end_of_day=liquidate_at_end_of_day,
            reindex_like_input=True,
            burn_in_bars=0,
        )
        research_pnl = stats["pnl"]
        actual = "\n".join(map(str, actual))
        return actual, research_pnl

    @staticmethod
    def _append(
        list_: List[str], label: str, data: Union[pd.Series, pd.DataFrame]
    ) -> None:
        data_str = hpandas.df_to_str(data, index=True, num_rows=None, decimals=3)
        list_.append(f"{label}=\n{data_str}")
