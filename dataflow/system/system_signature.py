"""
Import as:

import dataflow.system.system_signature as dtfsysysig
"""

import logging
from typing import Any, Dict, List, Tuple

import pandas as pd

import core.config as cconfig
import dataflow.core as dtfcore
import dataflow.model as dtfmod
import dataflow.system.system as dtfsyssyst
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import helpers.hunit_test as hunitest
import oms as oms

_LOG = logging.getLogger(__name__)

# #############################################################################
# Utils
# #############################################################################

# TODO(gp): What is the difference with _get_signature_from_result_bundle?
#  Can we unify?
def get_signature(
    system_config: cconfig.Config, result_bundle: dtfcore.ResultBundle, col: str
) -> str:
    """
    Compute the signature of a test in terms of:

    - system signature
    - result bundle signature
    """
    txt: List[str] = []
    #
    txt.append(hprint.frame("system_config"))
    txt.append(str(system_config))
    #
    txt.append(hprint.frame(col))
    result_df = result_bundle.result_df
    data = result_df[col].dropna(how="all").round(3)
    data_str = hunitest.convert_df_to_string(data, index=True, decimals=3)
    txt.append(data_str)
    #
    res = "\n".join(txt)
    return res


def _get_signature_from_result_bundle(
    self: Any,
    system: dtfsyssyst.System,
    result_bundles: List[dtfcore.ResultBundle],
    add_system_config: bool,
    add_run_signature: bool,
) -> str:
    """
    Compute the signature of a test in terms of:

    - system signature
    - run signature
    - output dir signature
    """
    portfolio = system.portfolio
    dag_runner = system.dag_runner
    txt = []
    # 1) Compute system signature.
    hdbg.dassert(system.is_fully_built)
    if add_system_config:
        # TODO(gp): Use check_system_config.
        txt.append(hprint.frame("system_config"))
        txt.append(str(system.config))
    # 2) Compute run signature.
    if add_run_signature:
        # TODO(gp): This should be factored out.
        txt.append(hprint.frame("compute_run_signature"))
        hdbg.dassert_isinstance(result_bundles, list)
        result_bundle = result_bundles[-1]
        # result_bundle.result_df = result_bundle.result_df.tail(40)
        # Check output.
        forecast_evaluator_from_prices_dict = system.config[
            "research_forecast_evaluator_from_prices"
        ].to_dict()
        txt_tmp = compute_run_signature(
            self,
            dag_runner,
            portfolio,
            result_bundle,
            forecast_evaluator_from_prices_dict,
        )
        txt.append(txt_tmp)
    # 3) Compute the signature of the output dir.
    txt.append(hprint.frame("system_log_dir signature"))
    log_dir = system.config["system_log_dir"]
    txt_tmp = hunitest.get_dir_signature(
        log_dir, include_file_content=False, remove_dir_name=True
    )
    txt.append(txt_tmp)
    #
    actual = "\n".join(txt)
    # Remove the following line:
    # ```
    # db_connection_object: <connection object; dsn: 'user=aljsdalsd
    #   password=xxx dbname=oms_postgres_db_local
    #   host=cf-spm-dev4 port=12056', closed: 0>
    # ```
    actual = hunitest.filter_text("db_connection_object", actual)
    actual = hunitest.filter_text("log_dir:", actual)
    actual = hunitest.filter_text("trade_date:", actual)
    return actual


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


def get_portfolio_signature(
    self, portfolio, num_periods: int = 10
) -> Tuple[str, pd.Series]:
    actual = ["\n# portfolio signature=\n"]
    actual.append(str(portfolio))
    actual = "\n".join(actual)
    statistics = portfolio.get_historical_statistics(num_periods=num_periods)
    pnl = statistics["pnl"]
    _LOG.debug("pnl=\n%s", pnl)
    return actual, pnl


def compute_run_signature(
    self,
    dag_runner: dtfcore.DagRunner,
    portfolio: oms.Portfolio,
    result_bundle: dtfcore.ResultBundle,
    forecast_evaluator_from_prices_dict: Dict[str, Any],
) -> str:
    hdbg.dassert_isinstance(result_bundle, dtfcore.ResultBundle)
    # Check output.
    actual = []
    #
    events = dag_runner.events
    actual.append(get_events_signature(self, events))
    signature, pnl = get_portfolio_signature(self, portfolio)
    actual.append(signature)
    signature, research_pnl = get_research_pnl_signature(
        self,
        result_bundle,
        forecast_evaluator_from_prices_dict,
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
        corr_samples = min(tail, pnl1.size)
        pnl2 = pd.Series(research_pnl.tail(corr_samples).values)
        _LOG.debug("research pnl=\n%s", pnl2)
        correlation = pnl1.corr(pnl2)
        actual.append("\n# pnl agreement with research pnl\n")
        actual.append(f"corr = {correlation:.3f}")
        actual.append(f"corr_samples = {corr_samples}")
    actual = "\n".join(map(str, actual))
    return actual


def get_research_pnl_signature(
    self,
    result_bundle: dtfcore.ResultBundle,
    forecast_evaluator_from_prices_dict: Dict[str, Any],
) -> Tuple[str, pd.Series]:
    hdbg.dassert_isinstance(result_bundle, dtfcore.ResultBundle)
    # TODO(gp): @all use actual.append(hprint.frame("system_config"))
    #  to separate the sections of the output.
    actual = ["\n# forecast_evaluator_from_prices signature=\n"]
    hdbg.dassert(
        forecast_evaluator_from_prices_dict,
        "`forecast_evaluator_from_prices_dict` must be nontrivial",
    )
    forecast_evaluator = dtfmod.ForecastEvaluatorFromPrices(
        **forecast_evaluator_from_prices_dict["init"],
    )
    result_df = result_bundle.result_df
    _LOG.debug("result_df=\n%s", hpandas.df_to_str(result_df))
    #
    signature = forecast_evaluator.to_str(
        result_df,
        style=forecast_evaluator_from_prices_dict["style"],
        **forecast_evaluator_from_prices_dict["kwargs"],
    )
    _LOG.debug("signature=\n%s", signature)
    actual.append(signature)
    #
    _, _, _, _, stats = forecast_evaluator.compute_portfolio(
        result_df,
        style=forecast_evaluator_from_prices_dict["style"],
        **forecast_evaluator_from_prices_dict["kwargs"],
    )
    research_pnl = stats["pnl"]
    actual = "\n".join(map(str, actual))
    return actual, research_pnl


def check_system_config(self: Any, system: dtfsyssyst.System, tag: str) -> None:
    txt = []
    tag = "system_config." + tag
    txt.append(hprint.frame(tag))
    # Ensure that the System was built and thus the config is stable.
    hdbg.dassert(system.is_fully_built)
    txt.append(str(system.config))
    txt = "\n".join(txt)
    txt = hunitest.filter_text("db_connection_object", txt)
    txt = hunitest.filter_text("log_dir:", txt)
    txt = hunitest.filter_text("trade_date:", txt)
    # Sometimes we want to check that the config has not changed, but it
    # was just reordered. In this case we
    # - set `sort=True`
    # - make sure that there are no changes
    # - set `sort=False`
    # - update the golden outcomes with the updated config
    # TODO(gp): Do not commit `sort = True`.
    # sort = True
    sort = False
    self.check_string(txt, tag=tag, purify_text=True, sort=sort)


def check_portfolio_state(
    self: Any, system: dtfsyssyst.System, expected_last_timestamp: pd.Timestamp
) -> None:
    """
    Check some high level property of the Portfolio, e.g.,

    - It contains data up to a certain `expected_last_timestamp`
    - It is not empty at the end of the simulation
    """
    portfolio = system.portfolio
    # 1) The simulation runs up to the right time.
    last_timestamp = portfolio.get_last_timestamp()
    self.assert_equal(str(last_timestamp), str(expected_last_timestamp))
    # 2) The portfolio has some holdings.
    has_no_holdings = portfolio.has_no_holdings()
    self.assertFalse(has_no_holdings)
