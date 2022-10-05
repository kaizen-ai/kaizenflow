# #############################################################################
# Reconciliation
# #############################################################################

# Logically the reconciliation process:
# 1) Run prod system
# 2) Dump market data
# 3) Run sim system
# 4) Dump TCA data
# 5) Run notebook and publish it
#
# User specifies start and end of prod run and everything gets derived from those
# dates.

"""
Import as:

import dev_scripts.lib_tasks_reconcile as dslitare
"""

import datetime
import logging
import os
from typing import Optional

from invoke import task

import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hprint as hprint
import helpers.hsystem as hsystem

_LOG = logging.getLogger(__name__)

_PROD_RECONCILIATION_DIR = "/data/shared/prod_reconciliation"


def _system(cmd):
    return hsystem.system(cmd, suppress_output=False, log_level="echo")


def _dassert_is_date(date: str) -> None:
    hdbg.dassert_isinstance(date, str)
    try:
        _ = datetime.datetime.strptime(date, "%Y%m%d")
    except ValueError as e:
        raise ValueError(f"date='{date}' doesn't have the right format: {e}")


def _get_run_date(run_date: Optional[str]) -> str:
    if run_date is None:
        run_date = datetime.date.today().strftime("%Y%m%d")
    _LOG.info(hprint.to_str("run_date"))
    _dassert_is_date(run_date)
    return run_date


@task
def reconcile_create_dirs(ctx, run_date=None):  # type: ignore
    """
    Create dirs for storing reconciliation data. Final dirs layout is: ```
    data/ shared/ prod_reconciliation/

    {run_date}/                 prod/                 ... simulation/
    ```
    """
    _ = ctx
    run_date = _get_run_date(run_date)
    # Create run date dir.
    run_date_dir = os.path.join(_PROD_RECONCILIATION_DIR, run_date)
    hio.create_dir(run_date_dir, incremental=True)
    # Create dirs for storing prod and simulation results.
    prod_dir = os.path.join(run_date_dir, "prod")
    simulation_dir = os.path.join(run_date_dir, "simulation")
    hio.create_dir(prod_dir, incremental=True)
    hio.create_dir(simulation_dir, incremental=True)
    # Sanity check the created dirs.
    cmd = f"ls -lh {run_date_dir}"
    _system(cmd)


# pylint: disable=line-too-long
# > rm -r system_log_dir/; pytest_log ./dataflow_orange/system/C1/test/test_C1b_prod_system.py::Test_C1b_Time_ForecastSystem_with_DataFramePortfolio_ProdReconciliation::test1 -s --dbg --update_outcomes
# pylint: enable=line-too-long
@task
def reconcile_run_sim(ctx, run_date=None):  # type: ignore
    """
    Run reconciliation simulation for `run_date`.
    """
    _ = ctx
    run_date = _get_run_date(run_date)
    # target_dir = f"{_PROD_RECONCILIATION_DIR}/{run_date}/simulation"
    # _LOG.info(hprint.to_str("target_dir"))
    # If the target dir doesn't exist we didn't downloaded the test data and we can't
    # continue.
    # Remove local dir with system logs.
    # TODO(Grisha): @Dan Pass target dir as a param.
    target_dir = "system_log_dir"
    if os.path.exists(target_dir):
        rm_cmd = f"rm -rf {target_dir}"
        _system(rm_cmd)
    # Run simulation.
    opts = "-s --dbg --update_outcomes"
    # pylint: disable=line-too-long
    test_name = "dataflow_orange/system/C1/test/test_C1b_prod_system.py::Test_C1b_Time_ForecastSystem_with_DataFramePortfolio_ProdReconciliation::test_run_simulation"
    # pylint: enable=line-too-long
    docker_cmd = f"AM_RECONCILE_SIM_DATE={run_date} pytest_log {test_name} {opts}"
    # docker_cmd += "; exit ${PIPESTATUS[0]})"
    cmd = f"invoke docker_cmd --cmd '{docker_cmd}'"
    _system(cmd)
    # Check that system log dir exists and is filled.
    hdbg.dassert_dir_exists(os.path.join(target_dir, "dag"))
    hdbg.dassert_dir_exists(os.path.join(target_dir, "process_forecasts"))
