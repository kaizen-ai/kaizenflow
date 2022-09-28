"""
Import as:

import helpers.lib_tasks_docker as hlitadoc
"""

import logging
from typing import Any, Dict, List, Match, Optional

from invoke import task


_LOG = logging.getLogger(__name__)

# pylint: disable=protected-access
# #############################################################################
# Reconciliation
# #############################################################################


@task
def reconcile_dump_prod_data(ctx, account_type, incremental=False):  # type: ignore
    """
    Dump prod data for the last run in the given account_type.
    """
    hdbg.dassert_in(account_type, ("live_trading", "candidate", "qa"))
    file_name = f"prod_submissions.{account_type}.log"
    if incremental and os.path.exists(file_name):
        _LOG.warning("Reusing existing %s", file_name)
    else:
        cmd += f" 2>&1 | tee {file_name}"
    _LOG.debug("Reading file_name=%s", file_name)
    txt = hio.from_file(file_name)
    job_id = run_date = None
    found_daily_flow = False
    for line in txt.split("\n"):
        _LOG.debug(hprint.to_str("found_daily_flow line"))
        if not found_daily_flow:
            if "DailyFlow Ran Jobs:" in line:
                found_daily_flow = True
            continue
        if account_type == "live_trading":
        elif account_type == "candidate":
        elif account_type == "qa":
        else:
            raise ValueError(f"Invalid account_type='{account_type}'")
        if found:
            _LOG.debug("Extracting job id from line='%s'", line)
            data = line.split()
            _LOG.debug(hprint.to_str("data"))
            #        |    job_id        |   jobname            | asof                           | status          | duration       | trade_date |'
            hdbg.dassert_is(job_id, None)
            job_id = data[1]
            _LOG.debug(hprint.to_str("job_id"))
            job_id = int(job_id)
            run_date = pd.Timestamp(data[-2]).date().strftime("%Y%m%d")
            _LOG.debug(hprint.to_str("run_date"))
    #
    hdbg.dassert_is_not(job_id, None)
    hdbg.dassert_is_integer(job_id)
    _LOG.info(hprint.to_str("job_id"))
    #
    today = datetime.date.today()
    if today != run_date:
        _LOG.warning("Running this script not on the trade date" +
            hprint.to_str("today run_date"))
    # > TARGET_DIR=/share/data/prod_production/$RUN_DATE/
    target_dir = f"/share/data/prod_production/{run_date}/job.{account_type}.{job_id}"
    _LOG.info(hprint.to_str("target_dir"))
    hio.create_dir(target_dir, incremental=True)
    # > RUN_DATE=20220914
    # > JOBID=1002436514
    # > TARGET_DIR=/share/data/prod_production/$RUN_DATE/job.$JOBID; echo $TARGET_DIR
    cmd += f" 2>&1 | tee {file_name}"
    #
    cmd = f"ls -l {target_dir}"
    _system(cmd)
    # TODO(gp): It doesn't always work. Maybe we should save it locally and then
    #  cp as user.
    #_system(cmd)


@task
def reconcile_dump_market_data(ctx, incremental=False, interactive=True):  # type: ignore
    """
    Dump the market data image and save it in the proper dir.
    """
    _ = ctx
    target_file = "test_save_data.csv.gz"
    if incremental and os.path.exists(target_file):
        _LOG.warning("Skipping generating %s", target_file)
    else:
        docker_cmd = "pytest ./dataflow_lime/system/E8/test/test_E8f_forecast_system.py::Test_NonTime_ForecastSystem_E8f_ProdReconciliation::test_save_EOD_data1"
        #docker_cmd += " -s --dbg"
        cmd = f"invoke docker_cmd --cmd '{docker_cmd}'"
        _system(cmd)
    hdbg.dassert_file_exists(target_file)
    #
    # Check the target_file.
    cmd = f"gzip -cd {target_file} | head -3"
    _system(cmd)
    cmd = f"gzip -cd {target_file} | tail -3"
    _system(cmd)
    if interactive:
        question = "Is the file ok?"
        hsystem.query_yes_no(question)
    # > RUN_DATE=20220914
    today = datetime.date.today()
    today = today.strftime("%Y%m%d")
    _LOG.info(hprint.to_str("today"))
    # > TARGET_DIR=/share/data/prod_production/$RUN_DATE/
    target_dir = f"/share/data/prod_production/{today}"
    hio.create_dir(target_dir, incremental=True)
    #
    cmd = f"ls {target_dir}"
    _system(cmd)
    #
    # > TARGET_FILE=$TARGET_DIR/test_save_data.as_of_$(timestamp).csv.gz
    timestamp = hlitauti.get_ET_timestamp()
    src_file = target_file
    dst_file = os.path.join(target_dir, f"{target_file}.as_of_{timestamp}.tgz")
    #
    cmd = f"cp -a {src_file} {dst_file}"
    _system(cmd)
    cmd = f"chmod -R -w {dst_file}"
    _system(cmd)
    # Sanity check remote data.
    cmd = f"gzip -cd {dst_file} | head -3"
    _system(cmd)
    cmd = f"gzip -cd {dst_file} | tail -3"
    _system(cmd)
    cmd = f"gzip -cd {dst_file} | wc -l"
    _system(cmd)
    cmd = f"ls -l {dst_file}"
    _system(cmd)


@task
def reconcile_ls(ctx, run_date=None):  # type: ignore
    """
    Run `ls` on the dir containing the reconciliation data.
    """
    _ = ctx
    if run_date is None:
        run_date = datetime.date.today().strftime("%Y%m%d")
    _LOG.info(hprint.to_str("run_date"))
    target_dir = f"/share/data/prod_production/{run_date}"
    _LOG.info(hprint.to_str("target_dir"))
    #
    cmd = f"ls -l {target_dir}"
    _system(cmd)
    #
    cmd = f"du -d 1 -h {target_dir}"
    _system(cmd)


@task
def reconcile_run_all(ctx, incremental=False):  # type: ignore
    """
    Run all phases for reconciling a prod run.

    - dump market data
    - dump prod live trading and candidate
    - run simulation
    - run notebook
    """
    reconcile_dump_market_data(ctx, incremental=incremental)
    #
    account_type = "live_trading"
    reconcile_dump_prod_data(ctx, account_type, incremental=incremental)
    #
    account_type = "candidate"
    reconcile_dump_prod_data(ctx, account_type, incremental=incremental)
    #
    reconcile_ls(ctx, run_date=None)


@task
def reconcile_rmrf(ctx, run_date=None):  # type: ignore
    reconcile_ls(ctx, run_date)
    if run_date is None:
        run_date = datetime.date.today().strftime("%Y%m%d")
    _LOG.info(hprint.to_str("run_date"))
    target_dir = f"/share/data/prod_production/{run_date}"
    _LOG.info(hprint.to_str("target_dir"))
    cmd = f"chmod +w -R {target_dir}; rm -rf {target_dir}"
    cmd = f"cmd"
    print("> cmd")
