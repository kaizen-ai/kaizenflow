# # #############################################################################
# # Reconciliation
# # #############################################################################
#
# """
# Import as:
#
# import helpers.lib_tasks_docker as hlitadoc
# """
#
# import logging
# from typing import Any, Dict, List, Match, Optional
#
# from invoke import task
#
#
# _LOG = logging.getLogger(__name__)
#
# PROD_RECONCILIATION_DIR = "/share/..."
#
#
#
# @task
# def reconcile_dump_prod_data(ctx, account_type, incremental=False):  # type: ignore
#     """
#     Dump prod data for the last run in the given account_type.
#     """
#     hdbg.dassert_in(account_type, ("live_trading", "candidate", "qa"))
#     file_name = f"prod_submissions.{account_type}.log"
#     if incremental and os.path.exists(file_name):
#         _LOG.warning("Reusing existing %s", file_name)
#     else:
#         # TODO(gp): Dump data.
#     #
#     hdbg.dassert_is_not(job_id, None)
#     hdbg.dassert_is_integer(job_id)
#     _LOG.info(hprint.to_str("job_id"))
#     #
#     today = datetime.date.today()
#     if today != run_date:
#         _LOG.warning("Running this script not on the trade date" +
#             hprint.to_str("today run_date"))
#     # > TARGET_DIR={PROD_RECONCILIATION_DIR}/$RUN_DATE/
#     target_dir = f"{PROD_RECONCILIATION_DIR}/{run_date}/job.{account_type}.{job_id}"
#     _LOG.info(hprint.to_str("target_dir"))
#     hio.create_dir(target_dir, incremental=True)
#     # > RUN_DATE=20220914
#     # > JOBID=1002436514
#     # > TARGET_DIR={PROD_RECONCILIATION_DIR}/$RUN_DATE/job.$JOBID; echo $TARGET_DIR
#     if account_type in ("live_trading", "candidate"):
#         ...
#     elif account_type == "qa":
#         ...
#     else:
#         raise ValueError(f"Invalid account_type='{account_type}'")
#     cmd += f" 2>&1 | tee {file_name}"
#     _system(cmd)
#     #
#     cmd = f"ls -lh {target_dir}"
#     _system(cmd)
#     # TODO(gp): It doesn't always work. Maybe we should save it locally and then
#     #  cp as user.
#     #_system(cmd)
#
#
# def _dassert_is_date(date: str) -> None:
#     hdbg.dassert_isinstance(date, str)
#     try:
#         _ = datetime.datetime.strptime(date, "%Y%m%d")
#     except ValueError as e:
#         raise ValueError(f"date='{date}' doesn't have the right format: {e}")
#
#
# import pwd
#
#
# def get_username() -> str:
#     return pwd.getpwuid(os.getuid())[0]
#
#
# def is_owned_by_user(file_name: str) -> bool:
#     hdbg.dassert_path_exists(file_name)
#     user_owner = pwd.getpwuid(os.stat(file_name).st_uid).pw_name
#     return get_username() == user_owner
#
#
# def _get_run_date(run_date: Optional[str]) ->  str:
#     if run_date is None:
#         run_date = datetime.date.today().strftime("%Y%m%d")
#     _LOG.info(hprint.to_str("run_date"))
#     _dassert_is_date(run_date)
#     return run_date
#
#
# @task
# def reconcile_dump_market_data(ctx, incremental=False, interactive=True):  # type: ignore
#     """
#     Dump the market data image and save it in the proper dir.
#     """
#     _ = ctx
#     target_file = "test_save_data.csv.gz"
#     if incremental and os.path.exists(target_file):
#         _LOG.warning("Skipping generating %s", target_file)
#     else:
#         docker_cmd = "pytest ./dataflow_orange/system/C1/test/test_C1f_forecast_system.py::Test_NonTime_ForecastSystem_C1f_ProdReconciliation::test_save_EOD_data1"
#         #docker_cmd += " -s --dbg"
#         cmd = f"invoke docker_cmd --cmd '{docker_cmd}'"
#         _system(cmd)
#     hdbg.dassert_file_exists(target_file)
#     #
#     # Check the target_file.
#     cmd = f"gzip -cd {target_file} | head -3"
#     _system(cmd)
#     cmd = f"gzip -cd {target_file} | tail -3"
#     _system(cmd)
#     if interactive:
#         question = "Is the file ok?"
#         hsystem.query_yes_no(question)
#     # > RUN_DATE=20220914
#     today = datetime.date.today()
#     today = today.strftime("%Y%m%d")
#     _LOG.info(hprint.to_str("today"))
#     # > TARGET_DIR={PROD_RECONCILIATION_DIR}/$RUN_DATE/
#     target_dir = f"{PROD_RECONCILIATION_DIR}/{today}"
#     hio.create_dir(target_dir, incremental=True)
#     #
#     cmd = f"ls {target_dir}"
#     _system(cmd)
#     #
#     # > TARGET_FILE=$TARGET_DIR/test_save_data.as_of_$(timestamp).csv.gz
#     timestamp = hlitauti.get_ET_timestamp()
#     src_file = target_file
#     dst_file = os.path.join(target_dir, f"{target_file}.as_of_{timestamp}.tgz")
#     #
#     cmd = f"cp -a {src_file} {dst_file}"
#     _system(cmd)
#     cmd = f"chmod -R -w {dst_file}"
#     _system(cmd)
#     # Sanity check remote data.
#     cmd = f"gzip -cd {dst_file} | head -3"
#     _system(cmd)
#     cmd = f"gzip -cd {dst_file} | tail -3"
#     _system(cmd)
#     cmd = f"gzip -cd {dst_file} | wc -l"
#     _system(cmd)
#     cmd = f"ls -lh {dst_file}"
#     _system(cmd)
#
#
# # TODO(gp): Move it to a more general library.
# @task
# def backup_file(ctx, file_name, action="move", dst_dir=None, timestamp=None, mark_as_read_only=True,
#                 abort_on_missing=True):  # type: ignore
#     """
#     Backup a file using a timestamp.
#
#     :param action: `move` or `copy`
#     :param dst_dir: destination dir for the backup. If `None` save the file in the
#         same dir as `file_name`
#     :param timestamp: use the given timestamp or get a new one if `None`
#     :param abort_on_missing: abort if the source file doesn't exist
#     """
#     _ = ctx
#     hdbg.dassert_in(action, ("move", "copy"))
#     # Create target file.
#     if timestamp is None:
#         timestamp = hlitauti.get_ET_timestamp()
#     #
#     suffix = f".backup.{timestamp}"
#     dst_file_name = hio.add_suffix_to_filename(file_name, suffix, with_underscore=False)
#     if dst_dir is not None:
#         # The destination file is in `dst_dir`.
#         hdbg.dassert_isinstance(dst_dir, str)
#         base_name = os.path.basename(file_name)
#         hio.create_dir(dst_dir, incremental=True)
#         dst_file_name = os.path.join(dst_dir, base_name)
#     _LOG.info(hprint.to_str("dst_file_name"))
#     hdbg.dassert_path_not_exists(dst_file_name)
#     # Check if exists.
#     if abort_on_missing:
#         hdbg.dassert_path_exists(file_name)
#     # Rename the file.
#     if os.path.exists(file_name):
#         if action == "move":
#             cmd = "mv"
#         elif action == "copy":
#             cmd = "cp -a"
#         else:
#             raise ValueError(f"Invalid action='{action}'")
#         cmd = f"{cmd} {file_name} {dst_file_name}"
#         _system(cmd)
#         _LOG.info("Backed up '%s' as '%s'", file_name, dst_file_name)
#         if mark_as_read_only:
#             cmd = f"chmod -w -R {dst_file_name}"
#             _system(cmd)
#     else:
#         _LOG.warning("File '%s' doesn't exist: skipping", file_name)
#     return dst_file_name
#
#
# def delete_file(file_name):
#     # TODO(gp): Do not allow to delete dir with Git or current dir.
#     #hdbg.dassert_ne(file_name, ".")
#     if os.path.exists(file_name):
#         cmd = f"chmod +w {file_name}; rm -rf {file_name}"
#         if not is_owned_by_user(file_name):
#             cmd = f"sudo bash -c '{cmd}'"
#         hsystem.system(cmd)
#
#
# @task
# def reconcile_save_sim(ctx, run_date=None, timestamp=None):  # type: ignore
#     """
#     Copy the output of the simulation in the proper dir.
#     """
#     if timestamp is None:
#         timestamp = hlitauti.get_ET_timestamp()
#     #
#     run_date = _get_run_date(run_date)
#     target_dir = f"{PROD_RECONCILIATION_DIR}/{run_date}"
#     _LOG.info("Saving results to '%s'", target_dir)
#     # If the target dir doesn't exist we didn't downloaded the test data and we can't
#     # continue.
#     hdbg.dassert_dir_exists(target_dir)
#     # Save results to the destination dir.
#     log_file = "pytest_log.txt"
#     file_names_to_backup = [log_file, "system_log_dir"]
#     for file_name in file_names_to_backup:
#         hdbg.dassert_path_exists(file_name)
#         backup_file(ctx, file_name, action="copy", timestamp=timestamp, dst_dir=target_dir,
#                     abort_on_missing=True)
#
#
# @task
# def reconcile_run_sim(ctx, run_date=None, action_before="backup"):  # type: ignore
#     """
#     Run reconciliation simulation for `run_date`.
#
#     :param action_before: action to perform (e.g., `backup` or `delete`)
#         for the files that should be overwritten by the simulation (e.g.,
#         test_save_data.csv.gz, log.txt, system_log_dir)
#     """
#     hdbg.dassert_in(action_before, ("backup", "delete"))
#     timestamp = hlitauti.get_ET_timestamp()
#     #
#     run_date = _get_run_date(run_date)
#     target_dir = f"{PROD_RECONCILIATION_DIR}/{run_date}"
#     _LOG.info(hprint.to_str("target_dir"))
#     # If the target dir doesn't exist we didn't downloaded the test data and we can't
#     # continue.
#     hdbg.dassert_dir_exists(target_dir)
#     # Copy the test_save_data for today simulation locally.
#     file_name = "test_save_data.csv.gz"
#     if action_before == "delete":
#         delete_file(file_name)
#     elif action_before == "backup":
#         backup_file(ctx, file_name, timestamp=timestamp, abort_on_missing=False)
#     else:
#         raise ValueError(f"Invalid action_before='{action_before}'")
#     #
#     cmd = f"find {target_dir} -name '{file_name}*' -printf '%p\n' | sort -r | head -1"
#     _, src_file_name = hsystem.system_to_string(cmd)
#     hdbg.dassert_eq(len(src_file_name.split("\n")), 1)
#     src_file_name = src_file_name.split("\n")[0]
#     _LOG.info(hprint.to_str("src_file_name"))
#     hdbg.dassert_file_exists(src_file_name)
#     # Check the market data.
#     cmd = f"cp -a {src_file_name} {file_name}"
#     hsystem.system(cmd)
#     #
#     cmd = f"ls -lh {file_name}"
#     _system(cmd)
#     # Backup data locally if needed.
#     log_file = "pytest_log.txt"
#     file_names_to_backup = [log_file, "system_log_dir"]
#     for file_name in file_names_to_backup:
#         if action_before == "delete":
#             delete_file(file_name)
#         elif action_before == "backup":
#             backup_file(ctx, file_name, timestamp=timestamp,
#                         abort_on_missing=False)
#         else:
#             raise ValueError(f"Invalid action_before='{action_before}'")
#     # Run simulation.
#     opts = "-s --dbg"
#     test_name = "./dataflow_lime/system/C1/test/test_C1f_forecast_system.py::Test_Time_ForecastSystem_with_DataFramePortfolio_C1f_ProdReconciliation::test1"
#     docker_cmd = f"pytest {test_name} {opts} 2>&1 | tee {log_file}"
#     docker_cmd = f"AM_RECONCILE_SIM_DATE={run_date} {docker_cmd}"
#     #docker_cmd += "; exit ${PIPESTATUS[0]})"
#     cmd = f"invoke docker_cmd --cmd '{docker_cmd}'"
#     _system(cmd)
#
#
# @task
# def reconcile_dump_tca_data(ctx, run_date=None):  # type: ignore
#     """
#     Retrieve and save the TCA data.
#     """
#     _ = ctx
#     run_date = _get_run_date(run_date)
#     target_dir = f"{PROD_RECONCILIATION_DIR}/{run_date}"
#     _LOG.info(hprint.to_str("target_dir"))
#     hio.create_dir(target_dir, incremental=True)
#     # TODO(gp): run fills downloading script.
#     cmd = f"aws s3 cp --recursive {src_dir} {dst_dir}"
#     _system(cmd)
#     #
#     cmd = f"ls -lh {target_dir}"
#     _system(cmd)
#     cmd = f"du -d 1 -h {target_dir}"
#     _system(cmd)
#
#
# @task
# def reconcile_run_all(ctx, incremental=False):  # type: ignore
#     """
#     Run all phases for reconciling a prod run.
#
#     - dump market data
#     - dump prod live trading and candidate
#     - run simulation
#     - run notebook
#     """
#     reconcile_dump_market_data(ctx, incremental=incremental)
#     #
#     account_type = "live_trading"
#     reconcile_dump_prod_data(ctx, account_type, incremental=incremental)
#     #
#     account_type = "candidate"
#     reconcile_dump_prod_data(ctx, account_type, incremental=incremental)
#     #
#     # TODO(gp): Download for the day before.
#     #reconcile_dump_tca_data(ctx, run_date=None)
#     reconcile_ls(ctx, run_date=None)
#
#
# @task
# def reconcile_ls(ctx, run_date=None):  # type: ignore
#     """
#     Run `ls` on the dir containing the reconciliation data.
#     """
#     _ = ctx
#     run_date = _get_run_date(run_date)
#     target_dir = f"{PROD_RECONCILIATION_DIR}/{run_date}"
#     _LOG.info(hprint.to_str("target_dir"))
#     hdbg.dassert_dir_exists(target_dir)
#     #
#     cmd = f"ls -lh {target_dir}"
#     _system(cmd)
#     cmd = f"du -d 1 -h {target_dir}"
#     _system(cmd)
#
#
# @task
# def reconcile_rmrf(ctx, run_date=None):  # type: ignore
#     run_date = _get_run_date(run_date)
#     reconcile_ls(ctx, run_date)
#     target_dir = f"{PROD_RECONCILIATION_DIR}/{run_date}"
#     _LOG.info(hprint.to_str("target_dir"))
#     cmd = f"chmod +w -R {target_dir}; rm -rf {target_dir}"
#     print(f"> {cmd}")