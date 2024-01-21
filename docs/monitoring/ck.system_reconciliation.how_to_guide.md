# System Reconciliation How to Guide

<!-- toc -->

- [Extract parameters to run the system reconciliation flow](#extract-parameters-to-run-the-system-reconciliation-flow)
  * [Rendered AirFlow template of a production run command](#rendered-airflow-template-of-a-production-run-command)
  * [Rendered AirFlow template of a system reconciliation run command](#rendered-airflow-template-of-a-system-reconciliation-run-command)
- [Run the reconciliation flow](#run-the-reconciliation-flow)
  * [Run the whole reconciliation flow](#run-the-whole-reconciliation-flow)
  * [Dump market data](#dump-market-data)
  * [Run production replayed time system simulation](#run-production-replayed-time-system-simulation)
  * [Run notebook](#run-notebook)
    + [Run the notebook manually](#run-the-notebook-manually)
- [Debug a failed system reconciliation run](#debug-a-failed-system-reconciliation-run)
  * [Master system reconciliation notebooks](#master-system-reconciliation-notebooks)

<!-- tocstop -->

# Extract parameters to run the system reconciliation flow

- See how to
  [access rendered template](/docs/monitoring/ck.monitor_system.how_to_guide.md#general-information)

## Rendered AirFlow template of a production run command

See a rendered template of a production run command in AirFlow when running the
system reconciliation flow for the first time.

E.g.:
```
"command": [
  "/app/amp/dataflow_amp/system/Cx/scripts/run_Cx_prod_system.py",
  "--strategy 'C1b'",
  "--dag_builder_ctor_as_str 'dataflow_orange.pipelines.C1.C1b_pipeline.C1b_DagBuilder'",
  "--trade_date 20231108",
  "--liveness 'CANDIDATE'",
  "--instance_type 'PROD'",
  "--exchange 'binance' ",
  "--stage 'preprod'",
  "--account_type 'trading'",
  "--secret_id '3'",
  "-v 'DEBUG' 2>&1",
  "--start_time '20231108_131000'",
  "--run_duration 86400",
  "--run_mode 'paper_trading'",
  "--log_file_name  '/data/shared/ecs/preprod/system_reconciliation/C1b/paper_trading/20231108_131000.20231109_130500/logs/log.scheduled.txt'",
  "--dst_dir '/data/shared/ecs/preprod/system_reconciliation/C1b/paper_trading/20231108_131000.20231109_130500/system_log_dir.scheduled'"
  "--set_config_value '(\"process_forecasts_node_dict\",\"process_forecasts_dict\",\"order_config\",\"passivity_factor\"),(float(0.55))'" \
  "--set_config_value '(\"process_forecasts_node_dict\",\"process_forecasts_dict\",\"optimizer_config\",\"params\",\"style\"),(str(\"longitudinal\"))'"
]
```

Note that `--set_config_value` should contain the full nested key to the value.

## Rendered AirFlow template of a system reconciliation run command

See a rendered template of a system reconciliation run command in AirFlow when
re-running the flow.

Parameters that are gotten from `--dst_dir`:

- `--prod-data-source-dir`, e.g.,
  `/data/shared/ecs/preprod/system_reconciliation/`
- `--start-timestamp-as-str`, e.g., `.../20231108_131000.20231109_130500/...` ->
  `20231108_131000`
- `--end-timestamp-as-str`, e.g., `.../20231108_131000.20231109_130500/...` ->
  `20231109_130500`
- `--mode`, e.g., `.../system_log_dir.scheduled` -> `scheduled`

E.g.:

```bash
"command": [
  "mkdir /.dockerenv",
  "&&",
  "invoke reconcile_run_all",
  "--dag-builder-ctor-as-str 'dataflow_orange.pipelines.C1.C1b_pipeline.C1b_DagBuilder'",
  "--dst-root-dir /data/shared/ecs/preprod/prod_reconciliation/",
  "--prod-data-source-dir /data/shared/ecs/preprod/system_reconciliation/",
  "--start-timestamp-as-str 20231108_131000",
  "--end-timestamp-as-str 20231109_130500",
  "--mode scheduled",
  "--run-mode 'paper_trading'",
  "--no-prevent-overwriting",
  "--run-notebook",
  "--mark-as-last-24-hour-run",
  "--set-config-values '(\"process_forecasts_node_dict\",\"process_forecasts_dict\",\"order_config\",\"passivity_factor\"),(float(0.55));\("process_forecasts_node_dict\",\"process_forecasts_dict\",\"optimizer_config\",\"params\",\"style\"),(str(\"longitudinal\"))'"
]
```

# Run the reconciliation flow

Run the invokes from `orange` from a docker container, i.e. run
`invoke docker_bash` first:

```bash
> (amp.client_venv) ninat@dev1:~/src/orange1 invoke docker_bash
```

## Run the whole reconciliation flow

Use [reconcile_run_all()](/oms/lib_tasks_reconcile.py#L970) to run the workflow
end-to-end.

If the production system was run with `--set-config-values`, it also should be
passed to the invoke.

E.g.:

```bash
> invoke reconcile_run_all \
    --dag-builder-ctor-as-str "dataflow_orange.pipelines.C1.C1b_pipeline.C1b_DagBuilder" \
    --run-mode "paper_trading" \
    --start-timestamp-as-str "20230828_130500" \
    --end-timestamp-as-str "20230829_131000" \
    --mode "scheduled" \
    --prod-data-source-dir ".../system_reconciliation" \
    --dst-root-dir ".../prod_reconciliation" \
    --no-prevent-overwriting \
    --run-notebook \
    --mark-as-last-24-hour-run \
    --set-config-values '("process_forecasts_node_dict","process_forecasts_dict","order_config","passivity_factor"),(float(0.55));("process_forecasts_node_dict","process_forecasts_dict","optimizer_config","params","style"),(str("longitudinal"))'
```

Look for the param description in the
[system reconciliation explanation document](/docs/monitoring/ck.monitor_system.how_to_guide.md#paper-trading-system-dag).

## Dump market data

Use [`reconcile_dump_market_data`](/oms/lib_tasks_reconcile.py#L221) to dump
market data for a certain period.

E.g.:

```bash
> invoke reconcile_dump_market_data \
    --dag-builder-ctor-as-str "dataflow_orange.pipelines.C1.C1b_pipeline.C1b_DagBuilder" \
    --run-mode "paper_trading" \
    --start-timestamp-as-str "20230828_130500" \
    --end-timestamp-as-str "20230829_131000" \
    --dst-root-dir ".../prod_reconciliation/"
```

## Run production replayed time system simulation

Use [reconcile_run_sim()](/oms/lib_tasks_reconcile.py#L319) to run the
simulation between a given interval.

If the reconciliation flow was run with `--set-config-values`, it also should be
passed to the invoke.

```bash
> invoke reconcile_run_sim \
    --dag-builder-ctor-as-str "dataflow_orange.pipelines.C1.C1b_pipeline.C1b_DagBuilder" \
    --run-mode "paper_trading" \
    --start-timestamp-as-str "20230828_130500" \
    --end-timestamp-as-str "20230829_131000" \
    --mode "scheduled" \
    --dst-root-dir ".../prod_reconciliation/" \
    --set-config-values '("process_forecasts_node_dict","process_forecasts_dict","order_config","passivity_factor"),(float(0.55));("process_forecasts_node_dict","process_forecasts_dict","optimizer_config","params","style"),(str("longitudinal"))'
```

## Run notebook

Use [reconcile_run_notebook()](/oms/lib_tasks_reconcile.py#L540) to run the
system reconciliation notebook, publish it locally and copy the results.

If the reconciliation flow was run with `--set-config-values`, it also should be
passed to the invoke.

E.g.:

```bash
> invoke reconcile_run_notebook
    --notebook-run-mode "fast" \
    --dag-builder-ctor-as-str {dag_builder_ctor_as_str} \
    --dag-builder-ctor-as-str "dataflow_orange.pipelines.C1.C1b_pipeline.C1b_DagBuilder" \
    --run-mode "paper_trading" \
    --start-timestamp-as-str "20230828_130500" \
    --end-timestamp-as-str "20230829_131000" \
    --mode "scheduled" \
    --set-config-values '("process_forecasts_node_dict","process_forecasts_dict","order_config","passivity_factor"),(float(0.55));("process_forecasts_node_dict","process_forecasts_dict","optimizer_config","params","style"),(str("longitudinal"))'
```

### Run the notebook manually

Update parameters under "if-else" condition in "Build the reconciliation config"
section to run the notebook manually. If the system reconciliation flow was run
with `set_config_values`, it should be defined as a string, i.e. the same way as
in the command.

E.g.:

```python
set_config_values = '("process_forecasts_node_dict","process_forecasts_dict","order_config","passivity_factor"),(float(0.55));("process_forecasts_node_dict","process_forecasts_dict","optimizer_config","params","style"),(str("longitudinal"))'
```

# Debug a failed system reconciliation run

See
[how to access logs](/docs/monitoring/ck.monitor_system.how_to_guide.md#how-to-identify-a-reason-for-a-failure).

## Master system reconciliation notebooks

A failed notebook is published without any updates, i.e. as it is in the branch.

A notebook reconciliation directory contains:

- A log file that contains a config
- A notebook result directory

E.g.:

```bash
> ls .../reconciliation_notebook/fast/
log.20231110-105122.txt  result_0
```

The notebook result directory contains:

- HTML version of the run notebook
- The run notebook
- Pickled and txt config
- Log text version, e.g., `run_notebook.0.log`, contains logs written by
  `run_notebook.py` script
- Log HTML version, e.g., `run_notebook.0.html.log`, contains a link to the
  published notebook
- File that marks run as a success (in case of successful run)

E.g.:
```
> ls .../reconciliation_notebook/fast/result_0
Master_system_reconciliation_fast.0.20231110-105736.html  Master_system_reconciliation_fast.0.ipynb  config.pkl  config.txt  run_notebook.0.html.log  run_notebook.0.log  success.txt
```

If `success.txt` is missing, notebook run failed. Check
`.../reconciliation_notebook/fast/result_0/run_notebook.0.log` to identify an
error. It also contains an error if the failure is inside the notebook: a failed
cell code, a traceback and an error output.

E.g.:
```
------------------
# Run for all timestamps.
bar_timestamp = "all"
# Compare DAG output at T with itself at time T-1.
dtfcore.check_dag_output_self_consistency(
    dag_path_dict["prod"],
    dag_node_names[-1],
    bar_timestamp,
    trading_freq=config["meta"]["bar_duration"],
    diff_threshold=diff_threshold,
    **compare_dfs_kwargs,
)
------------------

...

AssertionErrorFailed assertion
################################################################################
* Failed assertion *
25.010538642299124 <= 0.001
Comparison failed for node=predict.8.process_forecasts for current_timestamp=2023-11-06 11:25:00-05:00 and previous_timestamp=2023-11-06 11:20:00-05:00
################################################################################
```
