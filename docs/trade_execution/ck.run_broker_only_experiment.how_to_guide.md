

<!-- toc -->

- [How-to guide description](#how-to-guide-description)
- [Example command](#example-command)
- [Logging the experiment](#logging-the-experiment)
  * [Updating the Algo Execution Log](#updating-the-algo-execution-log)
  * [Logging notebooks](#logging-notebooks)

<!-- tocstop -->

<!--ts-->

- [How-to guide description](#how-to-guide-description)
- [Example command](#example-command)
- [Logging the experiment](#logging-the-experiment)
  - [Updating the Algo Execution Log](#updating-the-algo-execution-log)
  - [Logging notebooks](#logging-notebooks)

<!--te-->

# How-to guide description

This guide contains step-by-step instructions on running the broker-only
experiment.

# Example command

- Both commands should be run from inside docker (`> i docker_bash`)

This command will run the broker for 30 minutes, for 10 assets, for Danya's
Binance account, with maximum parent order cost of 100 USDT.

```bash
docker> oms/broker/ccxt/scripts/run_ccxt_broker.py \
      --log_dir /shared_data/CMTask5541_experiment_1 \
      --max_parent_order_notional 100 \
      --randomize_orders \
      --parent_order_duration_in_min 5 \
      --num_bars 6 \
      --num_parent_orders_per_bar 10 \
      --secret_id 4 \
      --clean_up_before_run \
      --clean_up_after_run \
      --passivity_factor 0.55 \
      --child_order_execution_freq 1T \
      --include_btc_usdt True \
      --volatility_multiple 0.75 0.7 0.6 0.8 1.0 \
      -v DEBUG
```

This command will run the broker by replaying the exchange behaviour by using
the logs from the `replayed_dir`.

```bash
docker> oms/broker/ccxt/scripts/run_replayed_ccxt_broker.py \
    --log_dir /shared_data/Replay_experiment_1 \
    --replayed_dir /shared_data/CMTask5541_experiment_1 \
    --secret_id 4 \
    --universe 'v7.4' \
    -v DEBUG
```

# Logging the experiment

- Run the following notebooks:
  - `oms/notebooks/Master_broker_debugging.ipynb`
  - `oms/notebooks/Master_execution_analysis.ipynb`
  - `oms/notebooks/Master_bid_ask_execution_analysis.ipynb`

- Before running a notebook, change the `system_log_dir` variable to the value
  of `log_dir` without committing the change.
- Update the
  [Algo Execution Log](https://docs.google.com/document/d/18xwzmdIuOKaXcUEN8extU8v1KdBN-krFkUJKxS6wrvg/edit)

## Updating the Algo Execution Log

- Log data is generated automatically in the location mirroring the location of
  the execution script.
  - E.g. `oms/broker/ccxt/scripts/run_ccxt_broker.py.log`
- In the **short description**, describe the parameters you used to run the
  command.
- In **Start/end time (UTC)** use the log timestamp of the first message like:

```python
# None: wall_clock_time=2023-10-05 07:40:00.078250-04:00: done waiting
```

this represents the actual start of the trade execution.

- In **Pre-run/post-run balance (USDT)**, provide data for the balance from the
  logs. For pre-run, search for a message in the beginning of the log that looks
  like:

```python
7:36:38 - [36mINFO [0m ccxt_broker_utils.py save_to_json_file_and_log:90      07:36:38 - [36mINFO [0m ccxt_broker_utils.py save_to_json_file_and_log:90      {'BNB': 0.0,
 'BTC': 0.0,
 'BUSD': 0.0,
 'ETH': 0.0,
 'TUSD': 0.0,
 'USDC': 0.0,
 'USDP': 0.0,
 'USDT': 6090.5617341,
 'XRP': 0.0}
```

For post-run balance, check the same-looking message at the end of the log.

## Logging notebooks

- After running each notebook end-to-end, run the command:
  `> dev_scripts/notebooks/publish_notebook.py --action publish --file {notebook_file_name}`

and post the output under the corresponding subheader.

- If the notebook does not run end-to-end, some cells fail, or some soft checks
  in the `Master_broker_debugging.ipynb` are not passing, post the description
  of the problem under **Notes** section and file an issue for them, tagging
  members of the Research team.
