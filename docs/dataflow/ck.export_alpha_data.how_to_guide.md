# Exporting alpha data. How to guide

<!-- toc -->

- [Overview](#overview)
- [How to configure the notebook](#how-to-configure-the-notebook)
  * [Process](#process)
  * [Config example](#config-example)
- [How to sanity check the results](#how-to-sanity-check-the-results)
  * [Research PnL self-consistency](#research-pnl-self-consistency)
  * [Research PnL with prices from the real-time Database](#research-pnl-with-prices-from-the-real-time-database)
  * [Reconstruct PnL from target positions](#reconstruct-pnl-from-target-positions)
- [How to save the results](#how-to-save-the-results)
- [How to load the results](#how-to-load-the-results)

<!-- tocstop -->

# Overview

- The notebook
  [`dataflow/model/notebooks/Master_save_pnl_and_trades.ipynb`](https://github.com/cryptokaizen/cmamp/blob/master/dataflow/model/notebooks/Master_save_pnl_and_trades.ipynb)

  - Loads results of a research backtest
  - Computes research portfolio and performance metrics
  - Performs prices and PnL cross-checks
  - Saves trades and PnL to a file

- In order to analyze research backtest results one needs to:
  - Save research backtest results to an accesible directory
    - E.g.
      `/shared_data/model/historical/build_tile_configs.C5b.ccxt_v7_4-all.5T.2022-10-01_2023-10-18.ins.run0`
  - Configure and run the notebook top-to-bottom

# How to configure the notebook

## Process

- To configure the notebook one needs to adjust the notebook config values in
  the `Build the config` section:
  - `"dir_name"`: change the value to a directory with backtest results using
    the following format `"{backtest_results_dir}/tiled_results"`
    - Make sure there is a `".../tiled_results"` subdir
    - E.g.
      `"/shared_data/model/historical/build_tile_configs.C5b.ccxt_v7_4-all.5T.2022-10-01_2023-10-18.ins.run0/tiled_results"`
  - `"start_date"`: set a date to start your analysis from
    - E.g. `datetime.date(2022, 10, 1)`
  - `"end_date"`: set a date to end your analysis at
    - E.g. `datetime.date(2023, 10, 1)`
  - `"save_data_dst_dir"`: a directory to save data to
    - E.g., `"/shared_data/marketing/cmtask5667"`
  - `"save_data"`: set to `True` to save the results
  - Set other config parameters according to experiment data

## Config example

```
config = {
    "dir_name": "/shared_data/model/historical/build_tile_configs.C5b.ccxt_v7_4-all.5T.2019-10-01_2023-07-02.ins.run0/tiled_results",
    "start_date": datetime.date(2022, 7, 2),
    "end_date": datetime.date(2023, 7, 2),
    "asset_id_col": "asset_id",
    "pnl_resampling_frequency": "D",
    "save_data_dst_dir": "/shared_data/marketing/cmtask4688",
    "annotate_forecasts_kwargs": {
        "burn_in_bars": 3,
        "style": "longitudinal",
        # Apply asset-specific rounding.
        "quantization": None,
        "target_dollar_risk_per_name": 50.0,
        "liquidate_at_end_of_day": False,
        "initialize_beginning_of_day_trades_to_zero": False,
        "asset_id_to_share_decimals": asset_id_to_share_decimals,
    },
    "column_names": {
        "price_col": "vwap",
        "volatility_col": "garman_klass_vol",
        "prediction_col": "feature",
    },
    "save_data": True,
}
config = cconfig.Config().from_dict(config)
```

# How to sanity check the results

## Research PnL self-consistency

- The notebook section is `Sanity check portfolio`.
- Sanity check the PnL by computing it in different ways and comparing to the
  original PnL from research portfolio:
  - If all the correlation values in a result series are 1.0 (or close to 1.0,
    e.g., 0.9954) then PnL is computed correctly
  - Use the lower correlation coefficient to detect an error

## Research PnL with prices from the real-time Database

- The notebook section is `Sanity check PnL vs target positions`.
  - Check that VWAP price correlation with research portfolio prices is close to
    1.0 for all the assets
  - Re-compute PnL using prices from the database and check that PnL correlation
    is close to 1.0 for all the asset

## Reconstruct PnL from target positions

Run a sanity check that correlates the research PnL with the PnL reconstructed
from target positions and prices.Per asset correlation coefficients must be
close to 1.0.

# How to save the results

The section is `Save data`.

Make sure to run the section which saves to CSV files to the
`"save_data_dst_dir"`:

- Research PnL
- Target positions and prices

Note that research PnL is resampled to `"pnl_resampling_frequency"` before saving.
PnL is marked at the beginning of the resampling interval, e.g., if resampled
by `"D"` then PnL value for `2022-10-01 00:00:00+00:00` indicates total PnL for
`2022-10-01`.

# How to load the results

The notebook
[`dataflow/model/notebooks/load_alpha_pnl_and_trades.tutorial.ipynb`](`https://github.com/cryptokaizen/cmamp/tree/master/dataflow/model/notebooks/load_alpha_pnl_and_trades.tutorial.ipynb`)
loads the data using a specified destination dir.

The notebook has minimal dependencies so that any customer can pull up the data.

To load and check the data:

- Replace `dst_dir` value with the directory where the files are stored
- Run the notebook top-to-bottom
- Make sure that per-asset PnL correlation coefficients are close to 1.0
