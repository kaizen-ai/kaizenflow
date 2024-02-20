

<!-- toc -->

- [What is model parameters sweeping](#what-is-model-parameters-sweeping)
- [How to sweep model parameters](#how-to-sweep-model-parameters)
  * [Regular sweeping](#regular-sweeping)
  * [Sweep weighted price column](#sweep-weighted-price-column)

<!-- tocstop -->

# What is model parameters sweeping

- Model parameters sweeping is implemented by running a backtest simulation
  when we want to assess the model's performance with different values for
  specified model parameters
- The notebook for parameters sweeping is located in Orange repo
  https://github.com/cryptokaizen/orange/blob/master/dataflow_orange/notebooks/CmTask6499_Sweep_C11a_price_col.ipynb

- Currently we allow sweeping values for only one parameter per run to preserve
  readability
- The notebook performs sweeping in the following steps:
  - Retrieves all necessary model parameter values from the default Notebook
    config, including the name and values of the parameter we want to sweep
  - Iterates over the values to sweep and builds a dictionary of Notebook configs
    where each config corresponds to a swept parameter value
  - Runs model performance analysis for each config in the dict
  - Concatenates model results and displays values and plots for each swept
    value together to observe the difference in results

# How to sweep model parameters

## Regular sweeping

- Open `dataflow_orange/notebooks/CmTask6499_Sweep_C11a_price_col.ipynb`
- Adjust the default notebook config:
  - Set a directory with tiled backtest results as `dst_dir`
  - Set default config values
    - `start_date` and `end_date` parameters set a timeframe for model
      performance analysis
    - Ensure that the `rule` parameter value matches the frequency of a
      backtest run
      - E.g., if the backtest config is `ccxt_v7_4-all.5T.2022-01-01_2023-10-31`
        then the `rule` value should be `5T`
    - Set a swept param name and values to sweep it in the `sweep_param` dict:
      - `keys` correspond to a chain of keys to a value of the swept param in
        the default dict
      - `values` correspond to param values to sweep
    - E.g.,
    ```
    dir_name: /shared_data/CmTask6464/build_tile_configs.C11a.ccxt_v7_4-all.5T.2022-01-01_2023-10-31.ins.run0/tiled_results
    start_date: 2023-01-01
    end_date: 2023-10-31
    asset_id_col: asset_id
    pnl_resampling_frequency: D
    rule: 5T
    im_client_config:
      data_version: v3
      universe_version: v7.4
      dataset: ohlcv
      contract_type: futures
      data_snapshot:
    annotate_forecasts_kwargs:
      style: longitudinal
      quantization: 30
      liquidate_at_end_of_day: False
      initialize_beginning_of_day_trades_to_zero: False
      burn_in_bars: 3
      compute_extended_stats: True
      target_dollar_risk_per_name: 1.0
      modulate_using_prediction_magnitude: False
      prediction_abs_threshold: 0.3
    column_names:
      price_col: open
      volatility_col: garman_klass_vol
      prediction_col: feature
    bin_annotated_portfolio_df_kwargs:
      proportion_of_data_per_bin: 0.2
      normalize_prediction_col_values: False
    load_all_tiles_in_memory: True
    sweep_param:
      keys: ('column_names', 'price_col')
      values: ['open', 'close']
    ```
  - Run the notebook

## Sweep weighted price column

- The notebook allows sweeping some specified column with its synthetic copies
  that are resampled with weights
- For this `("sweep_param", "keys")` should strictly contain some column from
  `"column_names"` param in the default config
- Set weights to create synthetic columns in `weights_dict`
  - Ensure that the number of weights equals the number of values within the
    resampling rule
- Run the notebook
