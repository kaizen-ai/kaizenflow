<!--ts-->
   * [DataPull Data Quality Assurance](#datapull-data-quality-assurance)
      * [Principles of data QA](#principles-of-data-qa)
      * [Interfaces for QA](#interfaces-for-qa)
         * [Classes](#classes)
         * [QA notebooks](#qa-notebooks)
         * [Flows](#flows)
            * [Examples](#examples)
      * [Example](#example)



<!--te-->

# DataPull Data Quality Assurance

In this document, we will cover basic principles, mechanisms and processes used
across our organization to conduct QA on the data

## Principles of data QA

- Data Quality Assurance is a process of performing data analysis on a
  particular dataset
- We can observe various attributes
  - Data continuity
    - E.g., are there gaps in the data?
  - Logical soundness
    - E.g., is volume always >= 0?
  - Data presence
    - E.g., if the universe of assets contains 25 assets, there should be data
      for all of them in any particular time interval
- We perform data QA for multiple reasons:
  - To confirm that the data sources we use provide high-quality data
  - To confirm that our flows do not contain bugs
  - To confirm that the data is logically sound
    - E.g. for OHLCV it should never happen that a particular data point stores
      information that `high` value < `low` value
  - To assert a match between datasets that (should) contain the same data but
    are collected and stored separately for various reasons
    - E.g., Parquet on S3 is good for data scientists but not good to use in
      real-time

## Interfaces for QA

### Classes

- To avoid repeating the same data analysis steps across different QA notebooks,
  we created reusable classes:
  - `DataFrameDatasetValidator` from `im_v2/common/data/qa/dataset_validator.py`
  - `QaCheck` from `sorrentum_sandbox/common/validate.py`
- A single QA check performs one or more data analysis operations to determine
  whether a dataset has a particular attribute or not
  - E.g., `im_v2/common/data/qa/qa_check.py::NaNCheck` confirms there are no NaN
    values present in a dataset
  - All existing implemented QA checks are stored in
    `im_v2/common/data/qa/qa_check.py`
- A single dataset validator performs a set of QA checks

### QA notebooks

- In general, all QA notebooks follow the same pattern:
  1. Load imports
  2. Load config for a given QA
  3. Load data to process in QA
  4. If necessary, lightly transform the data to make sure standardized QA can
     be applied
  5. Initialize suitable instances of `QaCheck`s and `DatasetValidator`s
  6. Run the checks
  7. Assert if there are issues, triggering notifications through Airflow

### Flows

To execute a QA flow in an automated way we use invoke targets
`run_single_dataset_qa_notebook` and `run_cross_dataset_qa_notebook`
TODO(Juraj): it will be better to convert the invoke targets to scripts

#### Examples
```
> invoke run_cross_dataset_qa_notebook \
  --stage 'preprod' \
  --start-timestamp '2024-01-05T00:00:00+00:00' \
  --end-timestamp '2024-01-05T23:59:00+00:00' \
  --dataset-signature1 'realtime.airflow.downloaded_1min.postgres.ohlcv.futures.v7_3.ccxt.binance.v1_0_0' \
  --dataset-signature2 'periodic_daily.airflow.downloaded_1min.parquet.ohlcv.futures.v7_3.ccxt.binance.v1_0_0' \
  --aws-profile 'ck' \
  --base-dst-dir '/data/shared/ecs/preprod/data_qa/periodic_daily'

> invoke run_single_dataset_qa_notebook \
  --stage 'preprod' \
  --start-timestamp '2024-01-06T20:20:00+00:00' \
  --end-timestamp '2024-01-06T20:30:00+00:00' \
  --bid-ask-depth 1 \
  --bid-ask-frequency-sec '60S' \
  --dataset-signature 'realtime.airflow.downloaded_200ms.postgres.bid_ask.futures.v7_3.ccxt.binance.v1_0_0' \
  --aws-profile 'ck' \
  --base-dst-dir '/data/shared/ecs/preprod/data_qa/periodic_10min'
```

## Example

For a concrete implementation of the principles see the `Quality Assurance`
section of `docs/datapull/ck.datapull_binance_ohlcv_data_pipeline.reference.md`
