<!--ts-->
   * [Binance OHLCV Data Pipeline](#binance-ohlcv-data-pipeline)
      * [Data Description](#data-description)
      * [Data Availability](#data-availability)
      * [Use Cases](#use-cases)
      * [ETL Process](#etl-process)
      * [Extraction](#extraction)
         * [Regions](#regions)
         * [Real-time extraction](#real-time-extraction)
         * [Historical (AKA bulk) extraction](#historical-aka-bulk-extraction)
            * [Daily extraction](#daily-extraction)
            * [Bulk extraction](#bulk-extraction)
      * [Data Quality Assurance](#data-quality-assurance)
         * [Continuous (Intra-day) QA](#continuous-intra-day-qa)
         * [Daily QA](#daily-qa)



<!--te-->

# Binance OHLCV Data Pipeline

In this document, we are going to overview ETL processes and data management
design choices related to Binance OHLCV data.

## Data Description

Binance OHLCV data represents open, high, low and close prices and volume
information about an asset over a given period (e.g. in a 1-minute interval).

## Data Availability

We obtain full OHLCV history for all available assets using a free public API
(websocket or REST)

## Use Cases

- Forecast computation: OHLCV data is used by models to generate forecasts and
  target orders that should be traded.

## ETL Process

## Extraction

- AKA downloading/pulling the data

- We collect Binance futures and Binance spot data
- The Python library CCXT is used for all downloads
- An example of an OHLCV bar as it's received by CCXT (which provides a wrapper
  around Binance's API) is below:

| timestamp     | open  | high  | low   | close | volume |
| ------------- | ----- | ----- | ----- | ----- | ------ |
| 1704550320000 | 43200 | 43250 | 43150 | 43225 | 125.6  |

- Each exchange/library can decide its own semantics of a timestamp value,
  - In the case of Binance, a UNIX ms timestamp `1704550320000` (01/06/2024
    15:12:00 UTC) represents a time interval
    `[01/06/2024 15:12:00 UTC, 01/06/2024 15:13:00 UTC)`
  - This timestamp convention violates our internal timestamp semantics, since
    we label an interval `[a, b)` as `b`, while Binance tags the bar with `a`
    - Nevertheless, we do not alter the timestamp as it comes from the data
      source before storing it in persistent storage. We let higher-level
      components (e.g. a client object) apply the necessary transformations.
  - We keep track of the timestamp semantics in a dedicated document
    [/docs/datapull/ck.ccxt_exchange_timestamp_interpretation.reference.md](/docs/datapull/ck.ccxt_exchange_timestamp_interpretation.reference.md)
- Once a data point has entered our container's Python runtime, it gets assigned
  an `end_download_timestamp`
- Right before inserting the data into persistent storage, a
  `knowledge_timestamp` is assigned (see similar discussion for Binance bid/ask
  data)
- All downloading is performed by Docker containers running Python scripts.
  - Scheduling of containers is performed by Airflow
  - An exception is a manual one-off download, in case we need to download a
    chunk of data
- We conduct two modes of extraction real-time (AKA streaming, continuous) and
  periodical

### Regions

- We collect data in two regions: Stockholm and Tokyo
  - The reason is that we trade from both of these regions and querying the data
    across regions would incur a significant delay.

### Real-time extraction

- WebSocket connections are used to obtain the data once a minute
- Containers are running 24/7
  - Each container lives for 1 hour
  - There is a brief overlap (a couple of minutes) between the lifecycle of
    consecutively scheduled containers to ensure seamless data collection
    without gaps
- Airflow DAG:
  [/im_v2/airflow/dags/preprod.download_periodic_1min_data_websocket_1_fargate.py](/im_v2/airflow/dags/preprod.download_periodic_1min_data_websocket_1_fargate.py)
  - This DAG relies on
    [/im_v2/ccxt/data/extract/download_exchange_data_to_db_periodically.py](/im_v2/ccxt/data/extract/download_exchange_data_to_db_periodically.py)

- A particular configuration parameter used for this type of extraction is `d` a
  download delay at the beginning of each iteration.
  - By delay, we mean that for an OHLCV bar labeled by time `T` corresponding to
    an interval `[T- 1, T)` we do not sample exactly at `T`, but rather at
    `T + d`, where `d` is in the order of several seconds
  - The reason for this delay is that Binance takes some time to compute a
    fully-finished OHLCV bar value after the end of a bar `T`
    - If the data is sampled before Binance fully closes it, we obtain an
      incorrect data point because it represents some intermediate state of the
      bar, for example at the 55th second of the bar instead of the final values
    - Empirically we have observed that `d` ~3.5 seconds is a safe delay to get
      fully accurate data most of the time
    - In practice, the delay is implemented by setting the start timestamp of
      the downloading script to be a couple of seconds later than the actual end
      of the bar. The Python code logic then automatically starts each
      consecutive iteration with the same delay.
- In the future, we can also collect trade data in a streaming fashion and
  compute OHLCV data on the fly.

### Historical (AKA bulk) extraction

#### Daily extraction

- This download is triggered once a day (during the early morning hours)
- It downloads all data from the previous day and appends it to the existing
  Parquet dataset
- Airflow DAG:
  [/im_v2/airflow/dags/preprod.download_periodic_daily_ohlcv_data_fargate.py](/im_v2/airflow/dags/preprod.download_periodic_daily_ohlcv_data_fargate.py)

#### Bulk extraction

- It is possible to download data in bulk using the script, e.g.:

  ```bash
  /app/amp/im_v2/common/data/extract/download_bulk.py \
    --end_timestamp '2023-12-01T23:59:00+00:00' \
    --start_timestamp '2023-12-31T00:00:00+00:00' \
    --vendor 'ccxt' \
    --exchange_id 'binance' \
    --universe 'v7.3' \
    --data_type 'ohlcv' \
    --contract_type 'futures' \
    --aws_profile 'ck' \
    --assert_on_missing_data \
    --s3_path 's3://cryptokaizen-data.preprod' \
    --download_mode 'periodic_daily' \
    --downloading_entity 'airflow' \
    --action_tag 'downloaded_1min' \
    --data_format 'parquet'
  ```

- We can backfill DB data in bulk (in case of a downloader outage or corruption
  of the data) using an Airflow DAG
  - Airflow DAG:
    [/im_v2/airflow/dags/preprod.backfill_bulk_ohlcv_data_fargate.py](/im_v2/airflow/dags/preprod.backfill_bulk_ohlcv_data_fargate.py)
  - This DAG has no schedule, upon a manual trigger we simply set the start and
    end timestamps as parameters, the DAG downloads the data and performs a QA
    check to confirm all rows have been backfilled successfully

## Data Quality Assurance

For a general explanation/reference to QA flow see
[/docs/datapull/ck.datapull_data_quality_assurance.reference.md](/docs/datapull/ck.datapull_data_quality_assurance.reference.md)

Both flows run a QA notebook
[/im_v2/common/data/qa/notebooks/cross_dataset_qa_ohlcv.ipynb](/im_v2/common/data/qa/notebooks/cross_dataset_qa_ohlcv.ipynb)

### Continuous (Intra-day) QA

- Every 10 minutes at time `T` we download data in the interval
  `[T - 10, T - 5]` and store it in an isolated location in S3 as Parquet
- The rationale is that since this data is downloaded using a simpler REST API
  with a delay of 5+ minutes, it should be 100% correct
- After that, we load the data together with the data downloaded using streaming
  WebSockets, compare them, and expect a 100% match
- Airflow DAG:
  [/im_v2/airflow/dags/preprod.cross_data_qa_periodic_10min_fargate.py](/im_v2/airflow/dags/preprod.cross_data_qa_periodic_10min_fargate.py)

### Daily QA

- Once a day we run a QA flow in which we load two datasets:
  - Data obtained in a streaming fashion using WebSockets (data stored in RDS)
  - Data downloaded in batch after the day is finished, using REST API (data
    stored as Parquet in S3)
- We compare the datasets and confirm all data is logically sound, nothing is
  missing
- We check that there is a 100% match between the datasets.
- Airflow DAG:
  [/im_v2/airflow/dags/preprod.cross_data_qa_periodic_10min_fargate.py](/im_v2/airflow/dags/preprod.cross_data_qa_periodic_10min_fargate.py)
  - TODO(gp): @juraj is this script correct? It is the same as the other one,
    and the name has `_10min_` while the frequency is 1D
