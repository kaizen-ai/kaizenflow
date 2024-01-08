

<!-- toc -->

- [Binance Bid/Ask Data Pipeline](#binance-bidask-data-pipeline)
  * [Data description](#data-description)
  * [Data Availability](#data-availability)
    + [Use cases](#use-cases)
  * [ETL Process](#etl-process)
    + [Extraction](#extraction)
    + [Archival](#archival)
  * [Resampling](#resampling)

<!-- tocstop -->

# Binance Bid/Ask Data Pipeline

In this document, we are going to overview ETL processes, and data management
design choices related to Binance bid/ask data.

## Data description

Binance bid/ask data (AKA order book data) captures a snapshot of the exchange's
order book for a given timestamp. It is possible to capture the top of the book
(AKA highest bid and lowest ask), but also lower levels.

## Data Availability

- Using a free public API (websocket or REST) it is only possible to request
  real-time data. In other words, at time `T` it is not possible to query data
  at `T - 2` hours
- With a higher VIP level on Binance, it is possible to request access to a full
  history of bid/ask data.

### Use cases

- Order limit price calculation: before placing an order when running a trading
  script, recent bid/ask data is used to compute a suitable limit price.
- Forecast computation: bid/ask data resampled to one minute is used by models
  to generate forecasts and target orders that should be traded.

## ETL Process

### Extraction

- We currently collect Binance futures and Binance spot data
  - Data is sampled in real-time every 200 milliseconds using the Binance
    WebSockets API.
- We collect the first 10 levels of the orderbook, i.e., depth = 10.
- Downloading is performed by Docker containers running a Python script that
  samples data, transforms it to a relational database-compatible format, and
  stores it in a Postgres database hosted by AWS RDS.
- Each container lives for 1 hour.
  - Scheduling of containers is performed by Airflow
  - There is a brief overlap (a couple of minutes) between the lifecycle of
    consecutively scheduled containers to ensure seamless data collection
    without
- Airflow DAG used for the collection:
  `im_v2/airflow/dags/preprod.download_resample_periodic_1min_data_websocket_fargate.py`
- During a particular iteration of data sampling, two things can happen:
  - There has been an update pushed by Binance's servers for a given asset since
    the latest one, so we obtain a new data point.
  - No updates have occurred since the latest one, we do not obtain a new data
    point.
- Each data point is specified by a timestamp and provides the following values:
  bid size, bid price, ask size, and ask price (for each level respectively).
  - By default, Binance provides two timestamps.
    - `T`, transaction time: the time at which a change in the order book
      occurred.
    - `E`, event time: the time at which the data point departed from Binance's
      servers
    - Based on the `DataPull` principles we do not alter the timestamp provided
      by the exchange in any way before inserting it in the database
  - We use a third-party library `CCXT` as a base of our data collection script.
    The library chose to use the `E` timestamp and does not readily expose the
    other one.
- Once a data point has entered our container's RAM it gets assigned an
  `end_download_timestamp`
- Currently, right before inserting the data, a `knowledge_timestamp`` is
  assigned to the collected data
  - This was not a design choice
  - The reason is that a conservative estimate of knowledge timestamp is when
    the data was actually inserted in the database and not when it was known by
    the system. E.g., when the database load is high, the "actual" knowledge
    timestamp might be later than the timestamp we tag the data with
  - The current implementation should be deprecated in favor of a solution where
    the database sets the knowledge timestamp upon insertion of the record.
- We are currently collecting the data in two regions: Stockholm and Tokyo
  - The reason is that we trade from both of these regions and querying the data
    across regions would incur a significant delay.

### Archival

- High-frequency bid/ask data can require a large amount of storage
- Storing a large amount of bid/ask data in the database would incur high
  storage costs and be detrimental to database performance
- For these reasons, bid/ask data older than 36 hours gets archived to S3 in a
  parquet format
- The archival and deletion is performed every 2 hours using an Airflow DAG:
  - The latest 2 hours of data is archived
  - Data older than 36 hours is deleted
  - File:
    `im_v2/airflow/dags/preprod.postgres_data_archival_to_s3_fargate_new.py`

## Resampling

- Archived data is resampled once a day using a job scheduled by Airflow. The
  data is appended to an existing Parquet dataset to be used in research
  - File
    `im_v2/airflow/dags/preprod.resample_periodic_daily_bid_ask_data_fargate.py`
