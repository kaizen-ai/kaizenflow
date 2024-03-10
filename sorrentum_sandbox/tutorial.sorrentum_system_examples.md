# Sorrentum system examples

- The following examples under `sorrentum_sandbox/examples/systems` demonstrate
  small standalone Sorrentum data nodes
- Each example implements concrete classes from the interfaces specified in
  `sorrentum_sandbox/common`, upon which command line scripts are built
- Initially, we want to run the systems directly
  - The actual execution of scripts can be orchestrated by Apache Airflow
- The code relies on the Sorretum Airflow container described in the session
  below

## Binance

- In this example we utilize Binance REST API, available free of charge
- We build a small ETL pipeline used to download and transform OHLCV market data
  for selected cryptocurrencies

  ```
  > (cd $GIT_ROOT/sorrentum_sandbox/examples/systems/binance && $GIT_ROOT/dev_scripts/tree.sh)
  ./
  |-- test/
  |   `-- test_download_to_csv.py
  |-- __init__.py
  |-- db.py
  |-- download.py
  |-- download_to_csv.py*
  |-- download_to_db.py*
  |-- load_and_validate.py*
  |-- load_validate_transform.py*
  `-- validate.py
  ```

### Run system in standalone mode

- The example code can be found in `sorrentum_sandbox/examples/systems/binance`

- There are various files:
  - `db.py`: contains the interface to load / save Binance raw data to Postgres
    - i.e., "Load stage" of an ETL pipeline
  - `download.py`: implement the logic to download the data from Binance
    - i.e., "Extract stage"
  - `download_to_csv.py`: implement Extract stage to CSV
  - `download_to_db.py`: implement Extract stage to PostgreSQL
  - `load_and_validate.py`: implement a pipeline loading data into a
    CSV file and then validating data
  - `load_validate_transform.py`: implement a pipeline loading data
    into DB, validating data, processing data, and saving data back to DB
  - `validate.py`: implement simple QA pipeline

#### Download data

- To get to know what type of data we are working with in this example you can run:
  ```
  > docker_bash.sh
  docker> cd /cmamp/sorrentum_sandbox/examples/systems/binance
  docker> ./download_to_csv.py --start_timestamp '2022-10-20 10:00:00+00:00' --end_timestamp '2022-10-21 15:30:00+00:00' --target_dir 'binance_data'
  INFO: > cmd='/cmamp/sorrentum_sandbox/examples/binance/download_to_csv.py --start_timestamp 2022-10-20 10:00:00+00:00 --end_timestamp 2022-10-21 15:30:00+00:00 --target_dir binance_data' report_memory_usage=False report_cpu_usage=False
  INFO: Saving log to file '/cmamp/sorrentum_sandbox/examples/binance/download_to_csv.py.log'
  06:45:25 - INFO  download.py download:120                               Downloaded data:
             currency_pair           open           high            low          close      volume      timestamp           end_download_timestamp
  0      ETH_USDT  1295.95000000  1297.34000000  1295.95000000  1297.28000000  1.94388000  1666260060000 2023-01-23 11:45:22.729119+00:00
  1      ETH_USDT  1297.28000000  1297.28000000  1297.28000000  1297.28000000  0.00000000  1666260120000 2023-01-23 11:45:22.729188+00:00
  2      ETH_USDT  1297.28000000  1297.28000000  1297.28000000  1297.28000000  0.00000000  1666260180000 2023-01-23 11:45:22.729201+00:00
  3      ETH_USDT  1297.28000000  1297.28000000  1297.28000000  1297.28000000  0.00000000  1666260240000 2023-01-23 11:45:22.729246+00:00
  4      ETH_USDT  1297.28000000  1297.28000000  1297.28000000  1297.28000000  0.00000000  1666260300000 2023-01-23 11:45:22.729261+00:00
  ```

- The script downloads around 1 day worth of OHLCV bars (aka candlestick) into a
  CSV file
  ```
  docker> ls binance_data/
  bulk.manual.download_1min.csv.ohlcv.spot.v7.binance.binance.v1_0_0.csv

  docker> more binance_data/bulk.manual.download_1min.csv.ohlcv.spot.v7.binance.binance.v1_0_0.csv
  currency_pair,open,high,low,close,volume,timestamp,end_download_timestamp
  ETH_USDT,1295.95000000,1297.34000000,1295.95000000,1297.28000000,1.94388000,1666260060000,2023-01-23 11:45:22.729119+00:00
  ETH_USDT,1297.28000000,1297.28000000,1297.28000000,1297.28000000,0.00000000,1666260120000,2023-01-23 11:45:22.729188+00:00
  ETH_USDT,1297.28000000,1297.28000000,1297.28000000,1297.28000000,0.00000000,1666260180000,2023-01-23 11:45:22.729201+00:00
  ```

- An example of an OHLCV data snapshot:

  |currency_pair|open          |high          |low           |close         |volume    |timestamp    |end_download_timestamp          |
  |-------------|--------------|--------------|--------------|--------------|----------|-------------|--------------------------------|
  |ETH_USDT     |1295.95000000 |1297.34000000 |1295.95000000 |1297.28000000 |1.94388000|1666260060000|2023-01-13 13:01:53.101034+00:00|
  |BTC_USDT     |19185.10000000|19197.71000000|19183.13000000|19186.63000000|1.62299500|1666260060000|2023-01-13 13:01:54.508880+00:00|

  - Each row represents the state of an asset for a given minute
  - In the above example we have data points for two currency pairs `ETH_USDT`
    and `BTC_USDT` for a given minute denoted by UNIX timestamp 1666260060000
    (`2022-10-20 10:01:00+00:00`), which in Sorrentum protocol notation represents
    time interval `[2022-10-20 10:00:00+00:00, 2022-10-20 10:00:59.99+00:00)`
  - Within this timeframe `ETH_USDT` started trading at `1295.95`, reached the
    highest (lowest) price of `1297.34`(`1295.95`) and ended at `1297.28`.  

#### Run QA
 
- To familiarize yourself with the concepts of data quality assurance /
  validation you can proceed with the example script
  `load_and_validate.py` which runs a trivial data QA operations (i.e.
  checking the dataset is not empty)
  ```
  docker> ./load_and_validate.py \
          --start_timestamp '2022-10-20 12:00:00+00:00' \
          --end_timestamp '2022-10-21 12:00:00+00:00' \
          --source_dir 'binance_data' \
          --dataset_signature 'bulk.manual.download_1min.csv.ohlcv.spot.v7.binance.binance.v1_0_0'
  INFO: Saving log to file '/cmamp/sorrentum_sandbox/examples/systems/binance/load_and_validate.py.log'
  06:05:21 - INFO  validate.py run_all_checks:87                          Running all QA checks:
  06:05:21 - INFO  validate.py run_all_checks:90                          EmptyDatasetCheck: PASSED
  06:05:21 - INFO  validate.py run_all_checks:90                          GapsInTimestampCheck: PASSED
  ```

#### Run unit tests

#### Run ETL pipeline

### Run inside Airflow

- Bring up the services via docker-compose as described above
- Visit `localhost:8091/home`
- Sign in using the default credentials `airflow`:`airflow`
- There are two Airflow DAGs preloaded for this example stored in the dir
  `$GIT_ROOT/sorrentum_sandbox/devops/airflow_data/dags`
  ```
  > vi $GIT_ROOT/sorrentum_sandbox/devops/airflow_data/dags/*binance*.py
  ```

   - `download_periodic_1min_postgres_ohlcv_binance`:
     - scheduled to run every minute
     - download the last minute worth of OHLCV data using
       `examples/binance/download_to_db.py`
   - `validate_and_resample_periodic_1min_postgres_ohlcv_binance`
     - scheduled to run every 5 minutes
     - load data from a postgres table, resample it, and save back the data
       using `examples/binance/load_validate_transform.py`
- A few minutes after enabling the DAGs in Airflow, you can check the PostgreSQL
  database to preview the results of the data pipeline
  - The default password is `postgres`
  ```
  > docker_exec.sh
  docker> psql -U postgres -p 5532 -d airflow -h host.docker.internal -c 'SELECT * FROM binance_ohlcv_spot_downloaded_1min LIMIT 5'

  docker> psql -U postgres -p 5532 -d airflow -h host.docker.internal -c 'SELECT * FROM binance_ohlcv_spot_resampled_5min LIMIT 5'
  ```

<!--  ///////////////////////////////////////////////////////////////////////////////////////////// -->

## Reddit

- In this example we use Reddit REST API, available free of charge, to build a
  small ETL pipeline to download and transform Reddit posts and comments for
  selected subreddits
- A prerequisite to use this code is to obtain [Reddit
  API](https://www.reddit.com/wiki/api/) keys (client ID and secret)

  ```
  > examples/reddit
  |-- __init__.py
  |-- db.py
  |-- download.py
  |-- download_to_db.py*
  |-- load_validate_transform.py*
  |-- transform.py
  `-- validate.py

  1 directory, 12 files
  ```

## Running outside Airflow

- The example code can be found in `sorrentum_sandbox/examples/reddit`
- There are various files:
  - `db.py`: contains the interface to load / save Reddit posts data to MongoDB
    (Load/Extract stage)
  - `download.py`: implement the logic to download raw data from Reddit (Extract
    stage)
  - `download_to_db.py`: implement extract stage to MongoDB
  - `load_validate_transform.py`: implement a pipeline loading data into DB,
    validating data, processing raw data (computing features), and saving
    transformed data back to DB
  - `validate.py`: implement simple QA pipeline
  - `transform.py`: implement simple feature computation utilities from raw data 

### Download data

- Make sure to specify Reddit API credentials in
  `$GIT_ROOT/sorrentum_sandbox/devops/.env` in the section labeled as `Reddit`
  before running the scripts
  ```
  # Reddit.
  REDDIT_CLIENT_ID=some_client_id
  REDDIT_SECRET=some_secret
  ```
- To explore the data structure run (assumes having mongo container up and running):
  ```bash
  > cd /cmamp/sorrentum_sandbox/examples/reddit/
  docker> ./download_to_db.py \
      --start_timestamp '2022-10-20 10:00:00+00:00' \
      --end_timestamp '2022-10-21 15:30:00+00:00'
  ```
- Since the script is setup to download new posts, it's optimal to specify as
  recent timestamps as possible
- Connect to a MongoDB and query some documents from the `posts` collection
  ```bash
  docker> python
  >>> import sorrentum_sandbox.examples.reddit.db as ssexredb
  >>> import pymongo; import pandas as pd
  >>> mongodb_client = pymongo.MongoClient(
  ...     host=ssexredb.MONGO_HOST, port=27017, username="mongo", password="mongo"
  ... )
  >>> reddit_mongo_client = ssexredb.MongoClient(mongodb_client, "reddit")
  >>> data = reddit_mongo_client.load(
  ...     dataset_signature="posts",
  ... )
  >>> data
  >>> <<output truncated for readability, example below>>
  ```
- An example database entry (truncated for readability) is below:
  ```json
    {
      "_id": {"$oid": "63bd466b85a76c62bb578e49"},
      ...
      "created": {"$date": "2023-01-10T11:01:29.000Z"},
      "created_utc": "1673348489.0",
      "discussion_type": "null",
      "distinguished": "null",
      "domain": "\"crypto.news\"",
      "downs": "0",
      "edited": "false",
      "permalink": "\"/r/CryptoCurrency/comments/108741k/us_prosecutors_urge_victims_of_ftx_collapse_to/\"",
      "send_replies": "false",
      "title": "\"US prosecutors urge victims of FTX collapse to speak out.\"",
      "ups": "1",
      "upvote_ratio": "1.0",
      "url": "\"https://crypto.news/us-prosecutors-urge-victims-of-ftx-collapse-to-speak-out/\"",
      "url_overridden_by_dest": "\"https://crypto.news/us-prosecutors-urge-victims-of-ftx-collapse-to-speak-out/\"",
      "user_reports": "[]",
      ...
    }
  ```

### Load, QA and Transform

- The Second step is extracting features. Run as: 
  ```
  > cd /cmamp/sorrentum_sandbox/examples/reddit/
  docker> ./load_validate_transform.py \
      --start_timestamp '2022-10-20 10:00:00+00:00' \
      --end_timestamp '2022-10-21 15:30:00+00:00'
  ```
- In MongoDB it can be found in the `processed_posts` collection
- To query the DB use the same code as above, in the **download data** section, specifying `processed_posts` in the `dataset_signature` argument

- Example:
  ```json
  {
    "_id": {"$oid": "63bd461de978f68eae1c4e11"},
    "cross_symbols": ["USDT"],
    "reddit_post_id": "\"108455o\"",
    "symbols": ["ETH", "USDT"],
    "top_most_comment_body": "\"Pro & con info are in the collapsed comments below for the following topics: [Crypto.com(CRO)](/r/CryptoCurrency/comments/108455o/cryptocom_is_delisting_usdt_what_do_they_know/j3q51fa/), [Tether](/r/CryptoCurrency/comments/108455o/cryptocom_is_delisting_usdt_what_do_they_know/j3q52ab/).\"",
    "top_most_comment_tokens": ["con", "pro", "cro", "collapsed", "are", "tether", "j3q51fa", "r", "usdt", "is", "comments", "topics", "for", "in", "com", "cryptocom", "delisting", "they", "know", "crypto", "what", "do", "j3q52ab", "cryptocurrency", "info", "108455o", "below", "following", "the"]
  }
  ```

## Running inside Airflow

- Bring up the services via docker-compose as described above
- Make sure to specify Reddit API credentials in
  `$GIT_ROOT/sorrentum_sandbox/devops/.env` in the section labeled as `Reddit`
  before running the setup
  ```
  # Reddit.
  REDDIT_CLIENT_ID=some_client_id
  REDDIT_SECRET=some_secret
  ```
- Visit `localhost:8091/home`
- Sign in using the default credentials airflow:airflow
- There are two Airflow DAGs preloaded for this example stored in the dir
  `$GIT_ROOT/sorrentum_sandbox/devops/airflow_data/dags`
  - `download_periodic_5min_mongo_posts_reddit`:
    - scheduled to run every 5 minutes
    - download new posts submitted in the last 5 minutes from chosen subreddits
      (in this example `r/Cryptocurrency` and `r/CryptoMarkets`) using
      `examples/reddit/download_to_db.py`
  - `validate_and_extract_features_periodic_5min_mongo_posts_reddit`
    - scheduled to run every 5 minutes
    - load data from a MongoDB collection, compute feature, and save back to the
      database using `examples/reddit/load_validate_transform.py`

