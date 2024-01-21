<!--ts-->
   * [The nature of data](#the-nature-of-data)
   * [Overall structure of DataPull](#overall-structure-of-datapull)
      * [ETL layer](#etl-layer)
         * [Client stage](#client-stage)
         * [ETL primitives](#etl-primitives)
      * [Asset representation](#asset-representation)
      * [Data pipelines](#data-pipelines)
      * [General conventions](#general-conventions)
         * [Data invariants](#data-invariants)
         * [Data layout on file system](#data-layout-on-file-system)
         * [Dataset naming convention](#dataset-naming-convention)
         * [Data pipeline classification](#data-pipeline-classification)
         * [Data universe](#data-universe)
      * [Data set downloading and handling](#data-set-downloading-and-handling)
         * [Data set naming scheme](#data-set-naming-scheme)
         * [Data set attributes](#data-set-attributes)
      * [Data on-boarding flow](#data-on-boarding-flow)
      * [Data formats](#data-formats)
         * [Data stored by-date](#data-stored-by-date)
         * [Data stored by-asset](#data-stored-by-asset)
         * [By-asset Parquet data](#by-asset-parquet-data)
         * [Data stored by-tile](#data-stored-by-tile)



<!--te-->

# The nature of data

**Large variety of data**. Data comes in an extremely large variety and with
different characteristics, for instance:

- Different time semantics, e.g.,
  - Intervals of data can be `\[a, b)` or `(a, b\]`
  - A data bar can be marked at the beginning or at the end of the corresponding
    interval, e.g., `a` or `b`

- Data can refer to multiple data sets
  - E.g., in finance there are multiple asset classes (e.g., equities, futures,
    crypto)

- Structured (e.g., market data) vs unstructured (e.g., alternative data)
-
- Historical vs real-time
  - The same data can be collected and delivered in batch for a large period of
    time for research purposes, and then in real-time for production
  - Although historical and real-time data for the same source should match
    exactly, this is not always the case, since the historical data can be
    acquired from different providers or can be lightly post-process to remove
    some artifacts

- The same data can be delivered at different time resolutions
  - E.g., in finance different data is provided for
    - Order book data
    - Trades
    - Minute OHLCV bars
    - Daily bars

- Different vendors can provide data for the same data source
  - E.g., Kibot, Binance, CryptoDataDownload provide data for the Binance
    exchange
  - Although one would expect data from different providers for the same data
    source to match, this is rarely the case due to multiple reasons (e.g.,
    different ways of collecting the data, different quality of collection
    methods)

- Data and metadata
  - Some vendors provide metadata associated with data, others don't
  - Examples of metadata are:
    - List of assets in the universe over time
    - Attributes of assets (e.g., industry and other classification)

# Overall structure of `DataPull`

`DataPull` is composed of:

- An ETL layer to download and store data
- A Client layer to retrieve and serve the data

## ETL layer

**ETtl approach**. `DataPull` employs a variation of the ETL approach, called
EtLT (i.e., extract, lightly transform, load, transform) for downloading both
data and metadata.

Data is extracted from an external data source, lightly transformed, and then
saved into permanent storage. Downstream data pipelines read the data with a
client interface, standardized in terms of timing semantic and format of the
data.

In general there are separate pipelines for data and metadata for the same data
source.

**Storage backend**. Data can be saved in multiple storage backends:

- Database (e.g., Postgres, MongoDB)
- Local filesystem
- Remote backends (e.g., AWS S3 bucket)

**Data format**. Data can be saved on filesystems in different formats (e.g.,
CSV, JSON, Parquet).

**AWS S3 vs local filesystem**. Unfortunately it's not easy to abstract the
differences between AWS S3 buckets and local filesystems, since the S3 interface
is more along a key-value store rather than a filesystem (supporting
permissions, deleting recursively a directory, moving, etc.).

Solutions based on abstracting a filesystem on top of S3 (e.g., mounting S3 with
Fuse filesystems) are not robust enough.

Some backends (e.g., Parquet) allow handling an S3 bucket transparently.

`DataPull` typical approach is:

- When writing to S3, use the local filesystem for staging the data in the
  desired structure and then copy all the data to S3

- When reading from S3, read the data directly (using the functionalities
  supported by the backend) or copy the data locally and then read it from local
  disk

**Data formats**. The main data formats that `DataPull` supports are:

- CSV
  - Pros
    - Easy to inspect
    - Easy to load / save
    - Everybody understands it
  - Cons
    - Data can't be easily sliced by asset ids / by time
    - Large footprint (non-binary), although it can be compressed on the fly
      (e.g., `.csv.gz`)

- Parquet
  - Pros
    - Compressed
    - AWS friendly
    - Data can be easily sliced by asset ids and time
  - Cons
    - Not easy to inspect
      - Solution: use wrapper to convert to CSV
    - Difficult to append
      - Solution: use chunking + defragmentation
    - Cumbersome for real-time data

- Database
  - Pros
    - Easy to inspect
    - Support any access pattern
    - Friendly for real-time data
  - Cons
    - Need DevOps to manage database instance
    - Difficult to track lineage and version

Unfortunately there is not an obvious best solution, so `DataPull` supports
multiple representations and converting data between them.

**Extract stage**. The goal is to acquire raw data from an external source and
archive it into a permanent storage backend (e.g., file-system and / or
database). The data can be either historical or real-time. `DataPull` typically
doesn't process the data at all, but rather we prefer to save the data raw as it
comes from the wire.

**Transform stage**. Typically, `DataPull` prefers to load the data in the
backend with minor or no transformation. Specifically we allow changing the
representation of the data / format (e.g., removing some totally useless
redundancy, compressing the data, transforming from strings to datetimes). We
don't allow changing the semantics or filter columns. This is done dynamically
in the `client` stage

**Load stage**. The load stage simply saves the data into one of the supported
backends.

Typically, we prefer to save

- Historical data into Parquet format since it supports more naturally the
  access patterns needed for long simulations

- Real-time data into a database since this makes it easy to append and retrieve
  data in real-time. Often we want to also append real-time data to Parquet

### Client stage

The client stage allows downstream pipelines to access data from the backend
storage. The access pattern from a downstream pipeline follows along the line of
"give me the columns XYZ for assets ABC in the period `[..., ...]`".

We prefer to perform some transformations that are lightweight (e.g., converting
Unix epochs in timestamps) or still evolving (e.g., understanding the timing
semantic of the data) are performed inside this stage, rather than in the
transform stage.

### ETL primitives

`DataPull` implements basic primitives that can be combined in different ways to
create various ETL pipelines.

TODO(gp): Improve this

- Extract:
  - Read data from an external source to memory (typically in the form of Pandas
    data structures)
  - E.g., downloading data from a REST or Websocket interface

- Load:
  - Load data stored in memory -> permanent storage (e.g., save as CSV or as
    Parquet)
  - E.g., pd.to_parquet()
  - DbSave
    - Save to DB
    - Create schema

- Client:
  - From a permanent storage (e.g., disk) -> Memory
  - E.g., pd.from_parquet()

- ClientFromDb
  - DB -> Memory
  - Creates the SQL query to read the data

- Validator
  - Https://github.com/cryptokaizen/cmamp/pull/3386/files

- Transform
- Just a class

**Example of ETL pipelines.**

- Download historical data and save it as CSV or PQ
- Download order book data, compress it, and save it on S3
- Insert 1 minute worth of data in the DB
  - Write data into DB
    - One could argue that operations on the DB might not look like `extract`
      but rather `load`
    - We treat any backend (S3, local, DB) in the same way and the DB is just a
      backend

- [[More detailed description]{.underline}](https://docs.google.com/document/d/1EH7RqeTdVCXDcrCKPm0PvrtftTA11X9V-AT7F20V7T4/edit#heading=h.t1ycoe6irr95)
  - **Examples**:
    - Transformations CSV -> Parquet <-> DB
    - Convert the CSV data into Parquet using certain indices
    - Convert Parquet by-date into Parquet by-asset

## Asset representation

TODO(gp): Ideally we want to use a single schema like `Vendor:ExchangeId:Asset`

## Data pipelines

Download data by asset (as a time series)

- `asset.csv.gz` or Parquet
- Historical format

Download data by time, e.g.,

- 20211105/...
- `csv.gz` or Parquet
- This is typical of real-time flow

## General conventions

### Data invariants

We use the following invariants when storing data during data on-boarding and
processing:

- Data quantities are associated to intervals are `[a, b)` (e.g., "the return
  over the interval [a, b)") or to a single point in time (e.g., "the close
  price at 9am UTC")
- Every piece of data is labeled with the end of the sampling interval or with
  the point-in-time
  - E.g., for a quantity computed in an interval `[06:40:00, 06:41:00)` the
    timestamp is `06:41:00`
- Timestamps are always time-zone aware, typically using UTC timezone
- Every piece of data has a knowledge timestamp (aka "as-of-date") which
  represent when `KaizenFlow` was aware of the data according to a wall-clock:
  - Multiple timestamps associated with different events can be tracked, e.g.,
    `start_download_timestamp`, `end_download_timestamp`
  - No system should depend on data available strictly before the knowledge
    timestamp
- Data is versioned: every time we modify the schema or the semantics of the
  data, we bump up the version using semantic versioning and update the
  changelog of what each version contains

An example of tabular data is below:

<img src="figs/datapull/data_format.png">

### Data layout on file system

`DataPull` keeps data together by execution run instead of by data element.

E.g., assume we run a flow called `XYZ_sanity_check` every day and the flow
generates three pieces of data, one file `output.txt` and two directories
`logs`, `temp_data`.

`DataPull` prefers to organize the data in a directory structure like:

**_Better_**
```
- XYZ_sanity_check/
  - run.{date}/
    - output.txt
    - logs/
    - temp_data/
  - run.{date}.manual/
    - output.txt
    - logs/
    - temp_data/
```

**_Worse_**
```
- XYZ_sanity_check/
  - output.{date}/
    - output.txt
  - logs.{date}/
  - temp_data.{date}/
    ...
```

The reasons why the first data layout is superior are:

1.  It's easier to delete a single run by deleting a single dir instead of
    deleting multiple files
2.  It allows the format of the data to evolve over time without having to
    change the schema of the data retroactively
3.  It allows scripts post-processing the data to point to a directory with a
    specific run and work out of the box
4.  It's easier to move the data for a single run from one dir (e.g., locally)
    to another (e.g., a central location) in one command
5.  There is redundancy and visual noise, e.g., the same data is everywhere

We can tag directory by a run mode (e.g., `manual` vs `scheduled`) by adding the
proper suffix to a date-dir.

**Directory with one file**. Having a directory containing one single file often
creates redundancy.

We prefer not to use directories unless they contain more than one file. We can
use directories if we believe that it's highly likely that more files will be
needed, but as often happens YANGI (you are not going to need it) applies.

### Dataset naming convention

- We use `.` to separate conceptually different pieces of a file or a directory.
- We don't allow white spaces since they are not Linux friendly and need to be
  escaped. We replace white spaces with `_`.
- We prefer not to use `-` whenever possible, since they create issues with
  Linux auto-completion and need to be escaped.
  - E.g., `bulk.airflow.csv` instead of `bulk_airflow.csv`

### Data pipeline classification

A data pipeline can be any of the following:

- A downloader
  - External API of data provider -> Internal DB
  - It downloads historical or real-time data from an external API and saves the
    dataset in an internal location
  - The name of the script and the location of the data downloaded follow the
    naming scheme described below
  - Implementation: typically a Python script

- A QA flow for a single or multiple datasets
  - Internal DB -> Process -> Exception
  - It computes some statistics from one or more datasets (primary or derived)
    and throws an exception if the data is malformed
  - It aborts if the data is not compliant to certain QA metrics
  - Implementation: typically Python notebook backed by a Python library

- A derived dataset flow
  - Internal DB -> Process -> Internal DB
  - It computes some data derived from an existing data set
    - E.g., resampling, computing features
  - Implementation: typically a Python script

- A model flow
  - Internal DB -> Process -> Outside API (e.g., exchange)
  - It runs a computation on internal data and takes some actions (e.g., place
    some trades through an external API)
  - Implementation: typically a Python script

### Data universe

Often the data relates to a set of entities

- E.g., assets, e.g., currency pairs on different exchanges

The support of the data is referred to as the "data universe". This metadata is
versioned as any other piece of data.

## Data set downloading and handling

### Data set naming scheme

Each data set is stored in a data lake with a path and name that describe its
metadata according to the following signature:
```
dataset_signature={download_mode}.{downloading_entity}.{action_tag}.{data_format}.{data_type}.{asset_type}.{universe}.{vendor}.{exchange_id}.{version\[-snapshot\]}.{extension}
```

TODO(gp): @juraj add a {backend} = s3, postgres, mongo, local_file

The signature schema might be dependent on the backend

E.g.,
```
bulk/airflow/downloaded_1min/csv/ohlcv/futures/universe_v1_0/ccxt/binance/v1_0-20220210/BTC_USD.csv.gz
```

We use `-` to separate pieces of the same attribute (e.g., version and snapshot)
and `_` as replacements of a space character.

The organization of files in directories should reflect the naming scheme. We
always use one directory per attribute for files (e.g., `bulk.airflow.csv/...`
or `bulk/airflow/csv/...`). When the metadata is used not to identify a file in
the filesystem (e.g., for a script or as a tag) then we use `.` as separators
between the attributes.

### Data set attributes

There are several "attributes" of a data set:

- `download_mode`: the type of downloading mode
  - `bulk`
    - Aka "one-shot", "one-off", and improperly "historical"
    - Data downloaded in bulk mode, as one-off documented operations
    - Sometimes it's referred to as "historical", since one downloads the
      historical data in bulk before the real-time flow is deployed
  - `periodic`
    - Aka "scheduled", "streaming", "continuous", and improperly "real-time"
    - Data is captured regularly and continuously
    - Sometimes it's referred as to "real-time" since one capture this data
    - It can contain information about the frequency of downloading (e.g.,
      `periodic-5mins`, `periodic-EOD`) if it needs to be identified with
      respect to others
  - `unit_test`
    - Data used for unit test (independently if it was downloaded automatically
      or created manually)

- `downloading_entity`: different data depending on whom downloaded it, e.g.,
  - `airflow`: data was downloaded as part of the automatic flow
  - `manual`: data download was triggered manually (e.g., running the download
    script)

- `action_tag`: information about the downloading, e.g., `downloaded_1min` or
  `downloaded_EOD`

- `data_format`: the format of the data, e.g.,
  - `csv` (always csv.gz, there is no reason for not compressing the data)
  - `parquet`

- `data_type`: what type of data is stored, e.g.,
  - `ohlcv`, `bid_ask`, `market_depth` (aka `order_book`), `bid_ask_market_data`
    (if it includes both), `trades`

- `asset_type`: what is the asset class
  - E.g., futures, spot, options

- `universe`: the name of the universe containing the possible assets
  - Typically, the universe can have further characteristics and it can be also
    versioned
  - E.g., `universe_v1_7`

- `vendor`: the source that provided the data
  - Aka "provider"
  - E.g., `ccxt`, `crypto_chassis`, `cryptodata_download`, `kaiko`,
  - Data can also be downloaded directly from an exchange (e.g., `coinbase`,
    `binance`)

- `exchange_id`: which exchange the data refers to
  - E.g., `binance`

- `version`: any data set needs to have a version
  - Version is represented as major, minor, patch according to semantic
    versioning in the format `v{a}_{b}_{c}` (e.g., v1_0_0)
  - If the schema of the data is changed the major version is increased
  - If a bug is fixed in the downloader that improves the semantic of the data,
    but it's not a backward incompatible change, the minor version is increased
  - The same version can also include an optional `snapshot` which refers to the
    date when the data was downloaded (e.g., a specific date `20220210` to
    represent when the day on which the historical data was downloaded, i.e.,
    the data was the historical data as-of 2022-02-10)
  - Note that `snapshot` and `version` have an overlapping but not identical
    meaning. `snapshot` represents when the data was downloaded, while `version`
    refers to the evolution of the semantic of the data and of the downloader.
    E.g., the same data source can be downloaded manually on different days with
    the same downloader (and thus with the same version).

- `asset_type`: which cryptocurrency the data refers to:
  - Typically, there is one file per asset (e.g., `BTC_USDT.csv.gz`)
  - Certain data formats can organize the data in a more complex way
    - E.g., Parquet files save the data in a directory structure
      `{asset}/{year}/{month}/data.parquet`

It is possible that a single data set covers multiple values of a specific
attribute

- E.g., a data set storing data for both futures and spot, can have
  `asset_type=futures_spot`

Not all the cross-products are possible, e.g.

- There is no data set with `download_mode=periodic` scheduled by Airflow and
  `downloading_entity=manual`

We organize the schema in terms of access pattern for the modeling and analysis
stage

- E.g., `snapshot` comes before `vendor` since in different snapshots we can
  have different universes
- E.g., snapshot -> dataset -> vendor -> exchange -> coin
- A universe is just a mapping of a tag (e.g., v5) to a set of directories

Each data set has multiple columns.

## Data on-boarding flow

**Downloader types**. For each data set, there are typically two types of
downloaders: bulk and periodic. This step is often the equivalent of the Extract
phase in an ETL/ELT/ EtLT pipeline.

E.g., an EtLT pipeline can consist of the following phases:

- `E`: extract one minute data from websocket,
- `t`: apply non-business logic related transformation from JSON to dataframe
- `L`: load into SQL
- `T`: transform the data resampling to 5 minute data

**Bulk downloaders**. Download past data querying from an API.

A characteristic of bulk downloaders is that the download is not scheduled to be
repeated on a regular basis. It is mostly executed once (e.g., to get the
historical data) or a few times (e.g., to catch up with an intermittent data
feed). It is executed or at least triggered manually.

The data is downloaded in bulk mode at $T_{dl,bulkhist}$ to catch up with the
historical data up to the moment of the deployment of the periodic downloader
scheduled every period $\Delta t_{deploy,periodic}$.

The bulk download flow is also needed any time we need to "catch up" with a
missing periodic download, e.g., if the real-time capture system was down.

**Preferred bulk data format**. Typically the data is saved in a format that
allows data to be loaded depending on what is needed from downstream systems
(e.g., Parquet using tiles on assets and period of times).

**Periodic downloaders**. Download the data querying an API every period
$\\Delta T_{dl,periodic}$, which depends on the application needs, e.g., every
second, minute. Typically periodic downloaders are triggered automatically
(e.g., by a workflow orchestrator like Apache Airflow).

Another possible name is "streaming" data. Typical example is a websocket feed
of continuous data.

TODO(samarth): a timeline with the bulk downloader and then the periodic
downloaders

**Preferred periodic data format**. Typically we save data in a DB to be able to
easily query the data from models. We also save the same data to an
historical-like format to have a backup copy of the data. Parquet format is not
ideal since it's not easy to append data.

TODO(gp): Add diagram

Providers -> us

It's the extract in ETL

**Downloader naming scheme**. A downloader has a name that represents the
characteristics of the data that is being downloaded in the format above.

The downloaders usually don't encapsulate logic to download only a single
dataset. This means that the naming conventions for downloaders are less strict
than for the datasets themselves.

- More emphasis is put into providing a comprehensive docstring

- We can't use `.` in filenames as attribute separators because Python uses them
  to separate packages in import statements, so we replace them with `_` in
  scripts

- The name should capture the most general use-case
  - E.g. if a downloader can download both OHLCV and Bid/Ask data for given
    exchange in a given time interval and save to relational DB or S3 we can
    simply name it `download_exchange_data.py`

TODO(Juraj): explain that Airflow DAG names follow similar naming conventions

Notebooks and scripts follow the naming scheme using a description (e.g.,
`resampler`, `notebook`) instead of `downloader` and a proper suffix (e.g.,
`ipynb`, `py`, `sh`)

TODO(gp): The first cell of a notebook contains a description of the content,
including which checks are performed

Production notebooks decide what is an error, by asserting

**Idempotency and catch-up mode**. TODO(gp): Add a paragraph on this.

This is used for any data pipeline (both downloading and processing).

**Example**. An example of system downloading price data has the following
components

TODO(gp): Fix this table
```
+------+--------------+--------------+--------+------------+-------+
| ** |
**Dataset | _ | _ | **Data | \* | | Acti | signature** | *Frequency\*\* | *Dashb
| location** | \*Acti | | on** | | | oard** | | ve?** |
+======+==============+==============+========+============+=======+ | Hi | | -
All of  | ht | s3://... | Yes | | stor | | the past | tps:// | | | | ical | | day
data    | | | | | down | | | | | | | load | | - Once a | | | | | | | day at | | | |
| | | 0:00:00 | | | | | | | UTC | | | |
+------+--------------+--------------+--------+------------+-------+ | R | | -
Last | ... | s3://... | Yes | | eal- | | minute | | | | | time | | data | | | |
| down | | | | | | | load | | - Every | | | | | | | minute | | | |
+------+--------------+--------------+--------+------------+-------+
```

## Data formats

**Storage-invariance of data**. Data should be independent from its storage
format, e.g., CSV, Parquet, relational DB. In other words, converting data from
one format to another should not yield losing any information.

### Data stored by-date

The by-date representation means that there is a file for the data for all the
assets:
```
1546871400 ... Bitcoin
1546871400 ... Ethereum
1546871401 ...
1546871402 ...
```

In other words, the same timestamp is repeated for all the assets.

The original format of the data is by-date, e.g., for a single day is
"20190107.pq" looks like:
```
vendor_date start_time end_time ticker open close volume id
2019-01-07 1546871400 1546871460 A 65.64 65.75 19809 16572.0
2019-01-07 1546871400 1546871460 AA 28.53 28.52 31835 1218568.0
2019-01-07 1546871400 1546871460 AAAU 12.92 12.92 11509 1428781.0
2019-01-07 1546871400 1546871460 AABA 58.90 58.91 7124 10846.0
```

![](docs/datapull/figs/datapull/data_format.png)

There are 2 special columns in the by-date file:

- One that represents the timestamp (`start_time` in the example). This is
  unique and monotonic `(start_time, ticker)` are unique
- One that represents the asset (`ticker` in the example)

### Data stored by-asset

By-asset data means that there is a single file for each single asset with the
data for all the timestamps (no timestamp is repeated in a single file)
```
Bitcoin.pq
1546871400 ...
1546871401
1546871402

Eth.pq
1546871400 ...
1546871401
1546871402
```

### By-asset Parquet data

Data pipelines can transform by-asset data into Parquet data, preserving all the
columns. Successive stages of the pipeline perform other data transformations.
By-asset means that the asset that is in the innermost directory
```
dst_dir/
year=2021/
month=12/
day=11/
asset=BTC_USDT/
data.parquet
asset=ETH_USDT/
data.parquet
```

Typically, the by-date format is just a format that we receive data from, and we
don't want to transform data to.

The name of the asset can depend on the data and it can be `asset`,
`currency_pair`, `ticker`.

By default we use names of columns from the data and we reindex the partitioned
dataframe on datetime, so saved parquet will all have the same datetime index.

Partitioning on year/month/day is optional and should be provided as a switch in
partitioning function.

### Data stored by-tile

Data is organized by tile when the Parquet files are partitioned by asset, by
year, and by month so that it's possible to read only a chunk of that
```
asset=BTC_USDT/
year=2021/
month=12/
data.parquet
year=2021/
month=11/
data.parquet
...
asset=ETH_USDT/
year=2021/
month=12/
data.parquet
...
```
