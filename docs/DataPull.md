# Asset representation

TODO(gp): Ideally we want to use a single schema like `Vendor:ExchangeId:Asset`

\Asset universe

# Sorrentum Data Specification

## ETL

We employ a variation of the ETL approach, called EtLT (i.e., extract, lightly
transform, load, transform) for downloading both data and metadata. We can have
a different pipeline for data and one metadata.

Data is extracted from an external data source, lightly transformed, and then
loaded into permanent storage. Then downstream data pipelines read the data with
a standard client interface.

**Large variety of data.** Data comes in a very large variety, for instance:

- Different vendor can provide the same data

    - E.g., Kibot, Binance, CryptoDataDownload provide data for the Binance
      exchange

- Different time semantics, e.g.,

    - Intervals can be \[a, b) or (a, b\]

    - A bar can be marked at the end or at the beginning of the interval

- Data and metadata

    - Some vendors provide metadata, others don't

- Multiple asset classes (e.g., equities, futures, crypto)

- Data at different time resolutions, e.g.,

    - daily bars

    - minute bars

    - trades

    - order book data

- Historical vs real-time

- Price data vs alternative data

**Storage backend**. Data can be saved in multiple storage backends:

- database (e.g., Postgres, MongoDB)

- local filesystem

- remote filesystems (e.g., AWS S3 bucket)

Data can be saved on filesystems in different formats (e.g., CSV, JSON,
Parquet).

**S3 vs local filesystem.** Unfortunately it's not easy to abstract the
differences between AWS S3 buckets and local filesystems, since the S3 interface
is more along a key-value store rather than a filesystem (supporting
permissions, deleting recursively a directory, moving, etc.).

Solutions based on abstracting a filesystem on top of S3 (e.g., mounting S3 with
Fuse filesystems) are not robust enough.

Some backends (e.g., Parquet) allow handling an S3 bucket transparently.

Our typical approach is:

- When writing to S3, use the local filesystem for staging the data in the
  desired structure and then copy all the data to S3

- When reading from S3, read the data directly, use the functionalities
  supported by the backend, or copy the data locally and then read it from local
  disk

**Data formats**. The main data formats that Sorrentum supports are:

- CSV
- Pros
- Easy to inspect
- Easy to load / save

- Everybody understands it
- Cons

    - Data can't be easily sliced by asset ids / by time

    - Large footprint (non binary), although it can be compressed (e.g., as
      `.csv.gz` on the fly)

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

- database
- Pros

    - Easy to inspect

    - Support any access pattern

    - Friendly for real-time data

- Cons

    - Need devops to manage database instance

    - Difficult to track lineage and version

Unfortunately there is not an obvious best solution so we have to deal with
multiple representations and transforming between them. In practice Parquet is
better suited to store historical data and database to store real time data.

**Extract stage**. The goal is to acquire raw data from an external source and
archive it into a permanent storage backend (e.g., file-system and/or database).
The data can be either historical or real-time. We typically don't process the
data at all, but rather we prefer to save the data raw as it comes from the
wire.

**Transform stage**. Typically we prefer to load the data in the backend with
minor or no transformation. Specifically we allow changing the representation of
the data / format (e.g., removing some totally useless redundancy, compressing
the data, transforming from strings to datetimes). We don't allow changing the
semantics or filter columns. This is done dynamically in the `client` stage

**Load stage.** The load stage simply saves the data into one of the supported
backends.

Typically we prefer to save

- Historical data into Parquet format since it supports more naturally the
  access patterns needed for long simulations

- Real-time data into a database since this makes it easy to append and retrieve
  data in real-time. Often we want to also append real-time data to Parquet

Client stage. The client stage allows downstream pipelines to access data from
the backend storage. The access pattern is always for a model is always "give me
the columns XYZ for assets ABC in the period \[..., ...\]".

We prefer to perform some transformations that are lightweight (e.g., converting
Unix epochs in timestamps) or still evolving (e.g., understanding the timing
semantic of the data) are performed inside this stage, rather than in the
transform stage.

**ETL primitives**. We implement basic primitives that can be combined in
different ways to create various ETL pipelines.

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

    - https://github.com/cryptokaizen/cmamp/pull/3386/files

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

## Data pipelines

Download data by asset (as a time series)

- asset.csv.gz or Parquet

- Historical format

Download data by time, e.g.,

- 20211105/...

- csv.gz or Parquet

- This is typical of real-time flow

## General conventions

**Data invariants**. We use the following invariants when storing data during
data on-boarding and processing:

- Data quantities are associated to intervals are \[a, b) (e.g., the return over
  an interval) or to a single point in time (e.g., the close price at 9am UTC)

- Every piece of data is labeled with the end of the sampling interval or with
  the point-in-time

    - E.g., for a quantity computed in an interval \[06:40:00, 06:41:00) the
      timestamp is 06:41:00

- Timestamps are always time-zone aware and use UTC timezone

- Every piece of data has a knowledge timestamp (aka "as-of-date") which
  represent when we were aware of the data according to our wall-clock:

    - Multiple timestamps associated with different events can be tracked, e.g.,
      `start_download_timestamp`, `end_download_timestamp`

    - No system should depend on data available strictly before the knowledge
      timestamp

- Data is versioned: every time we modify the schema or the semantics of the
  data, we bump up the version using semantic versioning and update the
  changelog of what each version contains

An example of tabular data is below:

![](./sorrentum_figs/image5.png){width="6.5in" height="1.0138888888888888in"}

**Data organization**. We keep data together by execution run instead of by data
element.

E.g., assume we run a flow called `XYZ_sanity_check` every day and the flow
generates three pieces of data, one file `output.txt` and two directories
`logs`, `temp_data`.

We want to organize the data in a directory structure like:

**_Better_**

```

-   XYZ_sanity_check/

    -   run.{date}/

        -   output.txt

        -   logs/

        -   temp_data/

    -   run.{date}.manual/

        -   output.txt

        -   logs/

        -   temp_data/

```

**_Worse_**

```

-   XYZ_sanity_check/

    -   output.{date}/

        -   output.txt

    -   logs.{date}/

    -   temp_data.{date}/

```

The reasons why the first data layout is superior are:

1.  It's easier to delete a single run by deleting a single dir instead of
    deleting multiple files

2.  It allows the format of the data to evolve over time without having to
    change the schema of the data retroactively

3.  It allows scripts post-processing the data to point to a directory with a
    specific run and work out of the box

4.  it's easier to move the data for a single run from one dir (e.g., locally)
    to another (e.g., a central location) in one command

5.  there is redundancy and visual noise, e.g., the same data is everywhere

We can tag directory by a run mode (e.g., `manual` vs `scheduled`) by adding the
proper suffix to a date-dir.

**Directory with one file**. Having a directory containing one single file often
creates redundancy.

We prefer not to use directories unless they contain more than one file. We can
use directories if we believe that it's highly likely that more files will be
needed, but as often happens YANGI (you are not going to need it) applies.

**Naming convention**.

- We use `.` to separate conceptually different pieces of a file or a directory.

- We don't allow white spaces since they are not Linux friendly and need to be
  escaped. We replace white spaces with `_`.

- We prefer not to use `-` whenever possible, since they create issues with
  Linux auto-completion and need to be escaped.

E.g., `bulk.airflow.csv` instead of `bulk_airflow.csv`

**Data pipeline classification**. A data pipeline can be any of the following:

- a downloader

    - External DB (e.g., data provider) -> Internal DB: the data flows from an
      external API to an internal DB

    - It downloads historical or real-time data and saves the dataset in a
      location

    - The name of the script and the location of the data downloaded follow the
      naming scheme described below

    - It is typically implemented as a Python script

- a QA flow for a single or multiple datasets

    - Internal DB -> Process

    - It computes some statistics from one or more datasets (primary or derived)
      and throws an exception if the data is malformed

    - It aborts if the data has data not compliant to certain QA metrics

    - It is typically implemented as a Python notebook backed by a Python library

- a derived dataset flow

    - Internal DB -> Process -> Internal DB

    - It computes some data derived from an existing data set

        - E.g., resampling, computing features

    - It is typically implemented as a Python script

- a model flow

    - Internal DB -> Process -> Outside DB (e.g., exchange)

    - E.g., it runs a computation from internal data and places some trades

    - It is typically implemented as a Python script

**Data classification**. Data can be from market sources or from non-market (aka
alternative) sources. Each data source can come with metadata, e.g.,

- List of assets in the universe over time

- Attributes of assets (e.g., industry and other classification)

**Asset universe**. Often the data relates to a set of assets, e.g., currency
pairs on different exchanges. The support of the data is referred to as the
"data universe". This metadata is versioned as any other piece of data.

## Data set downloading and handling

**Data set naming scheme**. Each data set is stored in a data lake with a path
and name that describe its metadata according to the following signature:

dataset_signature={download_mode}.{downloading_entity}.{action_tag}.{data_format}.{data_type}.{asset_type}.{universe}.{vendor}.{exchange_id}.{version\[-snapshot\]}.{extension}

TODO(gp): @juraj add a {backend} = s3, postgres, mongo, local_file

The signature schema might be dependent on the backend

E.g.,
bulk/airflow/downloaded_1min/csv/ohlcv/futures/universe_v1_0/ccxt/binance/v1_0-20220210/BTC_USD.csv.gz

We use `-` to separate pieces of the same attribute (e.g., version and snapshot)
and `_` as replacements of a space character.

The organization of files in directories should reflect the naming scheme. We
always use one directory per attribute for files (e.g., `bulk.airflow.csv/...`
or `bulk/airflow/csv/...`). When the metadata is used not to identify a file in
the filesystem (e.g., for a script or as a tag) then we use `.` as separators
between the attributes.

**Data set attributes**. There are several "attributes" of a data set:

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
      `periodic-5mins`, `periodic-EOD`) if it needs to be identified with respect
      to others

- `unit_test`

    - Data used for unit test (independently if it was downloaded automatically or
      created manually)

-
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

    - Typically the universe can have further characteristics and it can be also
      versioned

    - E.g., `universe_v1_7`

- `vendor`: the source that provided the data

    - Aka "provider"

- E.g., `ccxt`, `crypto_chassis`, `cryptodata_download`, `talos`, `kaiko`,

- Data can also be downloaded directly from an exchange (e.g., `coinbase`,
  `binance`)

- `exchange_id`: which exchange the data refers to
- E.g., `binance`
- `version`: any data set needs to have a version

    - Version is represented as major, minor, patch according to semantic
      versioning in the format `v{a}_{b}_{c}` (e.g., v1_0_0)

    - If the schema of the data is changed the major version is increased

    - If a bug is fixed in the downloader that improves the semantic of the data
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

    - Typically there is one file per asset (e.g., `BTC_USDT.csv.gz`)

    - Certain data formats can organize the data in a more complex way

        - E.g., Parquet files save the data in a directory structure
          `{asset}/{year}/{month}/data.parquet`

It is possible that a single data set covers multiple values of a specific
attribute

- E.g., a data set storing data for both futures and spot, can have
  `asset_type=futures_spot`

Not all the cross-products are possible, e.g.

- there is no data set with `download_mode=periodic` scheduled by Airflow and
  `downloading_entity=manual`

We organize the schema in terms of access pattern for the modeling and analysis
stage

- E.g., `snapshot` comes before `vendor` since in different snapshots we can
  have different universes

- E.g., snapshot -> dataset -> vendor -> exchange -> coin
- A universe is just a mapping of a tag (e.g., v5) to a set of directories

Each data set has multiple columns.

**References**.

The list of data sources on CK S3 bucket is
[[Bucket data organization]{.underline}](https://docs.google.com/document/d/1C-22QF_gOe1k4HgyD6E6iOO_F_FxKKECd4MXaJEuTxo/edit#heading=h.azfdjheqqtp)

Useful notebooks for processing data is
[[Master notebooks]{.underline}](https://docs.google.com/document/d/17N8OTI1zxXI-l3OYDVcft1spDf0-JPUU8UiNCfBLYvY/edit#heading=h.uxyv8hg7offz)

CK data specs:
[[Data pipelines - Specs]{.underline}](https://docs.google.com/document/d/1nLhaFBSHVrexCcwJMnpXlkqwn0l6bDiVer34GKVclYY/edit#)

## Data on-boarding flow

**Downloader types.** For each data set, there are typically two types of
downloaders: bulk and periodic. This step is often the equivalent of the Extract
phase in an ETL / ELT / EtLT pipeline.

E.g., an EtLT pipeline can consists of the following phases:

- E: extract 1 minute data from websocket,

- t: apply non-business logic related transformation from JSON to dataframe

- L: load into SQL

- T: transform the data resampling to 5 minute data

**Bulk downloaders**. Download past data querying from an API.

A characteristic of bulk downloaders is that the download is not scheduled to be
repeated on a regular basis. It is mostly executed once (e.g., to get the
historical data) or a few times (e.g., to catch up with an intermittent data
feed). It is executed (or at least triggered) manually.

The data is downloaded in bulk mode at $T_{dl,bulkhist}$ to catch up with the
historical data up to the moment of the deployment of the periodic downloader
scheduled every period $\\Delta
t_{deploy,periodic}$.

The bulk download flow is also needed any time we need to "catch up" with a
missing periodic download, e.g., if the real-time capture system was down.

**Preferred bulk data format**. Typically the data is saved in a format that
allows data to be loaded depending on what's needed from downstream systems
(e.g., Parquet using tiles on assets and period of times).

**Periodic downloaders**. Download the data querying an API every period
$\\Delta T_{dl,periodic}$, which depends on the application needs, e.g., every
second, minute. Typically periodic downloaders are triggered automatically
(e.g., by a workflow orchestrator like Apache Airflow).

Another possible name is "streaming" data. Typical example is a websocket feed
of continuous data.

**Preferred real-time data format**. Typically we save data in a DB to be able
to easily query the data from the model. Often we also save data to an
historical-like format to have a backup copy of the data. Parquet format is not
ideal since it's not easy to append data.

TODO(gp): Add diagram

Providers -> us

It's the extract in ETL

**Downloader naming scheme.** A downloader has a name that represents the
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
ipynb, py, sh)

TODO(gp): The first cell of a notebook contains a description of the content,
including which checks are performed

Production notebooks decide what is an error, by asserting

**Idempotency and catch-up mode**. TODO(gp): Add a paragraph on this.

This is used for any data pipeline (both downloading and processing).

**Example**. An example of system downloading price data has the following
components

+------+--------------+--------------+--------+------------+-------+ | ** |
**Dataset | _ | _ | **Data | \* | | Acti | signature** | *Frequency\*\* | *Dashb
| location** | \*Acti | | on** | | | oard** | | ve?** |
+======+==============+==============+========+============+=======+ | Hi | | -
All of | ht | s3://... | Yes | | stor | | the past | tps:// | | | | ical | | day
data | | | | | down | | | | | | | load | | - Once a | | | | | | | day at | | | |
| | | 0:00:00 | | | | | | | UTC | | | |
+------+--------------+--------------+--------+------------+-------+ | R | | -
Last | ... | s3://... | Yes | | eal- | | minute | | | | | time | | data | | | |
| down | | | | | | | load | | - Every | | | | | | | minute | | | |
+------+--------------+--------------+--------+------------+-------+

[[Airflow Active]{.underline}](https://docs.google.com/document/d/14C4PFU1nNd0l7sBLCz5DxvrTk_nakmiTHwCVbIUwgfs/edit#heading=h.ntti3mum3dph)
[[Downloaders]{.underline}](https://docs.google.com/document/d/14C4PFU1nNd0l7sBLCz5DxvrTk_nakmiTHwCVbIUwgfs/edit#heading=h.ntti3mum3dph)

Describe ETL layer from
[[Design - Software components]{.underline}](https://docs.google.com/document/d/1C-22QF_gOe1k4HgyD6E6iOO_F_FxKKECd4MXaJEuTxo/edit#heading=h.iwaxiv19a4zu)

[[OHLCV data pipeline]{.underline}](https://docs.google.com/document/d/1nLhaFBSHVrexCcwJMnpXlkqwn0l6bDiVer34GKVclYY/edit)

## Data QA workflows

**Quality-assurance metrics**. Each data set has QA metrics associated with it
to make sure the data has the minimum expected data quality.

- E.g., for 1-minute OHLCV data, the possible QA metrics are:

    - missing bars (timestamp)

    - missing / nan OHLCV values within an individual bar

    - points with volume = 0

    - data points where OHLCV data is not in the correct relationship (e.g., H and
      L are not higher or lower than O and C),

    - data points where OHLCV data are outliers (e.g., they are more than N
      standard deviations from the running mean)

**Bulk data single-dataset QA metrics**. It is possible to run the QA flow to
compute the quality of the historical data. This is done typically as a one-off
operation right after the historical data is downloaded in bulk. This touches
only one dataset, namely the one that was just downloaded.

-

**Periodic QA metrics**. Every N minutes of downloading real-time data, the QA
flow is run to generate statistics about the quality of the data. In case of low
data quality data the system sends a notification.

**Cross-datasets QA metrics**. There are QA workflows that compare different
data sets that are related to each other, e.g.,

- consider the case of downloading the same data (e.g., 1-minute OHLCV for spot
  BTC_USDT from Binance exchange) from different providers (e.g., Binance
  directly and a third-party provider) and wanting to compare the data under the
  assumption to be the same

-

- consider the case where there is a REST API that allows to get data for a
  period of data, and a WebSocket that streams the data

- consider the case where one gets an historical dump of the data from a third
  party provider vs the data from the exchange real-time stream

- consider the case of NASDAQ streaming data vs TAQ data disseminated once the
  market is close

**Historical / real-time QA flow.** Every period $T_{dl,hist}$, a QA flow is run
where the real-time data is compared to the historical data to ensure that the
historical view of the data matches the real-time one.

This is necessary but not sufficient to guarantee that the bulk historical data
can be reliably used as a proxy for the real-time data as-of, in fact this is
simply a self-consistency check. We do not have any guarantee that the data
source collected correct historical data.

**Data QA workflow naming scheme.** A QA workflow has a name that represents its
characteristics in the format:

{qa_type}.{dataset_signature}

E.g.,

production_qa.{download_mode}.{downloading_entity}.{action_tag}.{data_format}.{data_type}.{asset_type}.{universe}.{vendor}.{exchange}.{version\[-snapshot\]}.{asset}.{extension}

E.g.,

research_cross_comparison.periodic.airflow.downloaded_1sec_1min.all.bid_ask.futures.all.ccxt_cryptochassis.all.v1_0_0

where:

- `qa_type`: the type of the QA flow, e.g.,

    - `production_qa`: perform a QA flow on historical and real-time data. The
      interface should be an IM client and thus it should be possible to run QA on
      both historical and real-time data

    - `research_analysis`: perform a free-form analysis of the data. This can then
      be the basis for a `qa` analysis

    - `compare_historical_real_time`: compare historical and real-time data coming
      from the same source of data

    - `compare_historical_cross_comparison`: compare historical data from two
      different data sources

The same rules apply as in downloader and derived dataset for the naming scheme.

Since cross-comparison involves two (or more dataset) we use a short notation
merging the attributes that differ. E.g., a comparison between the datasets

- periodic.1minute.postgres.ohlcv.futures.1minute.ccxt.binance

- periodic.1day.postgres.ohlcv.futures.1minute.ccxt.binance

is called:

compare_qa.periodic.1minute-1day.postgres.ohlcv.futures.1minute.ccxt.binance

since the only difference is in the frequency of the data sampling.

It is possible to use a long format
`{dataset_signature1}-vs-{dataset_signature2}`.

**Examples**.

+------+---------+----------------+------------+------+-------+-----+
| **   | **      | *              | **F        | *    | *     | **  |
| Symb | Dataset | *Description** | requency** | *Das | *Data | Act |
| olic | sign    |                |            | hboa | locat | ive |
| na   | ature** |                |            | rd** | ion** | ?** |
| me** |         |                |            |      |       |     |
+======+=========+================+============+======+=======+=====+
| hist | His     | -              | -   All of |      | s3:/  | Yes |
| _dl1 | torical |                |     the    |      | /\... |     |
|      | d       |                |     past   |      |       |     |
|      | ownload |                |     day    |      |       |     |
|      |         |                |     data   |      |       |     |
|      |         |                |            |      |       |     |
|      |         |                | -   Once a |      |       |     |
|      |         |                |     day at |      |       |     |
|      |         |                |            |      |       |     |
|      |         |                |    0:00:00 |      |       |     |
|      |         |                |     UTC    |      |       |     |
+------+---------+----------------+------------+------+-------+-----+
| rt   | Re      | -              | Every      |      | s3:/  | Yes |
| _dl1 | al-time |                | minute     |      | /\... |     |
|      | d       |                |            |      |       |     |
|      | ownload |                |            |      |       |     |
+------+---------+----------------+------------+------+-------+-----+
| rt   | Re      | Check QA       | Every 5    |      | s3:/  | Yes |
| _dl1 | al-time | metrics for    | minutes    |      | /\... |     |
| .qa1 | QA      | dl1            |            |      |       |     |
|      | check   |                |            |      |       |     |
+------+---------+----------------+------------+------+-------+-----+
| h    | Check   | Check          | Once a day |      |       |     |
| ist_ | of      | consistency    | at 0:15:00 |      |       |     |
| dl1. | his     | between        | UTC        |      |       |     |
| rt_d | torical | historical and |            |      |       |     |
| l1.c | vs      | real-time CCXT |            |      |       |     |
| heck | re      | binance data   |            |      |       |     |
|      | al-time |                |            |      |       |     |
+------+---------+----------------+------------+------+-------+-----+
| rt   | Re      | -   vendor     | Every      |      | s3:/  | Yes |
| _dl2 | al-time | =CryptoChassis | minute     |      | /\... |     |
|      | d       |                |            |      |       |     |
|      | ownload | -   ex         |            |      |       |     |
|      |         | change=Binance |            |      |       |     |
|      |         |                |            |      |       |     |
|      |         | -   data       |            |      |       |     |
|      |         |                |            |      |       |     |
|      |         |   type=bid/ask |            |      |       |     |
+------+---------+----------------+------------+------+-------+-----+
| rt   | Re      | Check QA       | Every 5    |      | s3:/  | Yes |
| _dl2 | al-time | metrics for    | minutes    |      | /\... |     |
| .qa2 | QA      | dl3            |            |      |       |     |
|      | check   |                |            |      |       |     |
+------+---------+----------------+------------+------+-------+-----+
| rt_d | Cro     | Compare data   | Every 5    |      |       |     |
| l1_d | ss-data | from rt_dl1    | minutes    |      |       |     |
| l2.c | QA      | and rt_dl2     |            |      |       |     |
| heck | check   |                |            |      |       |     |
+------+---------+----------------+------------+------+-------+-----+

## Derived data workflows

**Derived data workflows**. Data workflows can transform datasets into other
datasets

- E.g., resample 1 second data into 1 minute data

The data is then written back to the same data source as the originating data
(e.g., DB for period / real-time data, Parquet / csv / S3 for historical data).

TODO(gp): Add a plot (we are the source of the provider)

**Derived data naming scheme**. We use the same naming scheme as in downloaded
data set {dataset_signature}

but we encode the information about the content of the newly generated data in
the `action_tag` attribute of the data, e.g., `resample_1min` to distinguish it
from `downloaded_1sec`.

We use this approach so that the scheme of the derived data is the same as a
downloaded data set.

**Derived data research flow.** The goal is to decide how to transform the raw
data into derived data and come up with QA metrics to assess the quality of the
transformation

- It can be cross-vendor or not

- E.g., sample 1sec data to 1min and compare to a reference. The sampling is
  done on the fly since the researcher is trying to understand how to resample
  (e.g., removing outliers) to get a match

**Derived data production flow.** The research flow is frozen and put in
production

- E.g., run the resample script to sample and write back to DB and historical
  data. This flow can be run in historical mode (to populate the backend with
  the production data) and in real-time mode (to compute the streaming data)

**Derived data QA flow**. the goal is to monitor that the production flow is
still performing properly with respect to the QA metrics

- E.g., the 1-sec to 1-min resampling is not performed on-the-fly, but it uses
  the data already computed by the script in the production flow.

- This flow is mainly run in real-time, but we might want to look at QA
  performance also historically

This same distinction can also be applied to feature computation and to the
machine learning flow.

Provider -> data -> Us -> derived flow -> Us -> features -> Us -> ML -> Exchange

## Data formats

**Storage-invariance of data.** Data should be independent from its storage
format, e.g., CSV, Parquet, Relational DB. In other words, converting data from
one format to another should not yield losing any information.

**Data stored by-date.** The by-date representation means that there is a file
for the data for all the assets:

1546871400 ... Bitcoin

1546871400 ... Ethereum

1546871401 ...

1546871402 ...

Thus the same timestamp is repeated for all the assets

The original format of the data is by-date and for a single day is "20190107.pq"
like:

vendor_date start_time end_time ticker open close volume id

2019-01-07 1546871400 1546871460 A 65.64 65.75 19809 16572.0

2019-01-07 1546871400 1546871460 AA 28.53 28.52 31835 1218568.0

2019-01-07 1546871400 1546871460 AAAU 12.92 12.92 11509 1428781.0

2019-01-07 1546871400 1546871460 AABA 58.90 58.91 7124 10846.0

![](./sorrentum_figs/image4.png){width="6.5in" height="1.0416666666666667in"}

There are 2 special columns in the by-date file:

- one that represents the timestamp ("start_time" in the example). This is
  unique and monotonic (start_time, ticker) are unique

- one that represents the asset ("ticker" in the example)

**Data stored by-asset.** By-asset data means that there is a single file for
each single asset with the data for all the timestamps (no timestamp is repeated
in a single file)

Bitcoin.pq

1546871400 ...

1546871401

1546871402

Eth.pq

1546871400 ...

1546871401

1546871402

By-asset Parquet data. Data pipelines can transform by-asset data into Parquet
data, preserving all the columns. Successive stages of the pipeline perform
other data transformations. By-asset means that the asset that is in the
innermost directory

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

Typically the by-date format is just a format that we receive data from, and we
don't want to transform data to.

The name of the asset can depend on the data and it can be `asset`,
`currency_pair`, `ticker`.

By default we use names of columns from the data and we reindex the partitioned
dataframe on datetime, so saved parquet will all have the same datetime index.

Partitioning on year/month/day is optional and should be provided as a switch in
partitioning function.

**Data stored by-tile.** Data is organized by tile when the Parquet files are
partitioned by asset, by year, and by month so that it's possible to read only a
chunk of that

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

## Sandbox

This paragraph describes an example of infrastructure that implements the
Sorrentum Data Protocol. The

It is a Docker Container containing the following services:

- Airflow

- Jupyter notebook

- Postgres

- MongoDB

**Notes for CK devs (TO REMOVE)**

- It is a separated code base from Sorrentum and it shares only a few base
  library (e.g., `helpers`)

- It is a scaled down version of CK production infrastructure (e.g., managed
  Airflow is replaced by a local Airflow instance)

- The system follows closely the production system described in
  [[Data pipelines - Specs]{.underline}](https://docs.google.com/document/d/1nLhaFBSHVrexCcwJMnpXlkqwn0l6bDiVer34GKVclYY/edit#)

## Data client stack

Once the data is downloaded, it needs to be retrieved for processing in a common
format (e.g., DataFlow format).

We use a two layer approach to split the complexity / responsibilities of
reading the data

- `ImClient`

    - Data is downloaded and saved with minimal or no transformation

    - Adapts from the vendor data to a standard internal "MarketData" format

- MarketData\` implements "behaviors" that are orthogonal to vendors, e.g.,

    - RealTime or Historical

    - Stitched (i.e.,"overlap" multiple data sources giving a single view of the
      data). E.g., the data from the last day comes from a real-time source while
      the data before that comes from an historical source

    - Replayed = serialize the data to disk and read it back, implementing also
      knowledge time as-of-time semantic. This behavior is orthogonal to RealTime,
      Historical, Stitched, i.e., one can replay any `MarketData`, including an
      already replayed one

Data format for `ImClient` / `MarketData` pipeline

- Both `ImClient` and `MarketData` have an output format that is enforced by the

base and the derived classes together

- `ImClient` and `MarketData` have 3 interfaces each:

1. an external "input" format for a class

- format of the data as input to a class derived from `MarketData` /

`ImClient`

2. an internal "input" format

- format that derived classes need to follow so that the corresponding base

class can do its job, i.e., apply common transformations to all

`MarketData` / `ImClient` classes

3. an external "output" format

- format of the data outputted by any derived class from `MarketData` /

`ImClient`

- The chain of transformations is:

- Class derived from `ImClient`

- The transformations are vendor-specific

- `ImClient`

- The transformations are fixed

- Class derived from `MarketData`

- The transformations are specific to the `MarketData` concrete class :qa

- `MarketData`

- The transformations are fixed

```plantuml

\[Vendor\] -> \[DerivedImClient\]

\[DerivedImClient\] -> \[AbstractImClient\]

\[AbstractImClient\] -> \[DerivedMarketData\]

\[DerivedMarketData\] -> \[AbstractMarketData\]

```

Transformations performed by classes derived from `ImClient`

- Whatever is needed to transform the vendor data into the internal
  format accepted

by base `ImClient`

- Only derived classes `ImClient` knows what is exact semantic of the
  vendor-data

Transformations performed by abstract class `ImClient`

- Implemented by `ImClient._apply_im_normalization()`

Output format of `ImClient`

- TODO(\*): Check the code in `ImClient` since that might be more up
  to date than

this document and, if needed, update this doc

- The data in output of a class derived from `ImClient` is normalized
  so that:

- the index:

- represents the knowledge time

- is the end of the sampling interval

- is called `timestamp`

- is a tz-aware timestamp in UTC

- the data:

- is resampled on a 1 minute grid and filled with NaN values

- is sorted by index and `full_symbol`

- is guaranteed to have no duplicates

- belongs to intervals like \[a, b\]

- has a `full_symbol` column with a string representing the canonical
  name

of the instrument

- TODO(gp): We are planning to use an `ImClient` data format closer
  to `MarketData`

by using `start_time`, `end_time`, and `knowledge_time` since
these can be

inferred only from the vendor data semantic

Transformations performed by classes derived from `MarketData`

- Classes derived from `MarketData` do whatever they need to do in
  `_get_data()` to

get the data, but always pass back data that:

- is indexed with a progressive index

- has asset, start_time, end_time, knowledge_time

- start_time, end_time, knowledge_time are timezone aware

- E.g.,

```

asset_id start_time end_time close volume

idx

0 17085 2021-07-26 13:41:00+00:00 2021-07-26 13:42:00+00:00 148.8600 400176

1 17085 2021-07-26 13:30:00+00:00 2021-07-26 13:31:00+00:00 148.5300 1407725

2 17085 2021-07-26 13:31:00+00:00 2021-07-26 13:32:00+00:00 148.0999 473869

```

Transformations performed by abstract class `MarketData`

- The transformations are done inside `get_data_for_interval()`,
  during normalization,

and are:

- indexing by `end_time`

- converting `end_time`, `start_time`, `knowledge_time` to the
  desired timezone

- sorting by `end_time` and `asset_id`

- applying column remaps

Output format of `MarketData`

- The base `MarketData` normalizes the data by:

- sorting by the columns that correspond to `end_time` and
  `asset_id`

- indexing by the column that corresponds to `end_time`, so that it
  is suitable

to DataFlow computation

- E.g.,

```

asset_id start_time close volume

end_time

2021-07-20 09:31:00-04:00 17085 2021-07-20 09:30:00-04:00 143.990 1524506

2021-07-20 09:32:00-04:00 17085 2021-07-20 09:31:00-04:00 143.310 586654

2021-07-20 09:33:00-04:00 17085 2021-07-20 09:32:00-04:00 143.535 667639

```

Asset ids format

- `ImClient` uses assets encoded as `full_symbols` strings (e.g.,
  `binance::BTC_UTC`)

- There is a vendor-specific mapping:

- from `full_symbols` to corresponding data

- from `asset_ids` (ints) to `full_symbols` (strings)

- If the `asset_ids` -> `full_symbols` mapping is provided by the
  vendor, then we

reuse it

- Otherwise, we build a mapping hashing `full_symbols` strings into
  numbers

- `MarketData` and everything downstream uses `asset_ids` that are
  encoded as ints

- This is because we want to use ints and not strings in dataframe

Handling of `asset_ids`

- Different implementations of `ImClient` backing a `MarketData`
  are possible,

e.g.:

- The caller needs to specify the requested `asset_ids`

- In this case the universe is provided by `MarketData` when calling
  the

data access methods

- The reading backend is initialized with the desired universe of
  assets and

then `MarketData` just uses or subsets that universe

- For these reasons, assets are selected at 3 different points:

1) `MarketData` allows to specify or subset the assets through
   `asset_ids`

through the constructor

2) `ImClient` backends specify the assets returned

- E.g., a concrete implementation backed by a DB can stream the data
  for

its entire available universe

3) Certain class methods allow querying data for a specific asset or
   subset

of assets

- For each stage, a value of `None` means no filtering

Handling of filtering by time

- Clients of `MarketData` might want to query data by:

- using different interval types, namely \`\[a, b), \[a, b\], (a, b\],
  (a, b)\`

- filtering on either the `start_ts` or `end_ts`

- For this reason, this class supports all these different ways of
  providing data

- `ImClient` has a fixed semantic of the interval `\[a, b\]`

- `MarketData` adapts the fixed semantic to multiple ones

Handling timezone

- `ImClient` always uses UTC as output

- `MarketData` adapts UTC to the desired timezone, as requested by
  the client

## Checklist for releasing a new data set

-   Decide what to do exactly (e.g., do we download only bulk data or
    also real-time?)

-   Review what code we have and what can be generalized to accomplish
    the task at hand

-   Decide what's the name of the data set according to our convention

-   Create DAGs for Airflow

-   Update the Raw Data Gallery
    im_v2/common/notebooks/Master_raw_data_gallery.ipynb

-   Quick exploratory analysis to make sure the data is not malformed

-   Update the table in [[Data pipelines -
    Specs]{.underline}](https://docs.google.com/document/d/1nLhaFBSHVrexCcwJMnpXlkqwn0l6bDiVer34GKVclYY/edit#heading=h.8g5ajvlq6zks)

-   Exploratory analysis for a thorough QA analysis

-   Add QA system to Airflow prod

