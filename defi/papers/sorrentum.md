TODO(gp): Once more stable switch to Latex. gdoc is good for interacting
and iterating quickly

# Surrentum: a protocol for virtual consolidation of distributed digital financial systems

GP Saggese, Paul Smith

v0.1 (Dec 1 2022)

# \# Abstract

Surrentum[^1] is a protocol to:

-   implement distributed investment strategies

-   enforce a unified view of market exchanges, both centralized and
    > decentralized, with benefits to the efficiency of the entire
    > system

-   incentivize participants that stake their cryptocurrencies to
    > provide liquidity to digital asset markets

Surrentum protocol contains:

-   A framework for describing, simulating, assessing, and deploying
    > financial machine learning models operating on any object on a
    > blockchain (e.g., cryptocurrencies, tokens, NFTs, smart contracts)

-   A suite of standards for interoperability between components of the
    > ecosystem (e.g., Surrentum Oracles, Surrentum Nodes, Market
    > exchanges)

-   Smart contracts allowing peer-to-peer staking of cryptocurrencies
    > used to provide liquidity to the financial system and be rewarded
    > with investment returns

# \# Motivation

Increasing adoption and continued decentralization of digital asset
markets fragments and distorts prices, leading to volatile markets.

![](media/image12.png){width="6.5in" height="2.5416666666666665in"}

At the current market capitalization of over \$2+ trillion and daily
trading volume exceeding \$50 billion across 20,000 currencies and 500
crypto exchanges, research estimates pricing inefficiencies in digital
markets to be between \$50m-200m per day.

The intrinsic appeals of crypto currency (i.e., the lack of a
centralized authority) and its rapid adoption across retail transactions
will only exacerbate the problem.

Historical solutions (e.g., Nasdaq regulation in the 1990s and US
Government Reg-NMS in 2005) have relied on government initiatives to
regulate stock exchanges and brokers to execute trades at the best
possible price across multiple exchanges. In the case of
cryptocurrencies, such a solution is impossible, given the decentralized
nature of its market and the ways in which consensus is achieved.

# \# Background

## \## Measures of fragmentation

TODO(gp): Redo the plots better

![](media/image3.png){width="6.5in"
height="2.3854166666666665in"}![](media/image1.png){width="6.499991251093613in"
height="2.3854166666666665in"}

![](media/image7.png){width="6.218640638670166in"
height="2.1625656167979in"}![](media/image11.png){width="6.1338145231846015in"
height="2.1625656167979in"}

## \## Arbitrage in financial crypto markets

Centralized market makers and hedge funds provide liquidity to markets
in exchange for substantial profits. Surrentum Protocol democratizes the
access to these profits by allowing anybody to stake arbitrage
activities, receiving profits in return.

Building a quantitative investment firm has high barriers to entry,
because it:

-   needs trading capital (it can be proprietary or open to outsiders)

-   collects fees when investing capital on behalf of outside investors

-   needs to spend to hire expensive researchers and machine learning
    > experts

-   needs to spend money to buy external data

Problems with quantitative investment firms in traditional financial
markets

-   Opacity of returns

-   Limitations for people to participate (e.g., accredited investors)

-   Lock-in of capital

## \## Arbitrage

Different types of arbitrage with different risk and return profiles

-   Mechanical arbitrage (single asset on different exchanges)

-   Cross-asset arbitrage (single or

-   Statistical arbitrage (enforce properties of

-   Market making

TODO(gp): all Describe a bit

# \# Surrentum Protocol

Surrentum Node

-   Receive data from exchanges and external oracles

-   Export data using the Surrentum Data Protocol

-   People stake their currencies which are then used to run the nodes

-   Nodes are scored in terms of their return quality, correlation with
    > markets, drawdowns

-   TODO(gp): This is System

Surrentum Data Protocol

-   Specifies how data can be fed to Surrentum Nodes

-   Point-in-time view (e.g., data is annotated with knowledge time)

-   Clear semantic (e.g., using intervals and specify clearly interval
    > semantic)

-   Data quality assurance (e.g., historical and real-time
    > reconciliation)

-   TODO(gp): This is MarketData

Surrentum Pools

-   Liquidity pools used by Surrentum nodes, where anybody can stake
    > cryptos that Surrentum Nodes can use and earn rewards

Surrentum Library

-   A library to build Surrentum Nodes using fixed API

-   Provides:

    -   Surrentum Data Protocol

    -   Surrentum Node Interface

    -   Configuration layer

    -   Financial ML: the universal pricing machine

    -   Connectivity to Exchanges

-   TODO(gp): This is amp

Surrentum Token

-   Governance token

-   Used to compensate staked cryptos

-   Used for returns for building and operating Surrentum Nodes

# \# Asset representation

TODO(gp): Ideally we want to use a single schema like
\`Vendor:ExchangeId:Asset\`

\\# Asset universe

# \# Surrentum Data Specification

## \## ETL

We employ a variation of the ETL approach, called EtLT (i.e., extract,
lightly transform, load, transform) for downloading both data and
metadata. We can have a different pipeline for data and one metadata.

Data is extracted from an external data source, lightly transformed, and
then loaded into permanent storage. Then downstream data pipelines read
the data with a standard client interface.

**Large variety of data.** Data comes in a very large variety, for
instance:

-   Different vendor can provide the same data

    -   E.g., Kibot, Binance, CryptoDataDownload provide data for the
        > Binance exchange

-   Different time semantics, e.g.,

    -   Intervals can be \[a, b) or (a, b\]

    -   A bar can be marked at the end or at the beginning of the
        > interval

-   Data and metadata

    -   Some vendors provide metadata, others don\'t

-   Multiple asset classes (e.g., equities, futures, crypto)

-   Data at different time resolutions, e.g.,

    -   daily bars

    -   minute bars

    -   trades

    -   order book data

-   Historical vs real-time

-   Price data vs alternative data

**Storage backend**. Data can be saved in multiple storage backends:

-   database (e.g., Postgres, MongoDB)

-   local filesystem

-   remote filesystems (e.g., AWS S3 bucket)

Data can be saved on filesystems in different formats (e.g., CSV, JSON,
Parquet).

**S3 vs local filesystem.** Unfortunately it's not easy to abstract the
differences between AWS S3 buckets and local filesystems, since the S3
interface is more along a key-value store rather than a filesystem
(supporting permissions, deleting recursively a directory, moving,
etc.).

Solutions based on abstracting a filesystem on top of S3 (e.g., mounting
S3 with Fuse filesystems) are not robust enough.

Some backends (e.g., Parquet) allow handling an S3 bucket transparently.

Our typical approach is:

-   When writing to S3, use the local filesystem for staging the data in
    > the desired structure and then copy all the data to S3

-   When reading from S3, read the data directly, use the
    > functionalities supported by the backend, or copy the data locally
    > and then read it from local disk

**Data formats**. The main data formats that Surrentum supports are:

-   CSV

```{=html}
<!-- -->
```
-   Pros

```{=html}
<!-- -->
```
-   Easy to inspect

```{=html}
<!-- -->
```
-   Easy to load / save

-   Everybody understands it

```{=html}
<!-- -->
```
-   Cons

    -   Data can\'t be easily sliced by asset ids / by time

    -   Large footprint (non binary), although it can be compressed
        > (e.g., as \`.csv.gz\` on the fly)

```{=html}
<!-- -->
```
-   Parquet

```{=html}
<!-- -->
```
-   Pros

    -   Compressed

    -   AWS friendly

    -   Data can be easily sliced by asset ids and time

-   Cons

    -   Not easy to inspect

        -   Solution: use wrapper to convert to CSV

    -   Difficult to append

        -   Solution: use chunking + defragmentation

    -   Cumbersome for real-time data

```{=html}
<!-- -->
```
-   database

```{=html}
<!-- -->
```
-   Pros

    -   Easy to inspect

    -   Support any access pattern

    -   Friendly for real-time data

-   Cons

    -   Need devops to manage database instance

    -   Difficult to track lineage and version

Unfortunately there is not an obvious best solution so we have to deal
with multiple representations and transforming between them. In practice
Parquet is better suited to store historical data and database to store
real time data.

**Extract stage**. The goal is to acquire raw data from an external
source and archive it into a permanent storage backend (e.g.,
file-system and/or database). The data can be either historical or
real-time. We typically don\'t process the data at all, but rather we
prefer to save the data raw as it comes from the wire.

**Transform stage**. Typically we prefer to load the data in the backend
with minor or no transformation. Specifically we allow changing the
representation of the data / format (e.g., removing some totally useless
redundancy, compressing the data, transforming from strings to
datetimes). We don't allow changing the semantics or filter columns.
This is done dynamically in the \`client\` stage

**Load stage.** The load stage simply saves the data into one of the
supported backends.

Typically we prefer to save

-   Historical data into Parquet format since it supports more naturally
    > the access patterns needed for long simulations

-   Real-time data into a database since this makes it easy to append
    > and retrieve data in real-time. Often we want to also append
    > real-time data to Parquet

Client stage. The client stage allows downstream pipelines to access
data from the backend storage. The access pattern is always for a model
is always \"give me the columns XYZ for assets ABC in the period \[\...,
\...\]\".

We prefer to perform some transformations that are lightweight (e.g.,
converting Unix epochs in timestamps) or still evolving (e.g.,
understanding the timing semantic of the data) are performed inside this
stage, rather than in the transform stage.

**ETL primitives**. We implement basic primitives that can be combined
in different ways to create various ETL pipelines.

-   Extract:

    -   Read data from an external source to memory (typically in the
        > form of Pandas data structures)

    -   E.g., downloading data from a REST or Websocket interface

```{=html}
<!-- -->
```
-   Load:

    -   Load data stored in memory -\> permanent storage (e.g., save as
        > CSV or as Parquet)

    -   E.g., pd.to_parquet()

    -   DbSave

        -   Save to DB

        -   Create schema

```{=html}
<!-- -->
```
-   Client:

    -   From a permanent storage (e.g., disk) -\> Memory

    -   E.g., pd.from_parquet()

```{=html}
<!-- -->
```
-   ClientFromDb

    -   DB -\> Memory

    -   Creates the SQL query to read the data

```{=html}
<!-- -->
```
-   Validator

    -   https://github.com/cryptokaizen/cmamp/pull/3386/files

```{=html}
<!-- -->
```
-   Transform

```{=html}
<!-- -->
```
-   Just a class

**Example of ETL pipelines.**

-   Download historical data and save it as CSV or PQ

-   Download order book data, compress it, and save it on S3

-   Insert 1 minute worth of data in the DB

    -   Write data into DB

        -   One could argue that operations on the DB might not look
            > like \`extract\` but rather \`load\`

        -   We treat any backend (S3, local, DB) in the same way and the
            > DB is just a backend

-   [[More detailed
    > description]{.underline}](https://docs.google.com/document/d/1EH7RqeTdVCXDcrCKPm0PvrtftTA11X9V-AT7F20V7T4/edit#heading=h.t1ycoe6irr95)

    -   **Examples**:

        -   Transformations CSV -\> Parquet \<-\> DB

        -   Convert the CSV data into Parquet using certain indices

        -   Convert Parquet by-date into Parquet by-asset

## \## Data pipelines

Download data by asset (as a time series)

-   asset.csv.gz or Parquet

-   Historical format

Download data by time, e.g.,

-   20211105/...

-   csv.gz or Parquet

-   This is typical of real-time flow

## \## General conventions

**Data invariants**. We use the following invariants when storing data
during data on-boarding and processing:

-   Data quantities are associated to intervals are \[a, b) (e.g., the
    > return over an interval) or to a single point in time (e.g., the
    > close price at 9am UTC)

-   Every piece of data is labeled with the end of the sampling interval
    > or with the point-in-time

    -   E.g., for a quantity computed in an interval \[06:40:00,
        > 06:41:00) the timestamp is 06:41:00

-   Timestamps are always time-zone aware and use UTC timezone

-   Every piece of data has a knowledge timestamp (aka \"as-of-date\")
    > which represent when we were aware of the data according to our
    > wall-clock:

    -   Multiple timestamps associated with different events can be
        > tracked, e.g., \`start_download_timestamp\`,
        > \`end_download_timestamp\`

    -   No system should depend on data available strictly before the
        > knowledge timestamp

-   Data is versioned: every time we modify the schema or the semantics
    > of the data, we bump up the version using semantic versioning and
    > update the changelog of what each version contains

An example of tabular data is below:

![](media/image5.png){width="6.5in" height="1.0138888888888888in"}

**Data organization**. We keep data together by execution run instead of
by data element.

E.g., assume we run a flow called \`XYZ_sanity_check\` every day and the
flow generates three pieces of data, one file \`output.txt\` and two
directories \`logs\`, \`temp_data\`.

We want to organize the data in a directory structure like:

***Better***

\`\`\`

-   XYZ_sanity_check/

    -   run.{date}/

        -   output.txt

        -   logs/

        -   temp_data/

    -   run.{date}.manual/

        -   output.txt

        -   logs/

        -   temp_data/

\`\`\`

***Worse***

\`\`\`

-   XYZ_sanity_check/

    -   output.{date}/

        -   output.txt

    -   logs.{date}/

    -   temp_data.{date}/

\`\`\`

The reasons why the first data layout is superior are:

1)  It\'s easier to delete a single run by deleting a single dir instead
    > of deleting multiple files

2)  It allows the format of the data to evolve over time without having
    > to change the schema of the data retroactively

3)  It allows scripts post-processing the data to point to a directory
    > with a specific run and work out of the box

4)  it\'s easier to move the data for a single run from one dir (e.g.,
    > locally) to another (e.g., a central location) in one command

5)  there is redundancy and visual noise, e.g., the same data is
    > everywhere

We can tag directory by a run mode (e.g., \`manual\` vs \`scheduled\`)
by adding the proper suffix to a date-dir.

**Directory with one file**. Having a directory containing one single
file often creates redundancy.

We prefer not to use directories unless they contain more than one file.
We can use directories if we believe that it\'s highly likely that more
files will be needed, but as often happens YANGI (you are not going to
need it) applies.

**Naming convention**.

-   We use \`.\` to separate conceptually different pieces of a file or
    > a directory.

-   We don\'t allow white spaces since they are not Linux friendly and
    > need to be escaped. We replace white spaces with \`\_\`.

-   We prefer not to use \`-\` whenever possible, since they create
    > issues with Linux auto-completion and need to be escaped.

E.g., \`bulk.airflow.csv\` instead of \`bulk_airflow.csv\`

**Data pipeline classification**. A data pipeline can be any of the
following:

-   a downloader

    -   External DB (e.g., data provider) -\> Internal DB: the data
        > flows from an external API to an internal DB

    -   It downloads historical or real-time data and saves the dataset
        > in a location

    -   The name of the script and the location of the data downloaded
        > follow the naming scheme described below

    -   It is typically implemented as a Python script

-   a QA flow for a single or multiple datasets

    -   Internal DB -\> Process

    -   It computes some statistics from one or more datasets (primary
        > or derived) and throws an exception if the data is malformed

    -   It aborts if the data has data not compliant to certain QA
        > metrics

    -   It is typically implemented as a Python notebook backed by a
        > Python library

-   a derived dataset flow

    -   Internal DB -\> Process -\> Internal DB

    -   It computes some data derived from an existing data set

        -   E.g., resampling, computing features

    -   It is typically implemented as a Python script

-   a model flow

    -   Internal DB -\> Process -\> Outside DB (e.g., exchange)

    -   E.g., it runs a computation from internal data and places some
        > trades

    -   It is typically implemented as a Python script

**Data classification**. Data can be from market sources or from
non-market (aka alternative) sources. Each data source can come with
metadata, e.g.,

-   List of assets in the universe over time

-   Attributes of assets (e.g., industry and other classification)

**Asset universe**. Often the data relates to a set of assets, e.g.,
currency pairs on different exchanges. The support of the data is
referred to as the \"data universe\". This metadata is versioned as any
other piece of data.

## \## Data set downloading and handling

**Data set naming scheme**. Each data set is stored in a data lake with
a path and name that describe its metadata according to the following
signature:

dataset_signature={download_mode}.{downloading_entity}.{action_tag}.{data_format}.{data_type}.{asset_type}.{universe}.{vendor}.{exchange_id}.{version\[-snapshot\]}.{extension}

TODO(gp): \@juraj add a {backend} = s3, postgres, mongo, local_file

The signature schema might be dependent on the backend

E.g.,
bulk/airflow/downloaded_1min/csv/ohlcv/futures/universe_v1_0/ccxt/binance/v1_0-20220210/BTC_USD.csv.gz

We use \`-\` to separate pieces of the same attribute (e.g., version and
snapshot) and \`\_\` as replacements of a space character.

The organization of files in directories should reflect the naming
scheme. We always use one directory per attribute for files (e.g.,
\`bulk.airflow.csv/\...\` or \`bulk/airflow/csv/\...\`). When the
metadata is used not to identify a file in the filesystem (e.g., for a
script or as a tag) then we use \`.\` as separators between the
attributes.

**Data set attributes**. There are several \"attributes\" of a data set:

-   \`download_mode\`: the type of downloading mode

```{=html}
<!-- -->
```
-   \`bulk\`

    -   Aka \"one-shot\", \"one-off\", and improperly \"historical\"

    -   Data downloaded in bulk mode, as one-off documented operations

    -   Sometimes it\'s referred to as \"historical\", since one
        > downloads the historical data in bulk before the real-time
        > flow is deployed

-   \`periodic\`

    -   Aka \"scheduled\", \"streaming\", \"continuous\", and improperly
        > \"real-time\"

    -   Data is captured regularly and continuously

    -   Sometimes it\'s referred as to \"real-time\" since one capture
        > this data

    -   It can contain information about the frequency of downloading
        > (e.g., \`periodic-5mins\`, \`periodic-EOD\`) if it needs to be
        > identified with respect to others

-   \`unit_test\`

    -   Data used for unit test (independently if it was downloaded
        > automatically or created manually)

-   

```{=html}
<!-- -->
```
-   \`downloading_entity\`: different data depending on whom downloaded
    > it, e.g.,

```{=html}
<!-- -->
```
-   \`airflow\`: data was downloaded as part of the automatic flow

-   \`manual\`: data download was triggered manually (e.g., running the
    > download script)

```{=html}
<!-- -->
```
-   \`action_tag\`: information about the downloading, e.g.,
    > \`downloaded_1min\` or \`downloaded_EOD\`

-   \`data_format\`: the format of the data, e.g.,

```{=html}
<!-- -->
```
-   \`csv\` (always csv.gz, there is no reason for not compressing the
    > data)

-   \`parquet\`

```{=html}
<!-- -->
```
-   \`data_type\`: what type of data is stored, e.g.,

```{=html}
<!-- -->
```
-   \`ohlcv\`, \`bid_ask\`, \`market_depth\` (aka \`order_book\`),
    > \`bid_ask_market_data\` (if it includes both), \`trades\`

```{=html}
<!-- -->
```
-   \`asset_type\`: what is the asset class

```{=html}
<!-- -->
```
-   E.g., futures, spot, options

```{=html}
<!-- -->
```
-   \`universe\`: the name of the universe containing the possible
    > assets

    -   Typically the universe can have further characteristics and it
        > can be also versioned

    -   E.g., \`universe_v1_7\`

-   \`vendor\`: the source that provided the data

    -   Aka "provider"

```{=html}
<!-- -->
```
-   E.g., \`ccxt\`, \`crypto_chassis\`, \`cryptodata_download\`,
    > \`talos\`, \`kaiko\`,

-   Data can also be downloaded directly from an exchange (e.g.,
    > \`coinbase\`, \`binance\`)

-   \`exchange_id\`: which exchange the data refers to

```{=html}
<!-- -->
```
-   E.g., \`binance\`

```{=html}
<!-- -->
```
-   \`version\`: any data set needs to have a version

    -   Version is represented as major, minor, patch according to
        > semantic versioning in the format \`v{a}\_{b}\_{c}\` (e.g.,
        > v1_0_0)

    -   If the schema of the data is changed the major version is
        > increased

    -   If a bug is fixed in the downloader that improves the semantic
        > of the data but it\'s not a backward incompatible change, the
        > minor version is increased

    -   The same version can also include an optional \`snapshot\` which
        > refers to the date when the data was downloaded (e.g., a
        > specific date \`20220210\` to represent when the day on which
        > the historical data was downloaded, i.e., the data was the
        > historical data as-of 2022-02-10)

    -   Note that \`snapshot\` and \`version\` have an overlapping but
        > not identical meaning. \`snapshot\` represents when the data
        > was downloaded, while \`version\` refers to the evolution of
        > the semantic of the data and of the downloader. E.g., the same
        > data source can be downloaded manually on different days with
        > the same downloader (and thus with the same version).

```{=html}
<!-- -->
```
-   \`asset_type\`: which cryptocurrency the data refers to:

    -   Typically there is one file per asset (e.g.,
        > \`BTC_USDT.csv.gz\`)

    -   Certain data formats can organize the data in a more complex way

        -   E.g., Parquet files save the data in a directory structure
            > \`{asset}/{year}/{month}/data.parquet\`

It is possible that a single data set covers multiple values of a
specific attribute

-   E.g., a data set storing data for both futures and spot, can have
    > \`asset_type=futures_spot\`

Not all the cross-products are possible, e.g.

-   there is no data set with \`download_mode=periodic\` scheduled by
    > Airflow and \`downloading_entity=manual\`

We organize the schema in terms of access pattern for the modeling and
analysis stage

-   E.g., \`snapshot\` comes before \`vendor\` since in different
    > snapshots we can have different universes

-   E.g., snapshot -\> dataset -\> vendor -\> exchange -\> coin

```{=html}
<!-- -->
```
-   A universe is just a mapping of a tag (e.g., v5) to a set of
    > directories

Each data set has multiple columns.

**References**.

The list of data sources on CK S3 bucket is [[Bucket data
organization]{.underline}](https://docs.google.com/document/d/1C-22QF_gOe1k4HgyD6E6iOO_F_FxKKECd4MXaJEuTxo/edit#heading=h.azfdjheqqtp)

Useful notebooks for processing data is [[Master
notebooks]{.underline}](https://docs.google.com/document/d/17N8OTI1zxXI-l3OYDVcft1spDf0-JPUU8UiNCfBLYvY/edit#heading=h.uxyv8hg7offz)

CK data specs: [[Data pipelines -
Specs]{.underline}](https://docs.google.com/document/d/1nLhaFBSHVrexCcwJMnpXlkqwn0l6bDiVer34GKVclYY/edit#)

## \## Data on-boarding flow

**Downloader types.** For each data set, there are typically two types
of downloaders: bulk and periodic. This step is often the equivalent of
the Extract phase in an ETL / ELT / EtLT pipeline.

E.g., an EtLT pipeline can consists of the following phases:

-   E: extract 1 minute data from websocket,

-   t: apply non-business logic related transformation from JSON to
    > dataframe

-   L: load into SQL

-   T: transform the data resampling to 5 minute data

**Bulk downloaders**. Download past data querying from an API.

A characteristic of bulk downloaders is that the download is not
scheduled to be repeated on a regular basis. It is mostly executed once
(e.g., to get the historical data) or a few times (e.g., to catch up
with an intermittent data feed). It is executed (or at least triggered)
manually.

The data is downloaded in bulk mode at \$T\_{dl,bulkhist}\$ to catch up
with the historical data up to the moment of the deployment of the
periodic downloader scheduled every period \$\\Delta
t\_{deploy,periodic}\$.

The bulk download flow is also needed any time we need to \"catch up\"
with a missing periodic download, e.g., if the real-time capture system
was down.

**Preferred bulk data format**. Typically the data is saved in a format
that allows data to be loaded depending on what\'s needed from
downstream systems (e.g., Parquet using tiles on assets and period of
times).

**Periodic downloaders**. Download the data querying an API every period
\$\\Delta T\_{dl,periodic}\$, which depends on the application needs,
e.g., every second, minute. Typically periodic downloaders are triggered
automatically (e.g., by a workflow orchestrator like Apache Airflow).

Another possible name is \"streaming\" data. Typical example is a
websocket feed of continuous data.

**Preferred real-time data format**. Typically we save data in a DB to
be able to easily query the data from the model. Often we also save data
to an historical-like format to have a backup copy of the data. Parquet
format is not ideal since it\'s not easy to append data.

TODO(gp): Add diagram

Providers -\> us

It\'s the extract in ETL

**Downloader naming scheme.** A downloader has a name that represents
the characteristics of the data that is being downloaded in the format
above.

The downloaders usually don't encapsulate logic to download only a
single dataset. This means that the naming conventions for downloaders
are less strict than for the datasets themselves.

-   More emphasis is put into providing a comprehensive docstring

-   We can\'t use \`.\` in filenames as attribute separators because
    > Python uses them to separate packages in import statements, so we
    > replace them with \`\_\` in scripts

-   The name should capture the most general use-case

    -   E.g. if a downloader can download both OHLCV and Bid/Ask data
        > for given exchange in a given time interval and save to
        > relational DB or S3 we can simply name it
        > \`download_exchange_data.py\`

TODO(Juraj): explain that Airflow DAG names follow similar naming
conventions

Notebooks and scripts follow the naming scheme using a description
(e.g., \`resampler\`, \`notebook\`) instead of \`downloader\` and a
proper suffix (e.g., ipynb, py, sh)

TODO(gp): The first cell of a notebook contains a description of the
content, including which checks are performed

Production notebooks decide what is an error, by asserting

**Idempotency and catch-up mode**. TODO(gp): Add a paragraph on this.

This is used for any data pipeline (both downloading and processing).

**Example**. An example of system downloading price data has the
following components

+------+--------------+--------------+--------+------------+-------+
| **   | **Dataset    | *            | *      | **Data     | *     |
| Acti | signature**  | *Frequency** | *Dashb | location** | *Acti |
| on** |              |              | oard** |            | ve?** |
+======+==============+==============+========+============+=======+
| Hi   |              | -   All of   | ht     | s3://\...  | Yes   |
| stor |              |     the past | tps:// |            |       |
| ical |              |     day data |        |            |       |
| down |              |              |        |            |       |
| load |              | -   Once a   |        |            |       |
|      |              |     day at   |        |            |       |
|      |              |     0:00:00  |        |            |       |
|      |              |     UTC      |        |            |       |
+------+--------------+--------------+--------+------------+-------+
| R    |              | -   Last     | \...   | s3://\...  | Yes   |
| eal- |              |     minute   |        |            |       |
| time |              |     data     |        |            |       |
| down |              |              |        |            |       |
| load |              | -   Every    |        |            |       |
|      |              |     minute   |        |            |       |
+------+--------------+--------------+--------+------------+-------+

[[Airflow
Active]{.underline}](https://docs.google.com/document/d/14C4PFU1nNd0l7sBLCz5DxvrTk_nakmiTHwCVbIUwgfs/edit#heading=h.ntti3mum3dph)
[[Downloaders]{.underline}](https://docs.google.com/document/d/14C4PFU1nNd0l7sBLCz5DxvrTk_nakmiTHwCVbIUwgfs/edit#heading=h.ntti3mum3dph)

Describe ETL layer from [[Design - Software
components]{.underline}](https://docs.google.com/document/d/1C-22QF_gOe1k4HgyD6E6iOO_F_FxKKECd4MXaJEuTxo/edit#heading=h.iwaxiv19a4zu)

[[OHLCV data
pipeline]{.underline}](https://docs.google.com/document/d/1nLhaFBSHVrexCcwJMnpXlkqwn0l6bDiVer34GKVclYY/edit)

## \## Data QA workflows

**Quality-assurance metrics**. Each data set has QA metrics associated
with it to make sure the data has the minimum expected data quality.

-   E.g., for 1-minute OHLCV data, the possible QA metrics are:

    -   missing bars (timestamp)

    -   missing / nan OHLCV values within an individual bar

    -   points with volume = 0

    -   data points where OHLCV data is not in the correct relationship
        > (e.g., H and L are not higher or lower than O and C),

    -   data points where OHLCV data are outliers (e.g., they are more
        > than N standard deviations from the running mean)

**Bulk data single-dataset QA metrics**. It is possible to run the QA
flow to compute the quality of the historical data. This is done
typically as a one-off operation right after the historical data is
downloaded in bulk. This touches only one dataset, namely the one that
was just downloaded.

-   

**Periodic QA metrics**. Every N minutes of downloading real-time data,
the QA flow is run to generate statistics about the quality of the data.
In case of low data quality data the system sends a notification.

**Cross-datasets QA metrics**. There are QA workflows that compare
different data sets that are related to each other, e.g.,

-   consider the case of downloading the same data (e.g., 1-minute OHLCV
    > for spot BTC_USDT from Binance exchange) from different providers
    > (e.g., Binance directly and a third-party provider) and wanting to
    > compare the data under the assumption to be the same

-   

-   consider the case where there is a REST API that allows to get data
    > for a period of data, and a WebSocket that streams the data

-   consider the case where one gets an historical dump of the data from
    > a third party provider vs the data from the exchange real-time
    > stream

-   consider the case of NASDAQ streaming data vs TAQ data disseminated
    > once the market is close

**Historical / real-time QA flow.** Every period \$T\_{dl,hist}\$, a QA
flow is run where the real-time data is compared to the historical data
to ensure that the historical view of the data matches the real-time
one.

This is necessary but not sufficient to guarantee that the bulk
historical data can be reliably used as a proxy for the real-time data
as-of, in fact this is simply a self-consistency check. We do not have
any guarantee that the data source collected correct historical data.

**Data QA workflow naming scheme.** A QA workflow has a name that
represents its characteristics in the format:

{qa_type}.{dataset_signature}

E.g.,

production_qa.{download_mode}.{downloading_entity}.{action_tag}.{data_format}.{data_type}.{asset_type}.{universe}.{vendor}.{exchange}.{version\[-snapshot\]}.{asset}.{extension}

E.g.,

research_cross_comparison.periodic.airflow.downloaded_1sec_1min.all.bid_ask.futures.all.ccxt_cryptochassis.all.v1_0_0

where:

-   \`qa_type\`: the type of the QA flow, e.g.,

    -   \`production_qa\`: perform a QA flow on historical and real-time
        > data. The interface should be an IM client and thus it should
        > be possible to run QA on both historical and real-time data

    -   \`research_analysis\`: perform a free-form analysis of the data.
        > This can then be the basis for a \`qa\` analysis

    -   \`compare_historical_real_time\`: compare historical and
        > real-time data coming from the same source of data

    -   \`compare_historical_cross_comparison\`: compare historical data
        > from two different data sources

The same rules apply as in downloader and derived dataset for the naming
scheme.

Since cross-comparison involves two (or more dataset) we use a short
notation merging the attributes that differ. E.g., a comparison between
the datasets

-   periodic.1minute.postgres.ohlcv.futures.1minute.ccxt.binance

-   periodic.1day.postgres.ohlcv.futures.1minute.ccxt.binance

is called:

compare_qa.periodic.1minute-1day.postgres.ohlcv.futures.1minute.ccxt.binance

since the only difference is in the frequency of the data sampling.

It is possible to use a long format
\`{dataset_signature1}-vs-{dataset_signature2}\`.

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

## \## Derived data workflows

**Derived data workflows**. Data workflows can transform datasets into
other datasets

-   E.g., resample 1 second data into 1 minute data

The data is then written back to the same data source as the originating
data (e.g., DB for period / real-time data, Parquet / csv / S3 for
historical data).

TODO(gp): Add a plot (we are the source of the provider)

**Derived data naming scheme**. We use the same naming scheme as in
downloaded data set {dataset_signature}

but we encode the information about the content of the newly generated
data in the \`action_tag\` attribute of the data, e.g.,
\`resample_1min\` to distinguish it from \`downloaded_1sec\`.

We use this approach so that the scheme of the derived data is the same
as a downloaded data set.

**Derived data research flow.** The goal is to decide how to transform
the raw data into derived data and come up with QA metrics to assess the
quality of the transformation

-   It can be cross-vendor or not

-   E.g., sample 1sec data to 1min and compare to a reference. The
    > sampling is done on the fly since the researcher is trying to
    > understand how to resample (e.g., removing outliers) to get a
    > match

**Derived data production flow.** The research flow is frozen and put in
production

-   E.g., run the resample script to sample and write back to DB and
    > historical data. This flow can be run in historical mode (to
    > populate the backend with the production data) and in real-time
    > mode (to compute the streaming data)

**Derived data QA flow**. the goal is to monitor that the production
flow is still performing properly with respect to the QA metrics

-   E.g., the 1-sec to 1-min resampling is not performed on-the-fly, but
    > it uses the data already computed by the script in the production
    > flow.

-   This flow is mainly run in real-time, but we might want to look at
    > QA performance also historically

This same distinction can also be applied to feature computation and to
the machine learning flow.

Provider -\> data -\> Us -\> derived flow -\> Us -\> features -\> Us -\>
ML -\> Exchange

## \## Data formats

**Storage-invariance of data.** Data should be independent from its
storage format, e.g., CSV, Parquet, Relational DB. In other words,
converting data from one format to another should not yield losing any
information.

**Data stored by-date.** The by-date representation means that there is
a file for the data for all the assets:

1546871400 ... Bitcoin

1546871400 ... Ethereum

1546871401 ...

1546871402 ...

Thus the same timestamp is repeated for all the assets

The original format of the data is by-date and for a single day is
\"20190107.pq\" like:

vendor_date start_time end_time ticker open close volume id

2019-01-07 1546871400 1546871460 A 65.64 65.75 19809 16572.0

2019-01-07 1546871400 1546871460 AA 28.53 28.52 31835 1218568.0

2019-01-07 1546871400 1546871460 AAAU 12.92 12.92 11509 1428781.0

2019-01-07 1546871400 1546871460 AABA 58.90 58.91 7124 10846.0

![](media/image4.png){width="6.5in" height="1.0416666666666667in"}

There are 2 special columns in the by-date file:

-   one that represents the timestamp (\"start_time\" in the example).
    > This is unique and monotonic (start_time, ticker) are unique

-   one that represents the asset (\"ticker\" in the example)

**Data stored by-asset.** By-asset data means that there is a single
file for each single asset with the data for all the timestamps (no
timestamp is repeated in a single file)

Bitcoin.pq

1546871400 \...

1546871401

1546871402

Eth.pq

1546871400 \...

1546871401

1546871402

By-asset Parquet data. Data pipelines can transform by-asset data into
Parquet data, preserving all the columns. Successive stages of the
pipeline perform other data transformations. By-asset means that the
asset that is in the innermost directory

\`\`\`

dst_dir/

year=2021/

month=12/

day=11/

asset=BTC_USDT/

data.parquet

asset=ETH_USDT/

data.parquet

\`\`\`

Typically the by-date format is just a format that we receive data from,
and we don\'t want to transform data to.

The name of the asset can depend on the data and it can be \`asset\`,
\`currency_pair\`, \`ticker\`.

By default we use names of columns from the data and we reindex the
partitioned dataframe on datetime, so saved parquet will all have the
same datetime index.

Partitioning on year/month/day is optional and should be provided as a
switch in partitioning function.

**Data stored by-tile.** Data is organized by tile when the Parquet
files are partitioned by asset, by year, and by month so that it\'s
possible to read only a chunk of that

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

## \## Sandbox

This paragraph describes an example of infrastructure that implements
the Surrentum Data Protocol. The

It is a Docker Container containing the following services:

-   Airflow

-   Jupyter notebook

-   Postgres

-   MongoDB

**Notes for CK devs (TO REMOVE)**

-   It is a separated code base from Surrentum and it shares only a few
    > base library (e.g., \`helpers\`)

-   It is a scaled down version of CK production infrastructure (e.g.,
    > managed Airflow is replaced by a local Airflow instance)

-   The system follows closely the production system described in [[Data
    > pipelines -
    > Specs]{.underline}](https://docs.google.com/document/d/1nLhaFBSHVrexCcwJMnpXlkqwn0l6bDiVer34GKVclYY/edit#)

## \## Data client stack

Once the data is downloaded, it needs to be retrieved for processing in
a common format (e.g., DataFlow format).

We use a two layer approach to split the complexity / responsibilities
of reading the data

-   \`ImClient\`

    -   Data is downloaded and saved with minimal or no transformation

    -   Adapts from the vendor data to a standard internal
        > \"MarketData\" format

-   MarketData\` implements \"behaviors\" that are orthogonal to
    > vendors, e.g.,

    -   RealTime or Historical

    -   Stitched (i.e.,\"overlap\" multiple data sources giving a single
        > view of the data). E.g., the data from the last day comes from
        > a real-time source while the data before that comes from an
        > historical source

    -   Replayed = serialize the data to disk and read it back,
        > implementing also knowledge time as-of-time semantic. This
        > behavior is orthogonal to RealTime, Historical, Stitched,
        > i.e., one can replay any \`MarketData\`, including an already
        > replayed one

\# Data format for \`ImClient\` / \`MarketData\` pipeline

\- Both \`ImClient\` and \`MarketData\` have an output format that is
enforced by the

base and the derived classes together

\- \`ImClient\` and \`MarketData\` have 3 interfaces each:

1\) an external \"input\" format for a class

\- format of the data as input to a class derived from \`MarketData\` /

\`ImClient\`

2\) an internal \"input\" format

\- format that derived classes need to follow so that the corresponding
base

class can do its job, i.e., apply common transformations to all

\`MarketData\` / \`ImClient\` classes

3\) an external \"output\" format

\- format of the data outputted by any derived class from \`MarketData\`
/

\`ImClient\`

\- The chain of transformations is:

\- Class derived from \`ImClient\`

\- The transformations are vendor-specific

\- \`ImClient\`

\- The transformations are fixed

\- Class derived from \`MarketData\`

\- The transformations are specific to the \`MarketData\` concrete class

\- \`MarketData\`

\- The transformations are fixed

\`\`\`plantuml

\[Vendor\] -\> \[DerivedImClient\]

\[DerivedImClient\] -\> \[AbstractImClient\]

\[AbstractImClient\] -\> \[DerivedMarketData\]

\[DerivedMarketData\] -\> \[AbstractMarketData\]

\`\`\`

\## Transformations performed by classes derived from \`ImClient\`

\- Whatever is needed to transform the vendor data into the internal
format accepted

by base \`ImClient\`

\- Only derived classes \`ImClient\` knows what is exact semantic of the
vendor-data

\## Transformations performed by abstract class \`ImClient\`

\- Implemented by \`ImClient.\_apply_im_normalization()\`

\## Output format of \`ImClient\`

\- TODO(\*): Check the code in \`ImClient\` since that might be more up
to date than

this document and, if needed, update this doc

\- The data in output of a class derived from \`ImClient\` is normalized
so that:

\- the index:

\- represents the knowledge time

\- is the end of the sampling interval

\- is called \`timestamp\`

\- is a tz-aware timestamp in UTC

\- the data:

\- is resampled on a 1 minute grid and filled with NaN values

\- is sorted by index and \`full_symbol\`

\- is guaranteed to have no duplicates

\- belongs to intervals like \[a, b\]

\- has a \`full_symbol\` column with a string representing the canonical
name

of the instrument

\- TODO(gp): We are planning to use an \`ImClient\` data format closer
to \`MarketData\`

by using \`start_time\`, \`end_time\`, and \`knowledge_time\` since
these can be

inferred only from the vendor data semantic

\## Transformations performed by classes derived from \`MarketData\`

\- Classes derived from \`MarketData\` do whatever they need to do in
\`\_get_data()\` to

get the data, but always pass back data that:

\- is indexed with a progressive index

\- has asset, start_time, end_time, knowledge_time

\- start_time, end_time, knowledge_time are timezone aware

\- E.g.,

\`\`\`

asset_id start_time end_time close volume

idx

0 17085 2021-07-26 13:41:00+00:00 2021-07-26 13:42:00+00:00 148.8600
400176

1 17085 2021-07-26 13:30:00+00:00 2021-07-26 13:31:00+00:00 148.5300
1407725

2 17085 2021-07-26 13:31:00+00:00 2021-07-26 13:32:00+00:00 148.0999
473869

\`\`\`

\## Transformations performed by abstract class \`MarketData\`

\- The transformations are done inside \`get_data_for_interval()\`,
during normalization,

and are:

\- indexing by \`end_time\`

\- converting \`end_time\`, \`start_time\`, \`knowledge_time\` to the
desired timezone

\- sorting by \`end_time\` and \`asset_id\`

\- applying column remaps

\## Output format of \`MarketData\`

\- The base \`MarketData\` normalizes the data by:

\- sorting by the columns that correspond to \`end_time\` and
\`asset_id\`

\- indexing by the column that corresponds to \`end_time\`, so that it
is suitable

to DataFlow computation

\- E.g.,

\`\`\`

asset_id start_time close volume

end_time

2021-07-20 09:31:00-04:00 17085 2021-07-20 09:30:00-04:00 143.990
1524506

2021-07-20 09:32:00-04:00 17085 2021-07-20 09:31:00-04:00 143.310 586654

2021-07-20 09:33:00-04:00 17085 2021-07-20 09:32:00-04:00 143.535 667639

\`\`\`

\# Asset ids format

\- \`ImClient\` uses assets encoded as \`full_symbols\` strings (e.g.,
\`binance::BTC_UTC\`)

\- There is a vendor-specific mapping:

\- from \`full_symbols\` to corresponding data

\- from \`asset_ids\` (ints) to \`full_symbols\` (strings)

\- If the \`asset_ids\` -\> \`full_symbols\` mapping is provided by the
vendor, then we

reuse it

\- Otherwise, we build a mapping hashing \`full_symbols\` strings into
numbers

\- \`MarketData\` and everything downstream uses \`asset_ids\` that are
encoded as ints

\- This is because we want to use ints and not strings in dataframe

\# Handling of \`asset_ids\`

\- Different implementations of \`ImClient\` backing a \`MarketData\`
are possible,

e.g.:

\- The caller needs to specify the requested \`asset_ids\`

\- In this case the universe is provided by \`MarketData\` when calling
the

data access methods

\- The reading backend is initialized with the desired universe of
assets and

then \`MarketData\` just uses or subsets that universe

\- For these reasons, assets are selected at 3 different points:

1\) \`MarketData\` allows to specify or subset the assets through
\`asset_ids\`

through the constructor

2\) \`ImClient\` backends specify the assets returned

\- E.g., a concrete implementation backed by a DB can stream the data
for

its entire available universe

3\) Certain class methods allow querying data for a specific asset or
subset

of assets

\- For each stage, a value of \`None\` means no filtering

\# Handling of filtering by time

\- Clients of \`MarketData\` might want to query data by:

\- using different interval types, namely \`\[a, b), \[a, b\], (a, b\],
(a, b)\`

\- filtering on either the \`start_ts\` or \`end_ts\`

\- For this reason, this class supports all these different ways of
providing data

\- \`ImClient\` has a fixed semantic of the interval \`\[a, b\]\`

\- \`MarketData\` adapts the fixed semantic to multiple ones

\# Handling timezone

\- \`ImClient\` always uses UTC as output

\- \`MarketData\` adapts UTC to the desired timezone, as requested by
the client

## \## Checklist for releasing a new data set

-   Decide what to do exactly (e.g., do we download only bulk data or
    > also real-time?)

-   Review what code we have and what can be generalized to accomplish
    > the task at hand

-   Decide what\'s the name of the data set according to our convention

-   Create DAGs for Airflow

-   Update the Raw Data Gallery
    > im_v2/common/notebooks/Master_raw_data_gallery.ipynb

-   Quick exploratory analysis to make sure the data is not malformed

-   Update the table in [[Data pipelines -
    > Specs]{.underline}](https://docs.google.com/document/d/1nLhaFBSHVrexCcwJMnpXlkqwn0l6bDiVer34GKVclYY/edit#heading=h.8g5ajvlq6zks)

-   Exploratory analysis for a thorough QA analysis

-   Add QA system to Airflow prod

# \# Surrentum Node: specification

## \## Time semantics

**Time semantics**. A Surrentum component can be executed or simulated
accounting for different ways to represent the passing of time. E.g., it
can be simulated in a timed or non-timed simulation, depending on how
data is delivered to the system (as it is generated or in bulk with
knowledge time).

**Clock.** A function that reports the current timestamp. There are 3
versions of clock:

1)  Static clock. A clock that remains the same during a system run.

    a.  Future peeking is allowed

2)  Replayed clock. A moving clock that can be in the past or future
    > with respect to a real clock

    a.  Use time passing at the same pace of real-time wall-clock or

    b.  Simulate time based on events, e.g., as soon as the workload
        > corresponding to one timestamp is complete we move to the next
        > timestamp, without waiting for the actual time to pass

    c.  Future peeking is technically possible but is prohibited

3)  Real clock. The wall-clock time matches what we observe in
    > real-life, data is provided to processing units as it is produced
    > by systems.

    a.  Future peeking is not possible in principle

**Knowledge time.** It is the time when data becomes available (e.g.,
downloaded or computed) to a system. Each row of data is tagged with the
corresponding knowledge time. Data with knowledge time after the current
clock time must not be observable in order to avoid future peeking.

**Timed simulation**. Sometimes referred to as historical, vectorized,
bulk, batch simulation. In a timed simulation the data is provided with
a clock that reports the current timestamp. Data with knowledge time
after the current timestamp must not be observable in order to avoid
future peeking.

TODO(gp): Add an example of df with forecasts explaining the timing

**Non-timed simulation**. (Sometimes referred to as event-based,
reactive simulation). Clock type is "static clock". Typically wall-clock
time is a timestamp that corresponds to the latest knowledge time (or
greater) in a dataframe. In this way all data in a dataframe is
available because every row has a knowledge time that is less than or
equal to the wall-clock time. Note that the clock is static, i.e. not
moving. In a non-timed simulation, the data is provided in a dataframe
for the entire period of interest.

E.g., for a system predicting every 5 mins, all the input data are
equally spaced on a 5-min grid and indexed with knowledge time.

TODO(gp): Add an example of df with forecasts explaining the timing

df\[\"c\"\] = (df\[\"a\"\] + df\[\"b\"\]).shift(1)

**Real-time execution**. In real-time the clock type is "real clock".

E.g., for a system predicting every 5 mins, one forecast is delivered
every 5 mins of wall-clock.

\# TODO(Grisha): add an example.

**Replayed simulation**. In replayed simulation, the data is provided in
the same \"format\" and with the same timing than it would be provided
in real-time, but the clock type is "replayed clock".

TODO(gp): Add an example of df with forecasts explaining the timing

## \## Different views of System components

**Different implementations of a component**. A Surrentum component is
described in terms of an interface and can have several implementations
at different levels of detail.

**Reference implementation**. A reference implementation is
vendor-agnostic implementation of a component (e.g., DataFrameImClient,
DataFrameBroker)

**Vendor implementation**. A vendor implementation is a vendor-specific
implementation of a component (e.g., CcxtImClient, CcxtBroker).

**Mocked implementation**. A mocked implementation is a simulated
version of a vendor-specific component (e.g., a DataFrameCcxtBroker). A
mocked component can have the same timing semantics as the
real-component (e.g., an asynchronous or reactive implementation) or
not.

## \## Architecture

In this section we summarize the responsibilities and the high level
invariants of each component of a \`System\`.

The entire System is represented in terms of a Config. Each piece of a
Config refers to and configures a specific part of the System. Each
component should be completely configured in terms of a Config.

### \### Component invariants

Each component has a way to know:

-   what is the current time (e.g., the real-time machine time or the
    > simulated one)

-   the timestamp of the current data bar it\'s working on

All data in components should be indexed by the knowledge time (i.e.,
when the data became available to that component) in terms of current
time.

Each component should print its state so that one can inspect how
exactly it has been initialized.

Each component can be serialized and deserialized from disk.

Each component can be mocked for simulating.

Each component should save data in a directory as it executes to make
the system observable.

Models are described in terms of DAGs using the DataFlowCompute
framework

**Misc**. Models read data from historical and real-time data sets,
typically not mixing these two styles.

Raw data is typically stored in S3 bucket in the same format as it comes
or in Parquet format.

## \## Config

**Config**. A \`Config\` is a dictionary-like object that represents
parameters used to build and configure other objects (e.g., a DAG or a
System).

Each config is a hierarchical structure which consists of **Subconfigs**
and **Leaves**.

**Subconfig** is a nested object which represents a Config inside
another config. A Subconfig of a Subconfig is a Subconfig of a Config,
i.e. the relation is transitive.

**Leaf** is any object inside a Config that is used to build another
object that is not in itself a Config.

Note that a dictionary or other mapping objects are not permitted inside
a Config: each dictionary-like object should be converted to a Config
and become a Subconfig.

### \### Config representation and properties

A Config can be represented as a dictionary or a string.

Example of a dictionary representation:

\`\`\`

config1 = {

\"resample_1min\": False,

\"client_config\": {

\"universe\": {

\"full_symbols\": \[\"binance::ADA_USDT\"\],

\"universe_version\": \"v3\",

},

},

\"market_data_config\": {\"start_ts\": start_ts, \"end_ts\": end_ts},

}

\`\`\`

In the example above:

-   "resample_1min" is a leaf of the \`config1\`

-   "client_config" is a subconfig of \`config1\`

-   "universe" is a subconfig of "client_config"

-   "market_data" config is a subconfig of "config1"

-   "start_ts" and "end_ts" are leaves of "market_data_config" and
    > \`config1\`

Example of a string representation:

![](media/image14.png){width="6.854779090113736in"
height="1.2303444881889765in"}

-   The same values are annotated with \`marked_as_used\`, \`writer\`
    > and \`val_type\`

    -   \`marked_as_used\` determines whether the object was used to
        > construct another object

    -   \`writer\` provides a stacktrace of the piece of code which
        > marked the object as used

    -   \`val_type\` is a type of the object

### \### Assigning and getting Config items

-   Config object has its own implementations of \`\_\_setitem\_\_\` and
    > \`\_\_getitem\_\_\`

-   A new value can be set freely like in a python Dict object

-   Overwriting the value is prohibited if the value has already been
    > used

Since Config is used to guarantee that the construction of any objects
is reproducible, there are 2 methods to \`get\` the value.

-   \`get_and_mark_as_used\` is utilized when a leaf of the config is
    > used to construct another object

    -   When the value is used inside a constructor

    -   When the value is used as a parameter in a function

Note that when selecting a subconfig the subconfig itself is not marked
as used, but its leaves are. For this reason, the user should avoid
marking subconfigs as used and instead select leaves separately.

Example of marking the subconfig as used:

\`\`\`

\_ = config.get_and_mark_as_used("market_data_config")

![](media/image13.png){width="6.5in" height="1.1944444444444444in"}

\`\`\`

Example of marking the leaf as used:

\`\`\`

\_ = config.get_and_mark_as_used((\"market_data_config\", \"end_ts\"))

![](media/image10.png){width="6.5in" height="1.1388888888888888in"}

\`\`\`

-   \`\_\_getitem\_\_\` is used to select items for uses which do not
    > affect the construction of other objects:

    -   Logging, debugging and printing

## \## DataFlow computing

-   **DataFlow framework**. DataFlow is a computing framework to
    > implement machine learning models that can run with minimal
    > changes in timed, non-timed, replayed simulation and real-time
    > execution.

The working principle underlying DataFlow is to run a model in terms of
time slices of data so that both historical and real-time semantics can
be accommodated without changing the model.

TODO(gp): Explain the advantages:

-   Tiling to fit in memory

-   Cached computation

-   Adapt a procedural semantic to a reactive / streaming semantic

-   Handle notion of time

-   Control for future peeking

-   Suite of tools to replay and debug executions from real-time

-   Support for market data and other tabular data feeds

-   Support for knowledge time

\# Resampling VWAP (besides potential errors). This implies hardcoded
formula in a mix with resampling functions.

vwap_approach_2 = (converted_data\[\"close\"\] \*
converted_data\[\"volume\"\]).resample(

resampling_freq

).mean() / converted_data\[\"volume\"\].resample(resampling_freq).sum()

vwap_approach_2.head(3)

**Dag Node**. It is a unit of computation of a DataFlow model. A Dag
node has inputs, outputs, a unique node id (aka \`nid\`), and a state.
Typically inputs and outputs are dataframes. A Dag node stores a value
for each output and method name (e.g., methods are \`fit\`, \`predict\`,
\`save_state\`, \`load_state\`). The DataFlow time slice semantics is
implemented in terms of \`Pandas\` and \`Sklearn\` libraries.

TODO(gp): Add picture.

**DataFlow model**. A DataFlow model (aka \`DAG\`) is a direct acyclic
graph composed of DataFlow nodes. It allows to connect, query the
structure

Running a method on a Dag means running that method on all its nodes in
topological order, propagating values through the Dag nodes.

TODO(gp): Add picture.

**DagConfig**. A \`Dag\` can be built assembling Nodes using a function
representing the connectivity of the nodes and parameters contained in a
Config (e.g., through a call to a builder
\`DagBuilder.get_dag(config)\`).

A DagConfig is hierarchical and contains one subconfig per Dag node. It
should only include \`Dag\` node configuration parameters, and not
information about \`Dag\` connectivity, which is specified in the
\`Dag\` builder part.

### **Template configs**

-   Are incomplete configs, with some \"mandatory\" parameters
    > unspecified but clearly identified with \`cconfig.DUMMY\` value

-   Have reasonable defaults for specified parameters

    -   This facilitates config extension (e.g., if we add additional
        > parameters / flexibility in the future, then we should not
        > have to regenerate old configs)

-   Leave dummy parameters for frequently-varying fields, such as
    > \`ticker\`

-   Should be completable and be completed before use

-   Should be associated with a \`Dag\` builder

**DagBuilder**. It is an object that builds a DAG and has a
\`get_config_template()\` and a \`get_dag()\` method to keep the config
and the Dag in sync.

The client:

-   calls \`get_config_template()\` to receive the template config

-   fills / modifies the config

-   uses the final config to call \`get_dag(config)\` and get a fully
    > built DAG

A \`DagBuilder\` can be passed to other objects instead of \`Dag\` when
the template config is fully specified and thus the \`Dag\` can be
constructed from it.

**DagRunner**. It is an object that allows to run a \`Dag\`. Different
implementations of a \`DagRunner\` allow to run a \`Dag\` on data in
different ways, e.g.,

-   \`FitPredictDagRunner\`: implements two methods \`fit\` /
    > \`predict\` when we want to learn on in-sample data and predict on
    > out-of-sample data

-   \`RollingFitPredictDagRunner\`: allows to fit and predict on some
    > data using a rolling pattern

-   \`IncrementalDagRunner\`: allows to run one step at a time like in
    > real-time

-   \`RealTimeDagRunner\`: allows to run using nodes that have a
    > real-time semantic

## \## DataFlow Computation Semantics

Often raw data is available in a \"long format\", where the data is
conditioned on the asset (e.g., full_symbol), e.g.,

![](media/image2.png){width="5.338542213473316in"
height="1.1036406386701663in"}

DataFlow represents data through multi-index dataframes, where

-   the outermost index is the \"feature\"

-   the innermost index is the asset, e.g.,

![](media/image6.png){width="6.5in" height="1.0416666666666667in"}

The reason for this convention is that typically features are computed
in an univariate fashion (e.g., asset by asset), and we can get
vectorization over the assets by expressing operations in terms of the
features. E.g., we can express a feature as \`(df\[\"close\",
\"open\"\].max() - df\[\"high\"\]).shift(2)\`.

Based on the example
./amp/dataflow/notebooks/gallery_dataflow_example.ipynb, one can work
with DataFlow at 4 levels of abstraction:

1)  Pandas long-format (non multi-index) dataframes and for-loops

    -   We can do a group-by or filter by full_symbol

    -   Apply the transformation on each resulting df

    -   Merge the data back into a single dataframe with the long-format

2)  Pandas multiindex dataframes

    -   The data is in the DataFlow native format

    -   We can apply the transformation in a vectorized way

    -   This approach is best for performance and with compatibility
        > with DataFlow point of view

    -   An alternative approach is to express multi-index
        > transformations in terms of approach 1 (i.e., single asset
        > transformations and then concatenation). This approach is
        > functionally equivalent to a multi-index transformation, but
        > typically slow and memory inefficient

3)  DataFlow nodes

    -   A node implements a certain transformations on DataFrames
        > according to the DataFlow convention and interfaces

    -   Nodes operate on the multi-index representation by typically
        > calling functions from level 2 above

4)  DAG

    -   A series of transformations in terms of DataFlow nodes

Note that there are degrees of freedom in splitting the work between the
various layers.

E.g., code can be split in multiple functions at level 2) and then

[[http://172.30.2.136:10051/notebooks/dataflow_orange/pipelines/C1/notebooks/C1b_debugging.ipynb]{.underline}](http://172.30.2.136:10051/notebooks/dataflow_orange/pipelines/C1/notebooks/C1b_debugging.ipynb)

[[http://172.30.2.136:10051/notebooks/dataflow_orange/pipelines/C1/notebooks/Implement_RH1E.ipynb]{.underline}](http://172.30.2.136:10051/notebooks/dataflow_orange/pipelines/C1/notebooks/Implement_RH1E.ipynb)

## \## Backtest and Experiment

### **\### \`ConfigBuilder\`**

-   Generates a list of fully formed (not template) configs that can be
    > then run

-   These configs can correspond to one or multiple Experiments, tiled
    > or not (see below)

-   Config builder accepts \`BacktestConfig\` as an input

### **\### Experiment in strict and loose sense**

Colloquially, we use experiment to mean different things, e.g., an
experiment can consists in:

-   a backtest where we run a single Dag with a single config (e.g.,
    > when we test the predictive power of a single model)

```{=html}
<!-- -->
```
-   running a Dag (e.g., E8d) through multiple configs (e.g., with
    > longer / shorter history) to perform an \"A / B experiment\"

-   running completely different Dags (e.g., E1 vs E8c) to compare their
    > performance

Strictly speaking, we refer to:

-   The first one as a \`Backtest\` (which can be executed in terms of
    > tiled configs or not)

-   The second and the third as an \`Experiment\`

In practice almost any experiment we run consists of one or more
backtests

### **\### \`Backtest\`**

-   In general a \"backtest\" is simply code that is configured by a
    > \*single\* \`Config\`s

    -   The code contained in a backtest experiment can be anything

```{=html}
<!-- -->
```
-   Typically a backtest consists of:

    -   creating a \`Dag\`(e.g., through a \`DagBuilder\`) or a
        > \`System\` based on a config

    -   running it over a period of time (e.g., through a \`DagRunner\`)

    -   saving the result into a directory

### **\### \`BacktestConfig\`**

-   = a config that has multiple parts configuring both what to run
    > (e.g., a \`Dag\`) and how to run it (e.g., the universe, the
    > period of time)

-   It can correspond to multiple configs (e.g., when running a
    > \`TiledBacktest\`)

-   The common pattern is
    > \`\<universe\>-\<top_n\>.\<trading_period\>.\<time_interval\>\`,
    > e.g., \`ccxt_v4-top3.5T.2019_2022\` where

    -   \`ccxt_v4\` is a specific version of universe

    -   \`top3\` is top 3 assets, \`all\` means all assets in the
        > universe

    -   \`5T\` (5 minutes) is trading period

    -   \`2019-2022\` is timeframe, i.e. run the model using data from
        > 2019 to 2022

### **\### \`Experiment\`**

-   A set of backtests to run, each of which corresponds to conceptually
    > a single \`Config\`

-   Each backtest can then be executed in a tiled fashion (e.g., by
    > expressing it in terms of different configs, one per tile

In order to create the list of fully built configs, both a \`Backtest\`
and a \`Experiment\` need:

-   an \`BacktestBuilder\` (what to run in a backtest)

-   a \`ConfigBuilder\` (how to configure)

-   dst_dir (destination dir of the entire experiment list, i.e., the
    > one that the user passes to the command)

### **\### Tiled backtest / experiment**

-   An experiment / backtest that is run through multiple tiles for time
    > and assets

-   In general this is just an implementation detail

### **\### Tiled vs Tile**

-   We call \"tiled\" objects that are split in tiles (e.g.,
    > \`TiledBacktest\`), and \"tile\" objects that refer to tiling
    > (e.g., \`TileConfig\`)

### **\### Experiment (list) manager**

-   TODO(gp): experiment_list manager?

-   Python code that runs experiments by:

    -   generating a list of \`Config\` object to run, based on a
        > \`ConfigBuilder\` (i.e., \`run_experiment.py\` and
        > \`run_notebook.py\`)

### **\### \`ExperimentBuilder\`**

-   TODO(gp): -\> BacktestBuilder

-   It is a function that:

    -   Creates a DAG from the passed config

    -   Runs the DAG

    -   Saves the results in a specified directory

### **\### \`BacktestRunner\`**

-   A test case object that:

    -   runs a backtest (experiment) on a Dag and a Config

    -   processes its results (e.g., check that the output is readable,
        > extract a PnL curve or other statistics)

### **\### System**

-   An object representing a full trading system comprising of:

    -   MarketData

        -   HistoricalMarketData (ImClient)

        -   RealTimeMarketData

    -   Dag

    -   DagRunner

    -   Portfolio

        -   Optimizer

        -   Broker

### **\### SystemRunner**

-   An object that allows to build and run a System

-   TODO(gp): Not sure it\'s needed

### **\### System_TestCase**

-   TODO(gp): IMO this is a TestCase + various helpers

### \### Data structures

**Fill**

**Order**

### \### Major software components

![](media/image9.png){width="6.5in" height="1.875in"}

[[https://lucid.app/lucidchart/9ee80100-be76-42d6-ad80-531dcfee277e/edit?page=0_0&invitationId=inv_5777ae4b-d8f4-41c6-8901-cdfb93d98ca8#]{.underline}](https://lucid.app/lucidchart/9ee80100-be76-42d6-ad80-531dcfee277e/edit?page=0_0&invitationId=inv_5777ae4b-d8f4-41c6-8901-cdfb93d98ca8#)

![](media/image15.png){width="6.5in" height="4.319444444444445in"}

**ImClient**

Responsibilities:

Interactions:

Main methods:

**MarketData**

Responsibilities:

Interactions:

Main methods:

**Forecaster.** It is a DAG system that forecasts the value of the
target economic quantities (e.g.,

for each asset in the target

Responsibilities:

Interactions:

Main methods:

**process_forecasts.** Interface to execute all the predictions in a
Forecast dataframe through TargetPositionAndOrderGenerator.

This is used as an interface to simulate the effect of given forecasts
under different optimization conditions, spread, and restrictions,
without running the Forecaster.

**TargetPositionAndOrderGenerator**. Execute the forecasts by generating
the optimal target positions according to the desired criteria and by
generating the corresponding orders needed to get the system from the
current to the desired state.

TODO(gp): It also submits the orders so ForecastProcessor?

Responsibilities:

-   Retrieve the current holdings from Portfolio

-   Perform optimization using forecasts and current holdings to compute
    > the target position

-   Generate the orders needed to achieve the target positions

-   Submit orders to the Broker

Interactions:

-   Forecaster to receive the forecasts of returns for each asset

-   Portfolio to recover the current holdings

Main methods:

-   compute_target_positions_and_generate_orders(): compute the target
    > positions and generate the orders needed to reach

-   \_compute_target_holdings_shares(): call the Optimizer to compute
    > the target holdings in shares

**Locates**.

**Restrictions**.

**Optimizer.**

Responsibilities:

Interactions:

Main methods:

**Portfolio**. A Portfolio stores information about asset and cash
holdings of a System over time.

Responsibilities:

-   hold the holdings in terms of shares of each asset id and cash
    > available

Interactions:

-   MarketData to receive current prices to estimate the value of the
    > holdings

-   Accumulate statistics and

Main methods:

-   mark_to_market(): estimate the value of the current holdings using
    > the current market prices

-   \...

**DataFramePortfolio**: an implementation of a Portfolio backed by a
DataFrame. This is used to simulate a system on an order-by-order basis.
This should be equivalent to using a DatabasePortfolio but without the
complexity of querying a DB.

**DatabasePortfolio**: an implementation of a Portfolio backed by an SQL
Database to simulate systems where the Portfolio state is held in a
database. This allows to simulate a system on an order-by-order basis.

**Broker.** A Broker is an object to place orders to the market and to
receive fills, adapting Order and Fill objects to the corresponding
market-specific objects. In practice Broker adapts the internal
representation of Order and Fills to the ones that are specific to the
target market.

Responsibilities:

-   Submit orders to MarketOms

```{=html}
<!-- -->
```
-   Wait to ensure that orders were properly accepted by MarketOms

-   Execute complex orders (e.g., TWAP, VWAP, pegged orders) interacting
    > with the target market

-   Receive fill information from the target market

Interactions:

-   MarketData to receive prices and other information necessary to
    > execute orders

```{=html}
<!-- -->
```
-   MarketOms to place orders and receive fills

Main methods:

-   submit_orders()

-   get_fills()

**MarketOms**. MarketOms is the interface that allows to place orders
and receive back fills to the specific target market. This is provided
as-is and it\'s not under control of the user or of the protocol

-   E.g., a specific exchange API interface

**OrderProcessor**

-   TODO(gp): Maybe MockedMarketOms since that\'s the actual function?

**OmsDb**

**\## TO REORG**

\# Invariants and conventions

\- In this doc we use the new names for concepts and use \"aka\" to
refer to the

old name, if needed

\- We refer to:

\- The as-of-date for a query as \`as_of_timestamp\`

\- The actual time from \`get_wall_clock_time()\` as
\`wall_clock_timestamp\`

\- Objects need to use \`get_wall_clock_time()\` to get the \"actual\"
time

\- We don\'t want to pass \`wall_clock_timestamp\` because this is
dangerous

\- It is difficult to enforce that there is no future peeking when one
object

tells another what time it is, since there is no way for the second
object

to double check that the wall clock time is accurate

\- We pass \`wall_clock_timestamp\` only when one \"action\" happens
atomically but

it is split in multiple functions that need to all share this
information.

This approach should be the exception to the rule of calling

\`get_wall_clock_time()\`

\- It\'s ok to ask for a view of the world as of \`as_of_timestamp\`,
but then the

queried object needs to check that there is no future peeking by using

\`get_wall_clock_time()\`

\- Objects might need to get \`event_loop\`

\- TODO(gp): Clean it up so that we pass event loop all the times and
event

loop has a reference to the global \`get_wall_clock_time()\`

\- The Optimizer only thinks in terms of dollar

\# Implementation

\## process_forecasts()

\- Aka \`place_trades()\`

\- Act on the forecasts by:

\- Get the state of portfolio (by getting fills from previous clock)

\- Updating the portfolio holdings

\- Computing the optimal positions

\- Submitting the corresponding orders

\- \`optimize_positions()\`

\- Aka \`optimize_and_update()\`

\- Calls the Optimizer

\- \`compute_target_positions()\`

\- Aka \`compute_trades()\`

\- \`submit_orders()\`

\- Call \`Broker\`

\- \`get_fills()\`

\- Call \`Broker\`

\- For IS it is different

\- \`update_portfolio()\`

\- Call \`Portfolio\`

\- For IS it is different

\- It should not use any concrete implementation but only \`Abstract\*\`

\## Portfolio

\- \`get_holdings()\`

\- Abstract because IS, Mocked, Simulated have a different
implementations

\- \`mark_to_market()\`

\- Not abstract

\- -\> \`get_holdings()\`, \`PriceInterface\`

\- \`update_state()\`

\- Abstract

\- Use abstract but make it NotImplemented (we will get some static
checks and

some other dynamic checks)

\- We are trying not to mix static typing and duck typing

\- CASH_ID, \`\_compute_statistics()\` goes in \`Portolio\`

\## Broker

\- \`submit_orders()\`

\- \`get_fills()\`

\# Simulation

\## DataFramePortfolio

\- This is what we call \`Portfolio\`

\- In RT we can run \`DataFramePortfolio\` and \`ImplementedPortfolio\`
in parallel

to collect real and simulated behavior

\- \`get_holdings()\`

\- Store the holdings in a df

\- \`update_state()\`

\- Update the holdings with fills -\> \`SimulatedBroker.get_fills()\`

\- To make the simulated system closer to the implemented

\## SimulatedBroker

\- \`submit_orders()\`

\- \`get_fills()\`

\# Implemented system

\## ImplementedPortfolio

\- \`get_holdings()\`

\- Check self-consistency and assumptions

\- Check that no order is in flight otherwise we should assert or log an

error

\- Query the DB and gets you the answer

\- \`update_state()\`

\- No-op since the portfolio is updated automatically

\## ImplementedBroker

\- \`submit_orders()\`

\- Save files in the proper location

\- Wait for orders to be accepted

\- \`get_fills\`

\- No-op since the portfolio is updated automatically

\# Mocked system

\- Our implementation of the implemented system where we replace DB with
a mock

\- The mocked DB should be as similar as possible to the implemented DB

\## DatabasePortfolio

\- \`get_holdings()\`

\- Same behavior of \`ImplementedPortfolio\` but using \`OmsDb\`

\## DatabaseBroker

\- \`submit_orders()\`

\- Same behavior of \`ImplementedBroker\` but using \`OmsDb\`

\## OmsDb

\- \`submitted_orders\` table (mocks S3)

\- Contain the submitted orders

\- \`accepted_orders\` table

\- \`current_position\` table

\## OrderProcessor

\- Monitor \`OmsDb.submitted_orders\`

\- Update \`OmsDb.accepted_orders\`

\- Update \`OmsDb.current_position\` using \`Fill\` and updating the
\`Portfolio\`

## 

## 

## \## Universal pricing machine

It promotes efficiency of decentralized digital asset markets by using
financial machine learning to automatically price any digital currency
or token.

It distills knowledge from a large variety of data sources (e.g., news,
social sentiment, financial databases, market data) to automatically
estimate any quantity of interest and its uncertainty, including:

-   price, trading volume

-   risk, volatility

-   probability of specific events

for digital assets such as: crypto currency, non-fungible tokens (NFT),
smart contract

at different time scales ranging from seconds to days.

## Evaluating models

The same goal can be achieved with different research pipelines (with
different complexity to set-up and / or execution time):

-   Run a model -\> results_df -\> post-processing + comparison

-   Run models sweeping the params -\> multiple results_df -\>
    > comparison

-   

We want to separate how we compute the metrics and how we apply it

I think we did some work with Max about this, but not sure where it is.

We want to compute metrics (hit rate, pnl, SR, drawdown, \$ per bet,
MAE, AUC) as function of:

-   assets

-   day of the week

-   time of the day (e.g., between 9am and 10am, 10am and 11am)

-   liquidity (e.g., how much volume was transacted)

-   spread (\...)

-   ...

There is a common idiom (which we will formalize in the Surrentum
standard). In practice the pipeline is:

**Step 1 (compute results_df)**

-   Run the model and generate the usual output, then you can save /
    > retrieve this output or just compute with the model on the flight

```{=html}
<!-- -->
```
-   The input is a multi-index dataframe in the DataFlow standard
    > (results_df), like:

![](media/image8.png){width="6.5in" height="2.5416666666666665in"}

**Step 2 (annotate metrics_df)**

-   IMO the best representation is multi-index (timestamp, asset_id) on
    > the rows and columns equal to the features

    -   We can call this format \"metrics_df\"

    -   Each row is a prediction of the model

        -   Feature1 (price), Feature2 (volume), \...

    -   (timestamp, asset_id)

-   There is a function that annotates each row (timestamp, asset_id)
    > based on certain criteria (e.g., asset_id, day of the week, a
    > value function of a vwap volume)

    -   We have a library of function that accept a metrics_df and then
        > decorates it with an extra tag

    -   E.g., to split the results by asset_id the function creating the
        > tag is something like (timestamp, asset_id) -\> tag = asset_id

    -   If you want to partition by time of the day you do (timestamp,
        > asset_id) -\> tag = timestamp.time()

    -   ...

**Step 3 (split and compute metrics)**

-   There is a function that computes a dict from tag to df (e.g., from
    > asset_id to the corresponding df)

-   Then we call the function to compute the metric (e.g., hit rate,
    > pnl, \...) on each df and build a new dict tag -\> transform(df)

    -   The transform df accepts various column names to know what is
        > the y, y_hat, \... (this dict is built from the model itself,
        > saying what\'s the prediction col, the volatility, etc)

**Step 4 (aggregate results)**

-   Finally we aggregate the results as function of the tag (e.g.,
    > asset_id -\> pnl) and plot

-   The output is a pd.DataFrame (with tag on the rows and metrics on
    > the columns)

> Metric1
>
> Tag_value1
>
> ...

-   E.g., asset_id -\> pnl

> asset_id pnl conf_int

-   1030... ...

# \# Building financial primitives with Surrentum Protocol

\## Building a Dark-Pool

\## Building algorithmic orders

\## Internal matching of orders

\## Smart order routing

\## Building a synthetic bond

\- You like staking because it gives you something to do with your
crypto

\- You like yield because banks give you 0% APR and you don\'t have
access

to investment opportunities

\- Hence all the madness for yield farming

\- People creates Ponzi scheme just because there was a huge demand for
yield

but no supply (see the empty box from SBF)

\- Meanwhile, our hedge fund needs cash to invest and gives you back the
cash flow

from trading

\- But this cash flow is bumpy. One month is good, one month is bad

\- You don\'t like that

\- The typical solution is \"pay once a year\" so that in average the
total return

is positive, with high watermarks to account from crappy years

\- Can we transform our hedge fund bumpy cash flow in something stable,
but worse

in terms of expected return?

\- Yes!

\- You stake your crypto, we give you 0.5% / month (6% a year) in a mix
of cryptos

\- We try to give you always ETH and BTC, but sometimes we give you our

Surrentum token

\- You can sell or buy Surrentum on the open market as any crypto

\- Where does the value of Surrentum come from (i.e., why does it trade
at

more than 0, why is the box not empty)?

\- Well, it\'s an IOU that we use to make the bumpy cash flow straight

\- As long as the algo makes money in the long run, the Surrentum token
has

value

\- Of course we pocket the difference between the 6% / yr that we pay
users and

the 20% / yr we made

\- Everybody wins

\- Financial alchemy achieved!

\- This would have been funded instantly few months ago

# \# Related projects

-   Numerai

-   Lean

-   Quantopian

-   Crypto-bots (e.g., Pionex)

-   Amber / FalconX / Talos

-   HummingbirdBot

-   https://vega.xyz/

# \# Refs

[[Surrentum Protocol - Technical
appendix]{.underline}](https://docs.google.com/document/d/1Jp_yPU1FXFF7TdQjLiWzPzTLZmRLJ3KX0dooxArMQeI/edit)

[[Surrentum protocol - Background
research]{.underline}](https://docs.google.com/document/d/170IAWtrPUmMGXER-yIEFQ5zaArh2ksRNfvzciF6FLe0/edit#)

[[Surrentum
dir]{.underline}](https://drive.google.com/drive/u/1/folders/1icv3ifB095AIOMWtgr91v7mfR7Y7KaEl)

[^1]: Surrentum is the Latin name of the coastal city of Sorrento in the
    South of Italy. In Greek mythology it was the place inhabited by
    Sirens, who tried to seduce Ulysses in one of the episodes of the
    Odyssey.
