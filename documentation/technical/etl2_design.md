<!--ts-->

* [Goal](#goal)
  <!--te-->

# Goal

- The acronym "ETL" stands for:
  - Extract
  - Transform
  - Load

- The goal of the `ETL2` framework is to:
  - Download historical and real-time data from a variety of data sources
  - Transform the data in research-friendly data structures that are mapped on
    the Knowledge Graph (KG)

# `ETL2` and `ETL1`

- We want to design the `ETL2` framework, learning from the lessons of the
  previous `ETL1`, but without being tied to:
  - Previous design choices
  - Code dependency that are result of technical debt (e.g.,
    `eia/filler_versions.py` depends on `twitter_data`)

- We will design the system from scratch and, if there is code that we can reuse
  from the `ETL1`, we will:
  - Extract one piece at a time
  - Clean it
  - Unit test it
  - Review it (like if it was written from scratch)
  - Reuse it in `ETL2`

- There is lots of code that can be repurposed (e.g., EIA, Twitter fillers,
  MongoDB interfaces)
- We will maintain `ETL1` as reference for the `TwitterSentiment1`, while we
  build `TwitterSentiment2` and start improving it and extending it to other
  textual data sets (News, WeChat, StockTwits)

# Design invariants

## Definitions

### Data source

- A data source (e.g., a website like EIA, Twitter) containing several time
  series

### Time series

- A univariate or multivariate series indexed by time with data (e.g., values,
  text)

### Data timestamp

- The data in the index of a time series

### Publication timestamp

- The time stamp that the website claims it corresponded to the publication of
  certain data

### Knowledge timestamp

- The time stamp we assigned to the data when we downloaded the time series
- This is the only timestamp we believe it's reliable
- It's our "knowledge date"
- Remember that MongoDB uses auto-generated `_id` field with type `ObjectId`
  which contains timestamp

### Timestamp relationship

- The relationship between the 3 timestamps is not obvious, e.g.,
  - EIA can publish the WTI supply number for Dec 2019 (data timestamp) on
    midnight 2020-01-01 (publication timestamp) and we retrieve on the data one
    hour later (knowledge timestamp)
  - There might be some situations where the data for Dec 2019 is published on
    midnight 2020-01-01, but the publication timestamp is set incorrectly
    2019-12-15. We should catch this problem with our knowledge timestamp

### History being rewritten

- It can happen that the data sources rewrites history unbeknownst to us
  - We can catch this problem by sampling the history multiple times
    - Ideally we should
      - Re-download all the data every day
      - Compare it to the previous sampled data
      - Keep the new pieces of data and the updated pieces of data

- What if the meaning of the data changed?

- What if data deleted?
  - We store the different versions of the data, since we sample and maintain a
    history of the data

## Timezone

- We store the timestamps as UTC
  - It can be called `datetime_UTC`, to avoid `tzinfo` for performance reason

- Interface should be mostly ET
  - We can use ET which is the reference of financial markets
  - Readers always return data as ET (e.g., `datetime_ET` without `tzinfo`)
  - The problem with UTC is that there is no daylight saving so it's not just
    the same number of hours to convert into ET

## Who are the users of ETL2?

- Both developers and researchers

## Code location

- The ETL2 framework code code is under `//p1/vendors/core`
- All the downloading / reading code goes into `//p1/vendors`
  - We will use one directory per vendor, e.g.,
    ```bash
    cme
    core
    csi
    eia
    ...
    ```

## User interface

- The downloading code is invoked with a command line representing the data to
  download what commodity, optionally
  - Probably the granularity is download everything. The transformer later will
    be in charge of unpacking data for each commodity
  - What data source (e.g., EIA)
  - What subset of time series (e.g., all, only a specific time series)
  - What time period (e.g., everything, all 2019 data
  - Where to save it (e.g., MongoDB instance, collection)

- We use Airflow as scheduler to automate the downloading jobs

## Classes

- TODO(gp): Check code for latest version of class responsibilities

### Design process

- They can add some functionality only through the process
  - Describe -> Discuss -> Implement
  - Otherwise, everybody will reinvent their own bicycles

### Adapters

- Use Adapter classes for reading data as it is

### Writers

- Use Writer classes for saving results as is
  - Same design process as for Adapters

- Use Transformer classes as is (the same conditions as for `Adapters`)
- All work that DS does should be somewhere between Adaptor and Writer
- Production system
- Use a command-line interface (through Docker) with fixed parameters and in a
  fixed order or with generators of those.
- Use classes (through Airflow) with fixed parameters and in a fixed order or
  with generators of those
- There should not be a situation where a programmer uses the CLI interface
  except for tests
- People should not use /data (always use MongoDB or S3 and let the caching
  layer do the work)
- [Bug]: Make sure caches are on SSD

## Parallel vs serial

- The code can download data:
  - In parallel with a certain number of threads; or
  - Serially
- The results should be the same, after applying proper sorting to obtain a
  canonical representation
  - It is possible that some differences might exist in the order of the data or
    in the metadata but we assume that is minor

## Metadata and payload

- We keep the time series data (aka payload) and the KG data separated
- The KG contains metadata and pointers to the payload for each time series

##

# Implementation details

## DataSourceAdapters

- `Adapters` are equivalent to `ETL1` "Fillers"
  - We can reuse `ETL1` Adapters that we have used for downloading data after
    they are ported to the new `ETL2` interfaces

- Each `Adapter` understands:
  - The structure of a website
  - How to access the data
  - What to download
  - How to access a period of time

- Each `Adapter` extracts the data by:
  - Scraping HTML
  - Taking advantage of the specific data API
  - Taking snapshot of the rendered pages and run OCR on it

- The output is the raw data stored in the website, e.g., CSV, XLS, PDF file,
  JPEG
- `Adapters` don't do any transformations
  - They are just retrieving the data in the native format
  - They are "Extract" phase in ETL

## Transformers

- Read data using `Adapters`, transform it, and passes to `Writer` class
- E.g., read an XLS spreadsheet (using `AdapterXLS`), extract time series, save
  data as CSV (using `WriterCSV`)
- They are the "Transform" phase in ETL

## Writers

- Know how to write using approved backends
  - Raw data on S3 (?)
    - Fuse S3 is not a great
    - Implement a minimal lawyer to make it look like an FS?
    - Use disk as a staging area
  - MongoDB
  - Parquet
  - File storage
  - Airflow hooks

- Know how the data should be formatted with specific approved formats
  - CSV
  - JSON
  - ...
- We use them in a pipeline fashion after DataSourceAdapters, Transformer

## Loader

- Loader is a class that combines Adapter, Transformer and Writer
- No additional behaviour should be added, except logging, notifying and maybe
  something else, not connected with parsing, converting or writing the data.
- Do not try to solve two problems at once. If downloading Historical data and
  Real-time data looks different don't try to put them in one class.