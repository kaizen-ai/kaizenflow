<!--ts-->
   * [Definitions and principles](#definitions-and-principles)
      * [DataSource](#datasource)
      * [Data in a DataSource](#data-in-a-datasource)
      * [Time series](#time-series)
      * [Raw metadata and payload](#raw-metadata-and-payload)
      * [P1 metadata and payload](#p1-metadata-and-payload)
      * [Conventions](#conventions)
   * [Our flow for onboarding data](#our-flow-for-onboarding-data)
      * [Idea generation](#idea-generation)
      * [Dataset collection](#dataset-collection)
      * [Prioritize data source downloading](#prioritize-data-source-downloading)
      * [Data download](#data-download)
      * [Transformation of data into P1 representation](#transformation-of-data-into-p1-representation)
      * [Data sanity check](#data-sanity-check)
      * [Exposing data to researchers](#exposing-data-to-researchers)
   * [Internal representations](#internal-representations)
      * [MonsterDataSource](#monsterdatasource)
         * [Refs](#refs)
         * [P1 fields](#p1-fields)
      * [MonsterMetaData](#monstermetadata)
         * [Refs](#refs-1)
         * [P1 fields](#p1-fields-1)
         * [Price / volume data](#price--volume-data)
      * [MonsterPayloadData](#monsterpayloaddata)
         * [Refs](#refs-2)
      * [KnowledgeGraph](#knowledgegraph)
   * [Flow of data among representations](#flow-of-data-among-representations)
   * [Complexities in the design](#complexities-in-the-design)
         * [How to handle data already in relational form?](#how-to-handle-data-already-in-relational-form)
         * [Successive approximations of data](#successive-approximations-of-data)
         * [Access control](#access-control)



<!--te-->

# Definitions and principles

## `DataSource`

- A `DataSource` is a collection of datasets from a specific origin (e.g., a
  website like `eia.org`, the WIND terminal)

## Data in a `DataSource`

- Each `DataSource` typically contains:

  1. Metadata, i.e., information about the data (e.g., a description of each
     time series)
  2. Payload data (e.g., time series, point-in-time data, tables, PDFs with
     text)

- Some `DataSource` might not have metadata and instead contain just payload
  data
- Both metadata and payload data come in "raw form"
  - E.g., the schema for both 1. and 2. is typically different across different
    data sources, irregular, and incomplete
- We want to convert any raw data into our internal data representation

## Time series

- The payload data in each `DataSource` typically is composed of many time
  series
  - Time series may be univariate or multivariate

## Raw metadata and payload

- We define as "raw" any data (both metadata and payload) in the form as it was
  made available in the data source at the time when we collected the data,
  e.g.,
  - Raw metadata in case there was a file with a directory of the data
  - Zipped CSV files containing time series data

- The raw data is stored in the ETL2 layer
  - We transform raw data into P1 data
  - We save it back in the ETL2 layer

- This is an example of raw metadata:
  ```
  ;updates;pub_date;document_type;organisation;part_of_a_collection;short_desc;title;updated;page_url;name;doc_url;doc_type;size;frequency
  0;['2020-01-14T15:33:56.000+00:00', '2019-10-10T09:30:00.000+01:00'];Published 10 October 2019;National Statistics;Department for Business, Energy & Industrial Strategy;Business Population Estimates;Annual business population estimates for the UK and regions in 2019.;Business population estimates 2019;14 January 2020;/government/statistics/business-population-estimates-2019;Business population estimates for the UK and regions 2019: Statistical Release (PDF);https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/852919/Business_Population_Estimates_for_the_UK_and_regions_-_2019_Statistical_Release.pdf;PDF;636KB;[]
  ```

## P1 metadata and payload

- P1 data is both metadata and payload data that has been transformed into our
  internal format

## Conventions

- Every piece of data downloaded or inserted manually should be traceable
  - Where did it come from?
    - E.g., `xyz.org` website, a paper, a book
  - Who added that information and when?
    - He who made the modification to a data structure (e.g., Max) might be
      different from him who made the change in the DB (e.g., Paul committed the
      change)
    - Git is tracking only the second part
    - Context about the data change should be available (e.g., GitHub task might
      be the best way): why was the data changed?
- All metadata and payload data should be described in this document
  - The field names should:
    - Have a name as long as needed to be clear, but no longer
    - Be capitalized
    - Use underscores and not spaces
    - Have a type associated with it (e.g., string, float)
      - TODO(\*): Enumerate the valid types
    - A description
- We should qualify if something is an estimate (e.g., ~\$1000) or not (e.g.,
  \$725 / month)
- How much do we believe in this information?
  - Is it a wild guess?
  - Is it an informed guess?
  - Is it what we were told by somebody on the street?
  - Please report any supporting justification for our belief level (e.g., is it
    because of the source quality? Our enlightened experience? Something else?)

# Our flow for onboarding data

- The process we follow has multiple stages

## Idea generation

- We come up with ideas from papers, books, etc. about interesting datasets and
  models
- Currently, this is done informally in GitHub tasks

## Dataset collection

- Dataset collection is informed by a modeling idea or just "this data makes us
  come up with a modeling idea"

- E.g., see GitHub tasks under the `Datasets` milestone
- The result of this activity should go in the `MonsterDataSource` (see below)
  - Currently is in the Monster Spreadsheet

## Prioritize data source downloading

- We decide which dataset to download based on several competing criteria:
  - Business objective, e.g.,
    - A source for oil is more interesting than one for ags
    - The number of models that can be built out of the data
  - The complexity of download (e.g., data in PDF vs data in CSV format)
  - Uniqueness
  - Cost
  - ...

- Currently, we:
  - Track these activities in the Monster Spreadsheet; and
  - File issues against ETL2

## Data download

- Download the raw data (both metadata and payload) and put it into a suitable
  form inside ETL2

- Ideally, we would like each data source to be available both historically and
  in real-time
  - On one side, only the real-time data can inform us about:
    - Publication delay,
    - Reliability of the download process
    - Delay in acquiring the data on our side
    - Throttling, ...
  - On the other side, we would prefer to do the additional work of putting data
    in production (with all the ongoing maintenance effort) only when we know
    this data is useful for our models or can be sold
  - We need to strike a balance between these two needs

- Currently we track these activities in GitHub tasks
  - We use the `DataEncyclopedia` and `etl_guides` to track data sources
    available and APIs

## Transformation of data into P1 representation

- We want to transform all metadata and payload data into a standard P1
  representation
- See below

## Data sanity check

- We want to check that the downloaded data is sane, e.g.,
  - Did we miss anything we wanted to download?
  - Does the data look good?
  - Compute statistics of the time series (e.g., using our time series stats
    flow)

## Exposing data to researchers

- Researchers can access the data from ETL2

- Ideally, we would like to share the access mechanisms with customers as much
  as possible (of course with the proper access control and logging)
  - E.g., we could build REST APIs that call into our internal APIs

# Internal representations

- We collect data into 4 data structures with a fixed schema:
  - `MonsterDataSource`: collects information about all data sources we are
    aware of
  - `MonsterMetaData`: collects all the metadata about all the time series we
    store
  - `MonsterPayloadData`: collects all the payload data about time series
  - `KnowledgeGraph`: collects all the relationships between economic entities
    and time series

- TODO(\*): Ok to come up with better names, but we might need to have names for
  these data structures so it's easier to understand what we are referring to
  (e.g., the Monster Spreadsheet)

## `MonsterDataSource`

- The `MonsterDataSource` stores all the data sources we are aware of
  - In practice, it is a machine-readable form of the Monster Spreadsheet
  - It is represented by a single CSV file
  - It is checked in the repo under `//p1/metadata`
- There is a notebook that loads the CSV as a pandas data frame
- There is a library that allows one to query, compute stats, and manipulate
  this data structure, e.g.,
  - What data sources are available already?
  - How many data sources are we aware of?
  - How many data sources are available in ETL2?
  - There are sanity checks to make sure the representation is consistent (e.g.,
    make sure that the values in special columns have the right type and values)

- This is the result of "Data sets collection" step
  - Typically analysts are in charge of manipulating it

- Probably this will evolve into a full-blown database table at some point
  - For now, we want to keep it as a CSV, so we can:
    - Version control
    - Review the changes before PR

### Refs

- [PartTask578 KG: Data source metadata (formerly known as Monster Spreadsheet)](https://github.com/ParticleDev/commodity_research/issues/578)

### P1 fields

- `ID`
  - P1 data source internal name
  - E.g., `EIA_001`
- `DATA_SOURCE`
  - The symbolic name of the data source
  - E.g., "USDA"
- `DATASET`
  - Optional
  - Represents the fact that one data set can be organized in multiple data
    sets, each with many time series
  - E.g., For USDA there are several data sets "Agricultural Transportation Open
    Data Platform", "U.S. Agricultural Trade Data"
- `SUMMARY`
  - Human-readable summary
    - What does this dataset contain?
    - This is a free-form description with links to make it easy for a human to
      understand what the data set is about
  - E.g., "The U.S. Energy Information Administration (EIA) collects, analyzes,
    and disseminates independent and impartial energy information to promote
    sound policy-making, efficient markets, and public understanding of energy
    and its interaction with the economy and the environment."
- `SUMMARY_SOURCE`
  - How did we come to know about it?
  - E.g., it can be a URL, a paper, a book
- `DATASOURCE_URL`
  - E.g., `www.eia.gov`
- `DATASET_URL`
  - Links to the webpage about the specific dataset
- `DESCRIPTION_URL`
  - Links to the webpage with some description of the data
  - E.g., `https://agtransport.usda.gov/`
- `COLLECTION_TYPE`
  - What is the predominant source of the data
    - Survey: data that is collected by 'survey' methodology
    - First-hand: it is the closest source of the data
    - Aggregation: the source just presents the information which comes from a
      third party
    - Search engine
- `DOWNLOAD_STATUS`
  - Represents whether we have:
    - Historical downloaded: the raw historical data is in ETL2
    - Historical metadata processed: the metadata has been processed and it's
      available
    - Historical payload data processed: the payload data is available through
      ETL2
    - ...
- `SUBSCRIPTION_TYPE`
  - Free
  - Subscription
  - Both: source has open data and paid services simultaneously
- `COST`
  - Indicative cost, if a subscription
- `HIGHEST_FREQUENCY`
  - Highest frequency available from an exploratory inspection, e.g.,
    - Annual
    - Daily
    - Hourly
    - Monthly
    - Quarterly
    - Unspecified
- `RELEASE_FREQUENCY`
  - When the data is released, e.g.,
    - Different releases
    - End of month
    - Third Friday of the month
    - Unspecified
- `TARGET_COMMODITIES`
  - What target commodity it can be used for (from exploratory analysis), e.g.,
    - Agriculture
    - Climate
    - Coal
    - Commodity: contains info about agricultural, metal, energy commodities as
      a whole
    - Copper
    - Corn
    - Energy: contains oil + gas or other oil products
    - Gold
    - Macroeconomic data
    - Market: contains data about market indicators
    - Metals
    - Natural gas
    - Oil
    - Other
    - Palladium
    - Platinum
    - Silver
    - Soybean
    - Steel
    - Sugar
    - Trade: trade data, freight data, etc.
  - We want to have our own internal representation (in terms of "PCA sectors")
    or a stable standard hierarchy to classify commodities
- `GEO`
  - The geographical location that this data is mainly about, e.g.,
    - Global
    - US
    - China
    - Europe
- `GITHUB_ISSUE`
  - Number (or link) for the GitHub issue tracking this specific data set
- `GITHUB_ETL2_ISSUE`
  - Number (or link) for the GitHub issue tracking the downloading of this
    specific data sets
- `TAGS`
  - Wind: WIND terminal data sources
  - Chinagov: Chinese government sources of data
  - Baidu: data sources found using Baidu
  - Shf: sources from data vendors of Shanghai Futures Exchange
  - Papers that referred to this
  - Edgar: EDGAR equivalents in a given country
  - Wind+: sources from WIND Commodity DB
  - 600: sources from Task 600 from Github issues
  - TODO(gp): To reorg
- `NOTES`
  - This is a free-form field which also incubates data that can become a field
    in the future
    - Why and how is this data relevant to our work?
    - Is there an API? Do we need to scrape?
    - Do we need to parse HTML, PDFs?
    - How complex do we believe it is to download?
- `PRIORITY`
  - Our subjective belief on how important a data source is. This information
    can help us prioritize data source properly
  - E.g., P0
- `RELATED_MATERIAL`
  - Pointers to papers, articles, books, blogs that contain information related
    to this specific time series

## `MonsterMetaData`

- For each data source in the `MonsterDataSource` there is a data frame with
  information about all the data contained in the data source

- Each metadata data frame for a time series contains a unique P1 `ID` that can
  be used to retrieve the data from ETL2

- The KnowledgeGraph contains pointers to the metadata of time series

### Refs

- [PartTask921 KG: Generate spreadsheet with time series info](https://github.com/ParticleDev/commodity_research/issues/921)

### P1 fields

- `ID`
  - Internal P1 ID
  - E.g., "EIA_NGASDS_001"
- `NAME`
  - A brief name that we can use to refer to it, if possible
- `SHORT_DESCRIPTION`
  - One-line description
- `LONG_DESCRIPTION`
  - Long description
- `DATA_SOURCE`
  - Pointer to the corresponding entry in `MonsterDataSource`
- `DATA_URL`
  - The URL where this time series was downloaded from
  - E.g., this is a link that will initiate a download (e.g., in case we want to
    go back to the source and re-download for any reason)
- `INFO_URL`
  - Url with information relevant for this specific time series, e.g.,
    description of the fields
- `SAMPLING_FREQUENCY`
  - What is the frequency (e.g., daily, weekly, monthly) of the time series
  - This should be computed automatically
- `RELEASE_FREQUENCY`
  - How often is released (e.g., every month, every quarter)
- `RELEASE_DELAY`
  - This is an estimate of how long it takes for the data to be published
- `START_DATE`
  - Timestamp when the time series starts
- `END_DATE`
  - Timestamp when the time series ends
- Fields we recompute internally from the historical / real-time data
  - `P1_SAMPLING_FREQUENCY`
  - `P1_RELEASE_FREQUENCY`
  - `P1_RELEASE_DELAY`
  - `P1_START_DATE`
  - `P1_END_DATE`
  - `P1_RELEASE_DELAY`
- `UNITS_OF_MEASURE`
  - Unit of measure of each column
- `COLUMN_DESCRIPTION`
  - A description of each column in the data in case of data frame
- `SUPPLY / DEMAND / INVENTORY`
  - Manual annotation of what we think this data applies to
  - This information might be redundant with the KG, and to be removed
- `INTERNAL_DATA_POINTER`
  - Pointer to ETL2 data
- `IMPORTANCE`
  - How important / market moving this time series is
    - E.g., 0 - 10 as a magnitude coefficient
  - This is a field we can estimate manually and / or automatically (e.g.,
    through automatic estimation from market volatility)

- `IS_HISTORICAL_OR_RT`
  - If the data was downloaded as historical data or real-time
  - We can keep multiple copies of the same time series, some downloaded
    historically and other real-time
  - For production, we stitch together historical and real-time to get a single
    view of the data (like we used to do with Tardis)
- `DOWNLOAD_TS`
  - When it was downloaded
  - It can be a list of timestamps

- Same metadata as `MonsterDataSource` but for specific time series since they
  might have different values than the including data source
  - `TARGET_COMMODITIES`
  - `COLLECTION_TYPE`
  - `DOWNLOAD_STATUS`
  - `GEO`
  - `RELATED_MATERIAL`
  - `GITHUB_ISSUE`
  - `GITHUB_ETL2_ISSUE`

### Price / volume data

- Note that price / volume time series (e.g., for commodities, equities, ETFs)
  have enough structure to warrant being in the database
  - Some additional data can be:
    - Informal name
    - Symbols and exchanges (with dates)
    - Pointers to price / volume data
    - Options / futures and contract specs
    - Class (e.g., energy / metals / ags)

## `MonsterPayloadData`

- ETL2 has interfaces to access data from each data source that we have
  downloaded
- We want to have a single interface sitting on top of the data source specific
  API
- This Uniform API should be able to return a time series given a unique ID
  - The format of this data is fixed, e.g., it is a `pd.DataFrame` or
    `pd.Series` indexed by `datetimes` with one or multiple columns

### Refs

- [PartTask951: ETL2: Uniform access to ETL2 data](https://github.com/ParticleDev/commodity_research/issues/951)

## `KnowledgeGraph`

- This graph represents relationships between economic entities and data in ETL2
  - E.g., what predicts crude oil demand, which time series are related to crude
    oil demand

- This is described in detail in the document `knowledge_graph_example.md`

# Flow of data among representations

- We need to transform data among different datasets
  - Some transformations are automatic, others require human annotation

- Download raw historical ETL2 data
  - `DataSource` -> `ETL2`, `MonsterDataSource`
  - Data is added to ETL2
  - Update the `MonsterDataSource`

- Download raw real-time ETL2 data
  - `DataSource` -> `ETL2`, `MonsterDataSource`
  - Same as above but for the real-time loop

- Transform raw metadata into our internal representation
  - `ETL2` -> `MonsterMetaData`
    - This consists of:
      - Mapping fields from the raw metadata into our P1 internal representation
      - Converting the values into Python types

- Transform raw payload data into our internal representation
  - `ETL2` -> `MonsterPayloadData`
  - Note that if the data is in a suitable format (e.g., CSV form) we might be
    able to convert it on the fly to our internal `pandas` representation
  - If it's in a PDF or other unstructured data format we can:
    - Decide not to process it for now
    - Pre-process the data and save it into a structured format

- Update P1 metadata after a download
  - `MonsterPayloadData` -> `MonsterMetaData`, `MonsterDataSource`
  - E.g., we want to compute some statistics about the data (e.g.,
    `P1_SAMPLING_FREQUENCY`, `P1_START_DATE`)

- Compute statistics from `MonsterMetaData`, `MonsterDataSource`
  - Given data from `MonsterDataSource`, `MonsterMetaData`, `MonsterPayloadData`
    we want to compute statistics / sanity checks, e.g.,
    - How many data sources do we have?
    - How many data sources have downloaded completely?
    - How many time series are we aware of or have downloaded?
    - How many `nans` there are in a subset of time series?

# Complexities in the design

### How to handle data already in relational form?

- Some data is already in a relational form, e.g.,
  - Information about the data source a time series comes from
    - We don't want to replicate information about a data source (e.g., its URL)
  - The source that informed a certain data source or relationship, e.g.,
    - Certain data sources are suggested by a paper, others from a book (e.g.,
      "The secrets of economic indicators"), others are our own theory
    - We don't want to repeat information about a paper (e.g., its URL)
      everywhere, but rather we would like to normalize this information

- We want to store information about our internal process, e.g.,
  - What is the priority of having a certain data source / time series available
    internally
  - What is the status of a data source (e.g., "downloaded", "only-historical
    data downloaded", "real-time")
  - What is the source of a data source (e.g., "WIND", ..., scraping website)

- It can be argued that information about infra data should not be mixed with
  research data
  - The issue is that the process of discovering data sources and on-boarding
    data sources move at different speeds
    - E.g., one researcher (or potentially even a customer!) might want to know:
      - "what are the sources about oil that are available?"
      - "what are the next sources to download?"
      - "do we have only historical data or real-time of a data source?"
      - "what are the models built in production from a data source?"
  - Thus inevitably we will need to "join multiple tables" from research and
    infra
    - At this point let's just make it simpler to do instead of maintaining
      different data structures
    - Simplicity needs to be met with discipline in maintaining the data
      - E.g., if data download status/priorities are not kept up-to-date, then
        their utility decreases (and can even go below zero!)

### Successive approximations of data

- It can happen that for a data source some fields are filled manually initially
  and then automatically updated
  - E.g., we can have an analyst fill out the duration of the data (e.g., it's
    from 2000 to today) and then have automatic processes populate this data
    automatically

### Access control

- We need to have policies to expose some data only internally; or to certain
  customers

- We can group fields into different "tables"
  - Shared: fields
  - Internal
  - Customer
