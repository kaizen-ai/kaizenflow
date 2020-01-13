# Why is it useful?

- Allow to judge news events and macroeconomic time series based on our
  understanding of the world
  - The usual approach is to consider events in an isolated fashion

- Allow to build models automatically based on the relationship between
  quantities existing in the real world

# Definitions and principles

## Data source and time series
- Each data source is typically comprised of many time series

- Time series may be univariate or multivariate

- This is a running documentation of the fields we are interested in capturing
  about the data
- Paul suggests to keep in the KG static or slowly changing data
  - I don't disagree with it, but we need to decide where to put the rest of the
    information we want to capture

## Everything should be tracked

- Self-evident

# Our flow for ingesting data

- The process we have been following is:

## 1. Idea generation
- We come up with ideas from papers, books, ...

- Currently this is done informally in GitHub tasks
  - TODO(*): We should have docs / repo only for this purpose since this is
    sensitive IP

## 2. Data sets collection
- It is informed by a modeling idea or just pre-emptively (e.g., "this data makes
  us come up with a modeling ideas")
  - E.g., see GitHub tasks under the `Datasets` milestone

- Currently, the result of this activity is in the Monster Spreadsheet

## 3. Exploratory analysis of data sets
- We look for data needed by a model; or
- We browse data and come up with modeling ideas

- Currently the result of this activity is in the GitHub tasks / Monster Spreadsheet

## 4. Prioritize data sources downloading
- We decide which data to download:
  - Based on business objective (e.g., a source for oil vs one for ags)
    - Amount of models that can be built out of the data
  - Complexity of downloading
  - Uniqueness
  - Cost
  - ...

- Currently we track this in the Monster Spreadsheet and file issues against ETL2

## 5. Data download
- Download data and put it into a suitable form inside ETL2

- Ideally we would like to have each data source to be available both
  historically and in real-time
  - On the one side only the real-time data can inform us on publication delay,
    reliability of the downloading process, delay to acquire the data on our
    side, throttling, ...
  - On the other side we would prefer to the additional work of putting data in
    production (with all the on-going maintenance effort) only when we know this
    data is useful for our models or can be sold
  - We need to strike a balance between these two needs

- Currently we track these activities into GH tasks
  - We use the Data Encyclopedia to track data sources available and APIs

## 6. Sanity check of the data
- We want to check that the download data is sane, e.g.,
  - Did we miss anything we wanted to download?
  - Does the data look good?
  - Compute statistics of the time series (e.g., using our timeseries stats flow)

- This task is inevitably subjective?

## 7. Expose data to researchers

- Researchers can access the data from ETL2

- Ideally we would like to share the access mechanisms with customers as much as
  possible (of course with the proper access control)
  - E.g., we could build REST APIs that call into our internal APIs

# Complexities in the design

### How to handle data already in relational form?

- Some data is already in a relational form, e.g.,
  - information about from which data source a time series come from
    - we don't want to replicate information about a data source (e.g., its
      `url`,)
  - the source that informed a certain data source or relationship, e.g.,
    - certain data sources are suggested by a paper, others from a book (e.g.,
      The secrets of economic indicators), others are our own theory
    - We don't want to repeat information about a paper (e.g., its url)
      everywhere, but rather we would like to normalize this information

- We want to store information about our internal process, e.g.,
  - what is the priority of having a certain data source / time series
    available internally
  - what is the status of a data source (e.g., "downloaded", "only-historical
    data downloaded", "real-time")
  - what is the source of a data source (e.g., "WIND", ..., scraping website)

- It can be argued that information about the infra should not be mixed with
  research ones
  - The issue is that the process of discovering data sources and on-boarding
    data sources moves at different speed
    - E.g., one research (or potentially even a customer!) might want to know
      - "what are the sources about oil that are available?"
      - "what are the next sources to download?"
      - "do we have only historical data or real-time of a data source?"
      - "what are the models built in production from a data source?"
  - Thus inevitably we will need to "join multiple tables" from research and
    infra
      - At this point let's just make it simpler to do instead of maintaining
        different data structures

### Successive approximations of data

- It can happen that for a data source some of the fields are filled manually
  initially and then automatically updated
  - E.g., we can have an analyst fill out the duration of the data (e.g., it's
    from 2000 to today) and then have automatic processes populate this data
    automatically

- We could have

### Access control

#### Problem

- We need to have policies to expose some of the data only internally; or
  to certain customers

#### Solution

- We can group fields into different "tables"
  - Shared: fields
  - Internal
  - Customer

# Fields

## Conventions

- We explicitly qualify if something is an estimate (e.g., ~$1000) or not
- Every information should be traceable (especially cut-and-paste data)
  - Where did it come from?
    - Website, paper, book
  - Who added that information and when
    - Note that relying on the Git commit history might not be enough, since
      maybe an analyst does some work, and somebody else adds it to the KB
    - Context about data should be available (e.g., GitHub task might be the best
      way)
- How much do we believe in this information?
  - Is it a wild guess? Is it an informed guess? Is it what we were told?
- All metadata should be described in this document
  - The field names
    - should have underscores and not spaces
    - should be as long as needed to be clear, although concise

## Data source metadata

- This represents a machine readable, revision controlled version of the Monster
  Spreadsheet
  - It's not clear how to represent it

- This is the result of "Data sets collection" step
  - Typically analysts are in charge of manipulating it

- For each data source we are interested in the following information

- ID
  - P1 data source internal name
  - E.g., `EIA_001`

- SOURCE
  - Data set source description
  - E.g., `US Energy Information Administration`

- URL
  - Main url
  - E.g., `www.eia.gov`

- SUMMARY
  - Human readable summary
    - What does it contain?
    - This is a free form description with links to make easier for a human to
      understand what the data set is about
  - E.g., "The U.S. Energy Information Administration (EIA) collects, analyzes,
    and disseminates independent and impartial energy information to promote
    sound policymaking, efficient markets, and public understanding of energy and
    its interaction with the economy and the environment."

- SUMMARY_SOURCE
  - Where was the summary taken from
  - E.g., it can be an url, a paper, a book

- GITHUB_TASK
  - Is there a GitHub task related to this?

- PRIORITY
  - Our subjective belief on how important a data source is. This information can
    help us prioritize data source properly
  - E..g, P0

- COST
  - Is it free or is by subscription?
  - What is the annual cost?

- STATUS
  - Download status
  - E.g., completed, in progress

- API_ACCESS
  - How to retrieve the data from ETL2?
  - E.g., pointer to code, a

- NOTES
  - This is a free-form field which also incubates data that can become a field
    in the future
    - Why and how is this data relevant to our work?
    - Is there an API? Do we need to scrape?
    - Do we need to parse HTML, PDFs?
    - How complex do we believe it is to download?

## Time series metadata

Task 921 - KG: Generate spreadsheet with time series info

- ID (internal)
- name
- aliases
- url
- short description
- long description
- sampling frequency
- release frequency
- release delay
- start date
- end date
- units of measure
- target commodities
- supply / demand / inventory
- geo
- related papers
- internal data pointer

## Knowledge base

- There is an ontology for economic phenomena
- Each time series relates to nodes in the ontology
