

<!-- toc -->

- [Data client stack](#data-client-stack)
  * [Interfaces](#interfaces)
  * [Transformations](#transformations)
    + [Output format of `ImClient`](#output-format-of-imclient)
    + [Transformations by classes derived from `MarketData`](#transformations-by-classes-derived-from-marketdata)
    + [Transformations by abstract class `MarketData`](#transformations-by-abstract-class-marketdata)
    + [Output format of `MarketData`](#output-format-of-marketdata)
  * [Asset ids format](#asset-ids-format)
    + [`ImClient` asset ids](#imclient-asset-ids)
    + [`MarketData` asset ids](#marketdata-asset-ids)
    + [Handling of `asset_ids`](#handling-of-asset_ids)
  * [Data](#data)
    + [Handling of filtering by time](#handling-of-filtering-by-time)
    + [Handling timezone](#handling-timezone)

<!-- tocstop -->

# Data client stack

As said in other documents, the data is downloaded and saved by `DataPull` with
minimal or no transformation. Once the data is downloaded, it needs to be
retrieved for processing in a common format (e.g., `DataPull` format).

We use a two-layer approach to handle the complexity of reading and serving the
data to clients.

```mermaid
flowchart
  Vendor Data --> ImClient --> MarketData --> User
```

- `ImClient`
  - Is data vendor and dataset specific
  - Adapt data from the vendor data to a standard internal `MarketData` format
  - Handle all the peculiarities in format and semantic of a specific vendor
    data
  - All timestamps are UTC
  - Asset ids are handled as strings

- `MarketData`
  - Is independent of the data vendor
  - Implement behaviors that are orthogonal to vendors, such as:
    - Streaming/real-time or batch/historical
    - Time-stitching of streaming/batch data, i.e., merge multiple data sources
      giving a single and homogeneous view of the data
      - E.g., the data from the last day comes from a real-time source while the
        data before that can come from an historical source. The data served by
        `MarketData` is a continuous snapshot of the data
    - Replaying, i.e., serialize the data to disk and read it back, implementing
      as-of-time semantic based on knowledge time
      - This behavior is orthogonal to streaming/batch and stitching, i.e., one
        can replay any `MarketData`, including an already replayed one
  - Data is accessed based on intervals `[start_timestamp, end_timestamp]` using
    different open/close semantics, but always preventing future peeking
  - Support real-time behaviors, such as knowledge time, wall clock time, and
    blocking behaviors (e.g., "is the last data available?")
  - Handle desired timezone for timestamps
  - Asset ids are handled as ints

## Interfaces

- Both `ImClient` and `MarketData` have an output format that is enforced by the
  base abstract class and the derived classes together

- `ImClient` and `MarketData` have 3 interfaces each:

  1. An external "input" format for a class
  - Format of the data as input to a class derived from `MarketData`/`ImClient`

  2. An internal "input" format
  - It's the format that derived classes need to adhere so that the base class
    can do its job, i.e., apply common transformations to all classes

  3. An external "output" format
  - It's the `MarketData`/`ImClient` format, which is fixed

## Transformations

- The chain of transformations of the data from `Vendor` to `User` are as
  follow:

  ```mermaid
  flowchart
    Vendor --> DerivedImClient --> AbstractImClient --> DerivedMarketData --> AbstractMarketData --> User
  ```

- Classes derived from `ImClient`
  - The transformations are vendor-specific
  - Only derived classes `ImClient` know what is exact semantic of the
    vendor-data
  - Whatever is needed to transform the vendor data into the internal format
    accepted by base `ImClient`

- Abstract class `ImClient`
  - The transformations are fixed
  - Implemented by `ImClient._apply_im_normalization()`

- Class derived from `MarketData`
  - The transformations are specific to the `MarketData` derived class

- `MarketData`
  - The transformations are fixed

### Output format of `ImClient`

- The data in output of a class derived from `ImClient` is normalized so that:
- The index:
  - Represents the knowledge time
  - Is the end of the sampling interval
  - Is called `timestamp`
  - Is a tz-aware timestamp in UTC

- The data:
  - (optional) Is re-sampled on a 1 minute grid and filled with NaN values
  - Is sorted by index and `full_symbol`
  - Is guaranteed to have no duplicates
  - Belongs to intervals like `[a, b]`
  - Has a `full_symbol` column with a string representing the canonical name of
    the instrument

- An example of data in output from an `ImClient` is:
  ```
                                  full_symbol     close     volume
                  timestamp
  2021-07-26 13:42:00+00:00  binance:BTC_USDT  47063.51  29.403690
  2021-07-26 13:43:00+00:00  binance:BTC_USDT  46946.30  58.246946
  2021-07-26 13:44:00+00:00  binance:BTC_USDT  46895.39  81.264098
  ```

- TODO(gp): We are planning to use an `ImClient` data format closer to
  `MarketData` by using `start_time`, `end_time`, and `knowledge_time` since
  these can be inferred only from the vendor data semantic

### Transformations by classes derived from `MarketData`

- Classes derived from `MarketData` do whatever they need to do in `_get_data()`
  to get the data, but always pass back data that:
  - Is indexed with a progressive index
  - Has `asset`, `start_time`, `end_time`, `knowledge_time`
  - `start_time`, `end_time`, `knowledge_time` are timezone aware

- E.g.,
  ```
    asset_id                 start_time                   end_time     close   volume
  idx
  0    17085  2021-07-26 13:41:00+00:00  2021-07-26 13:42:00+00:00  148.8600   400176
  1    17085  2021-07-26 13:30:00+00:00  2021-07-26 13:31:00+00:00  148.5300  1407725
  2    17085  2021-07-26 13:31:00+00:00  2021-07-26 13:32:00+00:00  148.0999   473869
  ```

### Transformations by abstract class `MarketData`

- The transformations are done inside `get_data_for_interval()`, during
  normalization, and are:
  - Indexing by `end_time`
  - Converting `end_time`, `start_time`, `knowledge_time` to the desired
    timezone
  - Sorting by `end_time` and `asset_id`
  - Applying column remaps

### Output format of `MarketData`

- The abstract base class `MarketData` normalizes the data by:
  - Sorting by the columns that correspond to `end_time` and `asset_id`
  - Indexing by the column that corresponds to `end_time`, so that it is
    suitable to DataFlow computation

- E.g.,
  ```
                            asset_id                start_time    close   volume
  end_time
  2021-07-20 09:31:00-04:00    17085 2021-07-20 09:30:00-04:00  143.990  1524506
  2021-07-20 09:32:00-04:00    17085 2021-07-20 09:31:00-04:00  143.310   586654
  2021-07-20 09:33:00-04:00    17085 2021-07-20 09:32:00-04:00  143.535   667639
  ```

## Asset ids format

### `ImClient` asset ids

- `ImClient` uses assets encoded as `full_symbols` strings
  - E.g., `binance::BTC_UTC`
- There is a vendor-specific mapping:
  - From `full_symbols` to corresponding data
  - From `asset_ids` (ints) to `full_symbols` (strings)
- If the `asset_ids` -> `full_symbols` mapping is provided by the vendor, then
  we reuse it
- Otherwise, we build a mapping hashing `full_symbols` strings into numbers

### `MarketData` asset ids

- `MarketData` and everything downstream uses `asset_ids` that are encoded as
  ints
  - This is because we want to use ints and not strings in dataframe

### Handling of `asset_ids`

- Different implementations of `ImClient` backing a `MarketData` are possible,
  e.g.:

- The caller needs to specify the requested `asset_ids`
- In this case the universe is provided by `MarketData` when calling the data
  access methods
- The reading backend is initialized with the desired universe of assets and
  then `MarketData` just uses or subsets that universe

- For these reasons, assets are selected at 3 different points:

  1. `MarketData` allows to specify or subset the assets through `asset_ids`
     through the constructor
  2. `ImClient` backends specify the assets returned
  - E.g., a concrete implementation backed by a DB can stream the data for its
    entire available universe

  3. Certain class methods allow querying data for a specific asset or subset of
     assets

- For each stage, a value of `None` means no filtering

## Data

### Handling of filtering by time

- Clients of `MarketData` might want to query data by:
- Using different interval types, namely `[a, b), [a, b], (a, b], (a, b)`
- Filtering on either the `start_ts` or `end_ts`
- For this reason, this class supports all these different ways of providing
  data
- `ImClient` has a fixed semantic of the interval `\[a, b\]`
- `MarketData` adapts the fixed semantic to multiple ones

### Handling timezone

- `ImClient` always uses UTC as output
- `MarketData` adapts UTC to the desired timezone, as requested by the client
