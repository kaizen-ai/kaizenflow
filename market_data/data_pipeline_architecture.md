# Data pipeline
- The data pipeline processes data from the vendor format into a common format that 
  can be used by DataFlow nodes

- We use a layered approach to split the complexity / responsibilities of
  processing the data

- Empirically it seems that 2 layers are enough, in brief:
    - `ImClient` adapts from the vendor data to a standard internal "MarketData"
      format
    - `MarketData` implements "behaviors" that are orthogonal to vendors, e.g.,
        - RealTime or Historical
        - Stitched
          - = "overlap" multiple data sources giving a single view of the data
          - E.g., the data from the last day comes from a real-time source while
            the data before that comes from an historical source
      - Replayed
          - = serialize the data to disk and read it back, implementing also
            knowledge time as-of-time semantic
          - This behavior is orthogonal to RealTime, Historical, Stitched, i.e.,
            one can replay any `MarketData`, including an already replayed one

# Data format for `ImClient` / `MarketData` pipeline
- Both `ImClient` and `MarketData` have an output format that is enforced by the
  base and the derived classes together
- `ImClient` and `MarketData` have 3 interfaces each:
    1) an external "input" format for a class
        - format of the data as input to a class derived from `MarketData` /
          `ImClient`
    2) an internal "input" format
        - format that derived classes need to follow so that the corresponding base
          class can do its job, i.e., apply common transformations to all
          `MarketData` / `ImClient` classes
    3) an external "output" format
        - format of the data outputted by any derived class from `MarketData` /
          `ImClient`

- The chain of transformations is:
    - Class derived from `ImClient`
      - The transformations are vendor-specific
    - `ImClient`
      - The transformations are fixed
    - Class derived from `MarketData`
      - The transformations are specific to the `MarketData` concrete class
    - `MarketData`
        - The transformations are fixed

```plantuml
[Vendor] -> [DerivedImClient]
[DerivedImClient] -> [AbstractImClient]
[AbstractImClient] -> [DerivedMarketData] 
[DerivedMarketData] -> [AbstractMarketData]
```

## Transformations performed by classes derived from `ImClient`
- Whatever is needed to transform the vendor data into the internal format accepted
  by base `ImClient`
- Only derived classes `ImClient` knows what is exact semantic of the vendor-data
 
## Transformations performed by abstract class `ImClient`
- Implemented by `ImClient._apply_im_normalization()`

## Output format of `ImClient`
- TODO(*): Check the code in `ImClient` since that might be more up to date than
  this document and, if needed, update this doc
 
- The data in output of a class derived from `ImClient` is normalized so that:
    - the index:
      - represents the knowledge time
      - is the end of the sampling interval
      - is called `timestamp`
      - is a tz-aware timestamp in UTC
    - the data:
      - is resampled on a 1 minute grid and filled with NaN values
      - is sorted by index and `full_symbol`
      - is guaranteed to have no duplicates
      - belongs to intervals like [a, b]
      - has a `full_symbol` column with a string representing the canonical name
        of the instrument

- TODO(gp): We are planning to use an `ImClient` data format closer to `MarketData`
  by using `start_time`, `end_time`, and `knowledge_time` since these can be
  inferred only from the vendor data semantic

## Transformations performed by classes derived from `MarketData`
- Classes derived from `MarketData` do whatever they need to do in `_get_data()` to
  get the data, but always pass back data that:
    - is indexed with a progressive index
    - has asset, start_time, end_time, knowledge_time
    - start_time, end_time, knowledge_time are timezone aware
- E.g.,
  ```
    asset_id                 start_time                   end_time     close   volume
  idx
    0  17085  2021-07-26 13:41:00+00:00  2021-07-26 13:42:00+00:00  148.8600   400176
    1  17085  2021-07-26 13:30:00+00:00  2021-07-26 13:31:00+00:00  148.5300  1407725
    2  17085  2021-07-26 13:31:00+00:00  2021-07-26 13:32:00+00:00  148.0999   473869
  ```

## Transformations performed by abstract class `MarketData`
- The transformations are done inside `get_data_for_interval()`, during normalization,
  and are:
  - indexing by `end_time`
  - converting `end_time`, `start_time`, `knowledge_time` to the desired timezone
  - sorting by `end_time` and `asset_id`
  - applying column remaps

## Output format of `MarketData`
- The base `MarketData` normalizes the data by:
    - sorting by the columns that correspond to `end_time` and `asset_id`
    - indexing by the column that corresponds to `end_time`, so that it is suitable
      to DataFlow computation
- E.g.,
  ```
                          asset_id                start_time    close   volume
  end_time
  2021-07-20 09:31:00-04:00  17085 2021-07-20 09:30:00-04:00  143.990  1524506
  2021-07-20 09:32:00-04:00  17085 2021-07-20 09:31:00-04:00  143.310   586654
  2021-07-20 09:33:00-04:00  17085 2021-07-20 09:32:00-04:00  143.535   667639
  ```

# Asset ids format
- `ImClient` uses assets encoded as `full_symbols` strings (e.g., `binance::BTC_UTC`)
  - There is a vendor-specific mapping:
    - from `full_symbols` to corresponding data
    - from `asset_ids` (ints) to `full_symbols` (strings)
  - If the `asset_ids` -> `full_symbols` mapping is provided by the vendor, then we
    reuse it
    - Otherwise, we build a mapping hashing `full_symbols` strings into numbers
- `MarketData` and everything downstream uses `asset_ids` that are encoded as ints
  - This is because we want to use ints and not strings in dataframe

# Handling of `asset_ids`
- Different implementations of `ImClient` backing a `MarketData` are possible,
  e.g.:
    - The caller needs to specify the requested `asset_ids`
        - In this case the universe is provided by `MarketData` when calling the
          data access methods
    - The reading backend is initialized with the desired universe of assets and
      then `MarketData` just uses or subsets that universe

- For these reasons, assets are selected at 3 different points:
    1) `MarketData` allows to specify or subset the assets through `asset_ids`
       through the constructor
    2) `ImClient` backends specify the assets returned
       - E.g., a concrete implementation backed by a DB can stream the data for
         its entire available universe
    3) Certain class methods allow querying data for a specific asset or subset
       of assets
     - For each stage, a value of `None` means no filtering

# Handling of filtering by time
- Clients of `MarketData` might want to query data by:
    - using different interval types, namely `[a, b), [a, b], (a, b], (a, b)`
    - filtering on either the `start_ts` or `end_ts`
- For this reason, this class supports all these different ways of providing
  data

- `ImClient` has a fixed semantic of the interval `[a, b]`
- `MarketData` adapts the fixed semantic to multiple ones

# Handling timezone
- `ImClient` always uses UTC as output
- `MarketData` adapts UTC to the desired timezone, as requested by the client
