# Data processing pipeline
- We use a layered approach to split the complexity / responsibilities of
  processing data from original sources to DataFlow nodes in multiple stages

- Empirically it seems that 2 layers are enough
- In brief:
    - `ImClient` adapts from the vendor to a standard format
    - `MarketData` adds "behaviors" that are orthogonal to vendors, e.g.,
        - RealTime or Historical
        - Stitched
          - = "overlap" multiple data sources giving a single view of the data
          - E.g., the data from the last day comes from a RT source and the data
            before that comes from an historical source
      - Replayed
          - = serialize the data to disk and read it back, implementing also
            knowledge time as-of semantic
          - This behavior is orthogonal to RealTime, Historical, Stitched, i.e.,
            one can replay any MarketData, including an already replayed one

# Data format for `ImClient` / `MarketData` pipeline

- Both `ImClient` and `MarketData` have an output format that is enforced by the
  base and the derived classes together
- `ImClient` and `MarketData` have 3 interfaces each:
    1) an external "input" format for a `MarketData` / `ImClient` class
        - format of the data as input to a class derived from `MarketData` /
          `ImClient`
    2) an internal "input" format of `MarketData` / `ImClient`
        - format that derived classes need to follow so that the corresponding base
          class can do its job, applying common transformations to all `MarketData`
          / `ImClient` classes
    3) an external "output" format
        - format of the data outputted by any derived class from `MarketData` /
          `ImClient`

- The chain of transformations is:
    - Class derived from `ImClient`
      - The transformations are vendor-specific
    - `ImClient`
      - This is fixed
    - Class derived from `MarketData`
      - The transformations are specific to the `MarketData` concrete class
    - `MarketData`

## Transformations by classes derived from `ImClient`
- Whatever is needed to transform the vendor data into the internal format accepted
  by base `MarketData`
- Only the vendor-specific part of `ImClient`
- 
    - normalized so that the index is a UTC timestamp (index is called `timestamp`)
    - resampled on a 1 min grid and filled with NaN values
    - sorted by index and `full_symbol`
    - guaranteed to have no duplicates
    - considered to belong to intervals like [a, b]

- Transformations
Classes derived from `MarketData`

# Asset ids format
- Everything after MarketData uses ints
- Everything before MarketData uses strings and there is a vendor-specific mapping

# Handling of `asset_ids`
- Different implementations of `ImClient` backing a `MarketData` are possible,
  e.g.:
  - stateless
    - = the caller needs to specify the requested `asset_ids`
    - In this case the universe is provided by `MarketData` when calling the
      data access methods
  - stateful
    - = the backend is initialized with the desired universe of assets and
      then `MarketData` just propagates or subsets the universe

- For these reasons, assets are selected at 3 different points:
    1) `MarketData` allows to specify or subset the assets through
        `asset_ids` through the constructor
    2) `ImClient` backends specify the assets returned
       - E.g., a concrete implementation backed by a DB can stream the data for
         its entire available universe
    3) Certain class methods allow to query data for a specific asset or subset
       of assets
    - For each stage, a value of `None` means no filtering

# Handling of filtering by time
- Clients of `MarketData` might want to query data by:
    - using different interval types, namely [a, b), [a, b], (a, b], (a, b)
    - filtering on either the `start_ts` or `end_ts`
- For this reason, this class supports all these different ways of providing
  data

- IM Client has a fixed semantic of the interval (I believe [a, b])
- MarketData adapts

# Handling timezone

- you specify what timezone you want in MarketData and MarketData guarantees 
  that all timestamps are in the right TZ
