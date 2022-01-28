# Data format for `ImClient` / `MarketData` pipeline

- Both `ImClient` and `MarketData` have an output format that is enforced together
  by the base and the derived classes
  - In this sense, `ImClient` and `MarketData` have 3 interfaces each:
    1) an external "input" format for a `MarketData` / `ImClient` class
    2) an internal "input" format of `MarketData` / `ImClient`
        - is what the derived classes need to follow so that the corresponding base
          class can do its job
    3) an external "output" format

- Transformations performed by classes derived from `ImClient`
    - Whatever is needed to transform the vendor data into the internal format
      accepted by base `MarketData`

- Transformations
Classes derived from `MarketData`


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
    1) `AbstractMarketData` allows to specify or subset the assets through
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