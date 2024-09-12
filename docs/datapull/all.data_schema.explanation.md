# Data Schema

<!-- toc -->

- [Data schema](#data-schema)
  * [Dataset schema](#dataset-schema)
  * [Description of fields](#description-of-fields)
- [Data signature validation](#data-signature-validation)
- [Code](#code)

<!-- tocstop -->

## Data schema

- The `dataset_schema` is a structured representation of metadata attributes
  used to describe a dataset
  - It comprises several fields separated by a dot `.`
  - Each field provides specific information about the dataset, such as the mode
    of download, the entity responsible for downloading, the format of the data,
    the type of data, the asset type, the vendor, the exchange ID, and the
    version of the dataset.

- This structured representation facilitates easy understanding and organization
  of dataset metadata, enabling efficient data management and analysis.

### Dataset schema

- The data schema signature has the following schema
  ```
  {download_mode}.{downloading_entity}.{action_tag}.{data_format}.{data_type}.{asset_type}.{universe}.{vendor}.{exchange_id}.{version\[-snapshot\]}.{extension}
  ```

- Examples of dataset names are in
  [/im_v2/common/notebooks/Master_raw_data_gallery.ipynb](/im_v2/common/notebooks/Master_raw_data_gallery.ipynb)
  - `realtime.airflow.resampled_1min.postgres.bid_ask.futures.v8.ccxt.binance.v1_0_0`
  - `periodic_daily.airflow.archived_200ms.postgres.bid_ask.spot.v7.ccxt.binance.v1_0_0`
  - `realtime.airflow.downloaded_200ms.postgres.bid_ask.futures.v7_4.ccxt.cryptocom.v1_0_0`

### Description of fields

- `download_mode`: Indicates the mode in which the dataset was downloaded.
  - E.g., `bulk`, `realtime` and `periodic_daily`

- `downloading_entity`: Specifies the entity responsible for downloading the
  dataset
  - E.g., `airflow` or `manual`.

- `action_tag`: Describes the action performed on the dataset
  - E.g., `downloaded_all`, `resampled_1min` and `archived_200ms`.

- `data_format` : Indicates the format of the dataset
  - E.g., `csv`, `parquet` or `postgres`.

- `data_type`: Specifies the type of data contained in the dataset
  - E.g., `ohlcv` (Open, High, Low, Close, Volume), `bid_ask`, `trades`

- `asset_type`: Describes the type of assets included in the dataset
  - E.g., `futures` or `spot`.

- `universe`: version of the universe to be used
  - E.g., `v7.4`

- `vendor`: Specifies the vendor from which the dataset originates
  - E.g., `ccxt`

- `exchange_id`: Indicates the ID of the exchange associated with the dataset.
  - E.g., `binance`, `okx` or `kraken`

- `version`: Denotes the version of the dataset
  - E.g., `v1_0_0`

## Data signature validation

Perform syntactic and semantic validation of a specified dataset signature.
Signature is validated by the latest dataset schema version.

1. **Syntax validation:** checks if the signature is not malformed.
   - If the schema specifies dataset signature as `{data_type}.{asset_type}`,
     then `ohlcv.futures` is a valid signature, but `ohlcv-futures` is not.

2. **Semantic validation:** checks if the signature tokens are correct.
   - If the schema specifies allowed values for
     `data_type = ["ohlcv", "bid_ask"]`, then for dataset signature
     `{data_type}.{asset_type}` `ohlcv.futures` is a valid signature, but
     `bidask.futures` is not.

## Code

- The code corresponding to parsing and validating is under `//data_schema/`
  ```
  > tree.sh -p data_schema
  data_schema/
  |-- dataset_schema_versions/
  |   `-- dataset_schema_v3.json
    Description of the current schema
  |-- test/
  |   |-- __init__.py
  |   `-- test_dataset_schema_utils.py
  |-- __init__.py
  |-- changelog.txt
    Changelog for dataset schema updates
  |-- dataset_schema_utils.py
    Utilities to parse schema
  `-- validate_dataset_signature.py*
    Script to test a schema
  ```

Last review: GP on 2024-05-14
