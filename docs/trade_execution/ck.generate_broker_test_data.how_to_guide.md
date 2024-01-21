

<!-- toc -->

- [How-to guide description](#how-to-guide-description)
- [Why and when generate real-life test data](#why-and-when-generate-real-life-test-data)
- [Data operations](#data-operations)
  * [How to get a new version of real-life test data](#how-to-get-a-new-version-of-real-life-test-data)
  * [How to copy real-life test data into the S3 bucket](#how-to-copy-real-life-test-data-into-the-s3-bucket)
  * [How copy data from S3 bucket to the local temporal dir during the test](#how-copy-data-from-s3-bucket-to-the-local-temporal-dir-during-the-test)
- [Naming Convention for Test Data Directories on S3](#naming-convention-for-test-data-directories-on-s3)
- [Additional data required for tests](#additional-data-required-for-tests)

<!-- tocstop -->

# How-to guide description

This guide contains instructions on generating real-life test data for testing
Broker.

# Why and when generate real-life test data

- Real-life data should be included in tests for any kind of manipulation of the
  `CcxtLogger` data, e.g. analysis since it often contains a level of complexity
  and variability that toy examples do not capture.
- The example of structure of real-life CCXT broker logs can be found in
  [`docs/trade_execution/ck.ccxt_broker_logs_schema.reference.md`](https://github.com/cryptokaizen/cmamp/blob/master/docs/trade_execution/ck.ccxt_broker_logs_schema.reference.md).
  Also expected files can be found in the code for `CcxtLogger`.
- Test files must be regenerated when new logging features are introduced,
  either as new files or as new fields in existing files. However, changes in
  calculations involving these files typically do not require regenerating the
  test directory.

# Data operations

## How to get a new version of real-life test data

1. Run a short experiment for 2 or 3 bars, instruction can be found in
   [`docs/trade_execution/ck.run_broker_only_experiment.how_to_guide.md`](https://github.com/cryptokaizen/cmamp/blob/master/docs/trade_execution/ck.run_broker_only_experiment.how_to_guide.md)
2. Take an already existing experiment which already contains the new feature.
   The logs from experiments are stored at
   `shared_data/ecs/test/system_reconciliation/`. The depth of the directory we
   copy depends on the functionality we want te test.

## How to copy real-life test data into the S3 bucket

```bash
aws s3 cp ecs/test/system_reconciliation/C5b/prod/20231124_093000.20231124_102500/system_log_dir.manual/process_forecasts/ s3://cryptokaizen-unit-test/outcomes/TestReplayedCcxtExchange3.test_submit_twap_orders1/input/ --recursive
```

## How copy data from S3 bucket to the local temporal dir during the test

```python
def test1(self) -> None:
    # Load the data from S3.
    use_only_test_class = False
    s3_input_dir = self.get_s3_input_dir(
        use_only_test_class=use_only_test_class
    )
    scratch_dir = self.get_scratch_space()
    aws_profile = "ck"
    hs3.copy_data_from_s3_to_local_dir(s3_input_dir, scratch_dir, aws_profile)
    ccxt_log_reader = obcccclo.CcxtLogger(scratch_dir)
    ....
```

# Naming Convention for Test Data Directories on S3

When naming a directory for test data on S3, the structure depends on the scope
of data usage:

1. **Class-Level Data Usage**:
   - If the same data copy is intended for use across all tests within a single
     class, the directory should be named using only the class name.
   - Example:
     `s3://cryptokaizen-unit-test/outcomes/Test_convert_bar_fills_to_portfolio_df/input/`

2. **Test-Level Data Usage**:
   - If different data copies are needed for each test within a class, include
     both the class name and the specific test name in the directory naming.
   - Example:
     `s3://cryptokaizen-unit-test/outcomes/Test_C8b_ProdReconciliation.test_run_short_reconciliation/input/`

# Additional data required for tests

- `test_ccxt_execution_quality.py`
  - To test
    `oms.broker.ccxt.ccxt_execution_quality.convert_bar_fills_to_portfolio_df`
    function we need bar reference price dataframe, which is stored in database.
  - To update this data, execute the `Master_execution_analysis.ipynb` notebook
    using the log directory from the new experiment.
  - Locate the cell where `convert_bar_fills_to_portfolio_df` is used and save
    the second argument as a CSV file.
  - Move the generated CSV file to
    `oms/broker/ccxt/test/mock/bar_reference_price.csv`.
  - Commit the change to ensure the test environment is updated with the new
    data.
