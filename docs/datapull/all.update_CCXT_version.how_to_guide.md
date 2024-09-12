# Update Ccxt Version

<!-- toc -->

- [Testing CCXT stability before docker container update](#testing-ccxt-stability-before-docker-container-update)
  * [Steps for performing CCXT API tests:](#steps-for-performing-ccxt-api-tests)
  * [Failure handling](#failure-handling)
- [Read CCXT exchange timestamp interpretation](#read-ccxt-exchange-timestamp-interpretation)
  * [Steps to confirm timestamp representation](#steps-to-confirm-timestamp-representation)

<!-- tocstop -->

## Testing CCXT stability before docker container update

In order to ensure the stability of our code following a CCXT update, a thorough
testing process is required. Prior to constructing a new container, we will
update the CCXT version locally and execute tests on the actual API to verify
the reliability of our codebase.

### Steps for performing CCXT API tests:

1. Update CCXT version locally in the container using the following command:

   ```bash
   docker> sudo /venv/bin/pip install ccxt --upgrade
   ```

2. Open the file `im_v2/test/test_ccxt.py` and comment the following code
   snippet:

   ```python
   @pytest.mark.skip(
       "Cannot be run from the US due to 451 error API error. Run manually."
   )
   ```

3. Run the test by executing the following code snippet in the terminal:

   ```bash
   docker> pytest im_v2/test/test_ccxt.py
   ```

4. Run the following commands locally with the new version installed and file a
   PR with fixes to any breaks that may appear
   - The PR will be merged directly after the new image release to minimize time
     when build is broken.

   ```bash
   > i run_fast_tests
   > i run_slow_tests
   > i run_superslow_tests
   ```

5. Verify that all test results are marked as "green" before proceeding with the
   update of the Docker container.

### Failure handling

In the event that any test fails to pass successfully, an issue should be filed.
The issue report must include details regarding the failure, allowing for an
accurate diagnosis of the problem.

## Read CCXT exchange timestamp interpretation

Read
[CCXT Exchange Timestamp Interpretation](amp/docs/datapull/ck.ccxt_exchange_timestamp_interpretation.reference.md)

### Steps to confirm timestamp representation

In order to ensure accurate and up-to-date information regarding the
interpretation of timestamps in the CCXT exchange library, follow these detailed
steps:

1. **Examine the Library Code:**
   - Thoroughly review the library code to confirm that the timestamp
     representation remains unchanged. Note that code refactoring might have
     occurred, but the semantics should remain consistent.
     - Update links to code references for precise navigation of all exchanges.

2. **Report Significant Changes:**
   - Identify and report any significant changes in the timestamp
     interpretation. For example, if the timestamp initially represented the
     start of the interval and has now been updated to represent the end of the
     interval, document this alteration.

3. **Update "As of Version" Information:**
   - Ensure that the "as of version" information is updated to reflect the
     version of the CCXT exchange library that is about to be used.

Last review: GP on 2024-04-20
