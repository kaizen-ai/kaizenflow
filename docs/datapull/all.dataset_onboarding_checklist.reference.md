

<!-- toc -->

- [Dataset onboarding checklist](#dataset-onboarding-checklist)
  * [Preparation and exploratory analysis](#preparation-and-exploratory-analysis)
  * [Implement historical downloader](#implement-historical-downloader)
  * [Automated AKA Scheduled downloader](#automated-aka-scheduled-downloader)
  * [Quality Assurance](#quality-assurance)
    + [1. Check for Existing QA DAGs](#1-check-for-existing-qa-dags)
    + [2. Create a New QA DAG (if necessary)](#2-create-a-new-qa-dag-if-necessary)
      - [2.1. Create and Test QA Notebook](#21-create-and-test-qa-notebook)
      - [2.2. Run QA Notebook via Invoke Command](#22-run-qa-notebook-via-invoke-command)
    + [2.3. Create a New DAG File](#23-create-a-new-dag-file)

<!-- tocstop -->

# Dataset onboarding checklist

We follow a standard flow when onboarding a new dataset.

Create a GH issue and paste the following checklist in the issue specification
whenever a request for a new dataset is made. The structure is pre-formatted as
a markdown checklist.

## Preparation and exploratory analysis

From `docs/datapull/all.dataset_onboarding_checklist.reference.md`

- [ ] Decide on the timeline
  - E.g., is this a high-priority dataset or a nice-to-have?
- [ ] Decide on the course of action
  - E.g., do we download only historical bulk data and/or also prepare a
    real-time downloader?
- [ ] Review existing code
  - Is there any downloader that is similar to the new one, in terms of
    interface, frequency, etc.?
  - What code already existing can be generalized to accomplish the task at
    hand?
  - What needs to be implemented from scratch?
- [ ] Create an exploratory notebook that includes:
  - Description of the data type, if this is the first time downloading a
    certain data type
  - Example code to obtain a snippet of historical/real-time data
  - If we are interested in historical data, e.g.,
    - How far in the past we need the data to be?
    - How far in the past the data source goes?
- [ ] Create example code to obtain data in realtime
  - Is there any issue with the realtime data?
    - E.g., throttling, issues with APIs, unreliability
- [ ] Perform initial QA on the data sample, e.g.,
  - Compute some statistics in terms of missing data, outliers
  - Does real-time and historical data match at first sight in terms of schema
    and content

## Implement historical downloader

- [ ] Decide what's the name of the data set according to `dataset_schema`
      conventions
- [ ] Implement the code to perform the historical downloader
  - TODO(Juraj): Add a pointer to examples and docs
- [ ] Test the flow to download a snippet of data locally in the test stage
  - Apply QA to confirm data is being downloaded correctly
- [ ] Perform a bulk download for historical datasets
  - Manually, i.e., via executing a script, if the history is short or the
    volume of data is low
  - Via an Airflow DAG if the volume of the data is too large for downloading
    manually
    - E.g.,
      `im_v2/airflow/dags/test.download_bulk_data_fargate_example_guide.py`

## Automated AKA Scheduled downloader

- [ ] Setup automatic download of data in pre-production:
  - Since pre-prod runs with code from the master branch (updated twice a day
    automatically), make sure to merge any PRs related to the dataset onboarding
    first
  - For historical datasets:
    - To provide a single S3 location to access the entire dataset, move the
      bulk history from the test bucket to the pre-prod bucket (source and
      destination path should be identical)
    - Add a daily download Airflow task to get data from a previous day and
      append it to the existing bulk dataset
  - For real-time datasets:
    - Add a real-time download Airflow task to get data continuously 24/7

- [ ] For some real-time datasets, an archival flow needs to be added in order
      not to overwhelm the storage
  - Consult with the team leader if it's needed for a particular dataset
  - Example Airflow DAG is
    [preprod.europe.postgres_data_archival_to_s3.py](/im_v2/airflow/dags/datapull/preprod.europe.postgres_data_archival_to_s3.py)

- [ ] Add an entry into the
  - [Monster dataset matrix](https://docs.google.com/spreadsheets/d/13Vyrxs9Eg-C6y91XIogLHi4A1_AFK7_KCF2KEnnxYv0)
- [ ] Once the download is enabled in production, update the
      [Master_raw_data_gallery](https://github.com/cryptokaizen/cmamp/blob/master/im_v2/common/notebooks/Master_raw_data_gallery.ipynb)

## Quality Assurance

### 1. Check for Existing QA DAGs

- [ ] **Verify if there is already a similar QA DAG running.**
  - [ ] Check for existing QA DAGs (e.g., bid_ask/OHLCV, Cross QA for OHLCV
        comparing real-time with historical data).
  - [ ] Action: If the new QA is just a change in the universe or vendor, append
        a new task to the existing running DAGs. Reference:
        [Link to Relevant Section](https://github.com/cryptokaizen/cmamp/blob/6f6feec46704c96b9929fb174e6d66f7e94e6776/docs/datapull/ck.create_airflow_dag.tutorial.md?plain=1#L219)].

### 2. Create a New QA DAG (if necessary)

#### 2.1. Create and Test QA Notebook

- [ ] **Develop a notebook to test the QA process.**
  - [ ] Test over a small period to ensure it functions as expected.
  - [ ] Tip: Use a small dataset or limited time frame for quick testing.

#### 2.2. Run QA Notebook via Invoke Command

- [ ] **Execute the QA notebook using the invoke command to validate
      functionality.**
  - [ ] Example:
        [Invoke Command Example](https://github.com/cryptokaizen/cmamp/blob/6f6feec46704c96b9929fb174e6d66f7e94e6776/dev_scripts/lib_tasks_data_qa.py#L266)

### 2.3. Create a New DAG File

- [ ] **Create a new DAG file after QA process validation.**
  - [ ] Follow the standard procedure for DAG creation. Reference:
        [DAG Creation Tutorial](https://github.com/cryptokaizen/cmamp/blob/6f6feec46704c96b9929fb174e6d66f7e94e6776/docs/datapull/ck.create_airflow_dag.tutorial.md).

Last review: GP on 2024-04-20
