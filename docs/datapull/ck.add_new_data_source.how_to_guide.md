<!--ts-->
   * [Data source analysis](#data-source-analysis)
   * [Storing the data](#storing-the-data)
   * [Implement the data adapters](#implement-the-data-adapters)
   * [Checklist](#checklist)
      * [Checklist for releasing a new data set](#checklist-for-releasing-a-new-data-set)



<!--te-->

# Data source analysis

- It's possible to have different interfaces for historical vs real-time

- Document the API for historical and real-time data source
  - Understand how to get the data

- Create a notebook with a few examples

# Storing the data

Historical data needs to be stored in Parquet format

- Use our data format for days/...

Real-time data is stored

- In a DB for real-time use
- In a Parquet (for archival)

# Implement the data adapters

Implement the classes in sorrentum_sandbox/common

- Get data
- Save

TODO(gp): Describe the classes

# Checklist

- [ ] Create an exploratory notebook for the data source interface (both
      historical and real-time)
- [ ] Copy a directory with boilerplate code
- [ ] Rename the dir after the data source
- [ ] Fill out the classes with the specific code
- Unit tests to make sure the code is flowing through (boilerplate, manual or
  superslow)
- Add it to the real-time system
  - Create an Airflow DAG historical and one for real-time
- Catch up for historical data
  - Run manually if it can be done in one shot (not too much data)
  - Run with Airflow to break it down in several executions
- Set up QA
  - Look at ./docs/datapull/ck.dataset_onboarding_checklist.reference.md
- Put QA in production with Airflow

- Write client to read the data
  - Verify carefully the timing of the data

- TODO(gp): Use Binance historical data
- TODO(gp): crypto.com
- TODO(gp): Create a mock example
  - Look at Sorrentum

## Checklist for releasing a new data set

- Decide what to do exactly (e.g., do we download only bulk data or also
  real-time?)
- Review what code we have and what can be generalized to accomplish the task at
  hand
- Decide what's the name of the data set according to our convention
- Create DAGs for Airflow
- Update the Raw Data Gallery
  im_v2/common/notebooks/Master_raw_data_gallery.ipynb
- Quick exploratory analysis to make sure the data is not malformed
- Update the table in
  [Data pipelines - Specs](https://docs.google.com/document/d/1nLhaFBSHVrexCcwJMnpXlkqwn0l6bDiVer34GKVclYY/edit#heading=h.8g5ajvlq6zks)
- Exploratory analysis for a thorough QA analysis
- Add QA system to Airflow prod
