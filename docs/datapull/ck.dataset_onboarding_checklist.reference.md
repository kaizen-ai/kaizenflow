

<!-- toc -->

- [Dataset onboarding checklist](#dataset-onboarding-checklist)

<!-- tocstop -->

# Dataset onboarding checklist

We follow a standard flow when on-boarding a new dataset.

Create a GH issue with the following checklist whenever a request for a new
dataset is made (The structure is pre-formatted as a markdown checklist).

1. [ ] Decide on the timeline
   - E.g., is this a high priority dataset or a nice-to-have?
2. [ ] Decide on the course of action
   - E.g., do we download only historical bulk data or also prepare a
     real-time downloader?
3. [ ] Review existing code
   - E.g., what can be generalized to accomplish the task at hand and what
     needs to be implemented from scratch?
4. [ ] Create an exploratory notebook which includes:
   - Description of the data type (if this is the first time downloading a
     certain data type)
   - Example code to obtain a snippet of historical data
   - Establish how far in the past do we need the data to be
   - Establish how far in the past the data source actually goes
5. [ ] Example code to obtain data in realtime (if needed)
6. [ ] Perform initial QA on the data sample
   - E.g., compute some statistics in terms of missing data, outliers
7. [ ] If a QA flow for a similar data type exists, evaluate if it is sufficient
       and, in that case, re-use.
8. [ ] Decide what's the name of the data set according to `dataset_schema/`
       conventions
9. [ ] Perform a bulk download
       - E.g., via a script, or Airflow DAG if the volume of the data is too
         large for one-shot downloading
10. [ ] If needed, add the dataset into `periodic_daily` download flow by
        choosing one of the following options:
    - Creating a new DAG
    - Extending existing DAG to include a new task (preferred, if no large
      modifications are needed)
11. [ ] Add an entry into the
    [Monster dataset matrix](https://docs.google.com/spreadsheets/d/1aN2TBTtDqX5itnlG70lS2otkKCHKPN2yE_Hu3JPhPVo/edit#gid=1908921737)
12. Once the download is enabled in production, update the
    [Master_raw_data_gallery](https://github.com/cryptokaizen/cmamp/blob/master/im_v2/common/notebooks/Master_raw_data_gallery.ipynb)
13. [ ] Add a QA flow executed using a Jupyter notebook
    - Individual QA steps are wrapped inside `QaCheck` interface so they can be
      reused
14. [ ] Schedule the QA flow to Airflow by choosing one of the following options:
    - Creating a new DAG
    - Extending existing DAG to include a new task (preferred, if no large
      modifications are needed)
