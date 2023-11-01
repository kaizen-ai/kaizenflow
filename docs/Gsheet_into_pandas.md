<!-- toc -->

- [Connecting Google Sheets to Pandas](#connecting-google-sheets-to-pandas)
  * [Installing gspread-pandas](#installing-gspread-pandas)
  * [Configuring gspread-pandas](#configuring-gspread-pandas)
- [Using gspread-pandas](#using-gspread-pandas)

<!-- tocstop -->

# Connecting Google Sheets to Pandas

- In order to load a google sheet into a pandas dataframe (or the other way
  around), one can use a library called `gspread-pandas`.

## Installing gspread-pandas

- The library should be automatically installed in your conda env
  - The detailed instructions on how to install the library are located here:
    [Installation/Usage](https://gspread-pandas.readthedocs.io/en/latest/getting_started.html#installation-usage).

## Configuring gspread-pandas

- You need to have a service account key that has access to the drive space for modification
  - Normally the default one `helpers/.google_credentials/service.json` would work.
  - If you need to modify a Google Drive space where the default service account does not have access to, follow the instruction [here](https://gspread-pandas.readthedocs.io/en/latest/getting_started.html#client-credentials) to get your own `your_service.json`, store it and use it as the service account key path in `hgoogle_file_api.py`.
    
  - The process is not complicated but it's not obvious since you need to click
    around in the GUI

- Some gotchas:
  - Make sure to act only under your `...` account.
  - `project/product/application` names don't really matter. It's convenient to
    pick a name connected to the project so that it's easy to find later.
  - Whenever you are given a choice of making something generally accessible or
    accessible only to the members of your organization, choose making it
    accessible only to the members of your organization.
  - When you are given a choice of time periods to create something for, choose
    the longest one.
  - When you are given a choice between `OAuth client ID` and `Service account`,
    choose `Service account`.

# Using gspread-pandas

- The notebook with the usage example is located at
  `amp/core/notebooks/gsheet_into_pandas_example.ipynb`.

- The official use documentation is provided
  [here](https://gspread-pandas.readthedocs.io/en/latest/using.html).

- **Don't feel stupid if you need multiple iterations to get this stuff
  working**
  - Clicking on GUI is always a recipe for low productivity
  - Go command line and vim!
