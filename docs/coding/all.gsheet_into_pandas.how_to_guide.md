# Gsheet Into Pandas

<!-- toc -->

- [Connecting Google Sheets to Pandas](#connecting-google-sheets-to-pandas)
  * [Installing libraries](#installing-libraries)
  * [Check installation](#check-installation)
  * [Authentication](#authentication)
    + [In short](#in-short)
- [Testing gspread-pandas](#testing-gspread-pandas)

<!-- tocstop -->

## Connecting Google Sheets to Pandas

- There are two layers of the API
  - [gspread](https://docs.gspread.org/)
    - This allows to connect to Google Sheets API
  - [gspread-pandas](https://gspread-pandas.readthedocs.io)
    - This allows to interact with Google Sheets through Pandas DataFrames,
      using `gspread`

### Installing libraries

- The library should be automatically installed in the Dev container

- If not, you can install it in the notebook with
  ```bash
  notebook> !pip install gspread-pandas
  ```
- Or in the Docker container with:

  ```bash
  docker> sudo /bin/bash -c "(source /venv/bin/activate; pip install gspread)"
  ```

### Check installation

- To check that the library is installed
  - In a notebook

    ```bash
    import gspread
    print(gspread.__version__)
    5.11.3

    import gspread_pandas
    print(gspread_pandas.__version__)
    3.2.3
    ```
  - In the dev container
    ```bash
    docker> python -c "import gspread; print(gspread.__version__)"
    5.10.0
    ```

### Authentication

- It's best to access Google API using a "Service Account", which is used for a
  bots
- Since `gspread-pandas` leverages `gspread`, you can follow the instructions
  for gspread https://docs.gspread.org/en/v6.0.0/oauth2.html

- There are two ways to authenticate
  - OAuth Client ID
  - Service account key (preferred)

- More details are in
  - `gspread`: https://docs.gspread.org/en/latest/oauth2.html
  - `gspread-pandas`:
    https://gspread-pandas.readthedocs.io/en/latest/configuration.html

#### In short

- Go to Google Developers Console and create a new project or select one you
  already have
  - E.g., name "gp-gspread", and ID "gp-gspread-426713"
- Search for "Google Drive API" and click on Enable API
- Search for "Google Sheets API" and click on Enable API
- Go to Credentials
  - Create credentials -> Service account key
- Service account details
  - Service account name: gspread
  - Service account ID: gspread
  - Email address: gspread@gp-gspread-426713.iam.gserviceaccount.com
  - Role: owner
- Click on `gspread`
  - Keys -> Create new key -> JSON
- A file is downloaded

  > more ~/Downloads/gspread-gp-94afb83adb02.json
  ```
  {
    "type": "service_account",
    "project_id": "gspread-gp",
    "private_key_id": "94afb...5258ac",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvg...FtmcXiHuZ46EMouxnQCEqrT5\n-----END PRIVATE KEY-----\n",
    "client_email": "gp-gspread@gspread-gp.iam.gserviceaccount.com",
    "client_id": "101087234904396404157",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/gp-gspread%40gspread-gp.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
  }
  ```

- Move the key in the right place
  ```
  > mv ~/Downloads/gspread-gp-94afb83adb02.json ~/.config/gspread_pandas/google_secret.json
  ```

- Check that the key is visible In the Docker container
  ```
  > user_501@d533075e6ade:/app$ more ~/.config/gspread_pandas/google_secret.json
  ```

- Go to your spreadsheet and share it with a client_email from the step above.
  If you don’t do this, you’ll get a `gspread.exceptions.SpreadsheetNotFound`
  exception when trying to access this spreadsheet from your application or a
  script.

## Testing gspread-pandas

- The notebook with the usage example is located at
  `amp/core/notebooks/gsheet_into_pandas_example.ipynb`.

- **Don't feel stupid if you need multiple iterations to get this stuff
  working**
  - Clicking on GUI is always a recipe for low productivity
  - Go command line and vim!
