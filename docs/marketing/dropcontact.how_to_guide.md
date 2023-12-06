# How to extract data from Signal

* An example notebook is at `marketing/notebooks/SorrTask606_Get_email_from_dropcontact.ipynb`. It contains a full process for adding extracted data into an existing Google Sheet.

## Dataflow

* Input: Three sequences representing `First Name`, `Last Name`, `Company Name`; The API key for dropcontact.
* Output: An dataframe containing all data available from DropContact. Empty string is set for data not present.

## Usage

* This module gives a single function for easier use of DropContact API.
* It takes three Sequence of data (can be python list, pandas column, etc.) and an API key for dropcontact. It will take about 1 minute to process per 50 investors.
* In any notebook, run the following script to get a pandas dataframe representing the data returned from DropContact:
    ```python
    import marketing.dropcontact as mrkdrop

    API_KEY = <DropContact_API_KEY>
    first_names = <First Name Sequence>
    last_names = <Last Name Sequence>
    company_names = <Company Name Sequence>
    dropcontact_dataframe = mrkdrop.get_email_from_dropcontact(first_names, last_names, company_names, API_KEY)
    ```