#!/usr/bin/env python
"""
Test script to download data from Athena queries
and upload to Google Sheets.

In order to run this script, you need to have the following:
- make custom container with the following packages:
  - gspread_pandas
  - google-auth
  - google-auth-httplib2
  - google-api-python-client
- create a service account in Google Cloud Platform and download the JSON file
- put the JSON file to `infra/scripts/aws/vlady_google_service_account.json`

"""

# TODO(Vlad): temporary comment out the code below till we don't have the
#            google library in the container.

# import time
# import logging

# from botocore.client import BaseClient
# import gspread_pandas
# import pandas as pd

# import helpers.haws as haws

# _LOG = logging.getLogger(__name__)

# AWS_PROFILE = "ck"
# SERVICE_NAME = "athena"
# BASE_QUERY = (
#     """
#     select t.currency_pair,
#     from_unixtime(min("t"."timestamp")/1000) as "first_timestamp",
#     from_unixtime(max("t"."timestamp")/1000) as "last_timestamp"
#     from "{database}"."{table}" as t
#     group by t.currency_pair;    
#     """
# )

# OUTPUT_LOCATION = "s3://cryptokaizen-data-test/athena_results/"
# MAX_EXECUTION_TIMES = 20
# SLEEP_TIME = 10
# ACCOUNT_FOLDER = "infra/scripts/aws"
# ACCOUNT_FILE = "vlady_google_service_account.json"
# SPREADSHEET_ID = '1gPhLTO7zlHOq2_1P_OM2wbAc0SK-hjoZd3drdR2sKpI'

# def populate_data_to_sheet(
#         client: BaseClient, row: pd.Series
#     ) -> None:
#     """
#     Populate Google Sheets with the results of the queries.

#     :param client: Athena client
#     :param row: row from the legend sheet
#     """
#     query = BASE_QUERY.format(
#         database=row["Database"],
#         table=row["Table"]
#     )
#     # Execute the query
#     _LOG.info("Executing query: %s", query)
#     query_start = client.start_query_execution(
#         QueryString=query,
#         ResultConfiguration = {'OutputLocation': OUTPUT_LOCATION}
#     )    
#     # Check if the query has succeeded
#     if has_query_succeeded(client, query_start['QueryExecutionId']):
#         # Get the results
#         results = client.get_query_results(
#             QueryExecutionId=query_start['QueryExecutionId']
#         )
#         # Get the columns
#         columns = [
#             field["VarCharValue"]
#             for field in results["ResultSet"]["Rows"][0]["Data"]
#         ]
#         # Get the rows
#         rows = [fields for fields in results["ResultSet"]["Rows"][1:]]
#         rows = [
#             [field["VarCharValue"] for field in row["Data"]]
#             for row in rows
#         ]
#         # Create a dataframe from the rows and columns
#         df = pd.DataFrame(rows, columns=columns)
#         # Get and clear the sheet
#         spread.open_sheet(row["Label"], create=True)
#         spread.clear_sheet()
#         # Populate the sheet
#         spread.df_to_sheet(df)    

# def populate_sheets_from_legend(
#         client: BaseClient,
#         spread: gspread_pandas.spread.Spread) -> None:
#     """
#     Populate Google Sheets with the results of the queries.

#     :param client: Athena client
#     :spread: gspread_pandas.Spread object
#     """
#     legend_data = spread.sheet_to_df(sheet="legend", index=False)
#     legend_data.apply(
#         lambda row: populate_data_to_sheet(client, row),
#         axis=1
#     )

# def has_query_succeeded(client: BaseClient, execution_id: str) -> bool:
#     """
#     Check if the Athena query has succeeded.

#     :param client: Athena client
#     :param execution_id: query execution id
#     """
#     current_state = "RUNNING"
#     current_execution = MAX_EXECUTION_TIMES
#     while current_execution > 0 and current_state in ["RUNNING", "QUEUED"]:
#         current_execution -= 1
#         response = client.get_query_execution(QueryExecutionId=execution_id)
#         if (
#             "QueryExecution" in response
#             and "Status" in response["QueryExecution"]
#             and "State" in response["QueryExecution"]["Status"]
#         ):
#             current_state = response["QueryExecution"]["Status"]["State"]
#             if current_state == "SUCCEEDED":
#                 return True
#         time.sleep(SLEEP_TIME)
#     _LOG.error("Query did not succeed, current state: %s", current_state)
#     _LOG.error(
#         "The reason for the failure: %s",
#         response["QueryExecution"]["Status"]["StateChangeReason"]
#     )
#     return False

# if __name__ == "__main__":
#     # Get the Athena client
#     client = haws.get_service_client(AWS_PROFILE, SERVICE_NAME)
#     # Get the Google Sheets client
#     config = gspread_pandas.conf.get_config(ACCOUNT_FOLDER, ACCOUNT_FILE)
#     spread = gspread_pandas.Spread(spread=SPREADSHEET_ID, config=config)
#     # Populate the sheets
#     populate_sheets_from_legend(client, spread)
