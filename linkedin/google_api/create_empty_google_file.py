#!/usr/bin/env python
import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Scopes required to make both API calls -> https://developers.google.com/docs/api/reference/rest/v1/documents/create
SCOPES = ["https://www.googleapis.com/auth/drive"]


# #############################################################################
def _create_empty_google_file(file_type:str, title: str, user: str) -> None:
    """
    Create an empty google file and share it to a user
    :return: None
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'client_secrets.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    try:
        service = build(file_type, 'v4', credentials=creds)
        spreadsheet = {
            'properties': {
                'title': title
            }
        }
        spreadsheet = service.spreadsheets().create(body=spreadsheet,
                                                    fields='spreadsheetId').execute()
        # Create a new document with a determined name. The contents of the document can be set using additional API calls.
        spreadsheet_id = spreadsheet.get('spreadsheetId')
        print('The file id of the new google sheet is: {}'.format(spreadsheet_id))
        service = build('drive', 'v3', credentials=creds)
        # Create the permission.
        parameters = {
            'role': 'reader',
            'type': 'user',
            'emailAddress': user
        }
        new_permission = service.permissions().create(fileId=spreadsheet_id, body=parameters).execute()
        print('The new permission id of the document is: {}'.format(new_permission.get('id')))
    except HttpError as err:
        print(err)

# #############################################################################
if __name__ == '__main__':
    _create_empty_google_file('sheets', 'new_gsheet', 'im.yiyun.lei@gmail.com')