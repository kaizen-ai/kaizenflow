#!/usr/bin/env python
import logging
import os.path
from typing import Optional, Any

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# _LOG = logging.getLogger(__name__)
# Scopes required making API calls.
SCOPES = ["https://www.googleapis.com/auth/drive"]


# #############################################################################
def create_empty_google_file(gsheet_name: str, folder_id: Optional[str], user: Optional[str]) -> None:
    """
    Create an empty Google file and share it to a user
    :return: None
    """
    #
    creds = _get_credentials()
    # 
    try:
        gsheet_id = _create_new_google_sheet(creds, gsheet_name)
        print('The file id of the new Google sheet is: {}'.format(gsheet_id))
        # Create a drive api client.
        gdrive_service = build('drive', 'v3', credentials=creds)
        # 
        if user:
            _share_google_file(gdrive_service, gsheet_id, user)
            print('Create the new google sheet {}'.format(gsheet_name))
        # Move spreadsheet to google drive shared dir.
        if folder_id:
            _move_gsheet_to_dir(gdrive_service, gsheet_id, folder_id)
            print('Move the new google sheet {} to the shared dir {}'.format(gsheet_name, folder_id))
    #
    except HttpError as err:
        print(err)

# #############################################################################
def _get_credentials() -> Credentials:
    """
    Get credentials for Google API.
    :return: Credentials
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and
    # is created automatically when the authorization flow completes for
    # the first time.
    token_path = os.path.join(os.path.dirname(__file__), "token.json")
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        client_secrets_path = os.path.join(os.path.dirname(__file__), "client_secrets.json")
        if not os.path.exists(client_secrets_path):
            raise RuntimeError("Please download client_secrets.json from Google API and put it under the same directory as google_sheets.py.")
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secrets_path, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(token_path, "w") as token:
            token.write(creds.to_json())
    return creds


def _create_new_google_document(creds: Credentials, doc_name: str, doc_type: str) -> str:
    """
    Create a new Google document (Sheet or Doc).
    :param creds: Credentials
    :param doc_name: str, the name of the new Google document.
    :param doc_type: str, the type of the Google document ('sheets' or 'docs').
    :return: doc_id
    """
    service = build(doc_type, 'v4' if doc_type == 'sheets' else 'v1', credentials=creds)
    document = {
        'properties': {
            'title': doc_name
        }
    }
    document = service.spreadsheets().create(
        body=document,
        fields='spreadsheetId' if doc_type == 'sheets' else 'documentId'
    ).execute()
    # 
    doc_id = document.get('spreadsheetId' if doc_type == 'sheets' else 'documentId')
    return doc_id


def _create_new_google_sheet(creds: Credentials, gsheet_name: str) -> str:
    """
    Create a new Google sheet.
    """
    return _create_new_google_document(creds, gsheet_name, 'sheets')


def _create_new_google_doc(creds: Credentials, gdoc_name: str) -> str:
    """
    Create a new Google doc.
    """
    return _create_new_google_document(creds, gdoc_name, 'docs')


def _share_google_file(gdrive_service: Optional[Any], gsheet_id: str, user: str) -> None:
    # Create the permission.
    parameters = {
        'role': 'reader',
        'type': 'user',
        'emailAddress': user
    }
    new_permission = gdrive_service.permissions().create(fileId=gsheet_id, body=parameters).execute()
    print('The new permission id of the document is: {}'.format(new_permission.get('id')))
    print('The google file is shared to {} successfully.'.format(user))


def _move_gsheet_to_dir(gdrive_service: Optional[Any], gsheet_id: str, folder_id: str) -> dict:
    """
    Moves a Google file to a specified folder in Google Drive.
    """
    res = gdrive_service.files().update(
        fileId=gsheet_id, 
        body={}, 
        addParents=folder_id, 
        removeParents='root', 
        supportsAllDrives=True
    ).execute()
    return res


def get_folders_in_gdrive(gdrive_service=None) -> list:
    if gdrive_service == None:
        # Create a drive api client.
        gdrive_service = build('drive', 'v3', credentials=creds)
    # 
    response = gdrive_service.files().list(
        q="mimeType='application/vnd.google-apps.folder' and trashed=false",
        spaces='drive',
        fields='nextPageToken, files(id, name)'
    ).execute()
    # Return list of folder id and folder name.
    return response.get('files')


def get_folder_id_by_foldername(folders: dict, foldername: str) -> list:
    folder_list = []
    for folder in folders:
        if folder.get("name") == foldername:
            folder_list.append(folder)
    if len(folder_list) == 1:
        return folder
    elif len(folder_list) > 1:
        for folder in folder_list:
            print(f'Found folder: {folder.get("name")}, {folder.get("id")}')
        print(f'Return the first found folder. {folder_list[0].get("name")}, {folder_list[0].get("id")}')
        print("if you want to use another folder id with the same filename {}, please copy the folder id manually.".format(foldername))
        return folder_list[0]
    else:
        print("Can't find the folder {}.".format(foldername))
        return 0


# #############################################################################
if __name__ == '__main__':
    # hdbg.init_logger(use_exec_path=True)
    create_empty_google_file("test", "1q57bUW7i0dAEo9Q88esAiYUuyApumlvL", "im.yiyun.lei@gmail.com")