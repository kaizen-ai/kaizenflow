#!/usr/bin/env python
import logging
import os.path
import helpers.hdbg as hdbg
from typing import Optional, Any

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Scopes required making API calls.
_LOG = logging.getLogger(__name__)
SCOPES = ["https://www.googleapis.com/auth/drive"]


# #############################################################################
def create_empty_google_file(
        gfile_type: str, 
        gfile_name: str,
        folder_id: Optional[str] = None,
        folder_name: Optional[str] = None,
        user: Optional[str] = None
    ) -> None:
    """
    Create an empty Google file and share it to a user.
    :return: None
    """
    #
    creds = _get_credentials()
    # 
    try:
        if gfile_type == "sheet":
            gfile_id = _create_new_google_sheet(creds, gfile_name)
        elif gfile_type == "doc":
            gfile_id = _create_new_google_doc(creds, gfile_name)
        else:
            _LOG.error("gfile_type must be either 'sheet' or 'doc'.")
            return
        _LOG.info('The file id of the new Google {} is: {}'.format(gfile_type, gfile_id))
        # Create a drive api client.
        gdrive_service = build('drive', 'v3', credentials=creds)
        # 
        if user:
            _share_google_file(gdrive_service, gfile_id, user)
            _LOG.info('Create the new Google {}: {}'.format(gfile_type, gfile_name))
        # Move Google file to a Google drive dir.
        if folder_id:
            _move_gsheet_to_dir(gdrive_service, gfile_id, folder_id)
            _LOG.info('Move the new Google {} {} to the shared dir: folder_id={}'.format(gfile_type, gfile_name, folder_id))
        elif folder_name:
            folders = _get_folders_in_gdrive(gdrive_service)
            _LOG.info('founder_name {}'.format(folder_name))
            folder = _get_folder_id_by_foldername(folders, folder_name)
            if folder:
                _move_gsheet_to_dir(gdrive_service, gfile_id, folder.get("id"))
                _LOG.info('Move the new google {} {} to the shared dir: {}'.format(gfile_type, gfile_name, folder.get("name")))
        else:
            _LOG.info('The new Google {} is created in your root dir.'.format(gfile_type))
    #
    except HttpError as err:
        _LOG.error(err)

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
    _LOG.info('The new permission id of the document is: {}'.format(new_permission.get('id')))
    _LOG.info('The google file is shared to {} successfully.'.format(user))


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


def _get_folders_in_gdrive(gdrive_service: Optional[Any]) -> list:
    response = gdrive_service.files().list(
        q="mimeType='application/vnd.google-apps.folder' and trashed=false",
        spaces='drive',
        fields='nextPageToken, files(id, name)'
    ).execute()
    # Return list of folder id and folder name.
    return response.get('files')


def _get_folder_id_by_foldername(folders: list, foldername: str) -> list:
    folder_list = []
    # 
    for folder in folders:
        if folder.get("name") == foldername:
            _LOG.info(f'folder.get("name") {folder.get("name")}')
            folder_list.append(folder)
    # 
    if len(folder_list) == 1:
        _LOG.info(f'Found folder: {folder_list[0].get("name")}, {folder_list[0].get("id")}')
        return folder_list[0]
    elif len(folder_list) > 1:
        for folder in folder_list:
            _LOG.info(f'Found folder: {folder.get("name")}, {folder.get("id")}')
        # 
        _LOG.info(f'Return the first found folder. {folder_list[0].get("name")}, {folder_list[0].get("id")}')
        _LOG.info("if you want to use another folder id, please copy the folder id manually.")
        return folder_list[0]
    else:
        _LOG.error("Can't find the folder {}.".format(foldername))
        return 0
