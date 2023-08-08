#!/usr/bin/env python

"""
Import as:

import linkedin.google_api.google_file_api as lggogfia
"""

import logging
import os.path
from typing import Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Scopes required making API calls.
_LOG = logging.getLogger(__name__)
SCOPES = ["https://www.googleapis.com/auth/drive"]


# #############################################################################
class GoogleFileApi:
    def __init__(self):
        # Get Google API credentials.
        self.creds = self._get_credentials()
        # Create a Google drive api client.
        self.gdrive_service = build("drive", "v3", credentials=self.creds)

    def create_empty_google_file(
        self,
        gfile_type: str,
        gfile_name: str,
        gdrive_folder: dict,
        user: Optional[str] = None,
    ) -> None:
        """
        Create a new Google file (sheet or doc).

        :param gfile_type: str, the type of the Google file ('sheet' or 'doc').
        :param gfile_name: str, the name of the new Google file.
        :param gdrive_folder: dict, the id and the name of the Google Drive folder.
        :param user: str, the email address of the user to share the Google file (Optional).
        :return: None
        """
        try:
            if gfile_type == "sheet":
                gfile_id = self._create_new_google_sheet(gfile_name)
            elif gfile_type == "doc":
                gfile_id = self._create_new_google_doc(gfile_name)
            else:
                _LOG.error("gfile_type must be either 'sheet' or 'doc'.")
                return
            _LOG.info("Created a new Google %s '%s'.", gfile_type, gfile_name )

            # Move the Google file to a Google Drive dir.
            if gdrive_folder:
                self._move_gfile_to_dir(gfile_id, gdrive_folder.get("id"))
                _LOG.info(
                    "Move the new Google %s '%s' to the dir '%s'",
                    gfile_type,
                    gfile_name,
                    gdrive_folder.get('name')
                )
            else:
                _LOG.info("The new Google '%s' is created in your root dir.", gfile_type)
            # Share the Google file to a user and send an email.
            if user:
                self._share_google_file(gfile_id, user)
                _LOG.info(
                    "The new Google '%s': '%s' is shared to '%s'", gfile_type, gfile_name, user
                )
            _LOG.info("Finished creating the new Google %s '%s'.", gfile_type, gfile_name)
        #
        except HttpError as err:
            _LOG.error(err)

    def create_google_drive_folder(self, folder_name: str, parent_folder_id: str) -> None:
        """
        Create a new Google Drive folder inside the given folder.

        :param folder_name: str, the name of the new Google Drive folder.
        :param parent_folder_id: str, the id of the parent folder.
        """
        try:
            file_metadata = {
                "name": folder_name,
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [parent_folder_id],
            }
            folder = (
                self.gdrive_service.files()
                .create(body=file_metadata, fields="id")
                .execute()
            )
            _LOG.info("Created a new Google Drive folder '%s'.", folder_name)
            _LOG.info("The new folder id is '%s'.", folder.get("id"))
        #
        except HttpError as err:
            _LOG.error(err)
            
    def get_folder_id_by_name(self, name: str) -> Optional[list]:
        folders = self._get_folders_in_gdrive()
        folder_list = []
        #
        for folder in folders:
            if folder.get("name") == name:
                folder_list.append(folder)
        #
        if len(folder_list) == 1:
            _LOG.info("Found folder: %s", folder_list[0])
            return folder_list[0]
        #
        elif len(folder_list) > 1:
            for folder in folder_list:
                _LOG.info(
                    "Found folder: '%s', '%s'",
                    folder.get("name"),
                    folder.get("id"),
                )
            #
            _LOG.info(
                "Return the first found folder. '%s' '%s' ",
                folder_list[0].get("name"),
                folder_list[0].get("id"),
            )
            _LOG.info(
                "if you want to use another '%s' folder, please change the folder id manually.",
                name,
            )
            return folder_list[0]
        #
        else:
            _LOG.error("Can't find the folder '%s'.", name)
            return

    # #########################################################################

    @staticmethod
    def _get_credentials() -> Credentials:
        """
        Get credentials for Google API.

        :return: Credentials
        """
        creds = None
        # The file token.json stores the user's access and refresh tokens, and
        # is created automatically when the authorization flow completes for
        # the first time.
        # TODO(Yiyun): Modify the path of token.json and client_secrets.json to follow the gspread-pandas package.
        token_path = os.path.join(os.path.dirname(__file__), "token.json")
        if os.path.exists(token_path):
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            client_secrets_path = os.path.join(
                os.path.dirname(__file__), "client_secrets.json"
            )
            if not os.path.exists(client_secrets_path):
                raise RuntimeError(
                    "Please download client_secrets.json from Google API."
                )
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    client_secrets_path, SCOPES
                )
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(token_path, "w") as token:
                token.write(creds.to_json())
        return creds

    def _create_new_google_document(self, doc_name: str, doc_type: str) -> str:
        """
        Create a new Google document (Sheet or Doc).

        :param doc_name: str, the name of the new Google document.
        :param doc_type: str, the type of the Google document ('sheets' or 'docs').
        :return: doc_id.
        """
        service = build(
            doc_type,
            "v4" if doc_type == "sheets" else "v1",
            credentials=self.creds,
        )
        document = {"properties": {"title": doc_name}}
        document = (
            service.spreadsheets()
            .create(
                body=document,
                fields="spreadsheetId" if doc_type == "sheets" else "documentId",
            )
            .execute()
        )
        #
        doc_id = document.get(
            "spreadsheetId" if doc_type == "sheets" else "documentId"
        )
        return doc_id

    def _create_new_google_sheet(self, gsheet_name: str) -> str:
        """
        Create a new Google sheet.
        """
        return self._create_new_google_document(gsheet_name, "sheets")

    def _create_new_google_doc(self, gdoc_name: str) -> str:
        """
        Create a new Google doc.
        """
        return self._create_new_google_document(gdoc_name, "docs")

    def _share_google_file(self, gsheet_id: str, user: str) -> None:
        # Create the permission.
        parameters = {"role": "reader", "type": "user", "emailAddress": user}
        new_permission = (
            self.gdrive_service.permissions()
            .create(fileId=gsheet_id, body=parameters)
            .execute()
        )
        _LOG.info(
            "The new permission id of the document is: '%s'",
            new_permission.get("id"),
        )
        _LOG.info("The google file is shared to '%s'.", user)

    def _move_gfile_to_dir(self, gfile_id: str, folder_id: str) -> dict:
        """
        Moves a Google file to a specified folder in Google Drive.
        """
        res = (
            self.gdrive_service.files()
            .update(
                fileId=gfile_id,
                body={},
                addParents=folder_id,
                removeParents="root",
                supportsAllDrives=True,
            )
            .execute()
        )
        return res

    def _get_folders_in_gdrive(self) -> list:
        response = (
            self.gdrive_service.files()
            .list(
                q="mimeType='application/vnd.google-apps.folder' and trashed=false",
                spaces="drive",
                fields="nextPageToken, files(id, name)",
            )
            .execute()
        )
        # Return list of folder id and folder name.
        return response.get("files")

