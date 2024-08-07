"""
Documentation for this module is at
docs/coding/all.hgoogle_file_api.explanation.md.

Import as:

import helpers.hgoogle_file_api as hgofiapi
"""


import logging
import os.path
from typing import List, Optional

# TODO(Henry): This package need to be manually installed until they are added
# to the container.
# Run the following line in any notebook would install it:
# !sudo /bin/bash -c "(source /venv/bin/activate; pip install --upgrade google-api-python-client)"
# Or run the following part in python:
# import subprocess
# install_code = subprocess.call(
# 'sudo /bin/bash -c "(source /venv/bin/activate; pip install --upgrade google-api-python-client)"',
# shell=True,
# )

import google.oauth2.service_account as goasea
import googleapiclient.discovery as godisc
import googleapiclient.errors as goerro

# Scopes required for making API calls.
_LOG = logging.getLogger(__name__)
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]


def get_credentials(
    *,
    service_key_path: Optional[str] = None,
) -> goasea.Credentials:
    """
    Get credentials for Google API with service account key.

    :param service_key_path: service account key file path.
    :return: google credentials retrieved.
    """
    if not service_key_path:
        service_key_path = ".google_credentials/service.json"
    service_key_path = os.path.join(os.path.dirname(__file__), service_key_path)
    if not os.path.exists(service_key_path):
        _LOG.info("Failed to read service key file: %s", service_key_path)
        raise RuntimeError(
            "Please download service.json from Google API, "
            "Then save it as helpers/.google_credentials/service.json\n"
            "Instructions: https://gspread-pandas.readthedocs.io/en/latest/getting_started.html#client-credentials"
        )
    creds = goasea.Credentials.from_service_account_file(
        service_key_path, scopes=SCOPES
    )
    return creds


def get_sheets_service(
    *, service_key_path: Optional[str] = None
) -> godisc.Resource:
    """
    Get Google Sheets service with current credentials.

    :param service_key_path: service key path.
    :return: google sheets service instance created.
    """
    creds = get_credentials(service_key_path=service_key_path)
    sheets_service = godisc.build(
        "sheets", "v4", credentials=creds, cache_discovery=False
    )
    return sheets_service


def get_sheet_id(
    sheet_id: str,
    *,
    sheet_name: Optional[str] = None,
    service_key_path: Optional[str] = None,
) -> str:
    """
    Get the sheet ID from the sheet name in a Google Sheets document.

    :param sheet_id: id of the Google Sheet document.
    :param sheet_name: name of the sheet (tab) in the Google Sheets
        document.
    :param service_key_path: path to the service key file.
    :return: sheet ID of the sheet with the given name or the first
        sheet if name is not provided.
    """
    sheets_service = get_sheets_service(service_key_path=service_key_path)
    sheet_metadata = (
        sheets_service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    )
    sheets = sheet_metadata.get("sheets", [])
    if sheet_name:
        for sheet in sheets:
            properties = sheet.get("properties", {})
            if properties.get("title") == sheet_name:
                return properties.get("sheetId")
        raise ValueError(f"Sheet with name '{sheet_name}' not found.")
    # Return the ID of the first sheet if no sheet name is provided.
    first_sheet_id = sheets[0].get("properties", {}).get("sheetId")
    return first_sheet_id


def freeze_rows(
    sheet_id: str,
    row_indices: List[int],
    *,
    sheet_name: Optional[str] = None,
    service_key_path: Optional[str] = None,
) -> None:
    """
    Freeze specified rows in the given sheet.

    :param sheet_id: id of the Google Sheet.
    :param row_indices: rows indices to freeze (zero-based index).
    :param sheet_name: name of the sheet (tab) to freeze rows in.
        Defaults to the first tab if not provided.
    :param service_key_path: path to the service key file.
    """
    sheet_id = get_sheet_id(
        sheet_id=sheet_id,
        sheet_name=sheet_name,
        service_key_path=service_key_path,
    )
    sheets_service = get_sheets_service(service_key_path=service_key_path)
    # Calculate the number of rows to freeze.
    num_rows_to_freeze = max(row_indices) + 1
    freeze_request = {
        "requests": [
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheet_id,
                        "gridProperties": {"frozenRowCount": num_rows_to_freeze},
                    },
                    "fields": "gridProperties.frozenRowCount",
                }
            }
        ]
    }
    response = (
        sheets_service.spreadsheets()
        .batchUpdate(spreadsheetId=sheet_id, body=freeze_request)
        .execute()
    )
    _LOG.debug("Freeze Response: %s", response)


def set_row_height(
    sheet_id: str,
    height: int,
    *,
    start_index: Optional[int] = None,
    end_index: Optional[int] = None,
    sheet_name: Optional[str] = None,
    service_key_path: Optional[str] = None,
) -> None:
    """
    Set the height for rows in the given sheet.

    - If only start_index is provided, applies height to all rows starting from start_index.
    - If only end_index is provided, applies height to all rows up to end_index.
    - If both start_index and end_index are provided, applies height to rows between them.
    - If neither is provided, applies height to all rows.

    :param sheet_id: id of the Google Sheet.
    :param height: height of the rows in pixels.
    :param start_index: starting index of the rows (zero-based). If None, applies to all rows.
    :param end_index: ending index of the rows (zero-based). If None, applies to all rows.
    :param sheet_name: name of the sheet (tab) to set row height in.
                       Defaults to the first tab if not provided.
    :param service_key_path: the path to the service key file.
    :return: none
    """
    sheet_id = get_sheet_id(
        sheet_id=sheet_id,
        sheet_name=sheet_name,
        service_key_path=service_key_path,
    )
    sheets_service = get_sheets_service(service_key_path=service_key_path)
    # Determine the range of rows to update.
    if start_index is None and end_index is None:
        # Fetch the sheet metadata to get the total number of rows.
        sheet_metadata = (
            sheets_service.spreadsheets().get(spreadsheetId=sheet_id).execute()
        )
        sheet_properties = next(
            sheet
            for sheet in sheet_metadata.get("sheets", [])
            if sheet.get("properties", {}).get("sheetId") == sheet_id
        ).get("properties", {})
        grid_properties = sheet_properties.get("gridProperties", {})
        end_index = grid_properties.get("rowCount", 1000)
        start_index = 0
    elif start_index is None:
        start_index = 0
    elif end_index is None:
        # Fetch the sheet metadata to get the total number of rows.
        sheet_metadata = (
            sheets_service.spreadsheets().get(spreadsheetId=sheet_id).execute()
        )
        sheet_properties = next(
            sheet
            for sheet in sheet_metadata.get("sheets", [])
            if sheet.get("properties", {}).get("sheetId") == sheet_id
        ).get("properties", {})
        grid_properties = sheet_properties.get("gridProperties", {})
        end_index = grid_properties.get("rowCount", 1000)
    set_row_height_request = {
        "requests": [
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": start_index,
                        "endIndex": end_index,
                    },
                    "properties": {"pixelSize": height},
                    "fields": "pixelSize",
                }
            }
        ]
    }
    response = (
        sheets_service.spreadsheets()
        .batchUpdate(spreadsheetId=sheet_id, body=set_row_height_request)
        .execute()
    )
    _LOG.debug("Row Height Response: %s", response)


def get_gdrive_service(
    *, service_key_path: Optional[str] = None
) -> godisc.Resource:
    """
    Get Google drive service with current credential.

    :param service_key_path: The service key path.
        - Will use default service key path in `get_credentials` if None is given.

    :return: the Google drive service instance created.
    """

    creds = get_credentials(service_key_path=service_key_path)
    gdrive_service = godisc.build(
        "drive", "v3", credentials=creds, cache_discovery=False
    )
    return gdrive_service


def create_empty_google_file(
    gfile_type: str,
    gfile_name: str,
    gdrive_folder_id: str,
    *,
    user: Optional[str] = None,
) -> Optional[str]:
    """
    Create a new Google file (sheet or doc).

    :param gfile_type: str, the type of the Google file ('sheet' or
        'doc').
    :param gfile_name: str, the name of the new Google file.
    :param gdrive_folder_id: the id of the Google Drive folder.
    :param user: str, the email address of the user to share the Google
        file (Optional).
    :return: str, the ID of the created Google file, or None if an error
        occurred.
    """
    try:
        if gfile_type == "sheet":
            gfile_id = _create_new_google_sheet(gfile_name)
        elif gfile_type == "doc":
            gfile_id = _create_new_google_doc(gfile_name)
        else:
            _LOG.error("gfile_type must be either 'sheet' or 'doc'.")
            return None
        _LOG.info("Created a new Google %s '%s'.", gfile_type, gfile_name)

        # Move the Google file to a Google Drive dir.
        if gdrive_folder_id:
            _move_gfile_to_dir(gfile_id, gdrive_folder_id)
        # Share the Google file to a user and send an email.
        if user:
            share_google_file(gfile_id, user)
            _LOG.info(
                "The new Google '%s': '%s' is shared to '%s'",
                gfile_type,
                gfile_name,
                user,
            )
        return gfile_id  # Return the file ID
    except goerro.HttpError as err:
        _LOG.error(err)
        return None  # Return None if an error occurred


def create_google_drive_folder(
    folder_name: str,
    parent_folder_id: str,
    *,
    service: godisc.Resource = None,
) -> str:
    """
    Create a new Google Drive folder inside the given folder.

    :param folder_name: str, the name of the new Google Drive folder.
    :param parent_folder_id: str, the id of the parent folder.
    :param service: the google drive service instance.
        - Will use GDrive file service as default if None is given.
    """
    try:
        if service is None:
            service = get_gdrive_service()
        file_metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_folder_id],
        }
        folder = service.files().create(body=file_metadata, fields="id").execute()
        _LOG.info("Created a new Google Drive folder '%s'.", folder_name)
        _LOG.info("The new folder id is '%s'.", folder.get("id"))
    except goerro.HttpError as err:
        _LOG.error(err)
    return folder.get("id")


def get_folder_id_by_name(name: str) -> Optional[list]:
    """
    Get the folder id by the folder name.

    :param name: str, the name of the folder.
    :return: list, the list of the folder id and folder name.
    """
    folders = _get_folders_in_gdrive()
    folder_list = []
    #
    for folder in folders:
        if folder.get("name") == name:
            folder_list.append(folder)
    if len(folder_list) == 1:
        _LOG.info("Found folder: %s", folder_list[0])
    elif len(folder_list) > 1:
        for folder in folder_list:
            _LOG.info(
                "Found folder: '%s', '%s'",
                folder.get("name"),
                folder.get("id"),
            )
        _LOG.info(
            "Return the first found folder. '%s' '%s' ",
            folder_list[0].get("name"),
            folder_list[0].get("id"),
        )
        _LOG.info(
            "if you want to use another '%s' folder, "
            "please change the folder id manually.",
            name,
        )
    else:
        _LOG.error("Can't find the folder '%s'.", name)
        return None
    return folder_list[0]


def share_google_file(
    gfile_id: str, user: str, *, service: godisc.Resource = None
) -> None:
    """
    Share a Google file to a user.

    :param gfile_id: str, the id of the Google file.
    :param user: str, the email address of the user.
    :param service: the google drive service instance.
        - Will use GDrive file service as default if None is given.
    """
    if service is None:
        service = get_gdrive_service()
    # Create the permission.
    parameters = {"role": "reader", "type": "user", "emailAddress": user}
    new_permission = (
        service.permissions().create(fileId=gfile_id, body=parameters).execute()
    )
    _LOG.info(
        "The new permission id of the document is: '%s'",
        new_permission.get("id"),
    )
    _LOG.info("The google file is shared to '%s'.", user)


def _create_new_google_document(
    doc_name: str, doc_type: str, *, service: godisc.Resource = None
) -> str:
    """
    Create a new Google document (Sheet or Doc).

    :param doc_name: str, the name of the new Google document.
    :param doc_type: str, the type of the Google document ('sheets' or 'docs').
    :param service: the google drive service instance.
        - Will auto-choose sheet or doc service as default if None is given.
    :return: doc_id. The id to the created document in GDrive.
    """
    if service is None:
        creds = get_credentials()
        service = godisc.build(
            doc_type,
            "v4" if doc_type == "sheets" else "v1",
            credentials=creds,
            cache_discovery=False,
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
    doc_id = document.get(
        "spreadsheetId" if doc_type == "sheets" else "documentId"
    )
    return doc_id


def _create_new_google_sheet(gsheet_name: str) -> str:
    """
    Create a new Google sheet.
    """
    doc_type = "sheets"
    return _create_new_google_document(gsheet_name, doc_type)


def _create_new_google_doc(gdoc_name: str) -> str:
    """
    Create a new Google doc.
    """
    doc_type = "docs"
    return _create_new_google_document(gdoc_name, doc_type)


def _move_gfile_to_dir(
    gfile_id: str, folder_id: str, *, service: godisc.Resource = None
) -> dict:
    """
    Move a Google file to a specified folder in Google Drive.

    :param gfile_id: str, the id of the Google file.
    :param folder_id: str, the id of the folder.
    :param service: the google drive service instance.
        - Will use GDrive file service as default if None is given.
    """
    if service is None:
        service = get_gdrive_service()
    res = (
        service.files()
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


def _get_folders_in_gdrive(*, service: godisc.Resource = None) -> list:
    """
    Get a list of folders in Google drive.

    :param service: the google drive service instance.
        - Will use GDrive file service as default if None is given.
    """
    if service is None:
        service = get_gdrive_service()
    response = (
        service.files()
        .list(
            q="mimeType='application/vnd.google-apps.folder' and trashed=false",
            spaces="drive",
            fields="nextPageToken, files(id, name)",
        )
        .execute()
    )
    # Return list of folder id and folder name.
    return response.get("files")
