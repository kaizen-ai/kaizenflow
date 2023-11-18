# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# This notebook is used to get email from DropContact API using first name,
# last name and company name. The input data is from a Google Sheet.

# %%
# !sudo /bin/bash -c "(source /venv/bin/activate; pip install --upgrade google-api-python-client)"

# %% [markdown]
# # Import

# %%
import time
from math import ceil
from typing import Iterable, List

import gspread_pandas
import pandas as pd
import requests
from tqdm import tqdm

import helpers.hgoogle_file_api as hgofiapi

# %% [markdown]
# # Get data from Google Sheet

# %%
# Set up the Google sheet name.
gsheet_name = "Search7.AI_VC_in_US_DropContact_Test"
#
creds = hgofiapi.get_credentials()
spread = gspread_pandas.Spread(gsheet_name, creds=creds)
df = spread.sheet_to_df(index=None)[:10]
print(df.shape)
df.head()

# %% [markdown]
# # Set up

# %%
# Batch size is how many data we send to the API per request.
# Batch endpoint can process up to 250 contacts with a single request.
# One contact data must be less than 10 kB.
#
# The API will cost 1 credit per data length.
batch_size = 10
# The column titles for first name, last name and company name in Given GSheet.
first_name_col = "firstName"
last_name_col = "lastName"
company_col = "companyName"
# API key of DropContact.
api_key = ""


# %% [markdown]
# # DropContact functions

# %%
def preprocess_data(
    first_name_list: Iterable[str],
    last_name_list: Iterable[str],
    company_list: Iterable[str],
) -> List:
    """
    Preprocess data for DropContact API.

    :param first_name_list: List of first names.
    :param last_name_list: List of last names.
    :param company_list: List of company names.
    :return: A list of dictionaries, each dictionary contains first name, last name and company name.
    """
    data = []
    # Check the input format.
    if not len(first_name_list) == len(last_name_list) == len(company_list):
        print("Error: length of input data must be the same.")
        return []
    # Format data for dropcontact API.
    for first_name, last_name, company in zip(
        first_name_list, last_name_list, company_list
    ):
        data.append(
            {
                "first_name": first_name,
                "last_name": last_name,
                "company": company,
            }
        )
    return data


def request_dropcontact(batch_data: List[dict], api_key: str) -> dict:
    """
    Send request to DropContact API.

    :param batch_data: List of dictionaries, each dictionary contains first name, last name and company name.
    :param api_key: API key of DropContact.
    :return: A dictionary contains the query result.
    """
    post_response = requests.post(
        "https://api.dropcontact.io/batch",
        json={
            "data": batch_data,
            "siren": True,
            "language": "en",
        },
        headers={
            "Content-Type": "application/json",
            "X-Access-Token": api_key,
        },
    ).json()
    return post_response


def generate_result_df(query_results: List[dict]) -> pd.DataFrame:
    """
    Generate dataframe from query result.

    :param query_results: List of query results.
    :return: A dataframe with columns: first name, last name, full name, email, phone, pronoun, job title.
    """
    result_list = []
    result_title = [
        "first name",
        "last name",
        "full name",
        "email",
        "phone",
        "pronoun",
        "job title",
    ]
    for result in query_results:
        first_name = ""
        last_name = ""
        full_name = ""
        email = ""
        phone = ""
        pronoun = ""
        job = ""
        if "first_name" in result:
            first_name = result["first_name"]
        if "last_name" in result:
            last_name = result["last_name"]
        if "full_name" in result:
            full_name = result["full_name"]
        if "email" in result:
            email = ";".join(map(lambda x: x["email"], result["email"]))
        if "phone" in result:
            phone += result["phone"]
        if "mobile_phone" in result:
            if phone:
                phone += ";"
            phone += result["mobile_phone"]
        if "civility" in result:
            pronoun = result["civility"]
        if "job" in result:
            job = result["job"]
        # Convert phone number to string.
        phone = str(phone)
        result_list.append(
            [first_name, last_name, full_name, email, phone, pronoun, job]
        )
    return pd.DataFrame(data=result_list, columns=result_title)


def send_batch_request(
    data: List[dict], api_key: str, batch_size: int
) -> List[dict]:
    """
    Send batch request to DropContact API.

    :param data: List of dictionaries, each dictionary contains first name, last name and company name.
    :param api_key: API key of DropContact.
    :param batch_size: Batch size.
    :return: A list of dictionaries, each dictionary contains the query result.
    """
    batches = []
    query_results = []
    data_length = len(data)
    # Splitting data into batches.
    for batch_i in range(ceil(data_length / batch_size)):
        batches.append(data[batch_i * batch_size : (batch_i + 1) * batch_size])
    # Executing query per batch.
    start_time = time.time()
    for batch_idx, batch_data in enumerate(
        tqdm(batches, desc="Processing batches", ascii=True, ncols=100)
    ):
        batch_start_time = time.time()
        print(f"Starting query batch {str(batch_idx)}.")
        batch_result = []
        # Send a search query.
        # This request will cost 1 credit per data length.
        post_response = request_dropcontact(batch_data, api_key)
        query_id = post_response["request_id"]
        print(f"Batch {str(batch_idx)}: Query ID: {str(query_id)}.")
        # Wait for query result, 11 seconds per attempt, 55 seconds timeout.
        for i in range(5):
            # Get query result using retrieved ID. This request won't cost any credit.
            get_response = requests.get(
                "https://api.dropcontact.io/batch/{}".format(query_id),
                headers={"X-Access-Token": api_key},
            ).json()
            query_finished = get_response["success"]
            if query_finished:
                batch_result = get_response["data"]
                credits_left = get_response["credits_left"]
                print(
                    f"Batch {str(batch_idx)}: Query finished. Credits left: {str(credits_left)}."
                )
                break
            else:
                reason = get_response["reason"]
            error = get_response["error"]
            if error:
                print(f"Error detected, reason: {str(reason)}.")
                break
            time.sleep(11)
        if not batch_result:
            print(f"Batch {str(batch_idx)}: Query failed, reason: timeout.")
            batch_result = [{}] * len(batch_data)
        query_results += batch_result
        batch_end_time = time.time()
        print(
            f"Batch {batch_idx} completed in {batch_end_time - batch_start_time:.2f} seconds."
        )
    # Total processing time.
    end_time = time.time()
    print(f"Total processing time: {end_time - start_time:.2f} seconds.")
    return query_results


def get_email_from_dropcontact(
    first_name_list: Iterable[str],
    last_name_list: Iterable[str],
    company_list: Iterable[str],
    api_key: str,
    batch_size: int,
) -> pd.DataFrame:
    """
    Get email from DropContact API using first name, last name and company
    name.

    :param first_name_list: List of first names.
    :param last_name_list: List of last names.
    :param company_list: List of company names.
    :param api_key: API key of DropContact.
    :return: A dataframe with columns: first name, last name, full name, email, phone, pronoun, job title.
    """
    data = preprocess_data(first_name_list, last_name_list, company_list)
    # Send batch request to DropContact API.
    query_results = send_batch_request(data, api_key, batch_size)
    # Generate dataframe from query result.
    result_df = generate_result_df(query_results)
    return result_df


# %% [markdown]
# # Get emails from DropContact

# %%
email_df = get_email_from_dropcontact(
    df[first_name_col], df[last_name_col], df[company_col], api_key, batch_size
)

# %%
email_df


# %% [markdown]
# # Write email_df to the same Google Sheet

# %%
# Fix phone number format.
def prepare_phone_number_for_sheets(phone_number):
    if phone_number != "":
        pattern = r'^'
        replacement = "'"
        return re.sub(pattern, replacement, phone_number)
    else:
        return phone_number

email_df['phone'] = email_df['phone'].apply(prepare_phone_number_for_sheets)

email_df


# %% run_control={"marked": true}
def df_to_gsheet(gsheet_name: str, df: pd.DataFrame) -> None:
    # Write to the sheet.
    # Make sure the sheet "email"(sheet_name) exists in the Google Sheet.
    sheet_name = "email"
    spread2 = gspread_pandas.Spread(
        gsheet_name, sheet=sheet_name, create_sheet=True, creds=creds
    )
    spread2.df_to_sheet(df, index=False)


#
df_to_gsheet(gsheet_name, email_df)

# %%
