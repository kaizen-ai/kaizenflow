# %%
"""
Import as:

import marketing.dropcontact.find_email as mdrfiema
"""


# %%
import time
from math import ceil
from typing import Any, Dict, List, Sequence

# %%
import pandas as pd
import requests
from tqdm import tqdm


# %%
def _preprocess_dropcontact_data(
    first_names: Sequence[str],
    last_names: Sequence[str],
    company_names: Sequence[str],
) -> List[Dict[str, str]]:
    """
    Preprocess data for DropContact API.

    :param first_names: List of first names
    :param last_names: List of last names
    :param company_names: List of company names
    :return: A list of dictionaries with first name, last name and company name
    """
    data: List[Dict[str, str]] = []
    # Check the input format.
    if not len(first_names) == len(last_names) == len(company_names):
        print("Error: length of input data must be the same.")
        return data
    # Format data for dropcontact API.
    for first_name, last_name, company in zip(
        first_names, last_names, company_names
    ):
        data.append(
            {
                "first_name": first_name,
                "last_name": last_name,
                "company": company,
            }
        )
    return data


# %%
def _request_dropcontact(batch_data: List[Dict[str, str]], api_key: str) -> Any:
    """
    Send request to DropContact API.

    :param batch_data: List of dictionaries with first name, last name and company name
    :param api_key: API key of DropContact
    :return: A dictionary contains the query result
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


# %%
def _generate_result_df(query_results: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Generate dataframe from query result.

    :param query_results: List of query results
    :return: A dataframe with columns:
            first name, last name, full name, email, phone, pronoun, job title
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


# %%
def _send_batch_request(
    data: List[dict], api_key: str, batch_size: int
) -> List[dict]:
    """
    Send batch request to DropContact API.

    :param data: List of dictionaries with first name, last name and company name
    :param api_key: API key of DropContact
    :param batch_size: Batch size
    :return: A list of dictionaries, each dictionary contains the query result
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
        post_response = _request_dropcontact(batch_data, api_key)
        query_id = post_response["request_id"]
        print(f"Batch {str(batch_idx)}: Query ID: {str(query_id)}.")
        # Wait for query result, 10 seconds per attempt, 120 seconds timeout.
        for _ in range(12):
            # Get query result using retrieved ID. This request won't cost any credit.
            get_response = requests.get(
                f"https://api.dropcontact.io/batch/{query_id}",
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
            reason = get_response["reason"]
            error = get_response["error"]
            if error:
                print(f"Error detected, reason: {str(reason)}.")
                break
            time.sleep(10)
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


# %%
def get_email_from_dropcontact(
    first_names: Sequence[str],
    last_names: Sequence[str],
    company_names: Sequence[str],
    api_key: str,
    batch_size: int = 50,
) -> pd.DataFrame:
    """
    Get email from DropContact API using first name, last name and company
    name.

    :param first_names: List of first names
    :param last_names: List of last names
    :param company_names: List of company names
    :param api_key: API key of DropContact
    :return: A dataframe with the following columns:
            first name, last name, full name, email, phone, pronoun, job title
    """
    data = _preprocess_dropcontact_data(first_names, last_names, company_names)
    # Send batch request to DropContact API.
    query_results = _send_batch_request(data, api_key, batch_size)
    # Generate dataframe from query result.
    result_df = _generate_result_df(query_results)
    return result_df
