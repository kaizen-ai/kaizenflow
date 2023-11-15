#!/usr/bin/env python3

"""
Retrieve emails from LinkedIn profiles that are first-degree connections with
your LinkedIn account.
It's advisable to use a 'fake' LinkedIn account for this purpose.
Ensure to modify the data_file_path, data_sheet_name, linkedin_username, and
linkedin_password variables in the code accordingly.

Usage:
    python email_extraction.py
"""
import random
import time
from typing import List

import pandas as pd
from tqdm import tqdm

import helpers.hsystem as hsystem

# Install selenium and openpyxl in the virtual environment.
cmd = 'sudo /bin/bash -c "(source /venv/bin/activate; pip install selenium)"'
hsystem.system(cmd, suppress_output=False, log_level="echo")
cmd = 'sudo /bin/bash -c "(source /venv/bin/activate; pip install openpyxl)"'
hsystem.system(cmd, suppress_output=False, log_level="echo")

from selenium import webdriver
from selenium.webdriver.common.by import By


def setup_selenium_linkedin(username: str, password: str) -> webdriver.Chrome:
    """
    Login to LinkedIn with Selenium.

    :param username: LinkedIn username
    :param password: LinkedIn password
    :return: Selenium webdriver
    """
    options = webdriver.ChromeOptions()
    # Comment this line and send code if manual verification needed for the
    # first time.
    options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)
    driver.get("https://www.linkedin.com")
    time.sleep(5)
    #
    username_input = driver.find_element(By.ID, "session_key")
    username_input.send_keys(username)
    password_input = driver.find_element(By.ID, "session_password")
    password_input.send_keys(password)
    driver.find_element(By.XPATH, "//button[@type='submit']").click()
    return driver


def get_email_from_url(driver: webdriver.Chrome, url: str) -> str:
    """
    Get email from LinkedIn profile URL.

    :param driver: Selenium webdriver
    :param url: LinkedIn profile URL
    :return: email
    """
    driver.get(url + "/overlay/contact-info/")
    time.sleep(random.randrange(2, 4))
    try:
        emailElement = driver.find_element(
            By.XPATH, "//h3[contains(., 'Email')]/following-sibling::div/a"
        )
        email = emailElement.text
    except Exception:
        email = "Email Not Found"
    return email


def get_email_list(url_list: List, username: str, password: str) -> List[str]:
    """
    Get the email list from the list of LinkedIn profile URLs.

    :param url_list: list of LinkedIn profile URLs
    :param username: LinkedIn username
    :param password: LinkedIn password
    :return: list of emails
    """
    driver = setup_selenium_linkedin(username, password)
    email_list = []
    for url in tqdm(
        url_list,
        desc="Processing URLs",
        position=0,
        bar_format="{desc}{percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
    ):
        email_list.append(get_email_from_url(driver, url))
    return email_list


def add_email_to_sheet(
    email_list: List[str], filepath: str, sheet_name: str
) -> None:
    """
    Add the email list to the Excel sheet.

    :param email_list: List of emails
    :param filepath: Path to the Excel file
    :param sheet_name: Name of the sheet in the Excel file
    :return: None
    """
    sheet_df["email"] = email_list
    with pd.ExcelWriter(filepath, mode="a", if_sheet_exists="replace") as writer:
        sheet_df.to_excel(writer, sheet_name=sheet_name, index=False)


if __name__ == "__main__":
    # Source data file.
    data_file_path = (
        "linkedin/extract_email/Search3-VCs_1st_degree_from_GP.LIn.xlsx"
    )
    # Default sheet name is "Sheet1".
    data_sheet_name = "Sheet1"
    # Add your LinkedIn username and password here.
    linkedIn_username = "your username"
    linkedIn_password = "your password"
    #
    sheet_df = pd.read_excel(data_file_path, sheet_name=data_sheet_name)
    profile_url_list = list(sheet_df["profileUrl"])
    email_list = get_email_list(
        profile_url_list, linkedIn_username, linkedIn_password
    )
    add_email_to_sheet(email_list, data_file_path, data_sheet_name)
