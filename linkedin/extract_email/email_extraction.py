#!/usr/bin/env python3

"""
Extract emails from LinkedIn profile URLs listed in the Excel sheet.
"""
import random
import subprocess
import time

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from tqdm import tqdm

# Install selenium and openpyxl in the virtual environment.
install_code = subprocess.call(
    'sudo /bin/bash -c "(source /venv/bin/activate; pip install selenium)"',
    shell=True,
)
install_code = subprocess.call(
    'sudo /bin/bash -c "(source /venv/bin/activate; pip install openpyxl)"',
    shell=True,
)


def setup_selenium_linkedin(username, password):
    options = webdriver.ChromeOptions()
    # Comment this line and send code if manual verification needed for the first time.
    options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)
    driver.get("https://www.linkedin.com")
    time.sleep(5)
    username_input = driver.find_element(By.ID, "session_key")
    username_input.send_keys(username)
    password_input = driver.find_element(By.ID, "session_password")
    password_input.send_keys(password)
    driver.find_element(By.XPATH, "//button[@type='submit']").click()
    return driver


def get_email_from_url(driver, url):
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


def get_email_list(url_list, username, password):
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


def add_email_to_sheet(filepath, sheet_name, username, password):
    sheet_df = pd.read_excel(filepath, sheet_name=sheet_name)
    url_list = list(sheet_df["profileUrl"])
    email_list = get_email_list(url_list, username, password)
    sheet_df["email"] = email_list
    with pd.ExcelWriter(filepath, mode="a", if_sheet_exists="replace") as writer:
        sheet_df.to_excel(writer, sheet_name=sheet_name, index=False)


if __name__ == "__main__":
    # LinkedIn username and password here
    li_username = "your_username"
    li_password = "your_password"
    # This is the excel to be read.
    li_filepath = "Search3-VCs_1st_degree_from_GP.LIn.xlsx"
    li_sheet_name = "Sheet1"
    add_email_to_sheet(li_filepath, li_sheet_name, li_username, li_password)
