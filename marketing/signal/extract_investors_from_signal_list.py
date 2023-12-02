"""
Import as:

import marketing.signal.extract_investors_from_signal_list as mseifsili
"""

# TODO(Henry): This package need to be manually installed until they are added
# to the container.
# Run the following line in any notebook would install it:
# !sudo /bin/bash -c "(source /venv/bin/activate; pip install --upgrade selenium webdriver-manager)"

import math
import time

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager


def extract_investors_from_signal_url(
    baseurl: str, start_idx: int, length: int
) -> pd.DataFrame:
    """
    Extract a dataframe of investor information from a signal investors list page.
    e.g. https://signal.nfx.com/investor-lists/top-fintech-seed-investors
    Available lists are in this page: https://signal.nfx.com/investor-lists/ 
    The page is only loading a few items for one click on the loading button,
    so please use the params to specify the range of data to be extracted,
    and avoid an unexpectable waiting time.

    :param baseurl: The page url to be extracted
    :param start_idx: The index of the first item to be extracted (start from 0)
    :param length: The number of items to be extracted
    """

    # Returning default text when element not present.
    class _emptyText:
        text = "None"

    # The xpath of useful elements.
    xpaths = {
        "header": "//tr[@class='header-row']/th",
        "contents": "//div[@class='sn-investor-name-wrapper']",
        "load": "//button[text()='LOAD MORE INVESTORS']",
        "name": ".//strong[contains(@class, 'sn-investor-name')]",
        "company": "./a",
        "job": "./span",
    }
    # Start driver.
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--window-size=1920x1080")
    driver = webdriver.Chrome(
        service=ChromeService(ChromeDriverManager().install()), options=options
    )
    investors_list = []
    # Perform page actions.
    try:
        driver.get(baseurl)
        driver.maximize_window()
        total = start_idx + length
        # Click load button until enough data loaded.
        # 8 items will be loaded on each click.
        for _ in range(math.ceil(total / 8) - 1):
            loadButton = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, xpaths["load"]))
            )
            loadButton.click()
            time.sleep(1)
        # Extract table content with selenium.
        contents = driver.find_elements(By.XPATH, xpaths["contents"])[
            start_idx : start_idx + length
        ]
        investors_list = list(
            map(
                lambda x: [
                    (
                        x.find_elements(By.XPATH, xpaths["name"])[0:]
                        or [_emptyText()]
                    )[0].text,
                    (
                        x.find_elements(By.XPATH, xpaths["company"])[0:]
                        or [_emptyText()]
                    )[0].text,
                    (
                        x.find_elements(By.XPATH, xpaths["job"])[0:]
                        or [_emptyText()]
                    )[0].text,
                ],
                contents,
            )
        )
    finally:
        driver.quit()
    # Write list to data frame.
    titles = ["investorName", "companyName", "jobTitle"]
    investors_df = pd.DataFrame(data=investors_list, columns=titles)
    return investors_df
