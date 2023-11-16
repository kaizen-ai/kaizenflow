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
# # Get LinkedIn Profile Link from Company Page

# %%
import re
from typing import List

import pandas as pd
from bs4 import BeautifulSoup

def _get_employee_row_content(content_soup: BeautifulSoup) -> List[str]:
    """
    Private method, using BeautifulSoup to get one row in the people table soup.
    
    :param content_soup: A div soup containing the people table.
    :return: A List representing one row of the people table.
    """
    employee_div = content_soup.select(".employeeCard__wrapper")[0]
    employee_name = ""
    employee_linkedin_profile = ""
    try:
        employee_name = employee_div.a.text
    except (AttributeError, IndexError):
        employee_name = "None"
    try:
        employee_linkedin_profile = employee_div.a.next_sibling.a["href"]
    except (AttributeError, IndexError):
        employee_linkedin_profile = "None"
    employee_info_list = [employee_name, employee_linkedin_profile]
    return employee_info_list


def get_employee_contents(soup: BeautifulSoup) -> List[List[str]]:
    """
    Extract the table content from a company's people page.
    
    :param soup: The BeautifulSoup instance of the VC search result page soup.
    :return: A 2D List representing the content of the page's people table.
    """
    contents_div = soup.find_all(
        "div",
        attrs={"data-walk-through-id": re.compile(r"^gridtable-row-[0-9]*$")},
    )
    contents_list = list(map(_get_employee_row_content, contents_div))
    return contents_list


def get_employees_from_html(html_file_path: str) -> pd.DataFrame:
    """
    Get a pandas dataframe containing the table in a company's people page.
    
    :param html_file_path: The path of the company's people page as an html file.
    :return: A pandas.DataFrame containing the people's name and LinkedIn profile link.
    """
    with open(html_file_path, encoding="utf-8") as employee_fp:
        soup = BeautifulSoup(employee_fp)
        employee_titles = ["Name", "LinkedIn Profile"]
        employee_contents = get_employee_contents(soup)
        employee_df = pd.DataFrame(
            data=employee_contents, columns=employee_titles
        )
        return employee_df


# %%
employee_html_path = "../data/Sequoia Capital _ Tracxn.html"
employee_csv_save_path = "../result_csv/Sequoia Capital _ Tracxn.csv"
employee_df = get_employees_from_html(employee_html_path)
employee_df.to_csv(employee_csv_save_path, sep=",", index=False)
employee_df

# %%
