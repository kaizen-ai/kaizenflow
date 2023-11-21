"""
Import as:

import marketing.tra.extract_people_from_Tra_company_mhtml as mtepftcmh
"""

import quopri
import re
from typing import List

import pandas as pd
from bs4 import BeautifulSoup


def _get_employee_row_content(content_soup: BeautifulSoup) -> List[str]:
    """
    Get one row from the table in an HTML page.

    :param content_soup: A div soup containing the people table
    :return: A list containing one row from the table.
    """
    employee_div = content_soup.select(".employeeCard__wrapper")[0]
    employee_name = ""
    employee_linkedin_profile = ""
    # Get properties of the table.
    try:
        employee_name = employee_div.a.text
    except (AttributeError, IndexError):
        employee_name = "None"
    try:
        employee_linkedin_profile = employee_div.a.next_sibling.a["href"]
    except (AttributeError, IndexError):
        employee_linkedin_profile = "None"
    # Combine the properties as List.
    employee_info_list = [employee_name, employee_linkedin_profile]
    return employee_info_list


def get_employee_contents(soup: BeautifulSoup) -> List[List[str]]:
    """
    Extract the table content from a company's people page.

    :param soup: The BeautifulSoup instance of the VC search result page soup
    :return: A 2D list containing the people page table content
    """
    contents_div = soup.find_all(
        "div",
        attrs={"data-walk-through-id": re.compile(r"^gridtable-row-[0-9]*$")},
    )
    contents_list = list(map(_get_employee_row_content, contents_div))
    return contents_list


def get_employees_from_mhtml(mhtml_file_path: str) -> pd.DataFrame:
    """
    Get a pandas dataframe from the table in a company's people page.

    :param mhtml_file_path: The path of the company's people page as an mhtml file
    :return: A pandas.DataFrame containing the people's name and LinkedIn profile link
    """
    with open(mhtml_file_path, "rb") as employee_fp:
        soup = BeautifulSoup(
            quopri.decodestring(employee_fp.read()), features="lxml"
        )
        employee_titles = ["Name", "LinkedIn Profile"]
        employee_contents = get_employee_contents(soup)
        employee_df = pd.DataFrame(
            data=employee_contents, columns=employee_titles
        )
        return employee_df
