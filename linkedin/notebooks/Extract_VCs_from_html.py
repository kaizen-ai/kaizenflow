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
# # Get Companies

# %% run_control={"marked": true}
import re
from typing import List

import pandas as pd
from bs4 import BeautifulSoup


def get_VC_name(content_soup: BeautifulSoup) -> str:
    name = ""
    try:
        name = content_soup.select(
            "div[data-walk-through-id$=-cell-name] .comp-txn-logo-container"
        )[0].next_sibling.text
    except (AttributeError, IndexError):
        name = "None"
    return name


def get_VC_url(content_soup: BeautifulSoup) -> str:
    url = ""
    try:
        url = content_soup.select(
            "div[data-walk-through-id$=-cell-name] .comp-txn-logo-container"
        )[0].parent["href"]
    except (AttributeError, IndexError):
        url = "None"
    return url


def get_VC_score(content_soup: BeautifulSoup) -> str:
    score = ""
    try:
        score = content_soup.select(
            "div[data-walk-through-id$=-cell-participationScore] a"
        )[0].text
    except (AttributeError, IndexError):
        score = "None"
    return score


def get_VC_rounds(content_soup: BeautifulSoup) -> str:
    rounds = ""
    try:
        rounds = content_soup.select(
            "div[data-walk-through-id$=-cell-investmentsCount] a"
        )[0].text
    except (AttributeError, IndexError):
        rounds = "None"
    return rounds


def get_VC_portfolio_companies(content_soup: BeautifulSoup) -> str:
    portfolio_companies = ""
    try:
        portfolio_companies_div = content_soup.select(
            "div[data-walk-through-id$=-cell-portfoliocompanies] > div > div "
            ".comp-txn-logo-container + span"
        )
        portfolio_companies = ";".join(
            map(lambda x: x.text, portfolio_companies_div)
        )
    except (AttributeError, IndexError):
        portfolio_companies = "None"
    return portfolio_companies


def get_VC_location(content_soup: BeautifulSoup) -> str:
    location = ""
    try:
        location = content_soup.select(
            "div[data-walk-through-id$=-cell-locations]"
        )[0].text
    except (AttributeError, IndexError):
        location = "None"
    return location


def get_VC_stages(content_soup: BeautifulSoup) -> str:
    stages = ""
    try:
        stages_div = content_soup.select(
            "div[data-walk-through-id$=-cell-investmentStages] > span"
        )[0]
        stages = stages_div.text.replace(",", ";")
    except (AttributeError, IndexError):
        stages = "None"
    return stages


def get_VC_sectors(content_soup: BeautifulSoup) -> str:
    sectors = ""
    try:
        sectors_div = content_soup.select(
            "div[data-walk-through-id$=-cell-investmentSectors] > span"
        )[0]
        sectors = sectors_div.text.replace(",", ";")
    except (AttributeError, IndexError):
        sectors = "None"
    return sectors


def get_VC_investment_locations(content_soup: BeautifulSoup) -> str:
    try:
        investment_locations_div = content_soup.select(
            "div[data-walk-through-id$=-cell-investmentLocations] > span"
        )[0]
        investment_locations = investment_locations_div.text.replace(",", ";")
    except (AttributeError, IndexError):
        investment_locations = "None"
    return investment_locations


def get_VC_row_content(content_soup: BeautifulSoup) -> List[str]:
    row_content_list = []
    VC_name = get_VC_name(content_soup)
    VC_score = get_VC_score(content_soup)
    VC_rounds = get_VC_rounds(content_soup)
    VC_portfolio_companies = get_VC_portfolio_companies(content_soup)
    VC_location = get_VC_location(content_soup)
    VC_stages = get_VC_stages(content_soup)
    VC_sectors = get_VC_sectors(content_soup)
    VC_investment_locations = get_VC_investment_locations(content_soup)
    VC_url = get_VC_url(content_soup)
    row_content_list = [
        VC_name,
        VC_score,
        VC_rounds,
        VC_portfolio_companies,
        VC_location,
        VC_stages,
        VC_sectors,
        VC_investment_locations,
        VC_url,
    ]
    return row_content_list


def get_VC_contents(soup: BeautifulSoup) -> List[List[str]]:
    contents_div = soup.find_all(
        "div",
        attrs={"data-walk-through-id": re.compile(r"^gridtable-row-[0-9]*$")},
    )
    contents_list = list(map(get_VC_row_content, contents_div))
    return contents_list


def get_VC_title(soup: BeautifulSoup) -> List[str]:
    titles_div = soup.find_all(
        attrs={"data-walk-through-id": "gridtable-column"}
    )[0].children
    titles = map(
        lambda x: x.select(".comp--gridtable__column-cell--menu-middle")[
            0
        ].contents[0]["title"],
        titles_div,
    )
    return list(titles)


def get_VCs_from_html(html_file_path: str) -> pd.DataFrame:
    with open(html_file_path, encoding="utf-8") as html_fp:
        soup = BeautifulSoup(html_fp)
        vc_titles = get_VC_title(soup)
        vc_titles.append("Company URL")
        vc_contents = get_VC_contents(soup)
        vc_df = pd.DataFrame(data=vc_contents, columns=vc_titles)
        return vc_df


# %% run_control={"marked": true}
vc_html_path = "../data/Tracxn_SeriesA_AI.html"
vc_csv_save_path = "../result_csv/Tracxn_SeriesA_AI.csv"
vc_df = get_VCs_from_html(vc_html_path)
vc_df.to_csv(vc_csv_save_path, sep=",", index=False)
vc_df


# %% [markdown]
# # Get LinkedIn Profile Link from Company Page

# %%
def get_employee_row_content(content_soup: BeautifulSoup) -> List[str]:
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
    contents_div = soup.find_all(
        "div",
        attrs={"data-walk-through-id": re.compile(r"^gridtable-row-[0-9]*$")},
    )
    contents_list = list(map(get_employee_row_content, contents_div))
    return contents_list


def get_employees_from_html(html_file_path: str) -> pd.DataFrame:
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
