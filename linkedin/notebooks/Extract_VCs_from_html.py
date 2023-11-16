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


def _get_VC_name(content_soup: BeautifulSoup) -> str:
    """
    Private method, see `_get_VC_row_content` for params.
    """
    name = ""
    try:
        name = content_soup.select(
            "div[data-walk-through-id$=-cell-name] .comp-txn-logo-container"
        )[0].next_sibling.text
    except (AttributeError, IndexError):
        name = "None"
    return name


def _get_VC_url(content_soup: BeautifulSoup) -> str:
    """
    Private method, see `_get_VC_row_content` for params.
    """
    url = ""
    try:
        url = content_soup.select(
            "div[data-walk-through-id$=-cell-name] .comp-txn-logo-container"
        )[0].parent["href"]
    except (AttributeError, IndexError):
        url = "None"
    return url


def _get_VC_score(content_soup: BeautifulSoup) -> str:
    """
    Private method, see `_get_VC_row_content` for params.
    """
    score = ""
    try:
        score = content_soup.select(
            "div[data-walk-through-id$=-cell-participationScore] a"
        )[0].text
    except (AttributeError, IndexError):
        score = "None"
    return score


def _get_VC_rounds(content_soup: BeautifulSoup) -> str:
    rounds = ""
    try:
        rounds = content_soup.select(
            "div[data-walk-through-id$=-cell-investmentsCount] a"
        )[0].text
    except (AttributeError, IndexError):
        rounds = "None"
    return rounds


def _get_VC_portfolio_companies(content_soup: BeautifulSoup) -> str:
    """
    Private method, see `_get_VC_row_content` for params.
    """
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


def _get_VC_location(content_soup: BeautifulSoup) -> str:
    """
    Private method, see `_get_VC_row_content` for params.
    """
    location = ""
    try:
        location = content_soup.select(
            "div[data-walk-through-id$=-cell-locations]"
        )[0].text
    except (AttributeError, IndexError):
        location = "None"
    return location


def _get_VC_stages(content_soup: BeautifulSoup) -> str:
    """
    Private method, see `_get_VC_row_content` for params.
    """
    stages = ""
    try:
        stages_div = content_soup.select(
            "div[data-walk-through-id$=-cell-investmentStages] > span"
        )[0]
        stages = stages_div.text.replace(",", ";")
    except (AttributeError, IndexError):
        stages = "None"
    return stages


def _get_VC_sectors(content_soup: BeautifulSoup) -> str:
    """
    Private method, see `_get_VC_row_content` for params.
    """
    sectors = ""
    try:
        sectors_div = content_soup.select(
            "div[data-walk-through-id$=-cell-investmentSectors] > span"
        )[0]
        sectors = sectors_div.text.replace(",", ";")
    except (AttributeError, IndexError):
        sectors = "None"
    return sectors


def _get_VC_investment_locations(content_soup: BeautifulSoup) -> str:
    """
    Private method, see `_get_VC_row_content` for params.
    """
    try:
        investment_locations_div = content_soup.select(
            "div[data-walk-through-id$=-cell-investmentLocations] > span"
        )[0]
        investment_locations = investment_locations_div.text.replace(",", ";")
    except (AttributeError, IndexError):
        investment_locations = "None"
    return investment_locations


def _get_VC_row_content(content_soup: BeautifulSoup) -> List[str]:
    """
    Private method, using BeautifulSoup to get one row in the VCs table soup.
    
    :param content_soup: A div soup containing the VCs table.
    :return: A List representing one row of the VCs table.
    """
    row_content_list = []
    VC_name = _get_VC_name(content_soup)
    VC_score = _get_VC_score(content_soup)
    VC_rounds = _get_VC_rounds(content_soup)
    VC_portfolio_companies = _get_VC_portfolio_companies(content_soup)
    VC_location = _get_VC_location(content_soup)
    VC_stages = _get_VC_stages(content_soup)
    VC_sectors = _get_VC_sectors(content_soup)
    VC_investment_locations = _get_VC_investment_locations(content_soup)
    VC_url = _get_VC_url(content_soup)
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
    """
    Extract the table content from a VC search result page.
    
    :param soup: The BeautifulSoup instance of the VC search result page soup.
    :return: A 2D List representing the table content.
    """
    contents_div = soup.find_all(
        "div",
        attrs={"data-walk-through-id": re.compile(r"^gridtable-row-[0-9]*$")},
    )
    contents_list = list(map(_get_VC_row_content, contents_div))
    return contents_list


def get_VC_title(soup: BeautifulSoup) -> List[str]:
    """
    Extract the table title from a VC search result page soup.
    
    :param soup: The BeautifulSoup instance of the VC search result page.
    :return: A List representing the table title.
    """
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
    """
    Get a pandas dataframe containing the table in a VC search result page.
    
    :param html_file_path: The path of the VC search page as an html file.
    :return: A pandas.DataFrame containing the infomation of the VCs table.
    """
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

# %%
