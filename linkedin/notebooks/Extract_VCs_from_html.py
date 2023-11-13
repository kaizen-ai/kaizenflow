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

# %%
from bs4 import BeautifulSoup
import pandas as pd
import re


# %%
def get_VC_name(content_soup):
    name = ""
    try:
        name = content_soup.select("div[data-walk-through-id$=-cell-name] .comp-txn-logo-container")[0].next_sibling.text
    except Exception as e:
        name = "None"
    return name

def get_VC_url(content_soup):
    url = ""
    try:
        url = content_soup.select("div[data-walk-through-id$=-cell-name] .comp-txn-logo-container")[0].parent["href"]
    except Exception as e:
        url = "None"
    return url

def get_VC_score(content_soup):
    score = ""
    try:
        score = content_soup.select("div[data-walk-through-id$=-cell-participationScore] a")[0].text
    except Exception as e:
        score = "None"
    return score

def get_VC_rounds(content_soup):
    rounds = ""
    try:
        rounds = content_soup.select("div[data-walk-through-id$=-cell-investmentsCount] a")[0].text
    except Exception as e:
        rounds = "None"
    return rounds

def get_VC_portfolio_companies(content_soup):
    portfolio_companies = ""
    try:
        portfolio_companies_div = content_soup.select("div[data-walk-through-id$=-cell-portfoliocompanies] > div > div .comp-txn-logo-container + span")
        portfolio_companies = ";".join(map(lambda x: x.text, portfolio_companies_div))
    except Exception as e:
        portfolio_companies = "None"
    return portfolio_companies

def get_VC_location(content_soup):
    location = ""
    try:
        location = content_soup.select("div[data-walk-through-id$=-cell-locations]")[0].text
    except Exception as e:
        location = "None"
    return location

def get_VC_stages(content_soup):
    stages = ""
    try:
        stages_div = content_soup.select("div[data-walk-through-id$=-cell-investmentStages] > span")[0]
        stages = stages_div.text.replace(",", ";")
    except Exception as e:
        stages = "None"
    return stages

def get_VC_sectors(content_soup):
    sectors = ""
    try:
        sectors_div = content_soup.select("div[data-walk-through-id$=-cell-investmentSectors] > span")[0]
        sectors = sectors_div.text.replace(",", ";")
    except Exception as e:
        sectors = "None"
    return sectors

def get_VC_investment_locations(content_soup):
    investment_location = ""
    try:
        investment_locations_div = content_soup.select("div[data-walk-through-id$=-cell-investmentLocations] > span")[0]
        investment_locations = investment_locations_div.text.replace(",", ";")
    except Exception as e:
        investment_locations = "None"
    return investment_locations
    
def get_row_content(content_soup):
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
    row_content_list = [VC_name, VC_score, VC_rounds, VC_portfolio_companies, VC_location, VC_stages, VC_sectors, VC_investment_locations, VC_url]
    return row_content_list

def get_contents(soup):
    contents_div = soup.find_all("div", attrs={"data-walk-through-id": re.compile(r"^gridtable-row-[0-9]*$")})
    contents_list = list(map(get_row_content, contents_div))
    return contents_list

def get_title(soup):
    titles_div = soup.find_all(attrs={"data-walk-through-id": "gridtable-column"})[0].children
    titles = map(lambda x: x.select(".comp--gridtable__column-cell--menu-middle")[0].contents[0]["title"], titles_div)
    return list(titles)

def get_VCs_from_html(filepath):
    fp = open(filepath)
    soup = BeautifulSoup(fp)
    titles = get_title(soup)
    titles.append("Company URL")
    contents = get_contents(soup)
    df = pd.DataFrame(data=np.array(contents), columns=titles)
    fp.close()
    return df



# %% run_control={"marked": true}
filepath = "../data/Tracxn_SeriesA_AI.html"
df = get_VCs_from_html(filepath)
df.to_csv("../result_csv/Tracxn_SeriesA_AI.csv", sep=',',index=False)

# %%
df["Company URL"][0]

# %%
