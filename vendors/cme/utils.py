import logging

import pandas as pd
import requests
from bs4 import BeautifulSoup

_log = logging.getLogger()
_log.setLevel(logging.INFO)


def extract_urls(soup_table):
    """
    Get hyperlinks from each cell of a beautiful soup table.

    :param soup_table: beautiful soup table
    :return: [[url]], the shape is (n_rows, n_cols). If there was no url
        in the cell, the corresponding element will be None
    """
    urls_in_rows = []
    for row in soup_table.find_all('tr'):
        urls_in_cols = []
        for td in row.find_all('td'):
            if td.find('a'):
                url = td.a['href']
            else:
                url = None
            urls_in_cols.append(url)
        urls_in_rows.append(urls_in_cols)
    return urls_in_rows


def _urls_list_to_df(urls_list):
    links_df = pd.DataFrame(urls_list).dropna(axis=1, how='all')
    links_df = links_df.add_prefix('link_')
    return links_df


def extract_urls_df(soup_table):
    """
    Extract hyperlinks from a beautiful soup table.

    :param soup_table: beautiful soup table
    :return: pd.DataFrame with urls. If the column of the table had a
        URL, the output DataFrame will have this column with
        "link_" prefix. All elements of that column that do not have a
        link will be None.
    """
    return _urls_list_to_df(extract_urls(soup_table))


def soup_table_to_df(soup_table):
    """
    Read beautiful soup table to pandas DataFrame.

    :param soup_table: beautiful soup table
    :return: pd.DataFrame
    """
    return pd.read_html(str(soup_table).replace('colspan', ''))[0]


def soup_table_to_df_with_links(soup_table):
    """
    Read beautiful soup table to pandas DataFrame. If a column contains
    hyperlinks, output dataframe will have this column with a prefix
    "link_"and extracted links. Elements that do not have a link
    will be None.

    :param soup_table: beautiful soup table
    :return: pd.DataFrame from that table with columns
        for extracted links.
    """
    df_without_links = soup_table_to_df(soup_table)
    links_df = extract_urls_df(soup_table)
    df_with_links = df_without_links.join(links_df)
    return df_with_links


def load_html_to_df(html_url):
    """
    Download html by link, extract tables with hyperlinks from it,
    concatenate them into one dataframe.

    :param html_url: html link
    :return: pd.DataFrame of the tables extracted from the html.
        If a column has hyperlinks, they will be extracted into a
        column with a prefix "link_".
    """
    req_res = requests.get(html_url)
    if req_res.status_code == 200:
        soup = BeautifulSoup(req_res.content, "lxml")
        soup_tables = soup.find_all('table')
        dfs = [soup_table_to_df_with_links(soup_table) for soup_table in
               soup_tables]
        if len(dfs) > 0:
            concatenated_df = pd.concat(dfs)
        else:
            _log.info('No tables were extracted from %s', html_url)
            concatenated_df = None
    else:
        _log.warning(f'Request status code is {req_res.status_code}')
        concatenated_df = None
    return concatenated_df
