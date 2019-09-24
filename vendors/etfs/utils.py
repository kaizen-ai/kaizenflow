import json
import os

import pandas as pd

import helpers.io_ as io_
import helpers.system_interaction as si


def get_etf_metadata():
    # http://www.masterdatareports.com/ETFData-Sample/Fundamentals.csv
    file_name = "./masterdatareports.fundamentals.csv"
    meta_df = pd.read_csv(file_name, encoding="ISO-8859-1")
    return meta_df


#
# - Attributes
#   - Commission free = Yes
#   - TODO(gp): check this
# - Issuer
#   - iShares
#   - Schwab
#   - Vanguard
# - Structure
#   - ETFs
# - Liquidity
#   - Assets > $10,000M ($10B)
#   - Avg daily volume > 1M
#   - Share > $10


def get_json_file_name(tab):
    json_file_name = "./etfdb_data/etfdb.%s.json" % tab
    return json_file_name


# TODO(gp): The API is unreliable / broken. Probably better to use beautifulsoup.
def download_etfdb_metadata():
    """
    We download and save all the data in one shot, since etfdb.com is flakey and doesn't always
    respond.
    """
    # https://github.com/janlukasschroeder/etfdb-api
    # https://etfdb.com/screener/#page=1&sort_by=price&sort_direction=asc&issuer=ishares&structure=ETF&assets_start=10000&average_volume_start=1000000&price_start=10
    tabs = [
        "overview",
        # "Returns",
        # "Fund-Flows",
        # "Expenses",
        # "ESG",
        # "Dividends",
        # "Risk",
        # "Holdings",
        # "Taxes",
        # "Technicals",
        # "Analysis",
        # "Realtime-Ratings"
    ]
    for tab in tabs:
        print("tab=%s" % tab)
        file_name = get_json_file_name(tab)
        if os.path.exists(file_name):
            print("Skip")
        else:
            json_post = {
                "page": 1,
                "per_page": 1000,
                # "commission_free": "true",
                # "structure": "ETF",
                # "issuer": "ishares",
                # "sort_by": "ytd",
                # "sort_direction": "desc",
                "only": ["meta", "data"],
                "tab": tab,
            }
            io_.to_file("json_post.txt", str(json_post))
            cmd = (
                'curl -d "@json_post.txt" -X POST https://etfdb.com/api/screener/ -o %s'
                % file_name
            )
            si.system(cmd)
            #
            txt = io_.from_file(file_name, split=False)
            if "error" in txt:
                print("Error: removing %s" % file_name)
                os.remove(file_name)


def get_ishares_fundamentals():
    """

    :return:
    """
    # file_name = "etfdb.json"
    file_name = json_file_name[:]
    txt = io_.from_file(file_name, split=False)
    dict_ = json.loads(txt)
    # {'symbol': {'type': 'link', 'text': 'MIDU', 'url': '/etf/MIDU/'},
    #   'name': {'type': 'link',
    #    'text': 'Direxion Daily Mid Cap Bull 3X Shares',
    #    'url': '/etf/MIDU/'},
    #   'mobile_title': 'MIDU - Direxion Daily Mid Cap Bull 3X Shares',
    #   'price': '$44.01',
    #   'assets': '$54.93',
    #   'average_volume': '48,132',
    #   'ytd': '56.95%',
    #   'overall_rating': {'type': 'restricted', 'url': '/members/join/'},
    #   'asset_class': 'Equity'},
    data = dict_["data"]
    data_tmp = []
    convert_ = lambda x: float(x.replace(",", "").replace("$", ""))
    for row in data:
        data_tmp.append(
            (
                row["symbol"]["text"],
                row["name"]["text"],
                convert_(row["price"]),
                convert_(row["assets"]),
                convert_(row["average_volume"]),
                row["asset_class"],
            )
        )
    df = pd.DataFrame(
        data_tmp,
        columns="symbol descr price assets avg_volume asset_class".split(),
    )
    return df
