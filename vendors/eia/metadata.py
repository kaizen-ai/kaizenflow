import pandas as pd
from typing import List


KEYWORDS = ['exports', 'imports', 'gasoline', 'fuel', 'oil', 'petroleum', 'distillate', 'refinery', 'blending', 'crude', 'ppm', 'motor', 'production', 'gas']


def extract_country_codes(
    df: pd.DataFrame, country_col: str = "iso3166"
) -> List[str]:
    """
    Check if geo code is longer than 3 letters and creates country codes list.

    These codes can be used later to select only country-aggregated data.
    Regional county data has code longer than 3 letters.
    :param df:
    :param country_col:
    :return:
    """
    geo_values = df[country_col].dropna().unique()
    # exclude country+region codes
    all_countries = list(filter(lambda x: len(x) < 4, geo_values))
    return all_countries


def check_if_country_aggregated(
    df: pd.DataFrame, country_col: str = "iso3166"
) -> pd.DataFrame:
    """
    Add a boolean column with 'True' for country-aggregated data.

    :param df:
    :param country_col:
    :return:
    """
    all_countries = extract_country_codes(df, country_col=country_col)
    return df[country_col].isin(all_countries)


# Naming in following function is questionable. As well as methodology.
# A suitable uscase for this method will be working
# with someone's free access data which can miss latest months/years,
# or working with data which wasn't updated for several months.
def check_if_still_active(
    df: pd.DataFrame,
    last_update_col: str = "last_updated",
    frequency_col: str = "average_timedelta",
) -> pd.DataFrame:
    """
    Add column with number of periods in skipped since latest dataset update.

    Latest value of last_update_col over all dataset is considered "latest".
    Compute timedelata (in days) betwen 'latest' timestamp and last update time
    for each row and devide it by frequency. The result is number of periods
    skipped compared to latest data timestamp in the dataset.
    Obtained value can be a metric of recent activity for each row and used
    later for filtering.

    :param df:
    :param last_update_col:
    :param frequency_col:
    :return:
    """
    timedeltas = df[last_update_col].apply(pd.to_datetime) - df[last_update_col].apply(pd.to_datetime).max()
    return (timedeltas.apply(lambda x: x.days) / df[frequency_col])


def check_if_keyword_in_description(metadata_df: pd.DataFrame, description_col = 'description', keywords=KEYWORDS):
    """
    Add boolean columns showing keyword presence in the description.
    """
    metadata_df_copy = pd.DataFrame.copy(metadata_df)
    for word in KEYWORDS:
        metadata_df_copy['has_'+word] = metadata_df_copy[description_col].str.lower().str.contains(word)
    return metadata_df_copy