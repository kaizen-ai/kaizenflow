import datetime
import os
import pickle

import pandas as pd


# TODO: Remove this function saving few characters. If typing is a problem learn
# to touchtype.
def is_exists(path):
    return os.path.exists(path)


# TODO: Remove this function saving few characters. If typing is a problem learn
# to touchtype.
def make_dirs(path):
    os.makedirs(path)


# TODO: Remove this and use os.path.dirname.
def folder_name(f_name):
    if f_name[-1] != '/':
        return f_name + '/'
    return f_name


# TODO: Remove this and use helpers.helper_io.create_dir(..., incremental=True)
def check_or_create_dir(dir_path):
    if not is_exists(dir_path):
        make_dirs(dir_path)


# TODO: These functions can be helper to the specific function.
def create_csv_file(full_name, columns, separator):
    df = pd.DataFrame(columns=columns)
    df.to_csv(full_name, sep=separator, encoding='utf-8', index=False)


# TODO: These functions can be helper to the specific function.
def append_csv(full_name, df, sep):
    df.to_csv(
        full_name,
        sep=sep,
        encoding='utf-8',
        index=False,
        mode='a',
        header=False)


# TODO: Already exists called to_pickle.
def save_to_pickle(full_name, df, *args):
    with open(full_name, 'wb') as f:
        pickle.dump(df, f)


def get_current_timestamp():
    """
    Return timestamp.
    :return: timestamp
    """
    return datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")


# TODO(Sergey): P1 This can be implemented in pandas using a range generation.
# E.g., pd.date_range('2013-08-01', '2014-06-01', freq='Y')
def generate_time_ranges(start_date, end_date, days=0, years=0):
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    ranges = []
    start = start_date
    if days:
        offset = pd.DateOffset(days=days)
    elif years:
        offset = pd.DateOffset(years=years)
    while start < end_date:
        end = start + offset
        if end > end_date:
            end = end_date
        ranges.append((start, end))
        start = end
    return ranges