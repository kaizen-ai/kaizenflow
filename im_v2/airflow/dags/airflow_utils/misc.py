"""
Import as:

import im_v2.airflow.dags.airflow_utils.misc as imvadautmi
"""

import datetime
import os
from typing import Dict

# import helpers.hdbg as hdbg

# TODO(Sameep): CmTask8281 Centralize the region names.
EUROPE_REGION = "eu-north-1"
ASIA_REGION = "ap-northeast-1"
_SCHEMA_TRADING = "stage.location.purpose.model.config"
_SCHEMA_DATAPULL = "stage.location.purpose.period.data_type.vendor.exchange.universe.optional_tag"
valid_stages = ["prod", "preprod", "test"]
valid_locations = ["tokyo", "europe"]


def extract_components_from_filename(filename: str, dag_type: str) -> Dict:
    """
    Extract components from the DAG file name following our name convention.

    :param filename: name of the file to extract object from
    :return: object extracted from the file name
    """
    # Extract the schema based on the DAG type.
    if dag_type == "trading":
        schema = _SCHEMA_TRADING.split(".")
    elif dag_type == "datapull":
        schema = _SCHEMA_DATAPULL.split(".")
    else:
        raise ValueError(f"Unsupported DAG type: {dag_type}")
    # Split the filename into components and skip the file extension.
    components_list = filename.split(".")[:-1]
    components_list = components_list[: len(schema)]
    components_dict = {
        schema[i]: components_list[i] for i in range(len(components_list))
    }
    if (
        dag_type == "datapull"
        and "universe" in components_dict
        and components_dict["universe"] is not None
    ):
        components_dict["universe"] = components_dict["universe"].replace(
            "_", "."
        )
    # Assert stage argument is one of the supported ones.
    # hdbg.dassert_in(components_dict["stage"], valid_stages)
    # hdbg.dassert_in(components_dict["location"], valid_locations)
    assert (
        components_dict["stage"] in valid_stages
    ), f"Invalid stage: {components_dict['stage']}"
    assert (
        components_dict["location"] in valid_locations
    ), f"Invalid location: {components_dict['location']}"
    # Set the region based on the location.
    components_dict["region"] = (
        ASIA_REGION if components_dict["location"] == "tokyo" else EUROPE_REGION
    )
    # Create a DAG ID from the components.
    dag_id = ".".join(components_list)
    components_dict["dag_id"] = dag_id
    return components_dict


def get_dag_id_from_filename(filename: str) -> str:
    """
    Extract DAG ID from the DAG file name following our name convention.

    To simplify coupling between DAG .py file names and the display name
    shown in UI, derive the UI name directly from the file name.

    {stage}.{dag_id}.py -> dag_id
    preprod.backfill_bulk_ohlcv_data_fargate.py ->
    backfill_bulk_ohlcv_data_fargate

    :param filename: name of the file to extract DAG ID from
    :return: DAG ID extracted from the file name
    """
    dag_id = filename.rsplit(".", 1)[0]
    return dag_id


def get_stage_from_filename(filename: str) -> str:
    """
    Extract stage from the DAG file name following our name convention.

    {stage}.{dag_name}.py -> stage
    preprod.backfill_bulk_ohlcv_data_fargate -> preprod

    :param filename: name of the file to extract stage from
    :return: stage extracted from the file name
    """
    stage = filename.split(".")[0]
    # Assert stage argument is one of the supported ones.
    assert stage in ["prod", "preprod", "test"]
    # hdbg.dassert_in(stage, valid_stages)
    return stage


def align_to_5min_grid(date: datetime.datetime) -> datetime.datetime:
    """
    Align a datetime object to 5 min grid.

    examples:
    2023-10-23T15:53:00+00:00 -> 2023-10-23T15:55:00+00:00
    2023-10-23T15:42:00+00:00 -> 2023-10-23T15:40:00+00:00

    :param date: datetime to align to a 5-min grid
    :return: datetime aligned to 5 min grid
    """
    return date.replace(minute=date.minute - date.minute % 5, second=0)


def align_to_12min_grid(date: datetime.datetime) -> datetime.datetime:
    """
    Align a datetime object to 12 min grid.

    examples:
    2023-10-23T15:53:00+00:00 -> 2023-10-23 16:00:00+0000
    2023-10-23T15:42:00+00:00 -> 2023-10-23 15:48:00+0000

    :param date: datetime to align to a 12-min grid
    :return: datetime aligned to 12 min grid
    """
    return align_to_grid(date, 12)


def assert_stage_name(stage: str) -> None:
    """
    Assert stage argument is one of the supported ones.

    :param stage: stage argument to assert
    """
    assert stage in ["prod", "preprod", "test"]


# def strptime(date_str: str) -> datetime.datetime:
#    """
#    Convert string to airflow datetime format.
#
#    :param date_str: date string to convert to datetime object
#    """
#    return datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S%z")\


def align_to_grid(date: datetime.datetime, grid_in_min: int) -> datetime.datetime:
    """
    Align a datetime object to a grid of specified minutes.

    Examples:
    1. If the input datetime is '2023-10-23T15:53:00+00:00' and the grid is 5 minutes,
       the function will return '2023-10-23T15:55:00+00:00'.
    2. If the input datetime is '2023-10-23T15:42:00+00:00' and the grid is 30 minutes,
       the function will return '2023-10-23T16:00:00+00:00'.

    :param date: datetime to align to a desired grid
    :param grid_in_min: what type of grid to align to (5 min, 30 min etc.)
    :return: datetime aligned to the specified grid
    """
    grid_in_sec = grid_in_min * 60
    # Calculate the number of seconds since the epoch
    timestamp_seconds = (
        date - datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    ).total_seconds()
    # Calculate the remainder
    remainder = timestamp_seconds % grid_in_sec
    # If the remainder is not zero, align the date to the next grid point
    if remainder != 0:
        date += datetime.timedelta(seconds=(grid_in_sec - remainder))
    return date


def create_date_path(date: datetime) -> str:
    """
    Create a directory path from a datetime object.

    The directory path will have the structure 'YYYY/MM/DD'.

    Example:
    If the input datetime is '2023-12-02T15:53:00', the function will return '2023/12/02'.

    :param date: datetime to create a directory path from
    :return: directory path as a string
    """
    return os.path.join(
        str(date.year), str(date.month).zfill(2), str(date.day).zfill(2)
    )


def create_s3_bucket_path_for_experiment(
    s3_bucket_name: str, experiment_dir: str
) -> str:
    """
    Create the S3 bucket path for experiment data.

    :param s3_bucket_name: name of the S3 bucket
    :param experiment_dir: directory name for experiments within the S3
        bucket
    :return: S3 bucket path for experiment data
    """
    date_path = create_date_path(datetime.datetime.today())
    s3_bucket_path = os.path.join(
        f"s3://{s3_bucket_name}", experiment_dir, date_path
    )
    return s3_bucket_path
