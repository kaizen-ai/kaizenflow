"""
Import as:

import helpers.s3 as hs3
"""

import argparse
import configparser
import functools
import logging
import os
import pprint
from typing import Any, Dict, Optional

_WARNING = "\033[33mWARNING\033[0m"

try:
    import s3fs
except ModuleNotFoundError:
    _module = "s3fs"
    print(_WARNING + f": Can't find {_module}: continuing")


import helpers.dbg as dbg  # noqa: E402 module level import not at top of file  # pylint: disable=wrong-import-position
import helpers.system_interaction as hsyste # noqa: E402 module level import not at top of file  # pylint: disable=wrong-import-position
import helpers.timer as htimer # noqa: E402 module level import not at top of file  # pylint: disable=wrong-import-position

_LOG = logging.getLogger(__name__)


# Architecture of the AWS authentication
#
# - There can be two or more AWS S3 systems with different credentials, paths to
#   bucket, and other properties
# - Some code needs to refer always and only to the AM S3 bucket (e.g., for Kibot
#   data)
# - Other code needs to work with different AWS S3 systems (e.g., publish_notebooks,
#   saving / retrieving experiments)
#
# - The different AWS S3 systems are selected through an `aws_profile` parameter
#   (e.g., `am`)
# - The value of AWS profile is obtained:
#   - from the `--aws_profile` command line option; or
#   - from the `AM_AWS_PROFILE` env var
#
# - The AWS profile is then used to access the `~/.aws` files and extract:
#   - the credentials (e.g., `aws_access_key_id`, `aws_secret_access_key`,
#     `aws_region`)
#   - other variables (e.g., `aws_s3_bucket`)
# - The variables that are extracted from the files can also be passed through
#   env vars directly (mainly for GitHub Actions CI)
#   - E.g., `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`,
#     `AWS_S3_BUCKET`)
#   - One can specify env vars conditioned to different profiles, using the AWS
#     profile (e.g., `am` profile for `AWS_ACCESS_KEY_ID` corresponds to
#     `AM_AWS_ACCESS_KEY_ID`)


# TODO(gp): Merge with get_path() to create get_s3_am_bucket_path().
#  Avoid to use alphamatic-data but rather use this.
def get_bucket() -> str:
    """
    Return the S3 bucket pointed by AM_S3_BUCKET (e.g., `alphamatic-data`).

    Make sure your ~/.aws/credentials uses the right key to access this
    bucket as default.
    """
    # TODO(gp): -> AM_AWS_S3_BUCKET
    env_var = "AM_S3_BUCKET"
    dbg.dassert_in(env_var, os.environ)
    s3_bucket = os.environ[env_var]
    return s3_bucket


# TODO(gp): -> get_bucket_path() ?
def get_path() -> str:
    """
    Return the path to the default S3 bucket (e.g., `s3://alphamatic-data`).
    """
    path = "s3://" + get_bucket()
    return path


# #############################################################################


def add_s3_args(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """
    Add the command line options for the AWS credentials.
    """
    parser.add_argument(
        "--aws_profile",
        action="store",
        type=str,
        default=None,
        help="The AWS profile to use for `.aws/credentials` or for env vars",
    )
    parser.add_argument(
        "--s3_path",
        action="store",
        type=str,
        default=None,
        help="Full S3 path to use (e.g., `s3://alphamatic-data/foobar/notebooks`), "
            "overriding any other setting",
    )
    return parser


def get_aws_profile_from_env(args: argparse.Namespace) -> str:
    """
    Return the AWS profile to access S3, based on command line option and env
    vars.
    """
    if args.aws_profile:
        aws_profile = args.aws_profile
    else:
        env_var = "AM_AWS_PROFILE"
        dbg.dassert_in(
            env_var, os.environ, "Env var '%s' is not set", env_var
        )
        aws_profile = os.environ[env_var]
    return aws_profile  # type: ignore


def get_s3_path_from_env(args: argparse.Namespace) -> Optional[str]:
    """
    Return the S3 path to use, based on command line option and env vars.
    """
    if args.s3_path:
        s3_path = args.s3_path
    else:
        env_var = "AM_S3_BUCKET"
        if env_var in os.environ:
            s3_path = os.environ[env_var]
        else:
            s3_path = None
    return s3_path  # type: ignore


def _get_aws_config(file_name: str) -> configparser.RawConfigParser:
    """
    Return a parser to the config in `~/.aws/{file_name]}`.
    """
    file_name = os.path.join(os.path.expanduser("~"), ".aws", file_name)
    dbg.dassert_file_exists(file_name)
    # Read the config.
    config = configparser.RawConfigParser()
    config.read(file_name)
    _LOG.debug("config.sections=%s", config.sections())
    return config


@functools.lru_cache()
def get_aws_credentials(
    aws_profile: str,
) -> Dict[str, Optional[str]]:
    """
    Read the AWS credentials for a given profile.

    :return: a dictionary with `access_key_id`, `aws_secret_access_key`,
        `aws_region` and optionally `aws_session_token`
    """
    _LOG.debug("Getting credentials for aws_profile='%s'", aws_profile)
    dbg.dassert_ne(aws_profile, "")
    #
    result: Dict[str, Optional[str]] = {}
    key_to_env_var: Dict[str, str] = {
        "aws_access_key_id": "AWS_ACCESS_KEY_ID",
        "aws_secret_access_key": "AWS_SECRET_ACCESS_KEY",
        # TODO(gp): AWS_DEFAULT_REGION -> AWS_REGION so we can use the invariant
        #  that the var is simply the capitalized version of the key.
        "aws_region": "AWS_DEFAULT_REGION",
    }
    # If all the AWS credentials are passed through env vars, they override the
    # config file.
    env_var_override = False
    set_env_vars = [(env_var in os.environ and os.environ[env_var] != "")
        for env_var in sorted(key_to_env_var.values())]
    if any(set_env_vars):
        if not all(set_env_vars):
            _LOG.warning("Some but not all AWS env vars are set (%s): ignoring",
                         str(set_env_vars))
        else:
            env_var_override = True
    if env_var_override:
        _LOG.warning("Using AWS credentials from env vars")
        # If one variable is defined all should be defined.
        for key, env_var in key_to_env_var.items():
            _LOG.debug("'%s' in env vars=%s", env_var, env_var in os.environ)
            _LOG.debug(
                "'%s' != ''=%s", env_var, os.environ.get(env_var, None) != ""
            )
            dbg.dassert_in(env_var, os.environ)
            result[key] = os.environ[env_var]
        # TODO(gp): We don't pass this through env var for now.
        result["aws_session_token"] = None
        # TODO(gp): Support also other S3 profiles. We can derive the names of the
        #  env vars from aws_profile. E.g., "am" -> AM_AWS_ACCESS_KEY.
        dbg.dassert_eq(aws_profile, "am")
    else:
        _LOG.warning("Using AWS credentials from files")
        # > more ~/.aws/credentials
        # [am]
        # aws_access_key_id=AKI...
        # aws_secret_access_key=mhg..
        # aws_session_token = Fwo...
        file_name = "credentials"
        config = _get_aws_config(file_name)
        #
        key = "aws_access_key_id"
        result[key] = config.get(aws_profile, key)
        #
        key = "aws_secret_access_key"
        result[key] = config.get(aws_profile, key)
        #
        key = "aws_session_token"
        if config.has_option(aws_profile, key):
            result[key] = config.get(aws_profile, key)
        else:
            result[key] = None
        # > more ~/.aws/config
        # [am]
        # region = us-east-1
        file_name = "config"
        config = _get_aws_config(file_name)
        key = "aws_region"
        result[key] = config.get(aws_profile, "region")
    #
    dbg.dassert_is_subset(key_to_env_var.keys(), result.keys())
    return result


@functools.lru_cache()
def get_key_value(
        aws_profile: str,
        key: str,
) -> Optional[str]:
    """
    Retrieve the value corresponding to `key` for the given `aws_profile`.

    This function accesses the `~/.aws` files or the env vars.
    """
    _LOG.debug("Getting key-value for aws_profile='%s'", aws_profile)
    dbg.dassert_ne(aws_profile, "")
    env_var = key.capitalize()
    env_var_override = env_var in os.environ and os.environ[env_var] != ""
    if env_var_override:
        _LOG.debug("Using '%s' from env vars '%s'", key, env_var)
        value = os.environ[env_var]
    else:
        # > more ~/.aws/credentials
        # [am]
        # aws_s3_bucket=AKI...
        file_name = "credentials"
        config = _get_aws_config(file_name)
        if config.has_option(aws_profile, key):
            value = config.get(aws_profile, key)
        else:
            _LOG.warning("AWS file '%s' doesn't have key '%s' for aws_profile '%s'",
                         file_name, key, aws_profile)
            value = None
    _LOG.debug("key='%s' -> value='%s'", key, value)
    return value


def is_s3_path(path: str) -> bool:
    return path.startswith("s3://")


def get_s3fs(*args: Any, **kwargs: Any) -> s3fs.core.S3FileSystem:
    # From https://stackoverflow.com/questions/62562945
    aws_credentials = get_aws_credentials(*args, **kwargs)
    _LOG.debug("%s", pprint.pformat(aws_credentials))
    s3 = s3fs.core.S3FileSystem(
        anon=False,
        key=aws_credentials["aws_access_key_id"],
        secret=aws_credentials["aws_secret_access_key"],
        token=aws_credentials["aws_session_token"],
        client_kwargs={"region_name": aws_credentials["aws_region"]},
    )
    return s3


# TODO(gp): -> is_s3_path()
def is_valid_s3_path(s3_path: str) -> bool:
    dbg.dassert_isinstance(s3_path, str)
    return s3_path.startswith("s3://")


# TODO(gp): -> dassert_is_s3_path
def check_valid_s3_path(s3_path: str) -> None:
    """
    Assert if a file is not a S3 path.
    """
    dbg.dassert(is_valid_s3_path(s3_path),
                "Invalid S3 file='%s' since it doesn't start with s3://", s3_path)


def dassert_s3_exists(s3_path: str, s3fs: s3fs.core.S3FileSystem) -> None:
    """
    Assert if an S3 file or dir doesn't exist.
    """
    check_valid_s3_path(s3_path)
    dbg.dassert(s3fs.exists(s3_path), "S3 file '%s' doesn't exist", s3_path)


# #############################################################################


def archive_data_on_s3(src_path: str, s3_path: str, aws_profile: str, tag: str = "") -> str:
    """
    Compress `src_path` and save it on AWS S3 under `s3_path`.

    :param s3_path: full S3 path starting with `s3://`
    :param aws_profile: the profile to use
    """
    _LOG.info("# Archiving '%s' to '%s' with aws_profile='%s'", src_path, s3_path, aws_profile)
    dbg.dassert_exists(src_path)
    check_valid_s3_path(s3_path)
    _LOG.info("The size of '%s' is %s", src_path, hsyste.du(src_path, human_format=True))
    # Add a timestamp if needed.
    dst_path = hsyste.append_timestamp_tag(src_path, tag) + ".tgz"
    _LOG.debug("Destination path is '%s'", dst_path)
    # Compress.
    with htimer.TimedScope(logging.INFO, "Compressing") as ts:
        cmd = f"tar czf {dst_path} {src_path}"
        _LOG.debug("cmd=%s", cmd)
        hsyste.system(cmd)
    _LOG.info("The size of '%s' is %s", dst_path, hsyste.du(dst_path, human_format=True))
    # Copy to S3.
    remote_path = os.path.join(s3_path, os.path.basename(dst_path))
    _LOG.info("Copying '%s' to '%s'", dst_path, remote_path)
    dbg.dassert_file_exists(dst_path)
    s3fs = get_s3fs(aws_profile)
    # TODO(gp): Make sure the S3 dir exists.
    s3fs.put(dst_path, remote_path)
    _LOG.info("Data archived on S3 to '%s'", remote_path)
    return remote_path


def retrieve_archived_data_from_s3(remote_path: str, aws_profile: str, dst_dir: str, incremental: bool =True) -> None:
    _LOG.info("# Retrieving archive from '%s' to '%s' with aws_profile='%s'", remote_path, dst_dir, aws_profile)
    #
    dbg.dassert_dir_exists(dst_dir)
    dst_file = os.path.join(dst_dir, os.path.basename(remote_path))
    if incremental and os.path.exists(dst_file):
        _LOG.info("Found '%s': skipping downloading", dst_file)
    else:
        s3fs = get_s3fs(aws_profile)
        dassert_s3_exists(remote_path, s3fs)
        s3fs.get(remote_path, dst_dir)
    #
    dbg.dassert_file_exists(dst_file)
    _LOG.info("Saved to '%s'", dst_file)
    #
    with htimer.TimedScope(logging.INFO, "Decompressing") as ts:
        cmd = f"tar xzf {dst_file} -C {dst_dir}"
        _LOG.debug("cmd=%s", cmd)
        hsyste.system(cmd)
    return dst_file
