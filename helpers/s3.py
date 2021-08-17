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
import helpers.io_ as hio  # noqa: E402 module level import not at top of file  # pylint: disable=wrong-import-position
import helpers.printing as hprint  # noqa: E402 module level import not at top of file  # pylint: disable=wrong-import-position
import helpers.system_interaction as hsyste  # noqa: E402 module level import not at top of file  # pylint: disable=wrong-import-position
import helpers.timer as htimer  # noqa: E402 module level import not at top of file  # pylint: disable=wrong-import-position

_LOG = logging.getLogger(__name__)


# ##########################################
# Basic utils.
# ##########################################


def is_s3_path(s3_path: str) -> bool:
    dbg.dassert_isinstance(s3_path, str)
    valid = s3_path.startswith("s3://")
    if s3_path.startswith("s3://s3://"):
        valid = False
    return valid


def dassert_is_s3_path(s3_path: str) -> None:
    """
    Assert if a file is not a S3 path.
    """
    dbg.dassert(
        is_s3_path(s3_path),
        "Invalid S3 file='%s'",
        s3_path,
    )


def dassert_is_not_s3_path(s3_path: str) -> None:
    """
    Assert if a file is a S3 path.
    """
    dbg.dassert(
        not is_s3_path(s3_path),
        "Passed an S3 file='%s' when it was not expected",
        s3_path,
    )

def dassert_s3_exists(s3_path: str, s3fs_: s3fs.core.S3FileSystem) -> None:
    """
    Assert if an S3 file or dir doesn't exist.
    """
    dassert_is_s3_path(s3_path)
    dbg.dassert(s3fs_.exists(s3_path), "S3 file '%s' doesn't exist", s3_path)


def dassert_s3_not_exists(s3_path: str, s3fs_: s3fs.core.S3FileSystem) -> None:
    """
    Assert if an S3 file or dir exist.
    """
    dassert_is_s3_path(s3_path)
    dbg.dassert(not s3fs_.exists(s3_path), "S3 file '%s' already exist", s3_path)


def split_path(s3_path: str) -> str:
    """
    Separate an S3 path in the bucket and the rest of the path as absolute from the root.

    E.g., for `s3://alphamatic-data/tmp/hello` returns (`alphamatic-data`, /tmp/hello`)
    """
    dassert_is_s3_path(s3_path)
    # Remove the s3 prefix.
    prefix = "s3://"
    dbg.dassert(s3_path.startswith(prefix))
    s3_path = s3_path[len(prefix):]
    # Break the path into dirs.
    dirs = s3_path.split("/")
    bucket = dirs[0]
    abs_path = os.path.join("/", *dirs[1:])
    dbg.dassert(abs_path.startswith("/"), "The path should be absolute instead of %s",
                abs_path)
    return bucket, abs_path


# ###########################################
# Bucket
# ###########################################


# TODO(gp): Merge with get_path() to create get_s3_am_bucket_path().
#  Avoid to use alphamatic-data but rather use this.
def get_bucket() -> str:
    """
    Return the S3 bucket pointed by AM_S3_BUCKET (e.g., `alphamatic-data`).

    The name should not start with `s3://`.

    Make sure your ~/.aws/credentials uses the right key to access this
    bucket as default.
    """
    # TODO(gp): -> AM_AWS_S3_BUCKET
    env_var = "AM_S3_BUCKET"
    dbg.dassert_in(env_var, os.environ)
    s3_bucket = os.environ[env_var]
    dbg.dassert(
        not s3_bucket.startswith("s3://"),
        "Invalid %s value '%s'",
        env_var,
        s3_bucket,
    )
    return s3_bucket


# TODO(gp): -> get_bucket_path() ?
def get_path() -> str:
    """
    Return the path to the default S3 bucket (e.g., `s3://alphamatic-data`).
    """
    path = "s3://" + get_bucket()
    return path


# #############################################################################
# Parser.
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
        help="Full S3 dir path to use (e.g., `s3://alphamatic-data/foobar/`), "
        "overriding any other setting",
    )
    return parser


def _get_variable_value(var_value: Optional[str], env_var: str) -> str:
    """
    Get the variable from the environment if `var_value` is `None`.
    """
    _LOG.debug("var_value=%s", var_value)
    if var_value is None:
        dbg.dassert_isinstance(env_var, str)
        _LOG.debug("Using the env var '%s'", env_var)
        dbg.dassert_in(env_var, os.environ, "Env var '%s' is not set", env_var)
        var_value = os.environ[env_var]
    else:
        dbg.dassert_isinstance(var_value, str)
        _LOG.debug("Using the passed value '%s'", var_value)
    return var_value


def get_aws_profile(aws_profile: Optional[str] = None) -> str:
    """
    Return the AWS profile to access S3, based on:

    - argument passed
    - command line option (i.e., `args.aws_profile`)
    - env vars (i.e., `AM_AWS_PROFILE`)
    """
    env_var = "AM_AWS_PROFILE"
    aws_profile = _get_variable_value(aws_profile, env_var)
    return aws_profile


def get_s3_path(s3_path: Optional[str] = None) -> Optional[str]:
    """
    Return the S3 path to use, based on:

    - argument passed
    - command line option (i.e., `--s3_path` through `args.s3_path`)
    - env vars (i.e., `AM_S3_BUCKET`)
    """
    env_var = "AM_S3_BUCKET"
    s3_path = _get_variable_value(s3_path, env_var)
    dassert_is_s3_path(s3_path)
    return s3_path


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


# #############################################################################
# Authentication.
# #############################################################################

# Architecture of the AWS authentication
#
# - There can be two or more AWS S3 systems with different credentials, paths to
#   bucket, and other properties
# - Some code needs to refer always and only to the AM S3 bucket (e.g., for Kibot
#   data)
# - Other code needs to work with different AWS S3 systems (e.g., publish_notebooks,
#   saving / retrieving experiments, caching)
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
    set_env_vars = [
        (env_var in os.environ and os.environ[env_var] != "")
        for env_var in sorted(key_to_env_var.values())
    ]
    if any(set_env_vars):
        if not all(set_env_vars):
            _LOG.warning(
                "Some but not all AWS env vars are set (%s): ignoring",
                str(set_env_vars),
            )
        else:
            env_var_override = True
    if env_var_override:
        _LOG.debug("Using AWS credentials from env vars")
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
        _LOG.debug("Using AWS credentials from files")
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
    value: Optional[str] = None
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
            _LOG.warning(
                "AWS file '%s' doesn't have key '%s' for aws_profile '%s'",
                file_name,
                key,
                aws_profile,
            )
    _LOG.debug("key='%s' -> value='%s'", key, value)
    return value


def get_s3fs(*args: Any, **kwargs: Any) -> s3fs.core.S3FileSystem:
    """
    Return an s3fs object.

    Same parameters as `get_aws_credentials()`.
    """
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


# #############################################################################
# Archive and retrieve data from S3.
# #############################################################################


def archive_data_on_s3(
    src_dir: str, s3_path: str, aws_profile: Optional[str], tag: str = ""
) -> str:
    """
    Compress dir `src_dir` and save it on AWS S3 under `s3_path`.

    A timestamp and a tag is added to make the name more informative.
    The tgz is created so that when expanded a dir with the name `src_dir` is
    created.

    :param s3_path: full S3 path starting with `s3://`
    :param aws_profile: the profile to use
    :param tag: a tag to add to the name of the file
    """
    aws_profile = get_aws_profile(aws_profile)
    _LOG.info(
        "# Archiving '%s' to '%s' with aws_profile='%s'",
        src_dir,
        s3_path,
        aws_profile,
    )
    dbg.dassert_dir_exists(src_dir)
    dassert_is_s3_path(s3_path)
    _LOG.info(
        "The size of '%s' is %s", src_dir, hsyste.du(src_dir, human_format=True)
    )
    # Add a timestamp if needed.
    dst_path = hsyste.append_timestamp_tag(src_dir, tag) + ".tgz"
    # Compress the dir.
    # > (cd .../TestRunExperimentArchiveOnS3.test_serial1; \
    #    tar cvzf /app/.../TestRunExperimentArchiveOnS3.test_serial1.tgz experiment.RH1E)
    # experiment.RH1E/
    # experiment.RH1E/log.20210802-123758.txt
    # experiment.RH1E/output_metadata.json
    # ...
    _LOG.debug("Destination path is '%s'", dst_path)
    with htimer.TimedScope(logging.INFO, "Compressing"):
        dir_name = os.path.dirname(src_dir)
        base_name = os.path.basename(src_dir)
        dbg.dassert_ne(base_name, "", "src_dir=%s", src_dir)
        cmd = ""
        if dir_name != "":
            cmd += f"cd {dir_name} && "
        cmd += f"tar czf {dst_path} {base_name}"
        hsyste.system(cmd)
    _LOG.info(
        "The size of '%s' is %s", dst_path, hsyste.du(dst_path, human_format=True)
    )
    # Test expanding the tgz. The package should expand to the original dir.
    # > tar tf /app/.../TestRunExperimentArchiveOnS3.test_serial1.tgz
    # experiment.RH1E/
    # experiment.RH1E/log.20210802-123758.txt
    # experiment.RH1E/output_metadata.json
    _LOG.info("Testing archive")
    cmd = f"tar tvf {dst_path}"
    hsyste.system(cmd, log_level=logging.INFO, suppress_output=False)
    # Copy to S3.
    s3_file_path = os.path.join(s3_path, os.path.basename(dst_path))
    _LOG.info("Copying '%s' to '%s'", dst_path, s3_file_path)
    dbg.dassert_file_exists(dst_path)
    s3fs_ = get_s3fs(aws_profile)
    # TODO(gp): Make sure the S3 dir exists.
    s3fs_.put(dst_path, s3_file_path)
    _LOG.info("Data archived on S3 to '%s'", s3_file_path)
    return s3_file_path


def retrieve_archived_data_from_s3(
    s3_file_path: str,
    dst_dir: str,
    aws_profile: Optional[str] = None,
    incremental: bool = True,
) -> str:
    """
    Retrieve archived tgz data from S3.

    E.g.,
    - given a tgz file like `s3://.../experiment.20210802-121908.tgz` (which is the
      result of compressing a dir like `/app/.../experiment.RH1E`)
    - expand it into a dir `{dst_dir}/experiment.RH1E`

    :param s3_file_path: path to the S3 file with the archived data. E.g.,
       `s3://.../experiment.20210802-121908.tgz`
    :param dst_dir: directory where expand the archive tarball
    :return: dir with the expanded data (e.g., `{dst_dir/experiment.RH1E`)
    """
    aws_profile = get_aws_profile(aws_profile)
    _LOG.info(
        "# Retrieving archive from '%s' to '%s' with aws_profile='%s'",
        s3_file_path,
        dst_dir,
        aws_profile,
    )
    dassert_is_s3_path(s3_file_path)
    # Download the tgz file.
    hio.create_dir(dst_dir, incremental=True)
    dst_file = os.path.join(dst_dir, os.path.basename(s3_file_path))
    _LOG.debug(hprint.to_str("s3_file_path dst_dir dst_file"))
    if incremental and os.path.exists(dst_file):
        _LOG.info("Found '%s': skipping downloading", dst_file)
    else:
        s3fs_ = get_s3fs(aws_profile)
        dassert_s3_exists(s3_file_path, s3fs_)
        _LOG.debug("Getting from s3: '%s' -> '%s", s3_file_path, dst_file)
        s3fs_.get(s3_file_path, dst_file)
        _LOG.info("Saved to '%s'", dst_file)
    # Expand the tgz file.
    # The output should be the original compressed dir under `{dst_dir}`.
    # E.g.,
    # > tar tzf /app/.../TestRunExperimentArchiveOnS3.test_serial1/experiment.20210802-133901.tgz
    # experiment.RH1E/
    # experiment.RH1E/log.20210802-133859.txt
    # experiment.RH1E/result_0/
    with htimer.TimedScope(logging.INFO, "Decompressing"):
        dbg.dassert_file_exists(dst_file)
        cmd = f"cd {dst_dir} && tar xzf {dst_file}"
        hsyste.system(cmd)
    # Get the name of the including dir, e.g., `experiment.RH1E`.
    cmd = f"cd {dst_dir} && tar tzf {dst_file} | head -1"
    rc, enclosing_tgz_dir_name = hsyste.system_to_one_line(cmd)
    _ = rc
    _LOG.debug(hprint.to_str("enclosing_tgz_dir_name"))
    tgz_dst_dir = os.path.join(dst_dir, enclosing_tgz_dir_name)
    dbg.dassert_dir_exists(tgz_dst_dir)
    # Return `{dst_dir}/experiment.RH1E`.
    return tgz_dst_dir
