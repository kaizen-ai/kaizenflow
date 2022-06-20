"""
Import as:

import helpers.lib_tasks as hlibtask
"""

import logging

# We want to minimize the dependencies from non-standard Python packages since
# this code needs to run with minimal dependencies and without Docker.

# Import this way to avoid complexity in propagating the refactoring in all
# the repos downstream.
from helpers.lib_tasks_docker import *  # isort:skip  # noqa: F401,F403 # pylint: disable=unused-import,unused-wildcard-import,wildcard-import
from helpers.lib_tasks_docker_release import *  # isort:skip  # noqa: F401,F403 # pylint: disable=unused-import,unused-wildcard-import,wildcard-import
from helpers.lib_tasks_find import *  # isort:skip  # noqa: F401,F403 # pylint: disable=unused-import,unused-wildcard-import,wildcard-import
from helpers.lib_tasks_gh import *  # isort:skip  # noqa: F401,F403 # pylint: disable=unused-import,unused-wildcard-import,wildcard-import
from helpers.lib_tasks_git import *  # isort:skip  # noqa: F401,F403 # pylint: disable=unused-import,unused-wildcard-import,wildcard-import
from helpers.lib_tasks_integrate import *  # isort:skip  # noqa: F401,F403 # pylint: disable=unused-import,unused-wildcard-import,wildcard-import
from helpers.lib_tasks_lint import *  # isort:skip  # noqa: F401,F403 # pylint: disable=unused-import,unused-wildcard-import,wildcard-import
from helpers.lib_tasks_perms import *  # isort:skip  # noqa: F401,F403 # pylint: disable=unused-import,unused-wildcard-import,wildcard-import
from helpers.lib_tasks_print import *  # isort:skip  # noqa: F401,F403 # pylint: disable=unused-import,unused-wildcard-import,wildcard-import
from helpers.lib_tasks_pytest import *  # isort:skip  # noqa: F401,F403 # pylint: disable=unused-import,unused-wildcard-import,wildcard-import
from helpers.lib_tasks_utils import *  # isort:skip  # noqa: F401,F403 # pylint: disable=unused-import,unused-wildcard-import,wildcard-import

_LOG = logging.getLogger(__name__)


# Conventions around `pyinvoke`:
# - `pyinvoke` uses introspection to infer properties of a task, but doesn't
#   support many Python3 features (see https://github.com/pyinvoke/invoke/issues/357)
# - Don't use type hints in `@tasks`
#   - we use `# ignore: type` to avoid mypy complaints
# - Minimize the code in `@tasks` calling other functions to use Python3 features
# - Use `""` as default instead None since `pyinvoke` can only infer a single type


# pylint: disable=line-too-long
# From https://stackoverflow.com/questions/34878808/finding-docker-container-processes-from-host-point-of-view
# Convert Docker container to processes id
# for i in $(docker container ls --format "{{.ID}}"); do docker inspect -f '{{.State.Pid}} {{.Name}}' $i; done
# 7444 /compose_app_run_d386dc360071
# 8857 /compose_jupyter_server_run_7575f1652032
# 1767 /compose_app_run_6782c2bd6999
# 25163 /compose_app_run_ab27e17f2c47
# 18721 /compose_app_run_de23819a6bc2
# pylint: enable=line-too-long
