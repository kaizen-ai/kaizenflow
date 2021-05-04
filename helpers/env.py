"""
Import as:

import helpers.env as henv
"""

import logging
import platform
from typing import Tuple

import helpers.git as git
import helpers.printing as hprint
import helpers.system_interaction as hsinte

_LOG = logging.getLogger(__name__)

# #############################################################################

# TODO(gp): Merge env in system_interaction or conda.py? Or split the functions.


def _get_version(lib_name: str) -> str:
    try:
        cmd = "import %s" % lib_name
        # pylint: disable=exec-used
        exec(cmd)
    except ImportError:
        version = "ERROR: can't import"
    else:
        cmd = "%s.__version__" % lib_name
        version = eval(cmd)
    return version


def get_system_signature(git_commit_type: str = "all") -> Tuple[str, int]:
    txt = []
    # Add git signature.
    try:
        txt.append("# Git")
        cmd = "git branch --show-current"
        _, branch_name = hsinte.system_to_one_line(cmd)
        txt.append("branch_name='%s'" % branch_name)
        #
        cmd = "git rev-parse --short HEAD"
        _, hash_ = hsinte.system_to_one_line(cmd)
        txt.append("hash='%s'" % hash_)
        #
        num_commits = 3
        if git_commit_type == "all":
            txt.append("# Last commits:")
            log_txt = git.git_log(num_commits=num_commits, my_commits=False)
            txt.append(hprint.indent(log_txt))
        elif git_commit_type == "mine":
            txt.append("# Your last commits:")
            log_txt = git.git_log(num_commits=num_commits, my_commits=True)
            txt.append(hprint.indent(log_txt))
        elif git_commit_type == "none":
            pass
        else:
            raise ValueError("Invalid value='%s'" % git_commit_type)
    except RuntimeError as e:
        _LOG.error(str(e))
    # Add package info.
    txt.append("# Packages")
    packages = []
    packages.append(("python", platform.python_version()))
    # import sys
    # print(sys.version)
    libs = [
        "gluonnlp",
        "gluonts",
        "joblib",
        "mxnet",
        "numpy",
        "pandas",
        "pyarrow",
        "scipy",
        "seaborn",
        "sklearn",
        "statsmodels",
    ]
    libs = sorted(libs)
    failed_imports = 0
    for lib in libs:
        version = _get_version(lib)
        if version.startswith("ERROR"):
            failed_imports += 1
        packages.append((lib, version))
    txt.extend(["%15s: %s" % (l, v) for (l, v) in packages])
    #
    txt = "\n".join(txt)
    return txt, failed_imports
