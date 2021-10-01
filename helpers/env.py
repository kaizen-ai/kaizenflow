"""
Import as:

import helpers.env as henv
"""

import logging
from typing import List, Tuple

import helpers.git as hgit
import helpers.printing as hprintin
import helpers.system_interaction as hsyint
import helpers.versioning as hversion

_LOG = logging.getLogger(__name__)

# #############################################################################

# TODO(gp): Merge env in system_interaction?


def _get_library_version(lib_name: str) -> str:
    try:
        cmd = "import %s" % lib_name
        # pylint: disable=exec-used
        exec(cmd)
    except ImportError:
        version = "?"
    else:
        cmd = "%s.__version__" % lib_name
        version = eval(cmd)
    return version


def _append(
    txt: List[str], to_add: List[str], num_spaces: int = 4
) -> Tuple[List[str], List[str]]:
    txt.extend(
        [
            " " * num_spaces + line
            for txt_tmp in to_add
            for line in txt_tmp.split("\n")
        ]
    )
    to_add: List[str] = []
    return txt, to_add


def get_system_signature(git_commit_type: str = "all") -> Tuple[str, int]:
    # TODO(gp): This should return a string that we append to the rest.
    hversion.check_version()
    #
    txt: List[str] = []
    # Add git signature.
    txt.append("# Git")
    txt_tmp: List[str] = []
    try:
        cmd = "git branch --show-current"
        _, branch_name = hsyint.system_to_one_line(cmd)
        txt_tmp.append("branch_name='%s'" % branch_name)
        #
        cmd = "git rev-parse --short HEAD"
        _, hash_ = hsyint.system_to_one_line(cmd)
        txt_tmp.append("hash='%s'" % hash_)
        #
        num_commits = 3
        if git_commit_type == "all":
            txt_tmp.append("# Last commits:")
            log_txt = hgit.git_log(num_commits=num_commits, my_commits=False)
            txt_tmp.append(hprintin.indent(log_txt))
        elif git_commit_type == "mine":
            txt_tmp.append("# Your last commits:")
            log_txt = hgit.git_log(num_commits=num_commits, my_commits=True)
            txt_tmp.append(hprintin.indent(log_txt))
        elif git_commit_type == "none":
            pass
        else:
            raise ValueError("Invalid value='%s'" % git_commit_type)
    except RuntimeError as e:
        _LOG.error(str(e))
    txt, txt_tmp = _append(txt, txt_tmp)
    # Add processor info.
    txt.append("# Machine info")
    txt_tmp: List[str] = []
    import platform

    uname = platform.uname()
    txt_tmp.append(f"system={uname.system}")
    txt_tmp.append(f"node name={uname.node}")
    txt_tmp.append(f"release={uname.release}")
    txt_tmp.append(f"version={uname.version}")
    txt_tmp.append(f"machine={uname.machine}")
    txt_tmp.append(f"processor={uname.processor}")
    try:
        import psutil

        has_psutil = True
    except ModuleNotFoundError as e:
        print(e)
        has_psutil = False
    if has_psutil:
        txt_tmp.append("cpu count=%s" % psutil.cpu_count())
        txt_tmp.append("cpu freq=%s" % str(psutil.cpu_freq()))
        # TODO(gp): Report in MB or GB.
        txt_tmp.append("memory=%s" % str(psutil.virtual_memory()))
        txt_tmp.append("disk usage=%s" % str(psutil.disk_usage("/")))
        txt, txt_tmp = _append(txt, txt_tmp)
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
        version = _get_library_version(lib)
        if version.startswith("ERROR"):
            failed_imports += 1
        packages.append((lib, version))
    txt_tmp.extend(["%s: %s" % (l, v) for (l, v) in packages])
    txt, txt_tmp = _append(txt, txt_tmp)
    #
    txt = "\n".join(txt)
    return txt, failed_imports
