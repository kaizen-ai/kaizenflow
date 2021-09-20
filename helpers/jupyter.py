"""
Import as:

import helpers.jupyter as hjupyte
"""

import os

import helpers.dbg as hdbg
import helpers.io_ as hio
import helpers.system_interaction as hsysinte



def run_notebook(
    file_name: str,
    scratch_dir: str,
    *,
    pre_cmd: str = "",
) -> None:
    """
    Run jupyter notebook.

    Assert if the notebook doesn't complete successfully.

    :param file_name: path to the notebook to run. If this is a .py file,
        convert to .ipynb first
    :param scratch_dir: temporary dir storing the output
    :param pre_cmd:
    """
    file_name = os.path.abspath(file_name)
    hdbg.dassert_exists(file_name)
    hio.create_dir(scratch_dir, incremental=True)
    # Build command line.
    cmd = []
    if pre_cmd:
        cmd.append(f"{pre_cmd} &&")
    # Convert .py file into .ipynb if needed.
    root, ext = os.path.splitext(file_name)
    if ext == ".ipynb":
        notebook_name = file_name
    elif ext == ".py":
        cmd.append(f"jupytext --update --to notebook {file_name};")
        notebook_name = f"{root}.ipynb"
    else:
        raise ValueError(f"Unsupported file format for `file_name`='{file_name}'")
    # Execute notebook.
    cmd.append("cd %s &&" % scratch_dir)
    cmd.append("jupyter nbconvert %s" % notebook_name)
    cmd.append("--execute")
    cmd.append("--to html")
    cmd.append("--ExecutePreprocessor.kernel_name=python")
    # No time-out.
    cmd.append("--ExecutePreprocessor.timeout=-1")
    # Execute.
    cmd_as_str = " ".join(cmd)
    hsysinte.system(cmd_as_str, abort_on_error=True, suppress_output=False)
