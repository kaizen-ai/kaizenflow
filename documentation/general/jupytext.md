<!--ts-->
<!--te-->

# Jupytext

## Why Jupytext?

-   In few words [Jupytext](https://github.com/mwouts/jupytext) associates a
    python representation to notebooks, which is kept in sync with the notebook
    in a sensible way

-   Jupytext allows to:
    -   Edit notebooks with your favorite editor (hopefully) vi or PyCharm
    -   Keep a python version of your notebook so that you can run long
        computations from shell instead of using notebooks
    -   Do a code review using the python code
    -   Diff changes or resolve conflicts using `git` (although
        `git_diff_notebook.py` and `git_merge.py` achieve the same goal)

## Reference documentation

-   [https://github.com/mwouts/jupytext](https://github.com/mwouts/jupytext)
-   [https://jupytext.readthedocs.io/en/latest/](https://jupytext.readthedocs.io/en/latest/)
-   [https://jupytext.readthedocs.io/en/latest/faq.html](https://jupytext.readthedocs.io/en/latest/faq.html)

## Installation

-   It is part of `*_develop` package

-   To install stand-alone

    ```bash
    > conda install -c conda-forge jupytext
    ```

-   Check what version you got:

    ```bash
    > jupytext --version
    1.2.1

    > conda list | grep jupytext
    jupytext                  1.2.1                         0    conda-forge
    ```

-   Check if you have a Jupyter config:

    ```bash
    > ls ~/.jupyter/jupyter_notebook_config.py
    ```

    if not generate it with:

    ```bash
    > jupyter notebook --generate-config
    ```

    edit `.jupyter/jupyter_notebook_config.py` and append the following:

    ```python
    #------------------------------------------------------------------------------
    # Jupytext
    #------------------------------------------------------------------------------
    c.NotebookApp.contents_manager_class = "jupytext.TextFileContentsManager"
    # Always pair ipynb notebooks to py files
    c.ContentsManager.default_jupytext_formats = "ipynb,py"
    # Use the percent format when saving as py
    c.ContentsManager.preferred_jupytext_formats_save = "py:percent"
    c.ContentsManager.outdated_text_notebook_margin = float("inf")
    ```

-   Now you need to restart the notebook server to pick up jupytext
-   We use the "percent" format where cells are delimited by a `%%` comment
    (Pycharm, black, and other tools understand / respect that this is a
    delimiter for jupyter cells)

## Using Jupytext

-   Remember that now when you `git add` a .ipynb file you need to add also the
    corresponding .py file.
-   Same thing if you rename (git mv) or delete a notebook. You need to
    explicitly take care of renaming and deleting also the .py file.
-   You can use PyCharm or your favorite editor (hopefully vim) to edit the .py
    file, when you want to do a refactoring, a replacement, or run the linter

## Example of uses

### Test that the conversion works {#test-that-the-conversion-works}

-   Jupytext keeps a notebook and the paired .py file in sync
    ```bash
    > jupytext Task22.ipynb --test --to py:percent -x
    > jupytext Task22.ipynb --test-strict --to py:percent
    ```

### Automatic syncing when using the server

-   Open a notebook with your notebook server (after you have installed the
    jupytext extension)

    -   You should see that there is a `.py` file close to the `.ipynb` file you
        opened
    -   Open the `.py` file
    -   You can see that there is the code in the cells separated by `%%`

-   Changes to the notebook are reflected in the file:

    -   Modify the notebook and save the notebook
    -   Open the `.py`
    -   Note that the cell that you modified in the notebook has changed in the
        `.py` file

-   Changes to the file are reflected in the notebook:
    -   Modify the `.py` file, e.g., changing one cell
    -   Go to the jupyter notebook and reload it
    -   The cell you modified has changed!

### Make sure that a notebook has a companion

    ```bash
    > jupytext --sync --to py:percent XYZ.ipynb
    ```

### Manual sync

    ```bash
    > jupytext --sync --to py:percent XYZ.ipynb
    ```

### To convert a notebook to script

    ```bash
    > jupytext --to py:percent XYZ.ipynb
    ```

### To convert a script into a notebook

    ```bash
    # Preserve output of the notebook
    > jupytext --to notebook --update XYZ.py
    ```

-   Otherwise
    ```bash
    > jupytext --to notebook XYZ.py
    ```

### Linter

-   The linter automatically reformats the `.py` files and then updates the
    `.ipynb` without losing the formatting
    ```bash
    > linter.py --file XYZ.py
    jupytext --sync --to py:percent XYZ.py
    ```

### Refresh all the scripts

-   The script `dev_scripts/notebooks/process_jupytext.py` automates some of the
    workflow in Jupytext (see the help)

-   The script `dev_scripts/notebooks/process_all_jupytext.sh` applies
    `process_jupytext.py` to all the `ipynb` files
    ```bash
    > dev_scripts/notebooks/process_all_jupytext.sh <ACTION>
    ```
