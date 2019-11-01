## Write beautiful code, even in notebooks
- Follow the conventions and suggestions for Python code
    - E.g., `code_style.md`, `design_philosophy.md`
- When prototyping with a notebook, notebook code can be of a little lower
  quality than code, but still needs to be readable and robust
    - IMO it's just better to always do write robust and readable code: it
      doesn't buy much time to cut corners

## General
- Print a few lines of data structures (e.g., `df.head(3)`) so one can see how
  data is transformed through the cells

## Use keyboard shortcuts
- Learn the default keyboard shortcuts to edit efficiently

## 
- Always make the notebook easy to be understood and run by somebody else

## Dependencies among cells
- Try to avoid dependencies between cells: ideally avoid any dependency

- E.g., put all the imports in one cell at the beginning, so with one cell
  execution you can make sure that all the imports are done
    - Compare this with the case where the imports are randomly sprinkled in the
      notebook, then you need to go execute them one by one if you re-initialize
      the notebook

- For the same reason group functions in one cell that you can easily re-execute

## Re-execute from scratch
- Once in a while (e.g., once a day) make sure you can re-execute everything from
  the top with `Kernel -> Restart & Clean output` and then `Kernel -> Run all`
    - Then visually verify that the results didn't change, so that there is no
      weird state or dependency in the code

- Before a commit (and definitively before a PR) do a clean run

## Comment 
-  Explain in a comment what a cell does, when a cell is too long, e.g.,
    ```python
    # Count stocks with all nans.
    num_nans = np.isnan(rets).sum(axis=0)
    num_nans /= rets.shape[0]

    num_nans.sort_values(ascending=False, inplace=True)

    num_stocks_with_no_nans = (num_nans == 0.0).sum()
    print "num_stocks_with_no_nans=%s" % perc(num_stocks_with_no_nans, rets.shape[1])
    ```

- Try to factor out code in cells that are too complicated into functions so that
  you can call it multiple times, and the flow is simple

## Do not cut & paste code
- It's *never* a good idea
- It takes more time to clean up than doing write in the first place
    - Just make a function and call it!

## Avoid "wall-of-code" cell

## Avoid data biases
- Try to compute statistics on the entire data set so that results are
  representative and not dependent on a particular slice of the data
- If it takes too long to compute statistics on the entire data set, report the
  problem and we can think of solutions

## Avoid hardwired constants
- Don't use hardwired constants, but try to parametrize the code

## 
- If you are using data from a file (e.g., `/data/wd/RP_1yr_13_companies.pkl`),
  explain in a comment how the file was generated
    - Ideally report a command line to regenerate the data

- Describe the data flow across notebooks so one can regenerate the data

## 
- Try to use Eastern Times (ET) since typically financial data refers to New York
  time
    ```python
    datetime_ET = df.tz_localize(pytz.timezone('UTC')).tz_convert('US/Eastern')
    ```
- If you don't use timezone info `tzinfo` clarify in the variable name what
  timezone is used (e.g., `datetime_ET` instead of `datetime`)

## Add a description for a notebook
- A notebook can be used for:
    - analysis
        - the notebook should always work so we need to treat it as part of the
          code base
        - we might want to add unit tests for it
    - tutorial / gallery
        - shows how some code works (e.g., functions in `signal_processing.py`)
        - E.g., `data_encyclopedia.ipynb`
        - the code should always work
        - we might want to add unit tests for it
    - prototyping
        - E.g., 
            - we prototyped some code, before it becomes library code
            - we did some one-off analysis

- At the top of the notebook add a markdown cell explaining what this notebook
  does, e.g.,
    ```python
    ## Description
    - This notebook was used for prototyping / debugging code that was moved
      in the file `abc.py`
    ```

## General format of a notebook
- Typically we use as first cells the following ones:
    0) description
        ```python
        ## Description
        - This notebook was used for prototyping / debugging code that was moved
          in the file `abc.py`
        ```

    1) import the needed libraries: it's better to put all the imports in one
        cell:
        ```python
        ## Imports

        %load_ext autoreload
        %autoreload 2
        import logging
        import os

        import matplotlib.pyplot as plt
        import pandas as pd

        import core.explore as exp
        import core.signal_processing as sigp
        ...
        ```

    2) configuration
        ```python
        # Print system signature.
        print(env.get_system_signature())

        # Configure the notebook style.
        pri.config_notebook()

        # Configure logger.
        dbg.init_logger(verb=logging.INFO)
        _LOG = logging.getLogger(__name__)

        # Packages
                 python: 3.7.3
                 joblib: 0.14.0
                  numpy: 1.17.3
                 pandas: 0.25.2
                pyarrow: 0.15.0
                  scipy: 1.3.1
                seaborn: 0.9.0
                sklearn: 0.21.3
            statsmodels: 0.10.1
        # Last commits:
          * 3c11dd7 Julia    PartTask461: Add correlation and autocorrelation analysis         (  22 hours ago) Thu Oct 31 14:31:14 2019  (HEAD -> PartTask461_PRICE_Familiarize_with_target_commodities, origin/PartTask461_PRICE_Familiarize_with_target_commodities)
          * 99417bc Julia    PartTask418: Address a TODO in _normalize_1_min()                 (    2 days ago) Wed Oct 30 07:48:34 2019
          * 6ad45a8 saggese  More docs and lint                                                (    3 days ago) Tue Oct 29 21:31:55 2019
        WARNING: Running in Jupyter
        ```

# Plots

## Use the proper y-scale
- E.g., if one quantity can vary from -1.0 to 1.0 force the y-scale between those
  limits so that the values are absolutes, unless this would squash the plot

## Make each plot self-explanatory
- Make sure that each plot has a descriptive title, x and y label
- Explain the set-up of a plot / analysis
    - E.g., what is the universe of stock used? What is the period of time?
    - Add this information also to the plots

## Avoid wall-of-text tables
- Try to use plots summarizing the results besides the raw results in a table

## Use common axes to allow visual comparisons
- Try to use same axes for multiple graphs when possible to allow visual
  comparison between graphs
- If that's not possible or convenient make individual plots with different
  scales and add a plot with multiple graphs inside on the same axis (e.g., with
  y-log)

## Use the right plot
- Pick the right type of graph to make your point
    - pandas, seaborn, matplotlib are your friends

# Notebooks and libraries

- It's ok to use functions in notebooks when building the analysis to leverage
  notebook interactivity
- Once the notebook is "stable", often it's better to move the code in a library,
  i.e., a python file.

## Pros
- The same notebook code can be used for different notebooks
    - E.g., the function to read the data from disk is an obvious example
- More people can reuse the same code for different analyses
- If one changes the code in a library, Git can help tracking changes and
  merging, while notebooks are difficult to diff / merge
- Cleaning up / commenting / untangling the code can help reason carefully about
  the assumptions to find issues
- The notebook becomes more streamlined and easy to understand since now it's a
  sequence of functions `do_this_and_that` and presenting the results
- One can speed up / parallelize analyses with multiprocessing
    - Notebooks are not great for this
    - E.g., when one does the analyses on a small subset of the data and then
      wants to run on the entire large dataset
- The exploratory analysis can be moved towards modeling and then production

## Cons
- One disadvantage is that changes that were immediate in the notebook are not
  immediate anymore
    - That's actually not true, since using
        ```python
        %load_ext autoreload
        %autoreload 2
        ```
      the notebook reads the changes automatically and you don't even need to
      execute the cell with the change
    - One doesn't have to scroll back and forth to execute the cell with the
      functions with all the possible mistakes

# Make the notebook flow clear

- Each notebook needs to follow a clear and logical flow, e.g.,
    - load data
        - for each data structure it’s good to show a snippet, e.g., df.head(3)
    - compute stats / results
    - clean data
    - compute stats, show some results
    - do analysis
    - show results

- The flow should be highlighted using headings in markdown:
    ```python
    # Level 1

    ## Level 2

    ### Level 3
    ```

- Use the extension for navigating the notebook (see our suggestions for Jupyter
  plug-ins)

- Keep related code and analysis close together so
    - you and others can understand the logical flow
    - one could split the notebook in parts (e.g., when it becomes too big) and
      it will not break when you want to re-run a section of the notebook, so you
      can do “Cells -> Run all below” after one heading, instead of having to do
      “Cells -> Run all”
    - you can collapse the cells and don’t scroll back and forth too much

# Useful plugins

- You can access the extensions menu:
    - `Edit -> nbextensions config`
    - `http://localhost:XYZ/nbextensions/`

## Vim bindings
- [VIM binding](https://github.com/lambdalisue/jupyter-vim-binding/wiki/Installation)
  will change your life

## Table of content (2)
- To see the entire logical flow of the notebook, when you use the headers
  properly

## ExecuteTime
- To see how long each cell takes to execute

## Spellchecker

## AutoSaveTime
- To save the code automatically every minute

## Notify
- Show a browser notification when kernel becomes idle

## Jupytext
