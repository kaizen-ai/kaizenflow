<!--ts-->
   * [Style guide references](#style-guide-references)
      * [References](#references)
   * [Naming conventions](#naming-conventions)
      * [Finding the best names](#finding-the-best-names)
      * [Horrible names](#horrible-names)
      * [No Hungarian notation please](#no-hungarian-notation-please)
      * [No code stutter](#no-code-stutter)
   * [Comments](#comments)
      * [Docstring conventions](#docstring-conventions)
      * [reST style](#rest-style)
      * [Descriptive vs imperative style](#descriptive-vs-imperative-style)
      * [Use type hints](#use-type-hints)
      * [Replace empty lines in code with comments](#replace-empty-lines-in-code-with-comments)
      * [Avoid distracting comments](#avoid-distracting-comments)
      * [If you find a bug or obsolete docstring/TODO in the code](#if-you-find-a-bug-or-obsolete-docstringtodo-in-the-code)
      * [Referring to an object in code comments](#referring-to-an-object-in-code-comments)
      * [Inline comments](#inline-comments)
   * [Linter](#linter)
      * [Disabling linter messages](#disabling-linter-messages)
      * [Prefer non-inlined linter comments](#prefer-non-inlined-linter-comments)
   * [Logging](#logging)
      * [Always use logging instead of prints](#always-use-logging-instead-of-prints)
      * [Our logging idiom](#our-logging-idiom)
      * [Logging level](#logging-level)
      * [Use positional args when logging](#use-positional-args-when-logging)
      * [Exceptions don't allow positional args](#exceptions-dont-allow-positional-args)
      * [Report warnings](#report-warnings)
   * [Assertions](#assertions)
      * [Use positional args when asserting](#use-positional-args-when-asserting)
      * [Report as much information as possible in an assertion](#report-as-much-information-as-possible-in-an-assertion)
   * [Imports](#imports)
      * [Importing code from a Git submodule](#importing-code-from-a-git-submodule)
      * [Don't use evil import *](#dont-use-evil-import-)
      * [Cleaning up the evil import *](#cleaning-up-the-evil-import-)
      * [Avoid from ... import ...](#avoid-from--import-)
      * [Examples of imports](#examples-of-imports)
      * [Exceptions to the import style](#exceptions-to-the-import-style)
      * [Always import with a full path from the root of the repo / submodule](#always-import-with-a-full-path-from-the-root-of-the-repo--submodule)
      * [Baptizing module import](#baptizing-module-import)
   * [Python scripts](#python-scripts)
      * [Use Python and not bash for scripting](#use-python-and-not-bash-for-scripting)
      * [Skeleton for a script](#skeleton-for-a-script)
      * [Some useful patterns](#some-useful-patterns)
      * [Use scripts and not notebooks for long-running jobs](#use-scripts-and-not-notebooks-for-long-running-jobs)
      * [Python executable characteristics](#python-executable-characteristics)
      * [Use clear names for the scripts](#use-clear-names-for-the-scripts)
   * [Functions](#functions)
      * [Arguments](#arguments)
      * [Try to make functions work on multiple types](#try-to-make-functions-work-on-multiple-types)
      * [Avoid hard-wired column name dependencies](#avoid-hard-wired-column-name-dependencies)
   * [Misc (to reorg)](#misc-to-reorg)
      * [Write robust code](#write-robust-code)
      * [Capitalized words](#capitalized-words)
      * [Regex](#regex)
      * [Order of functions in a file](#order-of-functions-in-a-file)
      * [Use layers design pattern](#use-layers-design-pattern)
      * [Write complete if-then-else](#write-complete-if-then-else)
      * [Do not be stingy at typing](#do-not-be-stingy-at-typing)
      * [Research quality vs production quality](#research-quality-vs-production-quality)
      * [No ugly hacks](#no-ugly-hacks)
      * [Life cycle of research code](#life-cycle-of-research-code)
   * [Document what notebooks are for](#document-what-notebooks-are-for)



<!--te-->

# Style guide references

- We care about consistency rather than arguing about which approach is better
    - E.g., see "tab vs space" flame-war from the 90s
- Unless explicitly noted we prefer to follow the style guide below

- As a rule of thumb we default to the Google style guidelines, unless the
  Python community (in the form of [PEPs](https://www.python.org/dev/peps/) or
  the tools we rely upon favor another style

## References

- [Google Python Style Guide (GPSG)](https://google.github.io/styleguide/pyguide.html)

- Commenting style
    - `http://www.sphinx-doc.org/en/master/`
    - `https://thomas-cokelaer.info/tutorials/sphinx/docstring_python.html`

- Code convention PEP8
    - `https://www.python.org/dev/peps/pep-0008/`

- Documentation best practices
    - `https://github.com/google/styleguide/blob/gh-pages/docguide/best_practices.md`

- Philosophical stuff
    - `https://github.com/google/styleguide/blob/gh-pages/docguide/philosophy.md`

- Unix rules (although a bit cryptic sometimes)
    - `https://en.wikipedia.org/wiki/Unix_philosophy#Eric_Raymond%E2%80%99s_17_Unix_Rules`

# Naming conventions

- Name executable files (scripts) and library functions using verbs (e.g.,
  `download.py`, `download_data()`)

- Name classes and (non-executable) files using nouns (e.g., `Downloader()`,
  `downloader.py`)

- For decorators we don't use a verb as we do for normal functions, but rather
  an adjective or a past tense verb, e.g.,
    ```python
    def timed(f):
        """
        Decorator adding a timer around function `f`.
        """
        ...
    ```

## Finding the best names

- Naming things properly is one of the most difficult task of a programmer / data
  scientist
    - The name needs to be short and memorable
    - The name should capture what the object represents, without reference to
      things that can change or to details that are not important
    - The name needs to be non-controversial: people need to be able to map the
      name in their mental model
    - The name needs to sound good in English

- Think hard about how to call functions, files, variables, classes

## Horrible names

- `raw_df` is a terrible name
    - "raw" with respect to what? Cooked? Read-After-Write race condition?

- `person_dict` is bad
    - What if we switch from a dictionary to an object?
        - Then we need to change the name everywhere!
    - The name should capture what the data structure represents and not how it
      is implemented

## No Hungarian notation please

- The concept is to use names including information about the type, e.g.,
  `vUsing adjHungarian nnotation vmakes nreading ncode adjdifficult`
    - [https://en.wikipedia.org/wiki/Hungarian_notation]
    - [https://stackoverflow.com/questions/111933]

- We are not at Microsoft in the 80s: don't use it

## No code stutter

- An example of code stutter is in a module `git` a function called
  `get_git_root_path()` and then client code does
    - **Bad**
        ```python
        import helpers.git as git

        ... git.get_git_root_path()
        ```
- You see that the module is already specifying we are talking about Git

- **Good**
    ```python
    import helpers.git as git

    ... git.get_root_path()
    ```

- This is not only aestitical reason but a bit related to a weak form of DRY

# Comments

## Docstring conventions

- Code needs to be properly commented

- We follow python standard [PEP 257](https://www.python.org/dev/peps/pep-0257/)
  for commenting
    - PEP 257 standardizes what comments should express and how they should do
      it (e.g., use triple quotes for commenting a function), but does not
      specify what markup syntax should be used to describe comments

- Different conventions have been developed for documenting interfaces
    - reST
    - Google (which is cross-language, e.g., C++, python, ...)
    - epytext
    - numpydoc

## reST style

- reST (aka re-Structured Text) style is:
    - the most widely supported in the python community
    - supported by all doc generation tools (e.g., epydoc, sphinx)
    - default in pycharm
    - default in pyment
    - supported by pydocstyle (which does not support Google style as explained
      [here](https://github.com/PyCQA/pydocstyle/issues/275))

- An example of a function comment is:
    ```python
    """
    This is a reST style.

    :param param1: this is a first param
    :type param1: str
    :param param2: this is a second param
    :type param2: int
    :returns: this is a description of what is returned
    :rtype: bool
    :raises keyError: raises an exception
    """
    ```
    - We pick lowercase after `:param XYZ: ...` unless the first word is a proper
      noun or type
    - Type hinting makes the `:type ...` redundant and you should use only type
      hinting

- More examples of and discussions on python docstrings are
  [here](https://stackoverflow.com/questions/3898572)

## Descriptive vs imperative style

- GPSG suggests using descriptive comments, e.g., "This function does
  this and that", instead of an imperative style "Do this and that"

- [PEP 257](https://www.python.org/dev/peps/pep-0257/)
    ```
    The docstring is a phrase ending in a period. It prescribes the function or
    method's effect as a command ("Do this", "Return that"), not as a description;
    e.g. don't write "Returns the pathname ...".
    ```
    - pylint and other python QA tools favor an imperative style
    - Since we prefer to rely upon automatic checks, we have decided to use
      the imperative style

## Use type hints

- We expect new code to use type hints whenever possible
    - See [PEP 484](https://www.python.org/dev/peps/pep-0484/)
    - [Type hints cheat sheet](https://mypy.readthedocs.io/en/latest/cheat_sheet_py3.html)
- At some point we will start adding type hints to old code

- We plan to start using static analyzers (e.g., `mypy`) to check for bugs from
  type mistakes and to enforce type hints at run-time, whenever possible

## Replace empty lines in code with comments

- If you feel that you need an empty line in the code, it probably means that a
  specific chunk of code is a logical piece of code performing a cohesive
  function.
    ```python
    ...
    end_y = end_dt.year
    # Generate list of file paths for ParquetDataset.
    paths = list()
    ...
    ```

- Instead of putting an empty line, you should put a comment describing at high
  level what the code does.
    ```python
    ...
    end_y = end_dt.year
    # Generate list of file paths for ParquetDataset.
    paths = list()
    ...
    ```

- If you don't want to add just use an empty comment.
    ```python
    ...
    end_y = end_dt.year
    #
    paths = list()
    ...
    ```

- The problem with empty lines is that they are visually confusing since one
  empty line is used also to separate functions. For this reason we suggest
  using an empty comment.

## Avoid distracting comments

- Use comments to explain the high level logic / goal of a piece of code and not
  the details
    - E.g., do not comment things that are obvious, e.g.,
    ```python
    # Print results.
    log.info("Results are %s", ...)
    ```

## If you find a bug or obsolete docstring/TODO in the code

- The process is:
    - do a `git blame` to find who wrote the code
    - if it's an easy bug, you can fix it and ask for a review from the author
    - you can comment on a PR (if there is one)
    - you can file a bug on Github with
        - clear info on the problem
        - how to reproduce it, ideally a unit test
        - stacktrace
        - you can use the tag "BUG: ..."

## Referring to an object in code comments 

- We prefer to refer to objects in the code using Markdown like `this` (this is a
  convention used in the documentation system Sphinx)

    ```python
    """
    Decorator adding a timer around function `f`.
    """
    ```
- This is useful for distinguishing the object code from the real-life object
- E.g.,
    ```python
    # The df `df_tmp` is used for ...
    ```

## Inline comments

- In general we prefer to avoid writing comments on the same line as code since
  they require extra maintenance (e.g., when the line becomes too long)
    - **Bad**
        ```python
        print("hello world")      # Introduce yourself.
        ```
    - **Good**
        ```python
        # Introduce yourself.
        print("hello world")
        ```

# Linter

## Disabling linter messages

- When the linter reports a problem
    - We assume that linter messages are correct, until the linter is proven
      wrong
    - We try to understand what the rationale for the linter's complaints
    - We then change the code to follow the linter's suggestion and remove the
      lint

1) If you think a message is too pedantic, please file a bug with the example
   and as a team we will consider whether to exclude that message from our list
   of linter suggestions

2) If you think the message is a false positive, then try to change the code to
   make the linter happy
    - E.g., if the code depends on some run-time behavior that the linter can't
      infer, then you should question whether that behavior is really needed
    - A human reader would probably be as confused as the linter is

3) If you really believe that you should override the linter in this particular
   case, then use something like:
    ```python
    # pylint: disable=some-message,another-one
    ```
    - You then need to explain in a comment why you are overriding the linter.

- Don't use linter code numbers, but the symbolic name whenever possible:
    - **Bad**
    ```python
     # pylint: disable=W0611
    import config.logging_settings
    ```
    - **Good**
    ```python
    # pylint: disable=unused-import
    # This is needed when evaluating code at run-time that depends from
    # this import.
    import config.logging_settings
    ```

## Prefer non-inlined linter comments

- Although we don't like inlined comments sometimes there is no other choice than
  an inlined comment to get the linter to understand which line we are referring
  too:
    - **Bad but ok if needed**
    ```python
    # pylint: disable=unused-import
    import config.logging_settings
    ```
    - **Good**
    ```python
    import config.logging_settings  # pylint: disable=unused-import
    ```

# Logging

## Always use logging instead of prints

- Always use logging and never `print()` to report debug, info, warning 

## Our logging idiom

- In order to use our logging framework (e.g., `-v` from command lines, and much
  more) use:
    ```python
    import helpers.dbg as dbg

    _LOG = logging.getLogger(__name__)

    dbg.init_logger(verb=logging.DEBUG)

    _LOG.debug("I am a debug function about %s", a)
    ```

- In this way one can decide how much debug info is needed (see Unix rule of
  silence)
    - E.g., when there is a bug one can run with `-v DEBUG` and see what's
      happening right before the bug

## Logging level

- Use `_LOG.warning` for messages to the final user related to something
  unexpected where the code is making a decision that might be controversial
    - E.g., processing a dir that is supposed to contain only `.csv` files
      the code finds a non-`.csv` file and decides to skip it, instead of
      breaking

- Use `_LOG.info` to communicate to the final user, e.g.,
    - when the script is started
    - where the script is saving its results
    - a progress bar indicating the amount of work completed

- Use `_LOG.debug` to communicate information related to the internal behavior of
  code
    - Do not pollute the output with information a regular user does not care
      about

- Make sure the script prints when the work is terminated, e.g., "DONE" or
  "Results written to ..."
    - This is useful to indicate that the script did not die in the middle:
      sometimes this happens silently and it is reported only from the OS return
      code

## Use positional args when logging

- **Bad**
    ```python
    _LOG.debug('cmd=%s %s %s' % (cmd1, cmd2, cmd3))
    _LOG.debug('cmd=%s %s %s'.format(cmd1, cmd2, cmd3))
    _LOG.debug('cmd={cmd1} {cmd2} {cmd3}')
    ```
- **Good**
    ```python
     _LOG.debug('cmd=%s %s %s', cmd1, cmd2, cmd3)
    ```

- The two statements are equivalent from the functional point of view
- The reason is that in the second case the string is not built unless the
  logging is actually performed, which limits time overhead from logging

## Exceptions don't allow positional args

- For some reason people tend to believe that using the logging / dassert
  approach of positional param to exceptions
    - **Bad** (use positional args)
        ```python
        raise ValueError("Invalid server_name='%s'", server_name)
        ```
    - **Good** (use string interpolation)
        ```python
        raise ValueError("Invalid server_name='%s'" % server_name)
        ```
    - **Best** (use string format)
        ```python
        raise ValueError(f"Invalid server_name='{server_name}'")
        ```
- The constructor of an exception accepts a string

- Using the string f-format is best since
    - it's more readable
    - there is little time overhead since if you get to the exception probably
      the code is going to terminate, and it's not in a hot loop

## Report warnings

- If there is a something that is suspicious but you don't feel like it's
  worthwhile to assert, report a warning with:
```
_LOG.warning(...)
```

- If you know that if there is a warning then there are going to be many many warnings
    - print the first warning
    - send the rest to warnings.log
    - at the end of the run, reports "there are warnings in warnings.log"

# Assertions

## Use positional args when asserting
- `dassert_*` is modeled after logging so for the same reasons one should use
  positional args
    **Bad**
    ```python
    dbg.dassert_eq(a, 1, "No info for %s" % method)
    ```
    **Good**
    ```python
    dbg.dassert_eq(a, 1, "No info for %s", method)
    ```

## Report as much information as possible in an assertion
- When using a `dassert_*` you want to give to the user as much information as
  possible to fix the problem
    - E.g., if you get an assertion after 8 hours of computation you don't want
      to have to add some logging and run for 8 hours to just know what happened
- A `dassert_*` typically prints as much info as possible, but it can't report
  information that is not visible to it:
    - **Bad**
        ```python
        dbg.dassert(string.startswith('hello'))
        ```
        - You don't know what is value of `string` is
    - **Good**
        ```python
        dbg.dassert(string.startswith('hello'), "string='%s'", string)
        ```
        - Note that often is useful to add `'` because sometimes there are pesky
          spaces that make the value unclear or to make the error as readable as
          possible

# Imports

## Importing code from a Git submodule
- If you are in `p1` and you need to import something from `amp`:
    - **Bad**
        ```python
        import amp.helpers.dbg as dbg
        ```
    - **Good**
        ```python
        import helpers.dbg as dbg
        ```

- We map submodules using `PYTHONPATH` so that the imports are independent from
  the position of the submodule

- In this way code can be moved across repos without changing the imports

## Don't use evil `import *`

- Do not use in notebooks or code this evil import
    - **Bad**
        ```python
        from edgar.utils import *
        ```
    - **Good**
        ```python
        import edgar.utils as edu
        ```
- The `from ... import *`
    - pollutes the namespace with the symbols and spreads over everywhere, making
      painful to clean up
    - obscures where each function is coming from, removing the context that
      comes from the namespace
    - is evil in many other ways (see
      [StackOverflow](https://stackoverflow.com/questions/2386714/why-is-import-bad))

## Cleaning up the evil `import *`

- To clean up the mess you can:
    - for notebooks
        - find & replace (e.g., using jupytext and pycharm)
        - change the import and run one cell at the time
    - for code
        - change the import and use linter on file to find all the problematic
          spots

- One of the few spots where the evil import * is ok is in the `__init__.py` to
  tweak the path of symbols exported by a library
    - This is an advanced topic and you should rarely use it

## Avoid `from ... import ...`

- Importing many different functions, like:
    - **Bad**
    ```python
    from edgar.officer_titles import read_documents, read_test_set, \
        get_titles, split_titles, get_titles_overview, \
        word_pattern, symbol_pattern, exact_title, \
        apply_patterns_to_texts, extract_canonical_names, \
        get_rules_coverage, text_contains_only_canonical_titles, \
        compute_stats, NON_MEANING_PATTERNS_BEFORE, patterns
    ```
    - creates lots of maintenance effort
        - e.g., anytime you want a new function you need to update the import
          statement
    - creates potential collisions of the same name
        - e.g., lots of modules have a `read_data()` function
    - importing directly in the namespace loses information about the module
        - e.g.,` read_documents()` is not clear: what documents?
        - `np.read_documents()` at least give information of which packages
          is it coming from
          
## Examples of imports

- Example 1
    - **Bad**
       ```python
       from edgar.shared import edgar_api as api
    - **Good**
       ```python
       import edgar.shared.edgar_api as edg_api
       ```

- Example 2
   - **Bad**
        ```python
        from edgar.shared import headers_extractor as he
        ```
    - **Good**
        ```python
        import edgar.shared.headers_extractor as he
        ```
      
- Example 3
    - **Bad**
        ```python
        from helpers import dbg
        ```
    - **Good**
        ```python
        import helpers.dbg as dbg
        ```
      
- Example 4
    - **Bad**
        ```python
       from helpers.misc import check_or_create_dir, get_timestamp
        ```
    - **Good**
        ```python
        import helpers.misc as hm
        ```

## Exceptions to the import style

- For `typing` it is ok to do:
    ```python
    from typing import Iterable, List
    ```

- Other exceptions are:
    ```python
    from tqdm.autonotebook import tqdm
    ```

## Always import with a full path from the root of the repo / submodule

- **Bad**
    ```python
    import timestamp
    ```
- **Good**
    ```
    import compustat.timestamp
    ```
- In this way your code can run without depending upon your current dir

## Baptizing module import

- Each module that can be imported should have a docstring at the very beginning
  (before any code) describing how it should be imported
    ```python
    """
    # Import as:

    import helpers.printing as prnt
    """
    ```
- Typically we use 4 letters trying to make the import unique
    - **Bad**
        ```python
        # Import as:

        import nlp.utils as util
        ```
    - **Good**
        ```python
        # Import as:

        import nlp.utils as nlut
        ```
- The goal is to have always the same imports so it's easy to move code around,
  without collisions

# Python scripts

## Use Python and not bash for scripting

- We prefer to always use python, instead of bash scripts with very few
  exceptions
    - E.g., scripts that need to modify the environment by setting env vars, like
      `setenv.sh`

- The problem with bash scripts is that it's very easy to put together a sequence
  of commands to automate a workflow
- Quickly things always become more complicated than what you thought, e.g.,
    - you might want to interrupt if one command in the script fails
    - you want to use command line options
    - you want to use logging to see what's going on inside the script
    - you want to do a loop with a regex check inside
- Thus you need to use the more complex features of bash scripting and bash
  scripting is absolutely horrible, much worse than perl (e.g., just think of
  `if [ ... ]` vs `if [[ ... ]]`)

- Our approach is to make simple to create scripts in python that are equivalent
  to sequencing shell commands, so that can evolve in complex scripts

## Skeleton for a script

- The ingredients are:
    - `dev_scripts/script_skeleton.py`: a template to write simple scripts
      you can copy and modify it
    - `helpers/system_interaction.py`: a set of utilities that make simple to
      run shell commands (e.g., capturing their output, breaking on error or not,
      tee-ing to file, logging, ...)
    - `helpers` has lots of useful libraries

- The official reference for a script is `dev_scripts/script_skeleton.py`
- You can copy this file and change it

- A simple example is: `dev_scripts/gup.py`
- A complex example is: `dev_scripts/linter.py`

## Some useful patterns

- Some useful patterns / idioms that are supported by the framework are:
    - incremental mode: you skip an action if its outcome is already present
      (e.g., skipping creating a dir, if it already exists and it contains all
      the results)
    - non-incremental mode: clean and execute everything from scratch
    - dry-run mode: the commands are written to screen instead of being executed

## Use scripts and not notebooks for long-running jobs

- We prefer to use scripts to execute code that might take long time (e.g.,
  hours) to run, instead of notebooks

- Pros of script
    - All the parameters are completely specified by a command line
    - Reproducible and re-runnable

- Cons of notebooks
    - Tend to crash / hang for long jobs
    - Not easy to understand if the notebook is doing progress
    - Not easy to get debug output

- Notebooks are designed for interactive computing / debugging and not batch jobs
    - You can experiment with notebooks, move the code into a library, and wrap
      it in a script

## Python executable characteristics

- All python scripts that are meant to be executed directly should:
    1) be marked as executable files with:
        ```bash
        > chmod +x foo_bar.py
        ```
    2) have the python code should start with the standard Unix shebang notation:
        ```python
        #!/usr/bin/env python
        ```
    - This line tells the shell to use the `python` defined in the conda
      environment

    3) have a:
        ```python
        if __name__ == "__main__":
            ...
        ```
    4) ideally use `argparse` to have a minimum of customization

- In this way you can execute directly without prepending with python

## Use clear names for the scripts

- In general scripts (like functions) should have a name like "action_verb".
    - **Bad**
        - Example of bad names are` timestamp_extractor.py` and
          `timestamp_extractor_v2.py`
            - Which timestamp data set are we talking about?
            - What type of timestamps are we extracting?
            - What is the difference about these two scripts?

- We need to give names to scripts that help people understand what they do and
  the context in which they operate
- We can add a reference to the task that originated the work (to give more
  context)

- E.g., for a script generating a dataset there should be an (umbrella) bug for
  this dataset, that we refer in the bug name,
  e.g.,`TaskXYZ_edgar_timestamp_dataset_extractor.py`

- Also where the script is located should give some clue of what is related to

# Functions

## Arguments

- Avoid using `bool` arguments
    - While a simple `True`/`False` switch may suffice for today's needs, very
      often more flexibility is eventually needed
    - If more flexibility is needed for a `bool` argument, you are faced with
      the choice of
        - Adding another parameter (then parameter combinations grow
            exponentially and may not all make sense)
        - Changing the parameter type to something else
        - Either way, you have to change the function interface
    - To maintain flexibility from the start, opt for a `str` parameter
      "mode", which is allowed to take a small well-defined set of values.
    - If an implicit default is desirable, consider making the default value
      of the parameter `None`. This is only a good route if the default
      operation is non-controversial / intuitively obvious.

## Try to make functions work on multiple types

- We encourage implementing functions that can work on multiple related types:
    - **Bad**: implement `demean_series()`, `demean_dataframe()`
    - **Good**: implement a function `demean(obj)` that can work `pd.Series` and
      `pd.DataFrame`
        - One convention is to call `obj` the variable whose type is not known
          until run-time
- In this way we take full advantage of duck typing to achieve something similar
  to C++ function overloading (actually even more expressive) 
- Try to return the same type of the input, if possible
    - E.g., the function called on a `pd.Series` returns a `pd.Series`

## Avoid hard-wired column name dependencies

- When working with dataframes, we often want need handle certain columns
  differently, or perform an operation on a strict subset of columns
- In these cases, it is tempting to assume that the special columns will have
  specific names, e.g., "datetime"
- The problem is that column names are
    - rarely obvious (e.g., compare "datetime" vs "timestamp" vs "Datetime")
    - tied to specific use cases
        - the function you are writing may be written for a specific use case
          today, but what if it is more general
        - if someone wants to reuse your function in a different setting where
          different column names make sense, why should they have to conform
          to your specific use case's needs?
    - may overwrite existing column names
        - for example, you may decided to call a column "output", but what if
          the dataframe already has a column with that name?
- To get around this, allow the caller to communicate to the function the names
  of any special columns
- Make sure that you require column names only if they are actually used by the
  function
- If you must use hard-write column names internally or for some application,
  define the column name in the library file, like
  ```
  DATETIME_COL = "datetime"
  ```
    - Users of the library can now access the column name through imports
    - This prevents hidden column name dependencies from spreading like a
      virus throughout the codebase

# Misc (to reorg)

- TODO(*): Start moving these functions in the right place once we have more
  a better document structure

## Write robust code

- Write code where there is minimal coupling between different different parts
    - This is a corollary of DRY, since not following DRY implies coupling

- Consider the following code:
    ```python
    if server_name == "ip-172-31-16-23":
        out = 1
    if server_name == "ip-172-32-15-23":
        out = 2
    ```
- This code is brittle since if you change the first part to:
    ```python
    if server_name.startswith("ip-172"):
        out = 1
    if server_name == "ip-172-32-15-23":
        out = 2
    ```
  executing the code with `server_name = "ip-172-32-15-23"` will give `out=2`

- The proper approach is to enumerate all the cases like:
    ```python
    if server_name == "ip-172-31-16-23":
        out = 1
    elif server_name == "ip-172-32-15-23":
        out = 2
    ...
    else:
        raise ValueError("Invalid server_name='%s'" % server_name)
    ```

## Capitalized words

- In documentation and comments we capitalize abbreviations (e.g., `YAML`, `CSV`)
- In the code we use camel case, when appropriate
    - E.g., `ConvertCsvToYaml`, since `ConvertCSVToYAML` is difficult to read
    - E.g., `csv_file_name` as a variable name

## Regex

- The rule of thumb is to compile a regex expression, e.g.,
    ```python
    backslash_regexp = re.compile(r"\\")
    ```
  only if it's called more than once, otherwise the overhead of compilation and
  creating another var is not justified

## Order of functions in a file

- We try to organize code in a file to represent the logical flow of the code
  execution, e.g.,
    - at the beginning of the file: functions / classes for reading data
    - then: functions / classes to process the data
    - finally at the end of the file: functions / classes to save the data

- Try to put private helper functions close to the functions that are using them
    - This rule of thumb is a bit at odds with clearly separating public and
      private section in classes
        - A possible justification is that classes typically contain less code
          than a file and tend to be used through their API
        - A file contains larger amount of loosely coupled code and so we want
          to keep implementation and API close together

## Use layers design pattern

- A "layer" design pattern (see Unix architecture) is a piece of code that talks
  / has dependency only to one outermost layer and one innermost layer

- You can use this approach in your files representing data processing pipelines

- You can split code in sections using 80 characters # comments, e.g.,
    ```python
    # ###################...
    # Read.
    # ###################...

    ...

    # ###################...
    # Process.
    # ###################...


    ...

    # ###################...
    # Process.
    # ###################...

- This often suggests to split the code in classes to represent namespaces of
  related functions

## Write complete `if-then-else`

- Consider this good piece of code

    ```python
    dbg.dassert_in(
        frequency,
        ["D", "T"]
        "Only daily ('D') and minutely ('T') frequencies are supported.",
    )
    if frequency == "T":
        ...
    elif frequency == "D":
        ...
    else:
        raise ValueError("The %s frequency is not supported" % frequency)
    ```

- This code is robust and correct

- Still the `if-then-else` is enough and the assertion is not needed
    - DRY here wins: you don't want to have to keep two pieces of code in sync

- It makes sense to check early only when you want to fail before doing more work
    - E.g., sanity checking the parameters of a long running function, so that it
      doesn't run for 1 hr and then crash because the name of the file is
      incorrect

## Do not be stingy at typing

- Why calling an object `TimeSeriesMinStudy` instead of `TimeSeriesMinuteStudy`?
    - Saving 3 letters is not worth
    - The reader might interpret `Min` as `Minimal` (or `Miniature`, `Minnie`,
      `Minotaur`)

- If you don't like to type, we suggest you get a better keyboard, e.g.,
  [this](https://kinesis-ergo.com/shop/advantage2/)

## Research quality vs production quality

- Code belonging to top level libraries (e.g., `//amp/core`, `//amp/helpers`) and
  production (e.g., `//p1/db`, `vendors`) needs to meet high quality standards,
  e.g.,
    - well commented
    - following our style guide
    - thoroughly reviewed
    - good design
    - comprehensive unit tests

- Research code in notebook and python can follow slightly looser standards, e.g.,
    - sprinkled with some TODOs
    - not perfectly general

- The reason is that:
    - research code is still evolving and we want to keep the structure flexible
    - we don't want to invest the time in making it perfect if the research
      doesn't pan out

- Note that research code still needs to be:
    - understandable / usable by not authors
    - well commented
    - follow the style guide
    - somehow unit tested

- We should be able to raise the quality of a piece of research code to
  production quality when that research goes into production

## No ugly hacks

- We don't tolerate "ugly hacks", i.e., hacks that require lots of work to be
  undone (much more than the effort to do it right in the first place)
    - Especially an ugly design hack, e.g., a Singleton, or some unnecessary
      dependency between distant pieces of code
    - Ugly hacks spreads everywhere in the code base

## Life cycle of research code

- Often the life cycle of a piece of code is to start as research and then be
  promoted to higher level libraries to be used in multiple research, after its
  quality reaches production quality
