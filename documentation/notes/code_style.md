# Style guide references

- We care about consistency rather than arguing about which approach is better
    - E.g., see "tab vs space" flame-war from the 90s
- Unless explicitly noted we prefer to follow the style guide below

- As a rule of thumb we default to the Google style guidelines, unless the
  Python community (in the form of [PEPs](https://www.python.org/dev/peps/) or
  the tools we rely upon favor another style

## Reference

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

# Comments

## Docstring conventions

- Code needs to be properly commented

- We follow python standard [PEP 257](https://www.python.org/dev/peps/pep-0257/)
  for commenting
    - PEP 257 standardizes what and how comments should be expressed (e.g., use
      a triple quotes for commenting a function), but it does not specify what
      markup syntax should be used to describe comments

- Different conventions have been developed for documenting interfaces
    - reST
    - Google (which is cross-language, e.g., C++, python, …)
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

- Examples are [here](https://stackoverflow.com/questions/3898572)

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
    - Since we prefer to rely upon automatic checks, we decided to use an
      imperative style of comments

## Use type hints

- See [PEP 484](https://www.python.org/dev/peps/pep-0484/)

- [Type hints cheat sheet](https://mypy.readthedocs.io/en/latest/cheat_sheet_py3.html)

## Avoid empty lines in code

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
  empty line is used also to separate functions. For this reason we suggest to
  use an empty comment.

## Avoid distracting comments

- Use comments to explain the high level logic / goal of a piece of code and not
  the details
    - E.g., do not comment things that are obvious, e.g.,
    ```python
    # Print results.
    log.info("Results are %s", ...)
    ```

## If you find a bug, obsolete docstring in the code
- The process is:
    - do a `git blame` to find who wrote the code
    - if it's an easy bug, you can fix it and ask for a review from the author
    - you can comment on a PR (if there is one)
    - you can file a bug on Github with
        - clear info on the problem
        - how to reproduce it, ideally a unit test
        - stacktrace
        - you can use the tag “BUG: ..."

# Logging

## Always use logging instead of prints

- Always use logging and never `print()` to report debug, info, warning 

## Our logging idiom

```python
import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)

dbg.init_logger(verb=logging.DEBUG)

_LOG.debug("I am a debug function about %s", a)
```

- In this way one can decide how much debug info are needed (see Unix rule of
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
  "Results written in ..."
    - This is useful to indicate that the script did not die in the middle:
      sometimes this happens silently and it is reported only from the OS return
      code

## Use positional args when logging

- Instead of doing this:
    **Bad**
    ```python
    _LOG.debug('cmd=%s %s %s' % (cmd1, cmd2, cmd3))
    _LOG.debug('cmd=%s %s %s'.format(cmd1, cmd2, cmd3))
    _LOG.debug('cmd={cmd1} {cmd2} {cmd3}')
  do this
    **Good**
    ```
     _LOG.debug('cmd=%s %s %s', cmd1, cmd2, cmd3)
    ```

- The two statements are equivalent from the functional point of view
- The reason is that in the second case the string is not built unless the
  logging is actually performed, which limits time overhead from logging

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

# Assertion

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
  information that are not visible to it:
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

# Import

## Importing code from Git submodule
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
    - makes unclear from where each function is coming from, losing context that
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
- In this way your code can run without dependency from your current dir

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

## Skeleton for a script

- The official reference for a script is `//amp/dev_scripts/script_skeleton.py`
- You can copy this file and change it

## Use the script framework
- We have several libraries that make writing scripts in python very easy, e.g.,
  `//amp/helpers/system_interaction.py`

- As an interesting example of complex scripts you can check out:
  `//amp/dev_scripts/linter.py`

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

- In general scripts (like functions) should have name like “action_verb”.
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

## Try to make functions work on multiple types

- We welcome functions that can work on multiple related types since in this case
  one doesn't have to remember multiple functions:
    - E.g., a function `demean(obj)` that can work `pd.Series` and
      `pd.DataFrame`, instead of `demean_series()`, `demean_dataframe()`
- In this way we take full advantage of duck typing to achieve something similar
  to C++ function overloading
- You can call the variable `obj` since its type is not known until run-time
- Try to return the same type of the input, if possible
    - E.g., the function called on a `pd.Series` returns a `pd.Series` and so on
- You can try to go full duck typing 

## Robust code
        if server_name == "ip-172-31-16-23":
            git_user_name = "gad26032"
            git_user_email = "malanin@particle.one"
            conda_sh_path = "/anaconda3/etc/profile.d/conda.sh"
            conda_env_path = "~/.conda/envs" jupyter_port = 9111
        if server_name == "particle-laptop":
            git_user_name = "gad26032"
            git_user_email = "malanin@particle.one"
            conda_sh_path = "~/anaconda3/etc/profile.d/conda.sh"
            conda_env_path = "~/.conda/envs"
            jupyter_port = 9111


## Decorator names

- For decorator we don't use a verb like for functions, but rather an adjective
  or a past tense verb, e.g.,
    ```python
    def timed(f):
        """
        Decorator adding a timer around function `f`.
        """
        ...
    ```

## Capitalized words

- In documentation we capitalize abbreviations (e.g., `YAML`, `CSV`)
- In the code we use camel case, when appropriate (e.g., `ConvertCsvToYaml`,
  since `ConvertCSVToYAML` is difficult to read)

## Referring to an object in the code
- We prefer to refer to objects in the code using Markdown like `this` (this is a
  convention used in the documentation system Sphinx)

    ```python
    """
    Decorator adding a timer around function `f`.
    """
    ```
- This is useful to distinguish the object code from the real life object
- E.g.,
    ```python
    # The df `df_tmp` is used for ...
    ```

## Inline comments
- In general we prefer to avoid comments on the same line as code since they
  require extra maintenance (e.g., when the line becomes too long)
    - **Bad**
        ```python
        print("hello world")      # Introduce yourself.
        ```
    - **Good**
        ```python
        # Introduce yourself.
        print("hello world")
        ```

## Disabling linter messages

- By default we assume that linter messages are correct
    - We try to understand what's the rationale for the lints and change the code
      to follow their suggestion

1) If you think a message is too pedantic please file a bug with the example
  and as a team we can consider to exclude that message from our list

2) If you think the message is a false positive then we try to change the code to
   make the linter happy
    - E.g., the code depends on some run-time behavior that the linter can't infer
      then you should wonder if that behavior is really needed
    - A human reader would probably be as confused as the linter is

3) If you really believe that you can override the linter in this particular case
   then use something like:
    ```python
    # pylint: disable=some-message,another-one
    ```
    - You need to explain why you are overriding the linter.

- Don't use code numbers, but the symbolic name whenever possible
  - **Bad**
    ```python
     # pylint: disable=W0611
    import config.logging_settings
    ```
  - **Good**
    ```python
    # pylint: disable=unused-import
    import config.logging_settings
    ```

- Although we don't like inlined comments sometimes there is no choice to get the
  linter to understand which line we are referring too:
  - **Bad but ok if needed**
    ```python
    # pylint: disable=unused-import
    import config.logging_settings
    ```
  - **Good**
    ```python
    import config.logging_settings  # pylint: disable=unused-import
    ```
