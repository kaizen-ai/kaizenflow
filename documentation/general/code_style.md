<!--ts-->
   * [Style guide references](#style-guide-references)
      * [References](#references)
         * [Coding](#coding)
         * [Documentation](#documentation)
         * [Design](#design)
   * [Naming](#naming)
      * [Conventions](#conventions)
      * [Some suggested spelling](#some-suggested-spelling)
      * [Finding the best names](#finding-the-best-names)
      * [Horrible names](#horrible-names)
      * [No Hungarian notation please](#no-hungarian-notation-please)
      * [No code stutter](#no-code-stutter)
   * [Using third-party libraries](#using-third-party-libraries)
      * [Problem](#problem)
      * [Solution](#solution)
   * [Comments](#comments)
      * [Docstring conventions](#docstring-conventions)
      * [reST style](#rest-style)
      * [Descriptive vs imperative style](#descriptive-vs-imperative-style)
      * [Comments](#comments-1)
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
      * [Don't mix real changes with linter changes](#dont-mix-real-changes-with-linter-changes)
   * [Assertions](#assertions)
      * [Use positional args when asserting](#use-positional-args-when-asserting)
      * [Report as much information as possible in an assertion](#report-as-much-information-as-possible-in-an-assertion)
   * [Imports](#imports)
      * [Importing code from a Git submodule](#importing-code-from-a-git-submodule)
      * [Don't use evil import *](#dont-use-evil-import-)
      * [Cleaning up the evil import *](#cleaning-up-the-evil-import-)
      * [Avoid from ... import ...](#avoid-from--import-)
      * [Exceptions to the import style](#exceptions-to-the-import-style)
      * [Examples of imports](#examples-of-imports)
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
   * [Process.](#process)
   * [###################...](#-1)
      * [Write complete if-then-else](#write-complete-if-then-else)
      * [Do not be stingy at typing](#do-not-be-stingy-at-typing)
      * [Research quality vs production quality](#research-quality-vs-production-quality)
      * [No ugly hacks](#no-ugly-hacks)
      * [Life cycle of research code](#life-cycle-of-research-code)
   * [Document what notebooks are for](#document-what-notebooks-are-for)
   * [Keep related code close](#keep-related-code-close)
   * [Single exit point from a function](#single-exit-point-from-a-function)
      * [Style for default parameter](#style-for-default-parameter)
      * [Calling functions with default parameters](#calling-functions-with-default-parameters)
      * [Always separate what changes from what stays the same](#always-separate-what-changes-from-what-stays-the-same)



<!--te-->

# Style guide references

## Meta

1. What we call the "rules" is actually just a convention
2. The rules:
    - are not about the absolute best way of doing something in all cases
    - are optimized for the common case
    - can become cumbersome or weird for some corner cases
3. We prefer simple rather than optimal rules that can be applied in most of the
   cases without thinking or going to check the documentation
4. The rules are striving to achieve consistency and robustness
    - E.g., see "tab vs space" flame-war from the 90s
    - We care about consistency rather than arguing about which approach is
      better in each case
5. The rules are optimized for the average developer / data scientist and not for
   power users
6. The rules try to minimize the maintenance burden
    - We don’t want a change somewhere to propagate everywhere
    - We want to minimize the propagation of a change
8. Some of the rules are evolving based on what we are seeing through the reviews

- Each rule tries to follow a format similar to Google style guide
    ```
    - XYZ
    - Problem
    - Decision
        - Good vs Bad
    - Rationale
        - Pros of Good vs Bad
        - Cons of Good vs Bad
    ```

## Follow Google / Python style guidelines

- Unless explicitly noted we prefer to follow the style guide below

- As a rule of thumb we default to the Google style guidelines, unless the
  Python community (in the form of [PEPs](https://www.python.org/dev/peps/) or
  the tools we rely upon favor another style

## References

### Coding

- [Google Python Style Guide (GPSG)](https://google.github.io/styleguide/pyguide.html)

- Code convention from [PEP8](https://www.python.org/dev/peps/pep-0008)

### Documentation

- Docstring convention from [PEP257](https://www.python.org/dev/peps/pep-0257)

- Commenting style
  - [Sphinx](http://www.sphinx-doc.org/en/master)
  - [Sphinx tutorial](https://thomas-cokelaer.info/tutorials/sphinx/index.html)

- [Google documentation best practices](https://github.com/google/styleguide/blob/gh-pages/docguide/best_practices.md)

### Design

- TODO(gp): Move to documentation/general/design_philosophy.md

- [Google philosophical stuff](https://github.com/google/styleguide/blob/gh-pages/docguide/philosophy.md)

- [Unix rules](https://en.wikipedia.org/wiki/Unix_philosophy#Eric_Raymond%E2%80%99s_17_Unix_Rules)
  (although a bit cryptic sometimes)

# High level principles

- In this paragraph we summarize the high-level principles that we follow for
  designing and implementing code and research 
- We should be careful in adding principles here
    - Ideally principles should be non-overlapping and generating all the other
      lower level principles we follow (like a basis for a vector space)

## DRY
- Do not Repeat Yourself

## Optimize for reading
- Make code easy to read even if it is more difficult to write
- (Good) Code is written 1x and read 100x

## Encapsulate what changes
- Separate what changes from what stays the same

## Least surprise principle
- Try to make sure that the reader is not surprised

## Pay the technical debt
- Any unpaid debt is guaranteed to bite you when you don't expect it
- Still some debt is inevitable: try to find the right trade-off

## End-to-end first
- Always focus on implementing things end-to-end, then improve each block
- Remember the analogy of building the car through the skateboard, the bike, etc.
    - Compare this approach to building wheels, chassis, with a big-bang
      integration at the end

## Unit test everything
- Code that matters needs to be unit tested
- Code that doesn't matter should not be checked in the repo
- The logical implication is: all code checked in the repo should be unit tested

## Don't get attached to code
- It's ok to delete, discard, retire code that is not useful any more
- Don't take it personally when people suggest changes or simplification

## Always plan before writing code
- File a GitHub issue
- Think about what to do and how to do it
- Ask for help or for a review
- The best code is the one that we avoid to write through a clever mental kung-fu
  move

## Think hard about naming
- Finding a name for a code object, notebook, is extremely difficult but very
  important to build a mental map
- Spend the needed time on it

## Look for inconsistencies
- Stop for a second after you have, before sending it out:
    - implemented code or a notebook
    - written documentation
    - written an email
    - ...
- Reset your mind and look at everything with fresh eyes like if it was the first
  time you saw it
    - Does everything make sense to someone that sees this for the first time?
    - Can (and should) it be improved?
    - Do you see inconsistencies, potential issues?
- It will take less and less time to become good at this

# Naming

## Conventions

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

## Some suggested spellings

- Capitalize the abbreviations, e.g.,
  - `CSV`
  - `DB` since it's an abbreviation of Database
- We spell
  - "Python"
  - "Git" (as program), "git" (as the command)
- We distinguish "research" (not "default", "base") vs "production"
- We use different names for indicating the same concept, e.g., `dir`, `path`,
  `folder`
  - Let's use `dir`

## Finding the best names

- Naming things properly is one of the most difficult task of a programmer /
  data scientist
  - The name needs to be (possibly) short and memorable
    - Don't be afraid to use long names, if needed, e.g.,
      `process_text_with_full_pipeline_twitter_v1`
    - Clarity is more important than number of bytes used
  - The name should capture what the object represents, without reference to
    things that can change or to details that are not important
  - The name should refer to what objects do (i.e., mechanisms), rather than how
    we use them (i.e., policies)
  - The name needs to be non-controversial: people need to be able to map the
    name in their mental model
  - The name needs to sound good in English
    - **Bad**: `AdapterSequential` sounds bad
    - **Good**: `SequentialAdapter` sounds good

- Think hard about how to call functions, files, variables, classes

## Horrible names

- `raw_df` is a terrible name
  - "raw" with respect to what?
  - Cooked?
  - Read-After-Write race condition?

- `person_dict` is bad
  - What if we switch from a dictionary to an object?
    - Then we need to change the name everywhere!
  - The name should capture what the data structure represents and not how it is
    implemented

## No Hungarian notation please

- The concept is to use names including information about the type, e.g.,
  `vUsing adjHungarian nnotation vmakes nreading ncode adjdifficult`
  - [https://en.wikipedia.org/wiki/Hungarian_notation]
  - [https://stackoverflow.com/questions/111933]

- E.g., instead of `categories_list`, use `categories`, and instead of
  `stopwords_dict`, use `stopwords`
  - Variable names should refer to what they mean and not how they are
    implemented
  - What if one decides to store data in a `pd.Series` instead of a `list`? With
    Hungarian notation, the name of the var needs to be changed everywhere.

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

- This is not only aesthetic reason but a bit related to a weak form of DRY

# Using third-party libraries

## Problem

- On one side, you are free (even encouraged) to install and experiment with any
  3-rd party library that can be useful
  - Anything goes for research, as long as it's clearly marked as such
  - E.g., the notebook should have in the description an indication that is an
    experiment with a library

- On the other side we don't want to add dependencies in `master` from packages
  that we experiment with but are not adopted
  - Otherwise the conda recipe becomes bloated and slow

## Solution

- Install the package in whatever way you want (`conda`, `pip`, install from
  source) on top of your `p1_develop` conda environment
  - If you don't know how to install, file a bug for Sergey and we can help

- You should document how you install the package so that anyone who runs the
  notebook can install the package on top of `p1_develop` in the same way

- Once the code is reviewed and promoted to a lib / unit tested, the conda
  recipe is updated as part of the PR
  - The team needs to update their `p1_develop` package to pick up the
    dependencies

- This applies to both code and notebooks

# Comments

## Docstring conventions

- Code needs to be properly commented

- We follow python standard [PEP 257](https://www.python.org/dev/peps/pep-0257/)
  for commenting
  - PEP 257 standardizes what comments should express and how they should do it
    (e.g., use triple quotes for commenting a function), but does not specify
    what markup syntax should be used to describe comments

- Different conventions have been developed for documenting interfaces
  - ReST
  - Google (which is cross-language, e.g., C++, python, ...)
  - Epytext
  - Numpydoc

## reST style

- ReST (aka re-Structured Text) style is:
  - The most widely supported in the python community
  - Supported by all doc generation tools (e.g., epydoc, sphinx)
  - Default in pycharm
  - Default in pyment
  - Supported by pydocstyle (which does not support Google style as explained
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

- GPSG suggests using descriptive comments, e.g., "This function does this and
  that", instead of an imperative style "Do this and that"

- [PEP 257](https://www.python.org/dev/peps/pep-0257/)
  ```
  The docstring is a phrase ending in a period. It prescribes the function or
  method's effect as a command ("Do this", "Return that"), not as a description;
  e.g. don't write "Returns the pathname ...".
  ```
  - Pylint and other python QA tools favor an imperative style
  - Since we prefer to rely upon automatic checks, we have decided to use the
    imperative style

## Comments

- Comments follow the same style of docstrings, e.g., imperative style with
  period `.` at the end

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
  _LOG.info("Results are %s", ...)
  ```

## If you find a bug or obsolete docstring/TODO in the code

- The process is:
  - Do a `git blame` to find who wrote the code
  - If it's an easy bug, you can fix it and ask for a review from the author
  - You can comment on a PR (if there is one)
  - You can file a bug on Github with
    - Clear info on the problem
    - How to reproduce it, ideally a unit test
    - Stacktrace
    - You can use the tag "BUG: ..."

## Referring to an object in code comments

- We prefer to refer to objects in the code using Markdown like `this` (this is
  a convention used in the documentation system Sphinx)

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
  - We assume that linter messages are correct, until the linter is proven wrong
  - We try to understand what the rationale for the linter's complaints
  - We then change the code to follow the linter's suggestion and remove the
    lint

1. If you think a message is too pedantic, please file a bug with the example
   and as a team we will consider whether to exclude that message from our list
   of linter suggestions

2. If you think the message is a false positive, then try to change the code to
   make the linter happy
   - E.g., if the code depends on some run-time behavior that the linter can't
     infer, then you should question whether that behavior is really needed
   - A human reader would probably be as confused as the linter is

3. If you really believe that you should override the linter in this particular
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

- Although we don't like inlined comments sometimes there is no other choice
  than an inlined comment to get the linter to understand which line we are
  referring too:
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

  dbg.init_logger(verbosity=logging.DEBUG)

  _LOG.debug("I am a debug function about %s", a)
  ```

- In this way one can decide how much debug info is needed (see Unix rule of
  silence)
  - E.g., when there is a bug one can run with `-v DEBUG` and see what's
    happening right before the bug

## Logging level

- Use `_LOG.warning` for messages to the final user related to something
  unexpected where the code is making a decision that might be controversial
  - E.g., processing a dir that is supposed to contain only `.csv` files the
    code finds a non-`.csv` file and decides to skip it, instead of breaking

- Use `_LOG.info` to communicate to the final user, e.g.,
  - When the script is started
  - Where the script is saving its results
  - A progress bar indicating the amount of work completed

- Use `_LOG.debug` to communicate information related to the internal behavior
  of code
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
  - It's more readable
  - There is little time overhead since if you get to the exception probably the
    code is going to terminate, and it's not in a hot loop

## Report warnings

- If there is a something that is suspicious but you don't feel like it's
  worthwhile to assert, report a warning with:

  ```python
  _LOG.warning(...)
  ```

- If you know that if there is a warning then there are going to be many many
  warnings
  - Print the first warning
  - Send the rest to warnings.log
  - At the end of the run, reports "there are warnings in warnings.log"

# Assertions

## Use positional args when asserting

- `dassert_*` is modeled after logging so for the same reasons one should use
  positional args **Bad**
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
  - E.g., if you get an assertion after 8 hours of computation you don't want to
    have to add some logging and run for 8 hours to just know what happened
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

- In this way, the submodule code can be moved in the repos without changing the
  imports

## Don't use evil `import *`

- Do not use in notebooks or code the evil `import *`
  - **Bad**
    ```python
    from edgar.utils import *
    ```
  - **Good**
    ```python
    import edgar.utils as edu
    ```
- The `from ... import *`:
  - Pollutes the namespace with the symbols and spreads over everywhere, making
    painful to clean up
  - Obscures where each function is coming from, removing the context that comes
    from the namespace
  - Is evil in many other ways (see
    [StackOverflow](https://stackoverflow.com/questions/2386714/why-is-import-bad))

## Cleaning up the evil `import *`

- To clean up the mess you can:
  - For notebooks
    - Find & replace (e.g., using jupytext and pycharm)
    - Change the import and run one cell at the time
  - For code
    - Change the import and use linter on file to find all the problematic spots

- One of the few spots where the evil import \* is ok is in the `__init__.py` to
  tweak the path of symbols exported by a library
  - This is an advanced topic and you should rarely use it

## Avoid `from ... import ...`

- The rule is:
  ```python
  import library as short_name
  import library.sublibrary as short_name
  ```
- This rule applies to imports of third party libraries and our library

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
  - **Good**

  ```python
  import edgar.officer_titles as edg_ot
  ...
  ... edg_ot.read_documents()
  ```

- The problem with the `from ... import ...` is that it:
  - Creates lots of maintenance effort
    - E.g., anytime you want a new function you need to update the import
      statement
  - Creates potential collisions of the same name
    - E.g., lots of modules have a `read_data()` function
  - Importing directly in the namespace loses information about the module
    - E.g.,`read_documents()` is not clear: what documents?
    - `np.read_documents()` at least give information of which packages is it
      coming from

## Exceptions to the import style

- We try to minimize the exceptions to this rule to avoid to keep this rule
  simple, rather than discussing about

- The current agreed upon exceptions are:

  1. For `typing` it is ok to do:
     ```python
     from typing import Iterable, List
     ```
     in order to avoid `typing` everywhere, since we want to use type hints as
     much as possible

## Examples of imports

- Example 1
  - **Bad**
    ```python
    from edgar.shared import edgar_api as api
    ```
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

- Example 5

import html import operator as oper import nltk import textblob as tb and then
uses like:

... nltk.tokenize.TweetTokenizer ... ... oper.itemgetter ... ... html.unescape
...

## Always import with a full path from the root of the repo / submodule

- **Bad**
  ```python
  import timestamp
  ```
- **Good**
  ```python
  import compustat.timestamp
  ```
- In this way your code can run without depending upon your current dir

## Baptizing module import

- Each module that can be imported should have a docstring at the very beginning
  (before any code) describing how it should be imported

  ```python
  """
  Import as:

  import helpers.printing as prnt
  """
  ```

- Typically we use 4 or 5 letters trying to make the import unique (underscores
  are fine):
  - **Bad**

    ```python
    Import as:

    import nlp.utils as util
    ```
  - **Good**

    ```python
    Import as:

    import nlp.utils as nlp_ut
    ```

- The goal is to have always the same imports so it's easy to move code around,
  without collisions

# Python scripts

## Use Python and not bash for scripting

- We prefer to always use python, instead of bash scripts with very few
  exceptions
  - E.g., scripts that need to modify the environment by setting env vars, like
    `setenv.sh`

- The problem with bash scripts is that it's very easy to put together a
  sequence of commands to automate a workflow
- Quickly things always become more complicated than what you thought, e.g.,
  - You might want to interrupt if one command in the script fails
  - You want to use command line options
  - You want to use logging to see what's going on inside the script
  - You want to do a loop with a regex check inside
- Thus you need to use the more complex features of bash scripting and bash
  scripting is absolutely horrible, much worse than perl (e.g., just think of
  `if [ ... ]` vs `if [[ ... ]]`)

- Our approach is to make simple to create scripts in python that are equivalent
  to sequencing shell commands, so that can evolve in complex scripts

## Skeleton for a script

- The ingredients are:
  - `dev_scripts/script_skeleton.py`: a template to write simple scripts you can
    copy and modify it
  - `helpers/system_interaction.py`: a set of utilities that make simple to run
    shell commands (e.g., capturing their output, breaking on error or not,
    tee-ing to file, logging, ...)
  - `helpers` has lots of useful libraries

- The official reference for a script is `dev_scripts/script_skeleton.py`
- You can copy this file and change it

- A simple example is: `dev_scripts/git/gup.py`
- A complex example is: `dev_scripts/linter.py`

## Some useful patterns

- Some useful patterns / idioms that are supported by the framework are:
  - Incremental mode: you skip an action if its outcome is already present
    (e.g., skipping creating a dir, if it already exists and it contains all the
    results)
  - Non-incremental mode: clean and execute everything from scratch
  - Dry-run mode: the commands are written to screen instead of being executed

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

- Notebooks are designed for interactive computing / debugging and not batch
  jobs
  - You can experiment with notebooks, move the code into a library, and wrap it
    in a script

## Python executable characteristics

- All python scripts that are meant to be executed directly should:

  1. Be marked as executable files with:
     ```bash
     > chmod +x foo_bar.py
     ```
  2. Have the python code should start with the standard Unix shebang notation:
     ```python
     #!/usr/bin/env python
     ```
  - This line tells the shell to use the `python` defined in the conda
    environment

  3. Have a:
     ```python
     if __name__ == "__main__":
         ...
     ```
  4. Ideally use `argparse` to have a minimum of customization

- In this way you can execute directly without prepending with python

## Use clear names for the scripts

- In general scripts (like functions) should have a name like "action_verb".
  - **Bad**
    - Example of bad names are`timestamp_extractor.py` and
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

## Avoid using non-exclusive `bool` arguments

- While a simple `True`/`False` switch may suffice for today's needs, very often
  more flexibility is eventually needed
- If more flexibility is needed for a `bool` argument, you are faced with the
  choice of
- Adding another parameter (then parameter combinations grow exponentially and
  may not all make sense)
- Changing the parameter type to something else
- Either way, you have to change the function interface
- To maintain flexibility from the start, opt for a `str` parameter "mode", which
  is allowed to take a small well-defined set of values.
- If an implicit default is desirable, consider making the default value of the
  parameter `None`. This is only a good route if the default operation is
  non-controversial / intuitively obvious.

## Try to make functions work on multiple types

- We encourage implementing functions that can work on multiple related types:
  - **Bad**: implement `demean_series()`, `demean_dataframe()`
  - **Good**: implement a function `demean(obj)` that can work `pd.Series` and
    `pd.DataFrame`
    - One convention is to call `obj` the variable whose type is not known until
      run-time
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
  - Rarely obvious (e.g., compare "datetime" vs "timestamp" vs "Datetime")
  - Tied to specific use cases
    - The function you are writing may be written for a specific use case today,
      but what if it is more general
    - If someone wants to reuse your function in a different setting where
      different column names make sense, why should they have to conform to your
      specific use case's needs?
  - May overwrite existing column names
    - For example, you may decided to call a column "output", but what if the
      dataframe already has a column with that name?
- To get around this, allow the caller to communicate to the function the names
  of any special columns
- Make sure that you require column names only if they are actually used by the
  function
- If you must use hard-write column names internally or for some application,
  define the column name in the library file, like
  ```python
  DATETIME_COL = "datetime"
  ```
  - Users of the library can now access the column name through imports
  - This prevents hidden column name dependencies from spreading like a virus
    throughout the codebase

## Single exit point from a function

```python
@staticmethod
def _get_zero_element(list_: list):
        if not list*: return None
    else:
        return list*[0]

vendors/kibot/utils.py:394: [R1705(no-else-return), ExpiryContractMapper.extract_contract_expiry] Unnecessary "else" after "return" [pylint]
```

- Try to have a single exit point from a function, since this guarantees that
  the return value is always the same
- In general returning different data structures from the same function (e.g., a
  list in one case and a float in another) is indication of bad design
  - There are exceptions like a function that works on different types (e.g.,
    accepts a dataframe or a series and then returns a dataframe or a series,
    but the input and output is the same)
  - Returning different types (e.g., float and string) is also bad
  - Returning a type or None is typically ok
- Try to return values that are consistent so that the client doesn't have to
  switch statement, using `isinstance(...)`
  - E.g., return a `float` and if the value can't be computed return `np.nan`
    (instead of `None`) so that the client can use the return value in a uniform
    way

```python
@staticmethod
def _get_zero_element(list_: list):
    if not list*:
        ret = None
    else:
        ret = list*[0] return ret

def _get_zero_element(list_: list):
    ret = None if not list* else list*[0] return ret
```

- It's ok to have functions like:

```python
def ...(...):
    // Handle simple cases.
    ...
    if ...:
        return
    // lots of code
    ...
    return
```

## Order of function parameters

### Problem

- We want to have a standard, simple, and logical order for specifying the
  arguments of a function

### Decision

- We follow [Google convention](https://google.github.io/styleguide/cppguide.html)
- The preferred order is:
    - input parameters
    - output parameters
    - in-out parameters
    - default parameters

## Consistency of ordering of function parameters

- Try to:
    - keep related variables close to each other
    - keep the order of parameters similar across functions that have similar
      interface
- Enforcing these rules is based on best effort
- PyCharm is helpful when changing order of parameters
- Use `mypy`, `linter` to check consistency of types between function definition
  and invocation

## Style for default parameter

### Problem

- How to assign default parameters in a function?

### Decision

- It's ok to use a default parameter in the interface as long as it is a Python
  scalar (which is immutable by definition)
    - **Good**
      ```python
      def function(
        value: int = 5,
        dir_name : str = "hello_world"
      ):
      ```

- You should not use list, maps, objects, but pass `None` and then initialize
  inside the function
    - **Bad**
      ```python
      def function(
        # These are actually a bug waiting to happen.
        obj: Object = Object(),
        list_: list[int] = [],
        ):
      ```
    - **Good**
      ```python
      def function(
        obj: Optional[Object] = None,
        list_: Optional[list[int]] = None,
        ):
      if obj is None:
        obj = Object()
      if list_ is None:
        list_ = []
      ```
- We use a `None` default value when a function needs to be wrapped and the
  default parameter needs to be propagated
    - **Good**
      ```python
      def function1(
        ...,
        dir_name : Optional[str] = None,
        ):
          if dir_name is None:
              dir_name = "/very_long_path"
          # You can also abbreviate the previous code as:
          # dir_name = dir_name or "/very_long_path"

      def function2(
        ...,
        dir_name : Optional[str] = None
        ):
        function1(..., dir_name=dir_name)
      ```

### Rationale

- Pros of the **Good** vs **Bad** style
  - When you wrap multiple functions, each function needs to propagate the
    default parameters, which:
        - violates DRY; and
        - adds maintenance burden (if you change the innermost default parameter,
          you need to change all of them!)
      - With the proposed approach, all the functions use `None`, until the
        innermost function resolves the parameters to the default values

  - The interface is cleaner
  - Implementation details are hidden (e.g., why should the caller know what is
    the default path?)

  - Mutable parameters can not be passed through (see
    [here](https://stackoverflow.com/questions/1132941/least-astonishment-and-the-mutable-default-argument)))

- Cons:
  - One needs to add `Optional` to the type hint

## Calling functions with default parameters

### Problem

- You have a function

  ```python
  def func(
    task_name : str,
    dataset_dir : str,
    clobber : bool = clobber):
    ...
  ```

- How should it be invoked?

### Decision

- We prefer to
    - assign directly the positional parameters
    - bind explicitly the parameters with a default value using their name

- **Good**

  ```python
  func(task_name, dataset_dir, clobber=clobber)
  ```

- **Bad**

  ```python
  func(task_name, dataset_dir, clobber)
  ```


### Rationale

- Pros of **Good** vs **Bad** style
  - If a new parameter with a default value is added to the function `func`
    before `clobber`:
    - The **Good** idiom doesn't need to be changed
    - All instances of the **Bad** idiom need to be updated
      - The **Bad** idiom might keep working but with silent failures
      - Of course `mypy` and `Pycharm` might point this out
  - The **Good** style highlights which default parameters are being overwritten,
    by using the name of the parameter
      - Overwriting a default parameter is an exceptional situation that should
        be explicitly commented

- Cons:
  - None

## Don't repeat non-default parameters

### Problem

- Given a function with the following interface:
    ```python
    def mult_and_sum(multiplier_1, multiplier_2, sum_):
       return multiplier_1 * multiplier_2 + sum_
    ```
  how to invoke it?

### Decision

- We prefer to invoke it as:
    - **Good**
        ```python
        a = 1
        b = 2
        c = 3
        mult_and_sum(a, b, c)
        ```

    - **Bad**
        ```python
        a = 1
        b = 2
        c = 3
        mult_and_sum(multiplier_1=a,
                     multiplier_2=b,
                     sum_=c)
        ```

### Rationale

- Pros of **Good** vs **Bad**
    - If one assigns one non-default parameters Python requires all the
      successive parameters to be name-assigned
        - This cause maintenance burden
    - The **Bad** approach is in contrast with our rule for the default parameters
        - We want to highlight which parameters are overriding the default
    - The **Bad** approach in practice requires all positional parameters to be
      assigned explicitly causing:
        - repetition in violation of DRY (e.g., you need to repeat the same
          parameter everywhere); and
        - maintainance burden (e.g., if you change the name of a function parameter you
          need to change all the invocations)
    - The **Bad** style is a convention used in no language (e.g., C, C++, Java)
        - All languages allow binding by parameter position
        - Only some languages allow binding by parameter name
    - The **Bad** makes the code very wide, creating problems with our 80 columns
      rule

- Cons of **Good** vs **Bad**
    - One could argue that the **Bad** form is clearer
        - IMO the problem is in the names of the variables, which are
          uninformative, e.g., a better naming achives the same goal of clarity
            ```python
            mul1 = 1
            mul2 = 2
            sum_ = 3
            mult_and_sum(mul1, mul2, sum_)
            ```

# Misc (to reorg)

- TODO(\*): Start moving these functions in the right place once we have more a
  better document structure

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

- In documentation and comments we capitalize abbreviations (e.g., `YAML`,
  `CSV`)
- In the code:
  - We try to leave abbreviations capitalized when it doesn't conflict with
    other rules
    - E.g., `convert_to_CSV`, but `csv_file_name` as a variable name that is not
      global
  - Other times we use camel case, when appropriate
    - E.g., `ConvertCsvToYaml`, since `ConvertCSVToYAML` is difficult to read

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
  - At the beginning of the file: functions / classes for reading data
  - Then: functions / classes to process the data
  - Finally at the end of the file: functions / classes to save the data

- Try to put private helper functions close to the functions that are using them
  - This rule of thumb is a bit at odds with clearly separating public and
    private section in classes
    - A possible justification is that classes typically contain less code than
      a file and tend to be used through their API
    - A file contains larger amount of loosely coupled code and so we want to
      keep implementation and API close together

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
  ```

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

- It makes sense to check early only when you want to fail before doing more
  work
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

- Code belonging to top level libraries (e.g., `//amp/core`, `//amp/helpers`)
  and production (e.g., `//p1/db`, `vendors`) needs to meet high quality
  standards, e.g.,
  - Well commented
  - Following our style guide
  - Thoroughly reviewed
  - Good design
  - Comprehensive unit tests

- Research code in notebook and python can follow slightly looser standards,
  e.g.,
  - Sprinkled with some TODOs
  - Not perfectly general

- The reason is that:
  - Research code is still evolving and we want to keep the structure flexible
  - We don't want to invest the time in making it perfect if the research
    doesn't pan out

- Note that research code still needs to be:
  - Understandable / usable by not authors
  - Well commented
  - Follow the style guide
  - Somehow unit tested

- We should be able to raise the quality of a piece of research code to
  production quality when that research goes into production

## Life cycle of research code

- Often the life cycle of a piece of code is to start as research and then be
  promoted to higher level libraries to be used in multiple research, after its
  quality reaches production quality

## No ugly hacks

- We don't tolerate "ugly hacks", i.e., hacks that require lots of work to be
  undone (much more than the effort to do it right in the first place)
  - Especially an ugly design hack, e.g., a Singleton, or some unnecessary
    dependency between distant pieces of code
  - Ugly hacks spreads everywhere in the code base

## Keep related code close

- Try to keep related code as close as possible to their counterparty to
  highlight logical blocks
- Add comments to explain the logical blocks composing complex actions

- E.g., consider this code:

  ```python
  func_info = collections.OrderedDict()
  func_sig = inspect.signature(self._transformer_func)
  # Perform the column transformation operations.
  if "info" in func_sig.parameters:
      df = self._transformer_func(
          df, info=func_info, **self._transformer_kwargs
      )
      info["func_info"] = func_info
  else:
      df = self._transformer_func(df, **self._transformer_kwargs)
  ```

- Observations:
  - `func_info` is used only in one branch of the `if-then-else`, so it should
    be only in that branch
  - Since `func_sig` is just a temporary alias for making the code easier to
    follow (good!), we want to keep it close to where it's used

  ```python
  # Perform the column transformation operations.
  func_sig = inspect.signature(self._transformer_func)
  if "info" in func_sig.parameters:
      # If info is available in the function signature, then pass it
      # to the transformer.
      func_info = collections.OrderedDict()
      df = self._transformer_func(
          df, info=func_info, **self._transformer_kwargs
      )
      info["func_info"] = func_info
  else:
      df = self._transformer_func(df, **self._transformer_kwargs)
  ```

## Always separate what changes from what stays the same

- In both main code and unit test it's not a good idea to repeat the same code

- **Bad**
  - Copy-paste-modify

- **Good**
  - Refactor the common part in a functionand then change the parameters used to
    call the function

- Example:
  - What code is clearer to you, VersionA or VersionB?
  - Can you spot the difference between the 2 pieces of code?
  - VersionA

    ```python
    stopwords = nlp_ut.read_stopwords_json(_STOPWORDS_PATH)
    texts = ["a", "an", "the"]
    stop_words = nlp_ut.get_stopwords(
        categories=["articles"], stopwords=stopwords
    )
    actual_result = nlp_ut.remove_tokens(texts, stop_words=stop_words)
    expected_result = []
    self.assertEqual(actual_result, expected_result)

    ...
    texts = ["do", "does", "is", "am", "are", "be", "been", "'s", "'m", "'re"]
    stop_words = nlp_ut.get_stopwords(
        categories=["auxiliary_verbs"], stopwords=stopwords,
    )
    actual_result = nlp_ut.remove_tokens(texts, stop_words=stop_words)
    expected_result = []
    self.assertEqual(actual_result, expected_result)
    ```
  - VersionB

    ```python
    def _helper(texts, categories, expected_result):
        stopwords = nlp_ut.read_stopwords_json(_STOPWORDS_PATH)
        stop_words = nlp_ut.get_stopwords(
            categories=categories, stopwords=stopwords
        )
        actual_result = nlp_ut.remove_tokens(texts, stop_words=stop_words)
        expected_result = []
        self.assertEqual(actual_result, expected_result)

    texts = ["a", "an", "the"]
    categories = ["articles"]
    expected_result = []
    _helper(texts, categories, expected_result)
    ...

    texts = ["do", "does", "is", "am", "are", "be", "been", "'s", "'m", "'re"]
    categories = ["auxiliary_verbs"]
    expected_result = []
    _helper(texts, categories, expected_result)
    ```
    - Yes, VersionA is **Bad** and VersionB is **Good**

## Don't mix real changes with linter changes

- The linter is in change of reformatting the code according to our conventions
  and reporting potential problems

1. We don't commit changes that modify the code together with linter
   reformatting, unless the linting is applied to the changes we just made
   - The reason for not mixing real and linter changes is that for a PR or to
     just read the code it is difficult to understand what really changed vs
     what was just a cosmetic modification

2. If you are worried the linter might change your code in a way you don't like,
   e.g.,
   - Screwing up some formatting you care about for some reason, or
   - Suggesting changes that you are worried might introduce bugs you can commit
     your code and then do a "lint commit" with a message "PartTaskXYZ: Lint"

- In this way you have a backup state that you can rollback to, if you want

3. If you run the linter and see that the linter is reformatting / modifying
   pieces of code you din't change, it means that our team mate forgot to lint
   their code
   - `git blame` can figure out the culprit
   - You can send him / her a ping to remind her to lint, so you don't have to
     clean after him / her
   - In this case, the suggested approach is:
     - Commit your change to a branch / stash
     - Run the linter by itself on the files that need to be cleaned, without
       any change
     - Run the unit tests to make sure nothing is breaking
     - You can fix lints or just do formatting: it's up to you
     - You can make this change directly on `master` or do a PR if you want to
       be extra sure: your call
