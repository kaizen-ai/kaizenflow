# Sorrentum - Python Style Guide

# Meta
- What we call the "rules" is actually just a convention
  - Not about the absolute best way of doing something in all cases
  - Optimized for the common case
  - Can become cumbersome or weird to follow for some corner cases
- We prefer simple rather than optimal rules that can be applied in most of the cases without thinking or going to check the documentation
- The rules are striving to achieve consistency and robustness
  - E.g., see "tab vs space" flame-war from the 90s
  - We care about consistency rather than arguing about which approach is better in each case
- The rules are optimized for the average developer / data scientist and not for power users
- The rules try to minimize the maintenance burden
  - We don't want a change somewhere to propagate everywhere
  - We want to minimize the propagation of a change
- Some of the rules are evolving based on what we are seeing through the reviews

# Disclaimer
This document was forked from [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html), therefore, the numbering of chapters sets off where the Style Guide ends. Make sure to familiarize yourself with it before proceeding to the rest of the doc, since it is the basis of our team’s code style.

Another important source is [The Pragmatic Programmer](https://drive.google.com/file/d/1g0vjtaBVq0dt32thNprBRJpeHJsJSe-Z/view?usp=sharing) by David Thomas and Andrew Hunt. While not Python-specific, it provides an invaluable set of general principles by which any person working with code (software developer, DevOps or data scientist) should abide. Read it on long commutes, during lunch, and treat yourself to a physical copy on Christmas. The book is summarized [here](https://github.com/cryptokaizen/cmamp/blob/master/documentation/general/the_pragramatic_programmer.md), but do not deprive yourself of the engaging manner in which Thomas & Hunt elaborate on these points -- on top of it all, it is a very, very enjoyable read.

## References
- Coding
  - [Google Python Style Guide (GPSG)](https://google.github.io/styleguide/pyguide.html)[](https://google.github.io/styleguide/pyguide.html)
  - [](https://google.github.io/styleguide/pyguide.html)[Code convention from PEP8](https://www.python.org/dev/peps/pep-0008/)[](https://www.python.org/dev/peps/pep-0008/)
- [](https://www.python.org/dev/peps/pep-0008/)Documentation
  - [Docstring convention from PEP257](https://github.com/google/styleguide/blob/gh-pages/docguide/best_practices.md)
  - [Google documentation best practices](https://github.com/google/styleguide/blob/gh-pages/docguide/best_practices.md)
- Commenting style
  - [Sphinx](https://www.sphinx-doc.org/en/master/)
- [Sphinx tutorial](https://thomas-cokelaer.info/tutorials/sphinx/index.html)
- Design
  - [Google philosophical stuff](https://github.com/google/styleguide/blob/gh-pages/docguide/philosophy.md)
  - [Unix rules (although a bit cryptic sometimes)](https://en.wikipedia.org/wiki/Unix_philosophy#Eric_Raymond%E2%80%99s_17_Unix_Rules)

# High-Level Principles
In this paragraph we summarize the high-level principles that we follow for designing and implementing code and research. We should be careful in adding principles here. Ideally principles should be non-overlapping and generating all the other lower level principles we follow (like a basis for a vector space)

- ### Follow the [DRY](https://en.wikipedia.org/wiki/Don%27t_repeat_yourself) principle
- ### Optimize for reading
  - Make code easy to read even if it is more difficult to write
  - Code is written 1x and read 100x
- ### Encapsulate what changes
  - Separate what changes from what stays the same
- ### [Least surprise principle](https://en.wikipedia.org/wiki/Principle_of_least_astonishment)
  - Try to make sure that the reader is not surprised
- ### Pay the technical debt
  - Any unpaid debt is guaranteed to bite you when you don't expect it
  - Still some debt is inevitable: try to find the right trade-off
- ### End-to-end first
  - Always focus on implementing things end-to-end, then improve each block
  - Remember the analogy of building the car through the skateboard, the bike, etc.
    - Compare this approach to building wheels, chassis, with a big-bang
    integration at the end
- ### Unit test everything
  - Code that matters needs to be unit tested
  - Code that doesn't matter should not be checked in the repo
  - The logical implication is: all code checked in the repo should be unit tested
- ### Don't get attached to code
  - It's ok to delete, discard, retire code that is not useful any more
  - Don't take it personally when people suggest changes or simplification
- ### Always plan before writing code
  - File a GitHub issue
  - Think about what to do and how to do it
  - Ask for help or for a review
  - The best code is the one that we avoid to write through a clever mental kung-fu move
- ### Think hard about naming
  - Finding a name for a code object, notebook, is extremely difficult but very important to build a mental map
  - Spend the needed time on it
- ### Look for inconsistencies
  - Stop for a second after you have, before sending out:
    - Implemented code or a notebook
    - Written documentation
    - Written an e-mail
    - ...
  - Reset your mind and look at everything with fresh eyes like if it was the first time you saw it
    - Does everything make sense to someone that sees this for the first time?
    - Can (and should) it be improved?
    - Do you see inconsistencies, potential issues?
  - It will take less and less time to become good at this

# Our coding suggestions

## Being careful with naming

### Follow the conventions
- Name executable files (scripts) and library functions using verbs (e.g., `download.py`, `download_data()`)
- Name classes and (non-executable) files using nouns (e.g., `Downloader()`, `downloader.py`)
- For decorators we don't use a verb as we do for normal functions, but rather an adjective or a past tense verb, e.g.,
  ```
  def timed(f):
      """
      Decorator adding a timer around function `f`.
      """
      …
  ```

### Follow spelling rules
- Capitalize the abbreviations, e.g.:
  - `CSV`
  - `DB` since it's an abbreviation of Database
- We spell commands in lower-case, and programs with initial upper case:
  - "Git" (as program), "git" (as the command)
- We distinguish "research" (not "default", "base") vs "production"
- We use different names for indicating the same concept, e.g., `dir`, `path`, `folder`
  - Preferred term is `dir`

### Search good names, avoid bad names
- Naming things properly is one of the most difficult task of a programmer /
  data scientist
  - The name needs to be (possibly) short and memorable
    - Don't be afraid to use long names, if needed, e.g.,
      `process_text_with_full_pipeline_twitter_v1`
    - However, clarity is more important than number of bytes used
  - The name should capture what the object represents, without reference to
    things that can change or to details that are not important
  - The name should refer to what objects do (i.e., mechanisms), rather than how
    we use them (i.e., policies)
  - The name needs to be non-controversial: people need to be able to map the
    name in their mental model
  - The name needs to sound good in English
    - Bad: `AdapterSequential` sounds bad
    - Good: `SequentialAdapter` sounds good

- Some examples of how NOT to do naming:
  - `raw_df` is a terrible name
    - "raw" with respect to what?
    - Cooked?
    - Read-After-Write race condition?
  - `person_dict` is bad
    - What if we switch from a dictionary to an object?
      - Then we need to change the name everywhere!
    - The name should capture what the data structure represents (its semantics) and not how it is implemented

### Avoid code stutter
- An example of code stutter: in a module `git` there is a function called
  `get_git_root_path()`.
- Bad
  ```
  import helpers.git as git

  ... git.get_git_root_path()
  ```
  - You see that the module is already specifying we are talking about Git
- Good
  ```
  import helpers.git as git

  ... git.get_root_path()
  ```
  - This is not only aesthetic reason but a bit related to a weak form of DRY

## Comments

### Docstring conventions
- Code needs to be properly commented
- We follow python standard [PEP 257](https://www.python.org/dev/peps/pep-0257/)
  for commenting
  - PEP 257 standardizes what comments should express and how they should do it (e.g., use triple quotes for commenting a function), but does not specify what markup syntax should be used to describe comments
- Different conventions have been developed for documenting interfaces
  - ReST
  - Google (which is cross-language, e.g., C++, python, ...)
  - Epytext
  - Numpydoc

### ReST style
- ReST (aka re-Structured Text) style is:
  - The most widely supported in the python community
  - Supported by all doc generation tools (e.g., epydoc, sphinx)
  - Default in pycharm
  - Default in pyment
  - Supported by pydocstyle (which does not support Google style as explained [here](https://github.com/PyCQA/pydocstyle/issues/275))
- An example of a function comment is:
  ```
  """
  This is a ReST style.

  :param param1: this is a first param
  :type param1: str
  :param param2: this is a second param
  :type param2: int
  :returns: this is a description of what is returned
  :rtype: bool
  :raises keyError: raises an exception
  """
  ```
  - We pick lowercase after `:param XYZ: ...` unless the first word is a proper noun or type
  - Type hinting makes the `:type ...` redundant and you should use only type hinting
  - As a result, an example of a function definition is:
    ```
    def my_function(param1: str) -> str:
        """
        A one-line description of what the function does.

        A longer description (possibly on multiple lines) with a more detailed
        explanation of what the function does, trying to not be redundant with
        the parameter / return description below. The focus is on the interface
        and what the user should know to use the function and not how the
        function is implemented.

        :param param1: this is a first param
        :return: this is a description of what is returned
        """
    ```
- [More examples of and discussions on python docstrings](https://stackoverflow.com/questions/3898572)

### Descriptive vs imperative style
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

### Comments
- Comments follow the same style of docstrings, e.g., imperative style with
  period . at the end

### Use type hints
- We expect new code to use type hints whenever possible
  - See [PEP 484](https://www.python.org/dev/peps/pep-0484/)
  - [Type hints cheat sheet](https://mypy.readthedocs.io/en/latest/cheat_sheet_py3.html)
- At some point we will start adding type hints to old code
- We plan to start using static analyzers (e.g., `mypy`) to check for bugs from
  type mistakes and to enforce type hints at run-time, whenever possible

### Replace empty lines in code with comments
- If you feel that you need an empty line in the code, it probably means that a
  specific chunk of code is a logical piece of code performing a cohesive
  function.
  ```
  ...
  end_y = end_dt.year
  # Generate list of file paths for ParquetDataset.
  paths = list()
  ...
  ```
- Instead of putting an empty line, you should put a comment describing at high
  level what the code does.
  ```
  ...
  end_y = end_dt.year
  # Generate list of file paths for ParquetDataset.
  paths = list()
  ...
  ```
- If you don't want to add comments,  just comment the empty line.
  ```
  ...
  end_y = end_dt.year
  #
  paths = list()
  ...
  ```
- The problem with empty lines is that they are visually confusing since one
  empty line is used also to separate functions. For this reason we suggest
  using an empty comment.

### Avoid distracting comments
- Use comments to explain the high level logic / goal of a piece of code and not
  the details
  - E.g., do not comment things that are obvious, e.g.,
  ```
  # Print results.
  _LOG.info("Results are %s", ...)
  ```

### Commenting out code
- When we comment out code, we should explain why it is no longer relevant and
- What we need to do before removing the commented out code:
  - E.g., instead of
    ```
    is_alive = pd.Series(True, index=metadata.index)
    # is_alive = kgutils.annotate_alive(metadata, self.alive_cutoff)
    ```
    use:
    ```
    # TODO(*): As discussed in PTask5047 for now we set all timeseries to be alive.
    # is_alive = kgutils.annotate_alive(metadata, self.alive_cutoff)
    is_alive = pd.Series(True, index=metadata.index)
    ```

### If you find a bug or obsolete docstring/TODO in the code
- The process is:
  - Do a `git blame` to find who wrote the code
  - If it's an easy bug, you can fix it and ask for a review from the author
  - You can comment on a PR (if there is one)
  - You can file a bug on Github with
    - Clear info on the problem
    - How to reproduce it, ideally a unit test
    - Stacktrace
    - You can use the tag "BUG: ..."

### Referring to an object in code comments
- We prefer to refer to objects in the code using Markdown like this (this is
  a convention used in the documentation system Sphinx)
  ```
  """
  Decorator adding a timer around function `f`.
  """
  ```
- This is useful for distinguishing the object code from the real-life object
- E.g.,
  ```
  # The df `df_tmp` is used for ...
  ```

### Inline comments
- In general we prefer to avoid writing comments on the same line as code since
  they require extra maintenance (e.g., when the line becomes too long)
  - Bad
    ```
    print("hello world")      # Introduce yourself.
    ```
  - Good
    ```
    # Introduce yourself.
    print("hello world")
    ```

## Linter

### Disabling linter messages
- When the linter reports a problem:
  - We assume that linter messages are correct, until the linter is proven wrong
  - We try to understand what is the rationale for the linter's complaints
  - We then change the code to follow the linter's suggestion and remove the lint
1. If you think a message is too pedantic, please file a bug with the example
  and as a team we will consider whether to exclude that message from our list
  of linter suggestions
2. If you think the message is a false positive, then try to change the code to
  make the linter happy
    - E.g., if the code depends on some run-time behavior that the linter can't infer, then you should question whether that behavior is really needed
    - A human reader would probably be as confused as the linter is
3. If you really believe you should override the linter in this particular
  case, then use something like:
    ```
    # pylint: disable=some-message,another-one
    ```
    - You then need to explain in a comment why you are overriding the linter.

- Don't use linter code numbers, but the [symbolic name](https://github.com/josherickson/pylint-symbolic-names) whenever possible:
  - Bad
    ```
    # pylint: disable=W0611
    import config.logging_settings
    ```
  - Good
    ```
    # pylint: disable=unused-import
    # This is needed when evaluating code at run-time that depends from
    # this import.
    import config.logging_settings
    ```

### Prefer non-inlined linter comments
- Although we don't like inlined comments sometimes there is no other choice
  than an inlined comment to get the linter to understand which line we are
  referring to:
  - Bad but ok if needed
    ```
    # pylint: disable=unused-import
    import config.logging_settings
    ```
  - Good
    ```
    import config.logging_settings  # pylint: disable=unused-import
    ```

## Logging

### Always use logging instead of prints
- Always use logging and never `print()` to monitor the execution

### Our logging idiom
- In order to use our logging framework (e.g., `-v` from command lines, and much
  more) use:
  ```
  import helpers.dbg as dbg

  _LOG = logging.getLogger(__name__)

  dbg.init_logger(verbosity=logging.DEBUG)

  _LOG.debug("I am a debug function about %s", a)
  ```
- In this way one can decide how much debug info is needed (see Unix rule of
  silence)
  - E.g., when there is a bug one can run with `-v DEBUG` and see what's
    happening right before the bug

### Logging level
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

### Use positional args when logging
- Bad
  ```
  _LOG.debug('cmd=%s %s %s' % (cmd1, cmd2, cmd3))
  _LOG.debug('cmd=%s %s %s'.format(cmd1, cmd2, cmd3))
  _LOG.debug('cmd={cmd1} {cmd2} {cmd3}')
  ```
- Good
  ```
  _LOG.debug('cmd=%s %s %s', cmd1, cmd2, cmd3)
  ```
- The two statements are equivalent from the functional point of view
- The reason is that in the second case the string is not built unless the
  logging is actually performed, which limits time overhead from logging
### Exceptions don't allow positional args
- For some reason people tend to believe that using the logging / dassert
  approach of positional param to exceptions
  - Bad (use positional args)
    ```
    raise ValueError("Invalid server_name='%s'", server_name)
    ```
  - Good (use string interpolation)
    ```
    raise ValueError("Invalid server_name='%s'" % server_name)
    ```
  - Best (use string format)
    ```
    raise ValueError(f"Invalid server_name='{server_name}'")
    ```
- The constructor of an exception accepts a string
- Using the string f-format is best since
  - It's more readable
  - There is little time overhead since if you get to the exception probably the
    code is going to terminate, and it's not in a hot loop

### Report warnings
- If there is a something that is suspicious but you don't feel like it's
  worthwhile to assert, report a warning with:
  ```
  _LOG.warning(...)
  ```
- If you know that if there is a warning then there are going to be many many
  warnings
  - Print the first warning
  - Send the rest to warnings.log
  - At the end of the run, reports "there are warnings in warnings.log"

## Assertions

### Use positional args when asserting
- `dassert_*` is modeled after logging so for the same reasons one should use
  positional args 
- Bad
  ```
  dbg.dassert_eq(a, 1, "No info for %s" % method)
  ```
  Good
  ```
  dbg.dassert_eq(a, 1, "No info for %s", method)
  ```
### Report as much information as possible in an assertion
- When using a `dassert_*` you want to give to the user as much information as
  possible to fix the problem
  - E.g., if you get an assertion after 8 hours of computation you don't want to
    have to add some logging and run for 8 hours to just know what happened
- A `dassert_*` typically prints as much info as possible, but it can't report
  information that is not visible to it:
  - Bad
    ```
    dbg.dassert(string.startswith('hello'))
    ```
    - You don't know what is value of `string` is
  - Good
    ```
    dbg.dassert(string.startswith('hello'), "string='%s'", string)
    ```
    - Note that often is useful to add ' (backtick) to fight pesky spaces that make the value unclear, or to make the error as readable as possible

## Imports

### Don't use evil `import *`
- Do not use in notebooks or code the evil `import *`
  - Bad
    ```
    from helpers.sql import *
    ```
  - Good
    ```
    import helpers.sql as hsql
    ```
- The `from ... import *`:
  - Pollutes the namespace with the symbols and spreads over everywhere, making it
    painful to clean up
  - Obscures where each function is coming from, removing the context that comes
    with the namespace
  - [Is evil in many other ways](https://stackoverflow.com/questions/2386714/why-is-import-bad)

### Cleaning up the evil `import *`
- To clean up the mess you can:
  - For notebooks
    - Find & replace (e.g., using jupytext and pycharm)
    - Change the import and run one cell at the time
  - For code
    - Change the import and use linter on file to find all the problematic spots
- One of the few spots where the evil `import *` is ok is in the `__init__.py` to
  tweak the path of symbols exported by a library
  - This is an advanced topic and you should rarely use it

### Avoid `from ... import ...`
- The rule is:
  ```
  import library as short_name
  import library.sublibrary as short_name
  ```
- This rule applies to imports of third party libraries and our library
- Importing many different functions, like:
  - Bad
    ```
    from helpers.sql import get_connection, get_connection_from_env_vars, \
        DBConnection, wait_connection_from_db, execute_insert_query
    ```
  - Good
    ```
    import helpers.sql as hsql
    ...
    ... hsql.get_connection()
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

### Exceptions to the import style
- We try to minimize the exceptions to this rule to avoid to keep this rule
  simple, rather than discussing about
- The current agreed upon exceptions are:
  - For `typing` it is ok to do:
    ```
    from typing import Iterable, List
    ```
    in order to avoid typing everywhere, since we want to use type hints as
    much as possible

### Examples of imports
- Example 1
  - Bad
    ```
    from im_v2.ccxt.data.extract import exchange_class as excls
    ```
  - Good
    ```
    import im_v2.ccxt.data.extract.exchange_class as imvcdeexcl
    ```
- Example 2
  - Bad
    ```
    from edgar.shared import headers_extractor as he
    ```
  - Good
    ```
    import edgar.shared.headers_extractor as he
    ```
- Example 3
  - Bad
    ```
    from helpers import dbg
    ```
  - Good
    ```
    import helpers.dbg as hdbg
    ```
- Example 4
  - Bad
    ```
    from helpers.misc import check_or_create_dir, get_timestamp
    ```
  - Good
    ```
    import helpers.misc as hm
    ```

### Always import with a full path from the root of the repo / submodule
- Bad
  ```
  import exchange_class
  ```
- Good
  ```
  import im_v2.ccxt.data.extract.exchange_class
  ```
- In this way your code can run without depending upon your current dir

### Baptizing module import
- Each module that can be imported should have a docstring at the very beginning
  (before any code) describing how it should be imported
  ```
  """
  Import as:

  import helpers.printing as prnt
  """
  ```
- The import abbreviations are created by linter
- The goal is to have always the same imports so it's easy to move code around,
  without collisions

### Use Python and not bash for scripting
- We prefer to use python instead of bash scripts with very few
  exceptions
  - E.g., scripts that need to modify the environment by setting env vars, like
    `setenv.sh`
- The problem with bash scripts is that it's too easy to put together a sequence of commands to automate a workflow
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

### Skeleton for a script
- The ingredients are:
  - `dev_scripts/script_skeleton.py`: a template to write simple scripts you can copy and modify it
  - `helpers/system_interaction.py`: a set of utilities that make simple to run shell commands (e.g., capturing their output, breaking on error or not, tee-ing to file, logging, ...)
  - `helpers` has lots of useful libraries
- The official reference for a script is `dev_scripts/script_skeleton.py`
- You can copy this file and change it
- A simple example is: `dev_scripts/git/gup.py`
- A complex example is: `dev_scripts/linter.py`

### Some useful patterns
- Some useful patterns / idioms that are supported by the framework are:
  - Incremental mode: you skip an action if its outcome is already present
    (e.g., skipping creating a dir, if it already exists and it contains all the
    results)
  - Non-incremental mode: clean and execute everything from scratch
  - Dry-run mode: the commands are written to screen instead of being executed

### Use scripts and not notebooks for long-running jobs
- We prefer to use scripts to execute code that might take long time (e.g., hours) to run, instead of notebooks
- Pros of script
  - All the parameters are completely specified by a command line
  - Reproducible and re-runnable
- Cons of notebooks
  - Tend to crash / hang for long jobs
  - Not easy to understand if the notebook is doing progress
  - Not easy to get debug output
- Notebooks are designed for interactive computing / debugging and not batch jobs
  - You can experiment with notebooks, move the code into a library, and wrap it in a script

### Follow the same structure
- All python scripts that are meant to be executed directly should:
  1. Be marked as executable files with:
      ```
      > chmod +x foo_bar.py
      ```
  2. Have the python code should start with the standard Unix shebang notation:
      ```
      #!/usr/bin/env python
      ```
    - This line tells the shell to use the `python` defined in the environment
  3. Have a:
      ```
      if __name__ == "__main__":
          ...
      ```
  4. Ideally use `argparse` to have a minimum of customization

- In this way you can execute directly without prepending with python

### Use clear names for the scripts
- In general scripts (like functions) should have a name like "action_verb".
  - Bad
    - Example of bad names are `timestamp_extractor.py` and `timestamp_extractor_v2.py`
      - Which timestamp data set are we talking about?
      - What type of timestamps are we extracting?
      - What is the difference about these two scripts?
- We need to give names to scripts that help people understand what they do and
  the context in which they operate
- We can add a reference to the task that originated the work (to give more
  context)
- E.g., for a script generating a dataset there should be an (umbrella) bug for
  this dataset, that we refer in the bug name, e.g., `TaskXYZ_edgar_timestamp_dataset_extractor.py`
- Also where the script is located should give some clue of what is related to

## Functions

### Avoid using non-exclusive `bool` arguments
- While a simple `True`/`False` switch may suffice for today's needs, very often
  more flexibility is eventually needed
- If more flexibility is needed for a `bool` argument, you are faced with the
  choice:
  - Adding another parameter (then parameter combinations grow exponentially and
    may not all make sense)
  - Changing the parameter type to something else
- Either way, you have to change the function interface
- To maintain flexibility from the start, opt for a `str` parameter "mode",
  which is allowed to take a small well-defined set of values.
- If an implicit default is desirable, consider making the default value of the
  parameter `None`. This is only a good route if the default operation is
  non-controversial / intuitively obvious.

### Try to make functions work on multiple types
- We encourage implementing functions that can work on multiple related types:
  - Bad: implement `demean_series()`, `demean_dataframe()`
  - Good: implement a function `demean(obj)` that can work with `pd.Series` and `pd.DataFrame`
    - One convention is to call `obj` the variable whose type is not known until
      run-time
- In this way we take full advantage of duck typing to achieve something similar
  to C++ function overloading (actually even more expressive)
- Try to return the same type of the input, if possible
  - E.g., the function called on a `pd.Series` returns a `pd.Series`

### Avoid hard-wired column name dependencies
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
  ```
  DATETIME_COL = "datetime"
  ```
  - Users of the library can now access the column name through imports
  - This prevents hidden column name dependencies from spreading like a virus
    throughout the codebase

### Single exit point from a function
  ```
  @staticmethod
  def _get_zero_element(list_: list):
          if not list*: return None
      else:
          return list*[0]

  im.kibot/utils.py:394: [R1705(no-else-return), ExpiryContractMapper.extract_contract_expiry] Unnecessary "else" after "return"
  [pylint]
  ```
- Try to have a single exit point from a function, since this guarantees that
  the return value is always the same
- In general returning different data structures from the same function (e.g., a list in one case and a float in another) is indication of bad design
  - There are exceptions like a function that works on different types (e.g., accepts a dataframe or a series and then returns a dataframe or a series, but the input and output is the same)
  - Returning different types (e.g., float and string) is also bad
  - Returning a type or None is typically ok
- Try to return values that are consistent so that the client doesn't have to switch statement, using isinstance(...)
  - E.g., return a float and if the value can't be computed return np.nan (instead of None) so that the client can use the return value in a uniform way
    ```
    @staticmethod
    def _get_zero_element(list_: list):
        if not list*:
            ret = None
        else:
            `ret = list*[0] return ret

    def _get_zero_element(list_: list):
        ret = None if not list* else list*[0] return ret
    ```
- It's ok to have functions like:
    ```
    def ...(...):
        # Handle simple cases.
        ...
        if ...:
            return
        # lots of code
        ...
        return
    ```

### Order of function parameters

#### Problem
- We want to have a standard, simple, and logical order for specifying the
  arguments of a function

#### Decision
- The preferred order is:
  - Input parameters
  - Output parameters
  - In-out parameters
  - Default parameters

### Consistency of ordering of function parameters
- Try to:
  - Keep related variables close to each other
  - Keep the order of parameters similar across functions that have similar
    interface
- Enforcing these rules is based on best effort
- PyCharm is helpful when changing order of parameters
- Use linter to check consistency of types between function definition
  and invocation

### Style for default parameter
#### Problem
- How to assign default parameters in a function?
#### Decision
- It's ok to use a default parameter in the interface as long as it is a Python scalar (which is immutable by definition)
  - Good
    ```
    def function(
      value: int = 5,
      dir_name : str = "hello_world",
    ):
    ```
- You should not use list, maps, objects, but pass `None` and then initialize
  inside the function
  - Bad
    ```
    def function(
      # These are actually a bug waiting to happen.
      obj: Object = Object(),
      list_: list[int] = [],
    ):
    ```
  - Good
    ```
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
  - Good
    ```
    def function1(
      ...,
      dir_name: Optional[str] = None,
    ):
        if dir_name is None:
          dir_name = "/very_long_path"
          # You can also abbreviate the previous code as:
          # dir_name = dir_name or "/very_long_path"

    def function2(
      ...,
      dir_name: Optional[str] = None,
    ):
      function1(..., dir_name=dir_name)
    ```

#### Rationale
- Pros of the Good vs Bad style
  - When you wrap multiple functions, each function needs to propagate the
    default parameters, which: - violates DRY; and - adds maintenance burden (if you change the innermost default parameter, you need to change all of them!)
    - With the proposed approach, all the functions use `None`, until the
      innermost function resolves the parameters to the default values
  - The interface is cleaner
  - Implementation details are hidden (e.g., why should the caller know what is the default path?)
  - Mutable parameters can not be passed through (see [here](https://stackoverflow.com/questions/1132941/least-astonishment-and-the-mutable-default-argument)))
- Cons:
  - One needs to add `Optional` to the type hint

### Calling functions with default parameters
#### Problem
- You have a function
  ```
  def func(
    task_name : str,
    dataset_dir : str,
    clobber : bool = clobber,
  ):
  ...
  ```
- How should it be invoked?

#### Decision
- We prefer to
  - Assign directly the positional parameters
  - Bind explicitly the parameters with a default value using their name
- Good
  ```
  func(task_name, dataset_dir, clobber=clobber)
  ```
- Bad
  ```
  func(task_name, dataset_dir, clobber)
  ```
#### Rationale
- Pros of Good vs Bad style
  - If a new parameter with a default value is added to the function `func`
    before `clobber`:
    - The Good idiom doesn't need to be changed
    - All instances of the Bad idiom need to be updated
      - The Bad idiom might keep working but with silent failures
      - Of course `mypy` and `Pycharm` might point this out
  - The Good style highlights which default parameters are being
    overwritten, by using the name of the parameter
    - Overwriting a default parameter is an exceptional situation that should be
      explicitly commented
- Cons:
  - None

### Don't repeat non-default parameters
#### Problem
- Given a function with the following interface:
  ```
  def mult_and_sum(multiplier_1, multiplier_2, sum_):
      return multiplier_1 * multiplier_2 + sum_
  ```
  how to invoke it?
#### Decision
- We prefer to invoke it as:
  - Good
    ```
    a = 1
    b = 2
    c = 3
    mult_and_sum(a, b, c)
    ```
  - Bad
    ```
    a = 1
    b = 2
    c = 3
    mult_and_sum(multiplier_1=a,
                 multiplier_2=b,
                 sum_=c)
    ```
#### Rationale
- Pros of Good vs Bad
  - Non-default parameters in Python require all the successive parameters to be name-assigned
    - This causes maintenance burden
  - The Bad approach is in contrast with our rule for the default parameters
    - We want to highlight which parameters are overriding the default
  - The Bad approach in practice requires all positional parameters to be
    assigned explicitly causing:
    - Repetition in violation of DRY (e.g., you need to repeat the same parameter everywhere); and
    - Maintainance burden (e.g., if you change the name of a function parameter you need to change all the invocations)
  - The Bad style is a convention used in no language (e.g., C, C++, Java)
    - All languages allow binding by parameter position
    - Only some languages allow binding by parameter name
  - The Bad makes the code very wide, creating problems with our 80 columns rule
- Cons of Good vs Bad
  - One could argue that the Bad form is clearer
    - IMO the problem is in the names of the variables, which are uninformative,
      e.g., a better naming achieves the same goal of clarity
      ```
      mul1 = 1
      mul2 = 2
      sum_ = 3
      mult_and_sum(mul1, mul2, sum_)
      ```

## Writing clear beautiful code

### Keep related code close
E.g., keep code that computes data close to the code that uses it.

This holds also for notebooks: do not compute all the data structure and then analyze them.

It’s better to keep the section that “reads data” close to the section that “processes it”. In this way it’s easier to see “blocks” of code that are dependent from each other, and run only a cluster of cells.

### Distinguish public and private functions
The public functions `foo_bar()` (not starting with `_`) are the ones that make up the interface of a module and that are called from other modules and from notebooks.

Use private functions like `_foo_bar()` when a function is a helper of another private or public function. Also follow the “keep related code close” close by keeping the private functions close to the functions (private or public) that are using them.

Some references:
- [StackOverflow](https://stackoverflow.com/questions/1641219/does-python-have-private-variables-in-classes?noredirect=1&lq=1)

### Keep public functions organized in a logical order
Keep the public functions in an order related to the use representing the typical flow of use, e.g.,
- common functions, used by all other functions
- read data
- process data
- save data

You can use banners to separate layers of the code. Use the banner long 80 cols (e.g., I have a vim macro to create banners that always look the same) and be consistent with empty lines before / empty and so on.

The banner is a way of saying “all these functions belong together”.

```
# #############################################################################
# Common functions
# #############################################################################

...

# #############################################################################
# Read data
# #############################################################################

def _helper1_to_func1():
...

def _helper2_to_func1():
...

def func1_read_data1():
    _helper1_to_func1()
    ...
    _helper2_to_func2()

# #############################################################################
# Process data
# #############################################################################

...

# #############################################################################
# Save data
# #############################################################################

...
```

1. Ideally each section of code should use only sections above, and be used by sections below (aka “Unix layer approach”).
2. If you find yourself using too many banners this is in indication that code might need to be split into different files.
    - Although we don’t have agreed upon rules, it might be ok to have large files as long as they are well organized. E.g., in pandas code base, all the code for DataFrame is in a single file long many thousands of lines (!), but it is nicely separated in sections that make easy to navigate the code
    - Too many files can become problematic, since one needs to start jumping across many files: in other words it is possible to organize the code too much (e.g. what if each function is in a single module?)
    - Let’s try to find the right balance.
3. It might be a good idea to use classes to split the code, but also OOP can have a dark side
    - E.g., using OOP only to reorganize the code instead of introducing “concepts”
    - IMO the worst issue is that they don’t play super-well with Jupyter autoreload

### Do not wrap printf()

Examples of horrible functions are:
```
# How many characters do we really saved? If typing is a problem, learn to touch type.
def is_exists(path):
    return os.path.exists(path)

# How many characters do we really saved? If typing is a problem, learn to touch type.
def make_dirs(path):
    os.makedirs(path)

# This is os.path.dirname.
def folder_name(f_name):
    if f_name[-1] != '/':
        return f_name + '/'
    return f_name
```

### Do not introduce another “concept” unless really needed

We want to introduce degrees of freedom and indirection only when we think this can be useful to make the code easy to maintain, read, and expand. 

If we add degrees of freedom everywhere just because we think that at some point in the future this might be useful, then there is very little advantage and large overhead.

Introducing a new variable, function, class introduces a new concept that one needs to keep in mind. People that read the code, needs to go back and forth in the code to see what each concept means.

Think about the trade-offs and be consistent.

Example 1

```
def fancy_print(txt):
    print "fancy: ", txt
```
Then people that change the code need to be aware that there is a function that prints in a special way. The only reason to add this shallow wrapper is that, in the future, we believe we want to change all these calls in the code.

Example 2

Another example is parametrizing a value used in a single function.
```
SNAPSHOT_ID = 'SnapshotId'
```

If multiple functions need to use the same value, then this practice can be a good idea. If there is a single function using this, one should at least keep it local to the function.

Still note that introducing a new concept can also create confusion. What if we need to change the code to:
```
SNAPSHOT_ID = 'TigerId'
```
then the variable and its value are in contrast.

### Return None or keep one type
Functions that return different types can make things complicated downstream, since the callers need to be aware of all of it and handle different cases. This also complicates the docstring, since one needs to explicitly explain what the special values mean, all the types and so on.

In general returning multiple types is an indication that there is a problem.

Of course this is a trade-off between flexibility and making the code robust and easy to understand.

E.g.,
```
if 'Tags' not in df.columns:
    df['Name'] = np.nan
else:
    df['Name'] = df['Tags'].apply(extract_name)
```

In this case it is better to either return None (to clarify that something special happened) or an empty dataframe pd.DataFrame(None) to allow the caller code being indifferent to what is returned.
```
if 'Tags' not in df.columns:
    df['Name'] = None
else:
    df['Name'] = df['Tags'].apply(extract_name)
```

### Avoid wall-of-text functions
```
def get_timestamp_v07(raw_df):
    timestamp_df = get_raw_timestamp(raw_df)
    documents_series = raw_df.progress_apply(he.extract_documents_v2, axis=1)
    documents_df = pd.concat(documents_series.values.tolist())
    documents_df.set_index(api.cfg.INDEX_COLUMN, drop=True, inplace=True)
    documents_df = documents_df[
        documents_df[api.cfg.DOCUMENTS_DOC_TYPE_COL] != '']
    types = documents_df.groupby(
        api.cfg.DOCUMENTS_IDX_COL)[api.cfg.DOCUMENTS_DOC_TYPE_COL].unique()
    timestamp_df[api.cfg.TIMESTAMP_DOC_TYPES_COL] = types
    is_xbrl_series = raw_df[api.cfg.DATA_COLUMN].apply(he.check_xbrl)
    timestamp_df[api.cfg.TIMESTAMP_DOC_ISXBRL_COL] = is_xbrl_series
    timestamp_df[api.cfg.TIMESTAMP_DOC_EX99_COL] = timestamp_df[
        api.cfg.TIMESTAMP_DOC_TYPES_COL].apply(
            lambda x: any(['ex-99' in t.lower() for t in x]))
    timestamp_df = timestamp_df.rename(columns=api.cfg.TIMESTAMP_COLUMN_RENAMES)
    return timestamp_df
```

This function is correct but it has few problems (e.g., lack of a docstring, extract_documents_v2, lots of unclear concepts, abuse of constants).

You should at least split the functions in chunks using # or even better comment what each chunk of code does.

```
def get_timestamp_v07(raw_df):
    # Get the df containing the timestamp information.
    timestamp_df = get_raw_timestamp(raw_df)
    # Extract the documents
    documents_series = raw_df.progress_apply(he.extract_documents_v2, axis=1)
    documents_df = pd.concat(documents_series.values.tolist())
    documents_df.set_index(api.cfg.INDEX_COLUMN, drop=True, inplace=True)
    documents_df = documents_df[
        documents_df[api.cfg.DOCUMENTS_DOC_TYPE_COL] != ''
    ]
    types = documents_df.groupby(
        api.cfg.DOCUMENTS_IDX_COL
    )[api.cfg.DOCUMENTS_DOC_TYPE_COL].unique()
    # Decorate the df with columns about types of information contained.
    timestamp_df[api.cfg.TIMESTAMP_DOC_TYPES_COL] = types
    is_xbrl_series = raw_df[api.cfg.DATA_COLUMN].apply(he.check_xbrl)
    timestamp_df[api.cfg.TIMESTAMP_DOC_ISXBRL_COL] = is_xbrl_series
    timestamp_df[api.cfg.TIMESTAMP_DOC_EX99_COL] = timestamp_df[
        api.cfg.TIMESTAMP_DOC_TYPES_COL
    ].apply(lambda x: any(['ex-99' in t.lower() for t in x]))
    # Rename columns to canonical representation.
    timestamp_df = timestamp_df.rename(columns=api.cfg.TIMESTAMP_COLUMN_RENAMES)
    return timestamp_df
```

## Writing robust code

### Don’t let your functions catch the default-itis
Default-itis is a disease of a function that manifests itself by getting too many default parameters. 

Default params should be used only for parameters that 99% of the time are constant.

In general we require the caller to be clear and specify all the params. 

Functions catch defaultitis when the programmer is lazy and wants to change the behavior of a function without changing all the callers and unit tests. Resist this urge! grep is friend. pycharm does this refactoring automatically.

### Explicitly bind default parameters
It’s best to explicitly bind functions with the default params so that if the function signature changes, your functions doesn’t confuse a default param was a positional one.

Instead of
  ```
  dbg.dassert(
      args.form or args.form_list,
      'You must specify one of the parameters: --form or --form_list',
  )
  ```

do this
  ```
  dbg.dassert(
      args.form or args.form_list,
      msg='You must specify one of the parameters: --form or --form_list',
  )
  ```

### Don’t hardwire params in a function call
E.g., code like 
```
esa_df = universe.get_esa_universe_mapped(False, True)
```
is difficult to read and understand without looking for the invoked function (aka write-only code) and it’s brittle since a change in the function params goes unnoticed.

It’s better to be explicit (as usual):
```
esa_df = universe.get_esa_universe_mapped(gvkey=False, cik=True)
```
This solution is robust since it will work as long as gvkey and cik are the only needed params, which is as much as we can require from the called function.

### Make if-then-else complete
In general if-then-else needs to be complete, so that the code is robust.

Instead of

```
if datafmt is not None and items is not None: 
    filters = list()
    for item in items:
        filters.append([('datafmt', '=', datafmt), ('item', '=', item)])
    elif datafmt is None and items is not None:
        filters = [('item', '=', item)]
    elif datafmt is not None and items is None:
        filters = [('datafmt', '=', datafmt)]
    elif datafmt is None and items is None:
        filters = None
```

use

```
if datafmt is not None and items is not None: 
    filters = list()        
    for item in items:
        filters.append([('datafmt', '=', datafmt), ('item', '=', item)])
    elif datafmt is None and items is not None:
        filters = [('item', '=', item)]
    elif datafmt is not None and items is None:
        filters = [('datafmt', '=', datafmt)]
    else:
        dbg.dassert(datafmt is None and items is None)
        filters = None
```

The last line is a catch-all that makes sure even if we modify the previous code, the code is robust.

### Encode the assumptions using assertions
If your code makes an assumption don’t just write a comment, but implement an assertion so the code can’t be executed if the assertion is not verified (instead of failing silently)
```
dbg.dassert_lt(start_date, end_date)
```

### Add TODOs when needed
- When there is something that you know you should have done, but you didn’t have time to do, add a TODO, possibly using your git name or github name e.g.,
  ```
  # TODO(gp): …
  ```
  In this way it’s easy to grep for your TODOs, which becomes complicated when using different names.

- If the TODO is associated with a Github issue, you can alternatively write the issue number inside the TODO, e.g.,
  ```
  # TODO(#123): …
  ```
- You can create a TODO for somebody else, or you can create a Upsource comment / review or Github bug, depending on how important the issue is
- If the TODO is general, e.g., anybody can fix it, then you can avoid to put a name. This should not be abused since it creates a culture when people don’t take responsibility for their mistakes.
- You can use P1, P2 to indicate if the issue is critical or not. E.g., P0 is the default for saying this is important, P1 is more of a “nice to have”.
  ```
  # TODO(Sergey): P1 This can be implemented in pandas using a range generation.
  ```

## Common Python mistakes

### `==` vs `is`
`is` checks whether two variables point to the same object (aka reference equality), while `==` checks if the two pointed objects are equivalent (value equality).

For checking against types, None we want to use `is`, `is not`

E.g., you want to do:
```
if var is None:
```
and NOT
```
if var == None
```
For more info checks [here](https://stackoverflow.com/questions/132988/is-there-a-difference-between-and-is-in-python)

### `type()` vs `isinstance()`
E.g., you want to do:
```
if isinstance(obj, list): 
```
and NOT
```
if type(obj) is list:
```
Note that `type(obj) == list` is even worse since we want to test for reference equality (the type of object is a list) and not the type of obj is equivalent to a list.

`isinstance` caters for inheritance (an instance of a derived class is an instance of a base class, too), while checking for equality of type does not (it demands identity of types and rejects instances of subtypes, AKA subclasses).

For more info check [here](https://stackoverflow.com/questions/1549801/what-are-the-differences-between-type-and-isinstance)

## Unit test
- Provide a minimal end-to-end unit test (which creates a conda environment and then run a few unit tests)
- Use
  - pytest <https://docs.pytest.org/en/latest/>
  - `unittest` library
- Usually we are happy with
  - lightly testing the tricky functions
  - some end-to-end test to make sure the code is working
- Use your common sense
  - E.g., no reason to test code that will be used only once
- To run unit tests in a single file
```
> pytest datetime_utils_test.py -x -s
```

## Refactoring

### When moving / refactoring code
If you move files, refactor code, move functions around make sure that:
- code and notebook work (e.g., imports and caller of the functions)
- documentation is updated (this is difficult, so best effort is enough)

For code find all the places that have been modified
```
> grep -r "create_dataframe" *
edgar/form_4/notebooks/Task252_EDG4_Coverage_of_our_universe_from_Forms4.ipynb:    "documents, transactions = edu.create_dataframes(\n",
edgar/form_4/notebooks/Task313_EDG4_Understand_Form_4_amendments.ipynb:    "documents, transactions = edu.create_dataframes(\n",
edgar/form_4/notebooks/Task193_EDG4_Compare_form4_against_Whale_Wisdom_and_TR.ipynb:    "documents, transactions, owners, footnotes = edu.create_dataframes(\n",
```

Or if you use mighty PyCharm, Ctrl + Mouse Left Click (Shows you all places where this function or variable was used) and try to fix them, at least to give your best shot at making things work

- You can edit directly the notebooks without opening, or open and fix it.

Good examples how you can safely rename anything for PyCharm users: <https://www.jetbrains.com/help/pycharm/rename-refactorings.html>

But remember, you must know how to do it without fancy IDE like PyCharm.

If it’s important code:
- run unit tests
- run notebooks (see [here](https://n-xovwktmtjsnaxyc2mwes2xu7pohqedmdm6zjw5q-0lu-script.googleusercontent.com/userCodeAppPanel#))

## Architectural and design pattern

### Organize scripts as pipelines
One can organize complex computations in stages of a pipeline

- E.g., to parse EDGAR forms
  - download -> (raw data) -> header parser -> (pq data) -> XBLR / XML / XLS parser -> (pq data) -> custom transformation

One should be able to run the entire pipeline or just a piece

- E.g., one can run the header parser from the raw data, save the result to file, then read this file back, and run the XBLR parser

Ideally one would always prefer to run the pipeline from scratch, but sometimes the stages are too expensive to compute over and over, so using chunks of the pipeline is better

This can also mixed with the “incremental mode”, so that if one stage has already been run and the intermediate data has been generated, that stage is skipped

- Each stage can save files in a tmp_dir/stage_name

The code should be organized to allow these different modes of operations, but there is not always need to be super exhaustive in terms of command line options

- E.g., I implement the various chunks of the pipeline in a library, separating functions that read / save data after a stage and then assemble the pieces into a throw-away script where I hardwire the file names and so on

### Incremental behavior
- Often we need to run the same code over and over
  - E.g., because the code fails on an unexpected point and then we need to re-run from the beginning
- We use options like:
    ```
    --incremental
    --force
    --start_date
    --end_date
    --output_file
    ```
- Check existence output file before start function (or a thread when using parallelism) which handle data of the corresponding period
  - if `--incremental` is set and output file already exists then skip the computation and report
    - log.info(“Skipping processing file %s as requested”, …)
  - if `--incremental` is not set
    - if output file exists then we issue a log.warn and abort the process
    - if output file exists and param `--force`, then report a log.warn and rewrite output file

### Run end-to-end
- Try to run things end-to-end (and from scratch) so we can catch these unexpected issues and code defensively
  - E.g., we found out that TR data is malformed sometimes and only running end-to-end we can catch all the weird cases
  - This also helps with scalability issues, since if takes 1 hr for 1 month of data and we have 10 years of data is going to take 120 hours (=5 days) to run on the entire data set

### Think about scalability
- Do experiments to try to understand if a code solution can scale to the dimension of the data we have to deal with
  - E.g., inserting data by doing SQL inserts of single rows are not scalable for pushing 100GB of data
- Remember that typically we need to run the same scripts multiple times (e.g., for debug and / or production)

### Use command line for reproducibility
- Try to pass params through command line options when possible
  - In this way a command line contains all the set-up to run an experiment

### Structure the code in terms of filters
- Focus on build a set of "filters" split into different functions, rather than a monolithic flow
- Organize the code in terms of a sequence of transformations that can be run in sequence, e.g.,
  1. create SQL tables
  2. convert json data to csv
  3. normalize tables 
  4. load csv files into SQL
  5. sanity check the SQL (e.g., mismatching TR codes, missing dates)
  6. patch up SQL (e.g., inserting missing TR codes and reporting them to us so we can check with TR)

## Code style for different languages

### SQL
- You can use the package https://github.com/andialbrecht/sqlparse to format
  SQL queries
- There is also an on-line version of the same formatter at https://sqlformat.org

## Misc (to reorg)
- TODO(*): Start moving these functions in the right place once we have more a
  better document structure

### Write robust code
- Write code where there is minimal coupling between different different parts
  - This is a corollary of DRY, since not following DRY implies coupling
- Consider the following code:
  ```
  if server_name == "ip-172-31-16-23":
      out = 1
  if server_name == "ip-172-32-15-23":
      out = 2
  ```
- This code is brittle since if you change the first part to:
  ```
  if server_name.startswith("ip-172"):
      out = 1
  if server_name == "ip-172-32-15-23":
      out = 2
  ```
  executing the code with `server_name = "ip-172-32-15-23"` will give `out=2`
- The proper approach is to enumerate all the cases like:
  ```
  if server_name == "ip-172-31-16-23":
      out = 1
  elif server_name == "ip-172-32-15-23":
      out = 2
  ...
  else:
      raise ValueError("Invalid server_name='%s'" % server_name)
  ```

### Capitalized words
- In documentation and comments we capitalize abbreviations (e.g., `YAML`,
  `CSV`)
- In the code:
  - We try to leave abbreviations capitalized when it doesn't conflict with
    other rules
    - E.g., `convert_to_CSV`, but `csv_file_name` as a variable name that is not global
  - Other times we use camel case, when appropriate
    - E.g., `ConvertCsvToYaml`, since `ConvertCSVToYAML` is difficult to read

### Regex
- The rule of thumb is to compile a regex expression, e.g.,
  ```
  backslash_regexp = re.compile(r"\\")
  ```
  only if it's called more than once, otherwise the overhead of compilation and creating another var is not justified

### Order of functions in a file
- We try to organize code in a file to represent the logical flow of the code
  execution, e.g.,
  - At the beginning of the file: functions / classes for reading data
  - Then: functions / classes to process the data
  - Finally at the end of the file: functions / classes to save the data
- Try to put private helper functions close to the functions that are using them
  - This rule of thumb is a bit at odds with clearly separating public and
    private section in classes
    - A possible justification is that classes typically contain less code than a file and tend to be used through their API
    - A file contains larger amount of loosely coupled code and so we want to keep implementation and API close together

### Use layers design pattern
- A "layer" design pattern (see Unix architecture) is a piece of code that talks
  / has dependency only to one outermost layer and one innermost layer
- You can use this approach in your files representing data processing pipelines
- You can split code in sections using 80 characters # comments, e.g.,
  ```
  # ###################...
  # Read.
  # ###################...

  ...

  # ###################...
  # Process.
  # ###################...
  ```
- This often suggests to split the code in classes to represent namespaces of related functions

### Write complete `if-then-else`
- Consider this good piece of code
  ```
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
  - E.g., sanity checking the parameters of a long running function, so that it doesn't run for 1 hr and then crash because the name of the file is incorrect

### Do not be stingy at typing
- Why calling an object `TimeSeriesMinStudy` instead of `TimeSeriesMinuteStudy`?
  - Saving 3 letters is not worth
  - The reader might interpret `Min` as `Minimal` (or `Miniature`, `Minnie`, `Minotaur`)
- If you don't like to type, we suggest you get a better keyboard, e.g.,
  [this](https://kinesis-ergo.com/shop/advantage2/)

### Research quality vs production quality
- Code belonging to top level libraries (e.g., `//amp/core`, `//amp/helpers`) and production (e.g., `//.../db`, `vendors`) needs to meet high quality standards, e.g.,
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

### Life cycle of research code
- Often the life cycle of a piece of code is to start as research and then be promoted to higher level libraries to be used in multiple research, after its quality reaches production quality

### No ugly hacks
- We don't tolerate "ugly hacks", i.e., hacks that require lots of work to be undone (much more than the effort to do it right in the first place)
  - Especially an ugly design hack, e.g., a Singleton, or some unnecessary
    dependency between distant pieces of code
  - Ugly hacks spreads everywhere in the code base

### Always separate what changes from what stays the same
- In both main code and unit test it's not a good idea to repeat the same code
- Bad
  - Copy-paste-modify
- Good
  - Refactor the common part in a function and then change the parameters used to
    call the function
- Example:
  - What code is clearer to you, VersionA or VersionB?
  - Can you spot the difference between the 2 pieces of code?
  - Version A
    ```
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
  - Version B
    ```
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
    - Yes, Version A is Bad and Version B is Good
### Don't mix real changes with linter changes
- The linter is in change of reformatting the code according to our conventions and reporting potential problems
1. We don't commit changes that modify the code together with linter
   reformatting, unless the linting is applied to the changes we just made
   - The reason for not mixing real and linter changes is that for a PR or to just read the code it is difficult to understand what really changed vs what was just a cosmetic modification
2. If you are worried the linter might change your code in a way you don't like, e.g.,
   - Screwing up some formatting you care about for some reason, or
   - Suggesting changes that you are worried might introduce bugs you can commit your code and then do a "lint commit" with a message "CMTaskXYZ: Lint"
  - In this way you have a backup state that you can rollback to, if you want
3. If you run the linter and see that the linter is reformatting / modifying pieces of code you din't change, it means that our team mate forgot to lint their code
   - `git blame` can figure out the culprit
   - You can send him / her a ping to remind her to lint, so you don't have to clean after him / her
   - In this case, the suggested approach is:
     - Commit your change to a branch / stash
     - Run the linter by itself on the files that need to be cleaned, without any change
     - Run the unit tests to make sure nothing is breaking
     - You can fix lints or just do formatting: it's up to you
     - You can make this change directly on `master` or do a PR if you want to be extra sure: your call

# Conventions (Addendum)

## Be patient
For some reason talking about conventions makes people defensive and uncomfortable, sometimes.

Conventions are not a matter of being right or wrong, but to consider pros and cons of different approaches, and make the decision only once instead of discussing the same problem every time. In this way we can focus on achieving the UltimateGoal.

If you are unsure or indifferent to a choice, be flexible and let other persons that seem to be less flexible decide.

## Goal
The goal of the conventions is to simplify our job by removing ambiguity

- There is no right or wrong: that's why it's a convention and not a law of nature
  - On the flip-side, if there is a right and wrong, then what we are discussing probably shouldn’t be considered as a convention
- We don't want to spend time discussing inconsequential points
- We don't want reviewers to pick lints instead of focusing on architectural issues and potential bugs
- Remove cognitive burden of being distracted by "this is an annoying lint" (or at least perceived lint)

Once a convention is stable, we would like to automate enforcing it by the linter

- ideally the linter should fix our mistakes so we don't even have to think about them, and reviewers don't have to be distracted with pointing out the lints

## Keep the rules simple
E.g., assume that we accepted the following rules:

- Git is capitalized if it refers to the tool and it's not capitalized when it refers to the command (this is what Git documentation suggests)
- Python is written capitalized (this is what Python documentation suggests)
- pandas is written lowercase, unless it is a beginning of the line in which case it's capitalized, but it's better to try to avoid to start a sentence with it (this is what pandas + English convention seems to suggest) 
- Any other library could suggest a different convention based on the preference of its author, who tries to finally force people to follow his / her convention …)

All these rules require mental energy to be followed and readers will spend time checking that these rules are enforced, rather than focusing on bugs and architecture.

In this case we want to leverage the ambiguity of "it's unclear what is the correct approach" by simplifying the rule

- E.g., every name of tools or library is always capitalized
- This is simple to remember and automatically enforce

### Allow turning off the automatic tools
We understand that tools can't always understand the context and the subtleties of human thoughts, and therefore they yield inevitably to false positives.

Then we always want to permit disabling the automatic checks / fixes e.g., by using directives in comments or special syntax (e.g., anything in a `...` or ``` … ``` block should be leaved untouched)

It can be tricky determining when an exception is really needed and when overriding the tool becomes a slippery slope for ignoring the rules.

Patience and flexibility is advised here.

## The writer is the reader
Remember that even if things are perfectly clear now to the person that wrote the code, in a couple of months the code will look foreign to whoever wrote the code.

So make your future-self's life easier by following the conventions and erring on the side of documenting for the reader.

## Gdoc vs markdown
We use this gdoc for staging proposed code conventions, idioms, architectural guidelines and so on.

Once the document is stable it can become a markdown text and be checked in the repo.


## Approved conventions

### Validate values before an assignment

#### Rationale
We consider this as an extension of a pre-condition ("only assign values that are correct") rather than a postcondition

Often is more compact since it doesn't have reference to `self`

#### Bad
```
self._tau = tau
dbg.dassert_lte(self._tau, 0)
```

#### Good
```
dbg.dassert_lte(tau, 0)
self._tau = tau
```

#### Exceptions
When we handle a default assignment, it's more natural to implement a post-condition:
```
col_rename_func = col_rename_func or (lambda x: x)
dbg.dassert_isinstance(col_rename_func, collections.Callable)
```

### Format docstrings and comments as markdown

Good
```
"""

Generate "random returns" in the form:


               vol_sq

2000-01-03   3.111881

2000-01-04   1.425590

2000-01-05   2.298345

2000-01-06   8.544551

"""
```

Bad
```
"""

Generate "random returns" in the form:

              `vol_sq

2000-01-03   3.111881

2000-01-04   1.425590

2000-01-05   2.298345

2000-01-06   8.544551

"""
```

### Do not abbreviate just to save characters

#### Rationale
Abbreviations just to save space are rarely beneficial to the reader. E.g.,

- fwd (forward)
- bwd (backward)
- act (actual)
- exp (expected)

#### Exceptions
We could relax this rule for short lived functions and variables in order to save some visual noise.

Sometimes an abbreviation is so short and common that it's ok to leave it E.g.,

- df (dataframe)
- srs (series)
- idx (index)
- id (identifier)
- val (value)
- var (variable)
- args (arguments)
- kwargs (keyword arguments)
- col (column)
- vol (volatility) while volume is always spelled out

### Use empty comments to group related code
We try to avoid the "wall-of-text", where there is lots of code without an obvious structure.

You can add comments to shortly describe chunks of code or just add an empty comment to represent the different logical pieces of code.

#### Rationale
The common reader can't easily understand a large piece or large blob of code.
#### Bad
```

dbg.dassert_isinstance(df_in, pd.DataFrame)

dbg.dassert_isinstance(df_out, pd.DataFrame)

dbg.dassert(cols is None or isinstance(cols, list))

cols = cols or df_out.columns.tolist()

col_rename_func = col_rename_func or (lambda x: x)

dbg.dassert_isinstance(col_rename_func, collections.Callable)

col_mode = col_mode or "merge_all"

```
#### Good
```

dbg.dassert_isinstance(df_in, pd.DataFrame)

dbg.dassert_isinstance(df_out, pd.DataFrame)

#

dbg.dassert(cols is None or isinstance(cols, list))

cols = cols or df_out.columns.tolist()

#

col_rename_func = col_rename_func or (lambda x: x)

dbg.dassert_isinstance(col_rename_func, collections.Callable)

#

col_mode = col_mode or "merge_all"

```

## Proposed conventions
### Order functions in topological order
- Order functions / classes in topological order so that the ones at the top of the files are the "innermost" and the ones at the end of the files are the "outermost"
- We are ok with not enforcing this too strictly

#### Rationale
In this way, reading the code top to bottom one should not find a forward reference that requires skipping back and forth

### Make the spell-checker happy
The spell-checker is not always right: false positives are often very annoying.

We prefer to find a way to make the spell-checker happy rather than argue that the spell-checker is wrong and ignore it.

#### Rationale
The risk with overriding the spell-checker (and any other tool) is that the decision is not binary anymore correct / not-correct and can't be automated and requires mental energy to see if the flagged error is real or not.

E.g., `insample` is flagged as erroneous, so we convert it into `in-sample`.

The solution for the obvious cases of missing a word (e.g., a technical word) is to add words to the vocabulary. This still needs to be done by everyone, until we find a way to centralize the vocabulary.

E.g.,  untradable is a valid English word, but Pycharm's spell-checker doesn't recognize it.
- TODO(*): Should we add it to the dictionary or write it as "un-tradable"?

Still we don't want to override the spell-checker when an alternative lower-cost solution is available. E.g.,

- `in-sample` instead of `insample`
- `out-of-sample` instead of `oos`

We decided that `hyper-parameter` can be written without hyphen: `hyperparameter`
### Convention for naming tests
According to PEP8 names of classes should always be camel case.

On the other hand, if we are testing a function `foo_bar()` we prefer to call the testing code `Test_foo_bar` instead of `TestFooBar`.

We suggest to name the class / method in the same way as the object we are testing, e.g.,:

- for testing a class `FooBar` we use  a test class `TestFooBar`
- for testing methods of the class `FooBar`, e.g., `FooBar.baz()`, we use a test method `TestFooBar.test_baz()`
- for testing a protected method `_gozilla()` of `FooBar` we use test methods `test__gozilla` (note the double underscore). This is needed to distinguish testing the public method `FooBar.gozilla()` and `FooBar._gozilla()`

We are ok with mixing camel case and snake case to mirror the code being tested.

We prefer to name classes `TestFooBar1` and methods `TestFooBar1.test1()`, even if there is a single class / method, to make it easier to add another test class, without having to rename class and `check_string` files.

We are ok with using suffixes like `01`, `02`, … , when we believe it's important that methods are tested in a certain order (e.g., from the simplest to the most complex)

### Interval notation
Intervals are represented with `[a, b), (a, b], (a, b), [a, b]`.

We don't use the other style `[a, b[`.

### Make filename unique
Problem

We have a lot of structure / boilerplate in our project around RH hypotheses. E.g., there are corresponding files for all the RH like:

- RHxyz/configs.py
- RHxyz/pipeline.py

It is not clear if it's better to make filenames completely unique by repeating the RH, e.g., `RH1E_configs.py`, or let the directories disambiguate.

Note that we are not referring to other common files like `utils.py`, which are made unique by their position in the file system and by the automatic shortening of the imports.

Decision

Invoking the principle of 'explicit is better than implicit', the proposal is to repeat the prefix.

Good: RH1E/RH1E_configs.py

Bad: RH1E/configs.py

Rationale

Pros of the repetition (e.g., `RH1E/RH1E_configs.py`):

- The filename is unique so there is no dependency on where you are
- Since pytest requires all files to be unique, we need to repeat the prefix for the test names and the rule is "always make the names of the files unique"
- We are going to have lots of these files and we want to minimize the risk of making mistakes

Cons of the repetition:

- Stuttering
- What happens if there are multiple nested dirs? Do we repeat all the prefixes?
  - This seems to be an infrequent case

### Be clear on the meaning of TODO
A `TODO(Batman): clean this up` can be interpreted as

1. "Batman suggested to clean this up" 
2. "Batman should clean this up"
3. "Batman has the most context to explain this problem or fix it"

On the one hand, `git blame` will report who created the TODO, so the first meaning is redundant.

On the other hand, since we follow a shared ownership of the code, the second meaning should be quite infrequent. In fact the code has mostly `TODO(*)` todos.

Given pros and cons, the proposal is to use the first meaning.

This is also what Google style guide suggests [here](https://github.com/google/styleguide/blob/gh-pages/pyguide.md#312-todo-comments)
### Convention on certain spellings
Profit-and-loss 

- PnL instead of pnl, PNL, PnL

Abbreviation

- JSON, CSV are abbreviations and thus should be capitalized in comments and docstrings, and treated as abbreviations in code with the usual rules (e.g., `WriteCsv` and not `WriteCSV`)

Name of columns

- The name of columns should be `..._col` and not `..._col_name` or `_column`.

Timestamp

- We spell `timestamp`, we do not abbreviate it as `ts`
- We prefer timestamp to `datetime`
- E.g., `start_timestamp` instead of `start_datetime`
### Always use imperative docstring and comments
PEP257 and Google standards seem to mix imperative and descriptive style without really a reason. The proposal is to always use "imperative" in both comments and all docstrings.
