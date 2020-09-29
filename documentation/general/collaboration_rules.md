# Required skills

## Dev
- [ ] Python3
- [ ] Familiarity with classes and OOP
- [ ] Git workflow
- [ ] GitHub workflow (e.g., Pull Requests, Reviews, ...)
- [ ] Docker
- [ ] Unit tests (e.g., `pytest`)
- [ ] Managing Python environments (`conda`, `pip`, `poetry`, virtual env)
- [ ] Type hints with `mypy`

# Rules for a good collaboration
- Ask questions when you have doubts

- Do quick iterations
  - We can use a git long-running branch or feature-branch model
  - E.g., a Pull Request (PR) on GitHub every 5-10 hours of coding, so we can
    review the code and discuss the direction

- Start from the high-level design
  - Write functions, classes without implementation
  - Add docstrings to each function explaining what it should do
  - Add comments in the body of functions explaining how things should work
  - Then do a PR to validate interfaces

- Increase level of detail of the implementation
  - Add detailed function interface (with type hints)
  - Write end-to-end tests for components of the code
  - Write more detailed unit tests for corner cases

# General rules
- [ ] Please use Python 3.7+
- [ ] Use type hints with `mypy` (e.g., `List[str]`)
- [ ] Format code with `black`
- [ ] Use docstrings everywhere using ReST standard
  ```
  """
  This is a reST style.


  :param param1: this is a first param
  :param param2: this is a second param
  :returns: this is a description of what is returned
  :raises keyError: raises an exception
  """
  ```
- [ ] Use Google python code style (https://google.github.io/styleguide/pyguide.html)
- [ ] Use logging instead of `print`

# Package dependencies
- There is a file `p1_develop.yaml` with some dependencies
  - You can handle the dependencies in your favorite way (e.g., `conda`, `pip`,
    `poetry`)

# Script examples
- Use this file `template/script_skeleton.py` as boilerplate for a script

# Try to use the libraries
- `helpers/dbg.py`
  - Assertions
- `helpers/io_.py`
  - Functions to handle filesystem operations.
- `helpers/system_interaction.py`
  - Contain code needed to interact with the outside world, e.g., through system
    commands, env vars, ...
- Take a look at the library code to get a sense of how we write code
  - Please comment every logical block of code

# Unit tests
- Please use `unittest` and `pytest`
- `helpers/unit_test.py` contains library to simplify writing unit tests
- Example of unit tests are `helpers/test/test_dbg.py`
