# First Review Process

We understand that receiving feedback on your code can be a difficult process, but it is an important part of our development workflow. Here we have gathered some helpful tips and resources to guide you through your first review.

## Read Python Style Guide

- Before submitting your code for review, we highly recommend that you read the Python style guide, which outlines the major conventions and best practices for writing Python code.
  - `TODO (Dan): Add link to .md guide after it is merged`
- Adhering to these standards will help ensure that your code is easy to read, maintain, and understand for other members of the team.

## Run Linter

- A linter is a tool that checks your code for syntax errors, style violations, and other issues.
- Run it on all the changed files to automatically catch any code issues before filing any PR or before requesting a review!
- To be able to run the linter, you need to activate the virtual environment first:
  ```
  source dev_scripts/setenv_amp.sh
  ```
  - This is a one-time action. There's no need to repeat it before each linter run.
  - You can easily check if the environment is activated by looking at your command line. Your username and directory name should be prepended by `(amp.client_venv)`, for example:
    ```
    (amp.client_venv) $YOUR_NAME@dev1:~/src/sorrentum1$ git status
    ```
- Run the linter with an invoke and pass all the files you need to lint in brackets after the `--files` option, separated by a space:
  ```
  > i lint --files "defi/tulip/implementation/order.py defi/tulip/implementation/order_matching.py"
  ```
  - Output example:
    ```
    defi/tulip/implementation/order_matching.py:14: error: Cannot find implementation or library stub for module named 'defi.dao_cross'  [import]
    defi/tulip/implementation/order_matching.py:69: error: Need type annotation for 'buy_heap' (hint: "buy_heap: List[<type>] = ...")  [var-annotated]
    defi/tulip/implementation/order_matching.py:70: error: Need type annotation for 'sell_heap' (hint: "sell_heap: List[<type>] = ...")  [var-annotated]
    ...
    ```
- Fix the lints
  - No need to obsessively fix all of them - just crucial and obvious ones
  - Post unresolved lints in your PR so reviewers could see them and know which should be fixed and which are not

## Compare Your Code to Example Code

- To get an idea of what well-formatted and well-organized code looks like, we suggest taking a look at some examples of code that adhere to our standards.
- We try to maintain universal approaches to all the parts of the code, so when looking at a code example, check for:
  - Code style
  - Docstrings and comments
  - Type hints
  - Containing directory structure

## Look at Examples of the First Reviews

- It can be helpful to review some examples of previous first reviews to get an idea of what common issues are and how to address them.
- Here are some links to previous first review examples:
  - Classes and functions:
    - `defi/tulip/implementation/order.py`
    - `defi/tulip/implementation/order_matching.py`
  - Unit tests:
    - `defi/tulip/test/test_order_matching.py`
    - `defi/tulip/test/test_optimize.py`