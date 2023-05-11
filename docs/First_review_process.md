# First Review Process

<!--ts-->
  * [Read Python Style Guide](#read-python-style-guide)
  * [Run linter](#run-linter)
  * [Compare your code to example code](#compare-your-code-to-example-code)
  * [Look at examples of the first reviews](#look-at-examples-of-the-first-reviews)

<!--te-->

We understand that receiving feedback on your code can be a difficult process,
but it is an important part of our development workflow. Here we have gathered
some helpful tips and resources to guide you through your first review.

## Read Python Style Guide

- Before submitting your code for review, we highly recommend that you read the
  [Python Style Guide](Coding_Style_Guide.md),
  which outlines the major conventions and best practices for writing Python code.
- Adhering to these standards will help ensure that your code is easy to read,
  maintain, and understand for other members of the team.

## Run linter

- Linter is a tool that checks (and tries to fix automatically) your code for syntax errors, style violations, and other issues.
- Run it on all the changed files to automatically catch any code issues before filing any PR or before requesting a review!
- To be able to run the linter, you need to you need to set up your client first since you're outside Docker:
  - The instructions are available at [Quick start for developing](Quick_start_for_developing.md)
  - In practice you need to have run
    ```
    > source dev_scripts/setenv_amp.sh
    ```
- Run the linter with `invoke` command (which is abbreviated as `i`) and pass all the files you need to lint in
  brackets after the `--files` option, separated by a space:
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
  - `i lint` has options for many workflows. E.g., you can automatically lint all the files that you touched in your PR with `--branch`, the files in the last commit with `--last-commit`. You can look at all the options with:
    ```
    > i lint --help
    ```
- Fix the lints
  - No need to obsessively fix all of them - just crucial and obvious ones
  - Post unresolved lints in your PR so reviewers could see them and know which
    should be fixed and which are not

## Compare your code to example code

- To get an idea of what well-formatted and well-organized code looks like, we
  suggest taking a look at some examples of code that adhere to our standards.
- We try to maintain universal approaches to all the parts of the code, so when
  looking at a code example, check for:
  - Code style
  - Docstrings and comments
  - Type hints
  - Containing directory structure
- Here are some links to example code:
  - Classes and functions:
    - `defi/tulip/implementation/order.py`
    - `defi/tulip/implementation/order_matching.py`
  - Unit tests:
    - `defi/tulip/test/test_order_matching.py`
    - `defi/tulip/test/test_optimize.py`

## Look at examples of the first reviews

- It can be helpful to review some examples of previous first reviews to get an
  idea of what common issues are and how to address them.
- Here are some links to a few "painful" first reviews:
  - https://github.com/sorrentum/sorrentum/pull/166
  - https://github.com/sorrentum/sorrentum/pull/186
