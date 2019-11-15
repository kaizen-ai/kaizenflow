# Why type hints?
- We use python3 type hints to:
    1) improve documentation
    2) allow `mypy` to perform static checking of the code, looking for bugs
    3) enforce the type checks at run-time, through automatic assertions (not
       implemented yet)

## What to annotate with type hints
- We expect all new library code (i.e., that is not in a notebook) to have type
  annotations
- We annotate the function signature
- We don't annotate the variables inside a function unless `mypy` reports that it
  can't infer the type

- We strive to get no errors / warnings from the linter, including `mypy`

## Handling the annoying `Incompatible types in assignment`
- `mypy` assigns a single type to each variable for its entire scope

- The problem is in common idioms like
    ```python
    output : str = ...
    output = output.split("\n")
    ...
    # Process output.
    ...
    output = "\n".join(output)
    ```
  (where we use the same variable to store different representations of the same
  data) annoy `mypy`, which in turns annoys us

- Unfortunately the proper solution is to use different variables
    ```python
    output : str = ...
    output_as_array = output.split("\n")
    ...
    # Process output.
    ...
    output = "\n".join(output_as_array)
    ```

## Disabling `mypy` errors

- If `mypy` reports an error and you don't understand why, please ping one of the
  python experts asking for help

- If you are sure that you understand why `mypy` reports and error and that you
  can override it, you 
- When you want to disable an error reported by `mypy`:
    - add a comment reporting the mypy error
    - explain why this is not a problem
    - add `# type: ignore` with two spaces as usual for the inline comment

    ```python
    # mypy: Cannot find module named 'pyannotate_runtime'
    # pyannotate is not always installed
    from pyannotate_runtime import collect_types  # type: ignore
    ```

# Inferring types using unit tests

- Install pyannotate
    ```bash
    > pip install pyannotate
    ```
- In `conftest.py` set `pyannotate = True`
    - Remember not to check this in

Run pytest, e.g., on a subset of unit tests:
    ```bash
    > pytest --pyannotate helpers/
    ```
    - A file `type_info.json` is generated

- Annotate the code with the inferred types:
    ```bash
    > pyannotate -w --type-info type_info.json helpers --py3
    ```
