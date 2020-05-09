<!--ts-->
      * [Convention](#convention)
      * [What to annotate with type hints](#what-to-annotate-with-type-hints)
      * [reveal_type](#reveal_type)
      * [Library without types](#library-without-types)
      * [Handling the annoying Incompatible types in assignment](#handling-the-annoying-incompatible-types-in-assignment)
      * [Disabling mypy errors](#disabling-mypy-errors)
      * [What to do when you don't know what to do](#what-to-do-when-you-dont-know-what-to-do)
   * [Inferring types using unit tests](#inferring-types-using-unit-tests)



<!--te-->

- We use python3 type hints to:
  1. Improve documentation
  2. Allow `mypy` to perform static checking of the code, looking for bugs
  3. Enforce the type checks at run-time, through automatic assertions (not
     implemented yet)

## Convention

- Return `-> None` if your function doesn't return
  - Pros:
    - `mypy` checks functions only when there is at least an annotation: so
      using `-> None` enables `mypy` to do type checking
    - It remind us that we need to use type hints
  - Cons:
    - `None` is the default value and so it might seem redundant

## What to annotate with type hints

- We expect all new library code (i.e., that is not in a notebook) to have type
  annotations
- We annotate the function signature
- We don't annotate the variables inside a function unless `mypy` reports that
  it can't infer the type

- We strive to get no errors / warnings from the linter, including `mypy`

## `reveal_type`

- To find out what type `mypy` infers for an expression anywhere in your
  program, wrap it in `reveal_type()`
- `mypy` will print an error message with the type; remove it again before
  running the code
- See
  [here](https://mypy.readthedocs.io/en/stable/cheat_sheet_py3.html#when-you-re-puzzled-or-when-things-are-complicated)

## Library without types

- `mypy` is unhappy when a library doesn't have types
- Lots of libraries are starting to add type hints now that python 2 has been
  deprecated
```
*.py:14: error: No library stub file for module 'sklearn.model_selection' [mypy]
```

- You can go in `mypy.ini` and add the library (following the alphabetical
  order) to the list

- Note that you need to ensure that different copies of `mypy.ini` in different
  subprojects are equal

```bash
> vimdiff mypy.ini amp/mypy.ini

or

> cp mypy.ini amp/mypy.ini
```

## Handling the annoying `Incompatible types in assignment`

- `mypy` assigns a single type to each variable for its entire scope

- The problem is in common idioms like:

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

- If `mypy` reports an error and you don't understand why, please ping one of
  the python experts asking for help

- If you are sure that you understand why `mypy` reports and error and that you
  can override it, you
- When you want to disable an error reported by `mypy`:
  - Add a comment reporting the mypy error
  - Explain why this is not a problem
  - Add `# type: ignore` with two spaces as usual for the inline comment

  ```python
  # mypy: Cannot find module named 'pyannotate_runtime'
  # pyannotate is not always installed
  from pyannotate_runtime import collect_types  # type: ignore
  ```

## What to do when you don't know what to do

- You can check
  [here](https://mypy.readthedocs.io/en/stable/cheat_sheet_py3.html)

# Inferring types using unit tests

- Install pyannotate
  ```bash
  > pip install pyannotate
  ```
- To enable collecting type hints run

  ```bash
  > export PYANNOTATE=True
  ```

- Run `pytest`, e.g., on a subset of unit tests:

- Run pytest, e.g., on a subset of unit tests like `helpers`:

  ```bash
  > pytest helpers
  ```
  - A file `type_info.json` is generated

- Annotate the code with the inferred types:
  ```bash
  > pyannotate -w --type-info type_info.json . --py3
  ```
