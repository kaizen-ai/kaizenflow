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
