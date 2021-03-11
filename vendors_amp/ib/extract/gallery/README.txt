# Virtual env

- To install the necessary packages

  ```bash
  python -m venv venv
  source venv/bin/activate
  pip install -r requirements.txt
  ```
- To disable virtual env
  ```
  deactivate
  ```

# Conda

- To install conda:
  ```
  conda env create -n conda_lem -f dev_scripts_lem/conda.yml
  conda activate conda_lem
  ```
