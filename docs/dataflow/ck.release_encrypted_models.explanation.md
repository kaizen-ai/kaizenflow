# Introduction

The encryption flow script is `//amp/dev_scripts/encrypt_models/encrypt_model.py`

The working principle is to encrypt an entire directory

It consists of several phases:

1) Test the original model before encryption
   - This consists of importing the model inside the Dev container:
     ```
     import dataflow_lemonade.pipelines.C5.C5a_pipeline as f;
     a = f.C5a_DagBuilder(); print(a)
     ```
   - TODO(gp): It would be good to try to run a test of the model.
2) Encrypt the dir
   - This entails building a Docker container with `pyarmor` and then run the
     encryption command on the source code
   - The encrypted code is created in a dir typically parallel to the source code
     (e.g., `dataflow_lemonade/pipelines` and 
     `dataflow_lemonade/encrypted_pipelines`)
3) Tweak `__init__.py` file
   - This is needed to work around some problems that we have found with Pyarmor7
   - TODO(gp): Check if new Pyarmor version solves this problem.
4) Test the encrypted model
   - Check that we can import the model
   - TODO(gp): It would be good to try to run a test of the model.
5) Release the encrypted model
   - Copy the encrypted models from one repo (e.g., `lemonade`) to another repo
     (e.g., `orange`)
   - The invariant is that the encrypted code in the release dir has exactly the 
     same position and layout of the original code

There are several objects involved in the encryption flow:

- `input_dir`: the dir storing the models
  - E.g., `//lem/dataflow_lemonade/pipelines`
  - All the code below `input_dir` will be encrypted
  - All the code needed to run an encrypted model needs to be contained:
    - under `input_dir` (all this code will be encrypted); or 
    - in the unencrypted code in the destination repo (e.g., `orange`)
  - E.g., the encryption flow doesn't work if there is an import from 
    `input_dir` to a dir that is outside `input_dir` and is not in the destination
    repo

- `output_dir`: the dir storing the encrypted dir in the same repo
 
- `release_dir`: specify the encrypted model output directory (optional)
    - By default, it's inferred as `$input_dir/encrypted_pipelines`
    - E.g., encrypt models from `//lem/dataflow_lemonade/pipelines` to
      `//orange/dataflow_lemonade/pipelines`

- `model_dag_builder`, `model_dag_builder_file`: information about a model 
  to test that both the original and the encrypted code work properly
  - E.g., `C1a_DagBuilder`, `C1/C1a_pipeline.py`
  - This is used 

- `build_target`, `docker_image_tag`: specify cross-build options for Docker
  container
  - This is used when we need to encrypt code on a Mac and run the code on x86

## Automated flow

- See `release_encrypted_models.how_to_guide.md`

## Manual flow

- Go to dir with the source (unencrypted) models (e.g., `lemonade1`)

- Encrypt the directory using `pyarmor`
  ```bash
  > cd ~/src/lemonade1

  > i docker_bash
  # Install pyarmor.
  docker> sudo /bin/bash -c "(source /venv/bin/activate; pip install pyarmor)"

  # Encrypt the entire directory.
  docker> export MODEL_DIR=dataflow_lemonade
  docker> rm -rf $MODEL_DIR/encrypted_pipelines; pyarmor-7 obfuscate --restrict=0 --recursive $MODEL_DIR/pipelines --output $MODEL_DIR/encrypted_pipelines
  ```

- Patch the `__init__.py`
  - We need to add an absolute import to all the `__init__.py` files excluding the
    one that belongs to `pyarmor` `encrypted_pipelines/pytransform/__init__.py`
  - Before the file looks like:
    ```python
    pyarmor_runtime()
    ```
  - After it should like:
    ```python
    from dataflow_lemonade.encrypted_pipelines.pytransform import pyarmor_runtime; pyarmor_runtime()
    ```

  - We need to change all the files under `$MODEL_DIR/encrypted_pipelines`
    excluding `$MODEL_DIR/encrypted_pipelines/pytransform/__init__.py`
    ```
    > find dataflow_lemonade/encrypted_pipelines -name "__init__.py"
    dataflow_lemonade/encrypted_pipelines/C7/__init__.py
    dataflow_lemonade/encrypted_pipelines/C9/__init__.py
    dataflow_lemonade/encrypted_pipelines/volatility_adjusted_returns/__init__.py
    dataflow_lemonade/encrypted_pipelines/C8/__init__.py
    dataflow_lemonade/encrypted_pipelines/C1/__init__.py
    dataflow_lemonade/encrypted_pipelines/C6/__init__.py
    dataflow_lemonade/encrypted_pipelines/pytransform/__init__.py
    dataflow_lemonade/encrypted_pipelines/__init__.py
    dataflow_lemonade/encrypted_pipelines/residualized_returns/__init__.py
    dataflow_lemonade/encrypted_pipelines/C3/__init__.py
    dataflow_lemonade/encrypted_pipelines/C4/__init__.py
    dataflow_lemonade/encrypted_pipelines/C10/__init__.py
    dataflow_lemonade/encrypted_pipelines/C5/__init__.py
    dataflow_lemonade/encrypted_pipelines/C2/__init__.py
    ```

  - We can use a little bash kung-fu:
    ```
    > more tweak.sh
    (echo "from $MODEL_DIR.encrypted_pipelines.pytransform import pyarmor_runtime; pyarmor_runtime()" >tmp; cat $1 >>tmp); mv tmp $1
    > chmod +x tweak.sh
    > find $MODEL_DIR/encrypted_pipelines -name "__init__.py" -not -path "*/pytransform/__init__.py" -exec tweak.sh {} \;
    ```

  - In this way, before the `__init__.py` file was like
    ```python
    1 __pyarmor__(__name__, __file__, b'\x50\x59\x41\x52\x4d\x4f\x52\x00\x00\x03\x08\x00\x55\x0d\x0d\x0a\x09\x34\xe0\x02\x00\x00\x00\x00    \x01\x00\x00\x00\x40\x00\x00\x00\x3d\x01\x00\x00\x00\x00\x00\x00\x8a\x3c\x71\xd4\xb1\xce\
    ```

  - After it is like
    ```python
    1 from $MODEL_DIR.encrypted_pipelines.pytransform import pyarmor_runtime; pyarmor_runtime()
    2 __pyarmor__(__name__, __file__, b'\x50\x59\x41\x52\x4d\x4f\x52\x00\x00\x03\x08\x00\x55\x0d\x0d\x0a\x09\x34\xe0\x02\x00\x00\x00\x00    \x01\x00\x00\x00\x40\x00\x00\x00\x3d\x01\x00\x00\x00\x00\x00\x00\x8a\x3c\x71\xd4\xb1\xce\
    ```

- Test that the encrypted model works:
  ```python
  docker> python -c "import $MODEL_DIR.encrypted_pipelines.C5.C5a_pipeline as f; a = f.C5a_DagBuilder(); print(a)"
  ```

- To release the code, the code needs to be copied and the dir renamed
  ```bash
  > cp -r /data/saggese/src_vc/lemonade1/$MODEL_DIR/encrypted_pipelines /data/saggese/src/orange1/$MODEL_DIR/pipelines
  ```

- Test whether the code can be imported in the destination repo
  ```bash
  docker> python -c "import $MODEL_DIR.pipelines.C5.C5a_pipeline as f; a = f.C5a_DagBuilder(); print(a)"
  ```
