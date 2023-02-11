# Python

- Start the Docker container with `docker_bash.sh`
  - Python is already installed
- To use Python, you can just do `python3` (or `ipython`), and it will start up the
  shell
  ```
  docker> python3
  ```

# Jupyter / IPython

- IPython Notebook / Jupyter is an enhanced command shell for Python, that offers
  enhanced introspection, rich media, tab completion, and history
- IPython Notebook started as a web browser-based interface to IPython and
  proved especially popular with data scientists
- A few years ago, the Notebook functionality was forked off as a separate project,
  called [Jupyter](http://jupyter.org/). Jupyter provides support for many other
  languages in addition to Python

# Run Jupyter

- Take a look
  ```
  > cd $GIT_REPO/tutorials/tutorial_jupyter
  > ls
  > vi docker_bash.sh 
  ```

- Run container
  ```
  > docker_bash.sh
  + IMAGE_NAME=umd_data605_postgres
  + REPO_NAME=gpsaggese
  + CONTAINER_NAME=umd_data605_postgres
  + docker image ls gpsaggese/umd_data605_postgres
  REPOSITORY                       TAG       IMAGE ID       CREATED        SIZE
  gpsaggese/umd_data605_postgres   latest    632cd8f5aaae   22 hours ago   1.78GB
  ++ pwd
  + docker run --rm -ti --name umd_data605_postgres -p 8888:8888 -p 5432:5432 -v /Users/saggese/src/umd_data605/tutorials/tutorial_jupyter:/data gpsaggese/umd_data605_postgres
  ```

- Start Jupyter inside container:
  ```
  docker> /data/run_jupyter.sh
  + jupyter-notebook --port=8888 --no-browser --ip=0.0.0.0 --NotebookApp.token= --NotebookApp.password=
  [I 08:45:16.978 NotebookApp] Writing notebook server cookie secret to /var/lib/postgresql/.local/share/jupyter/runtime/notebook_cookie_secret
  [I 08:45:16.983 NotebookApp] Authentication of /metrics is OFF, since other authentication is disabled.
  [W 08:45:17.575 NotebookApp] All authentication is disabled.  Anyone who can connect to this server will be able to run code.
  [I 08:45:17.613 NotebookApp] [jupyter_nbextensions_configurator] enabled 0.6.1
  [I 08:45:17.638 NotebookApp] Serving notebooks from local directory: /
  [I 08:45:17.638 NotebookApp] Jupyter Notebook 6.4.8 is running at:
  [I 08:45:17.638 NotebookApp] http://bdd82a232a2c:8888/
  [I 08:45:17.638 NotebookApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
  ```

- This will start a Jupyter server in the container listening on port 8888
    - You will access it from the host (as discussed above, the Docker start command
      maps the 8888 port on the container to the 8888 port on the host)
    - To do that start the browser and point it to: http://127.0.0.1:8888

- Go with your local browser to `localhost:8888`

- Navigate to `data/tutorial_jupyter.ipynb`

- Read and execute the notebook
