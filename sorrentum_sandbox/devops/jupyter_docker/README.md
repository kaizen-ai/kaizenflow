# Python

- We will be using Python and Jupyter for all the research projects

- IPython Notebook / Jupyter is an enhanced command shell for Python, that offers
  enhanced introspection, rich media, tab completion, and history
- IPython Notebook started as a web browser-based interface to IPython and
  proved especially popular with data scientists
- A few years ago, the Notebook functionality was forked off as a separate project,
  called [Jupyter](http://jupyter.org/). Jupyter provides support for many other
  languages in addition to Python

## Running Jupyter

- To start Jupyter do:
  ```
  > cd $GIT_ROOT/sorrentum_sandbox/devops/jupyter_docker
  > docker_jupyter.sh
  ++ git rev-parse --show-toplevel
  + GIT_ROOT=/Users/saggese/src/sorrentum1
  + REPO_NAME=sorrentum
  + IMAGE_NAME=jupyter
  + FULL_IMAGE_NAME=sorrentum/jupyter
  + docker image ls sorrentum/jupyter
  REPOSITORY   TAG       IMAGE ID   CREATED   SIZE
  + CONTAINER_NAME=jupyter
  + docker run --rm -ti --name jupyter -p 8888:8888 -v /Users/saggese/src/sorrentum1/sorrentum_sandbox:/data sorrentum/jupyter /data/devops/jupyter_docker/run_jupyter.sh
  Unable to find image 'sorrentum/jupyter:latest' locally
  latest: Pulling from sorrentum/jupyter
  677076032cca: Already exists
  4c9de205ab0e: Pull complete
  a892cf7a32e9: Pull complete
  9a40f96cecbd: Pull complete
  750a9e837af3: Pull complete
  531de5edc8b0: Pull complete
  4f71d2b85657: Pull complete
  3acb30401c68: Pull complete
  8ce104f4b2d4: Pull complete
  17ee1092dc19: Pull complete
  cd7fbd9424d1: Pull complete
  1e8ffcadb90b: Pull complete
  7d1edc5584d3: Pull complete
  ac0e0cb28d27: Pull complete
  Digest: sha256:3be2a9bcbae4919d532d891d76ddb57cd8a13d0a8dd518cbdd5e7ed6ab3fa30a
  Status: Downloaded newer image for sorrentum/jupyter:latest
  + jupyter-notebook --port=8888 --no-browser --ip=0.0.0.0 --allow-root --NotebookApp.token= --NotebookApp.password=
  [I 16:29:44.061 NotebookApp] Writing notebook server cookie secret to /root/.local/share/jupyter/runtime/notebook_cookie_secret
  [I 16:29:44.061 NotebookApp] Authentication of /metrics is OFF, since other authentication is disabled.
  [W 16:29:44.273 NotebookApp] All authentication is disabled.  Anyone who can connect to this server will be able to run code.
  [I 16:29:44.276 NotebookApp] [jupyter_nbextensions_configurator] enabled 0.6.1
  [I 16:29:44.284 NotebookApp] Serving notebooks from local directory: /
  [I 16:29:44.284 NotebookApp] Jupyter Notebook 6.5.2 is running at:
  [I 16:29:44.284 NotebookApp] http://917f7e30d7c6:8888/
  [I 16:29:44.284 NotebookApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
  ```

- This will start a Jupyter server in the container listening on port 8888
    - You will access it from the host (as discussed above, the Docker start command
      maps the 8888 port on the container to the 8888 port on the host)
    - To do that start the browser and point it to: http://127.0.0.1:8888

- Go with your local browser to `localhost:8888`

- Navigate to `/data` to see the directories mounted on Docker

- Read and execute the notebook

## Running bash

- Run container
  ```
  > docker_bash.sh
  ```
