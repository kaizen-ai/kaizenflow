<!--ts-->
   * [Description](#description)
   * [Detailed instructions](#detailed-instructions)
   * [Webserver](#webserver)
      * [Old documents](#old-documents)
   * [Technical](#technical)



<!--te-->

# Description

- `publish_notebook.py` is a little tool that allows to:
  1) Opening a notebook in your browser (useful for read-only mode)
     - E.g., without having to use Jupyter notebook (which modifies the file in
       your client) or github preview (which is slow or fails when the notebook
       is too large)
  2) Sharing a notebook with others in a simple way
  3) Pointing to detailed documentation in your analysis Google docs
  4) Reviewing someone's notebook
  5) Comparing multiple notebooks against each other in different browser windows
  6) Taking a snapshot / checkpoint of a notebook as a backup or before making
     changes
     - This is a lightweight alternative to "unit testing" to capture the desired
       behavior of a notebook
     - One can take a snapshot and visually compare multiple notebooks
       side-by-side for changes

# Detailed instructions

- You can get details by running:
  ```bash
  > dev_scripts/notebooks/publish_notebook.py -h
  ```

# Webserver

- From now on `publish_notebook.py` will work from the Docker container
- We have deployed a new service for storing and viewing notebooks in HTML
  format
- The new version of `publish_notebook.py` works using HTTP protocol and does
  not require ssh key authorization as before

## Old notebooks

- We have synchronized all the old documents on the new service

- The old links `http://research.p1:8077/...` still work for now
- To use the new service you need to replace the URL with the new ones
  `http://notebook-keeper.p1/...`
  - If you see any link starting with http://research.p1:8077 please replace it
    with the new link

- We will disable the old service on Nov 30, 2020

# Technical

- The code for the server is at:
- TODO(Sergey): Add details
