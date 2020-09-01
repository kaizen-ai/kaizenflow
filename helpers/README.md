# Particle Helpers Distribution Package

This document describes build, distribution and installation workflow for the corporate helpers package.

**Note for dev/data science members:** if you are looking for how to install packages for the daily work, go to **Client Configuration**.

## General Information

We use [pypiserver](https://github.com/pypiserver/pypiserver) as a corpatete PyPI Index server for installing `pip`. It implements the same interfaces as [PyPI](https://pypi.org/), allowing standard Python packaging tooling such as `pip` to interact with it as a package index just as they would with [PyPI](https://pypi.org/). The server is based on bottle and serves packages from regular directories. Wheels, bdists, eggs can be uploaded either with pip, setuptools or simply copied with scp to the server directory.

## Client Configuration / Installation

You have two options:

1. Since `pypiserver` redirects `pip install` to the pypi.org index if it doesn't have a requested package, it is a good idea to configure them to always use your local pypi index. 

    For pip command this can be done by setting the environment variable PIP_EXTRA_INDEX_URL:

    ```bash
    export PIP_EXTRA_INDEX_URL=http://172.31.16.23:8855/simple/
    ```

    or by adding the following lines to ~/.pip/pip.conf:

    ```ini
    [global]
    extra-index-url = http://172.31.16.23:8855/simple/
    trusted-host = 172.31.16.23
    ```

2. Manual installation:

    ```bash
    > pip install --extra-index-url http://172.31.16.23:8855/simple --trusted-host 172.31.36.23 helpers
    ```

    or

    ```bash
    > pip install --extra-index-url http://172.31.16.23:8855 helpers
    ```

    Search hosted packages:

    ```bash
    > pip search --index http://172.31.16.23:8855 ...
    ```

    **Note** that pip search does not currently work with the /simple/ endpoint.

## Server Details

**Simple Index WebUI**: http://172.31.16.23:8855/simple

**Host**: ubuntu@172.31.16.23

**Runtime**: by docker (standalone container)

## Server Configuration

The corporate PyPI Index server runs with Docker as a standalone container with mapped volumes on the host.

All corporate packages serve from host directory: `/home/ubuntu/pypi/packages`

Uploading a new packages or updates existing pacakges password-proteced. Only authorize user can upload packages. Authorized user credential serves from host Apache-Like authentication file: `/home/ubuntu/pypi/.htpasswd`.

Credential details you can find on the server in a file `~/.pypi_credentials`

To serve packages and authenticate against local `.htpasswd`:

```bash
> docker run -d -p 8855:8080 -v ~/pypi/packages:/data/packages -v ~/pypi/.htpasswd:/data/.htpasswd --restart=always pypiserver/pypiserver:latest -v  -P .htpasswd packages
```

## Distribute workflow

**Note**: This section desribes temporary solution until we will not introduce CI pipeline.

The mainteneer of helpers package must do after merging changes of `helpers/` into master:

1. Edit or create a ~/.pypirc file with a similar content:

    ```ini
    [distutils]
    index-servers =
    particle
    [particle]
    username:<upload_pypi_username>
    password:<upload_pypi_passwd>
    ```

2. Update the `helpers/CHANGELOG`, add version and new changes.

3. Edit `setup.py`, change `version` with accordance in `CHANGELOG`, update `install_requires` parameters.

4. Run `python setup.py sdist upload -r particle`

## Limitation

The `pypiserver` does not implement the full API as seen on [PyPI](https://pypi.org/). It implements just enough to make `pip install`, and `search` work.

## Links

[pip user guide](https://pip.pypa.io/en/stable/user_guide/#user-guide)

[pypiserver](https://github.com/pypiserver/pypiserver)

[setuptool](https://setuptools.readthedocs.io/en/latest/index.html
)

[packaging python projects](https://packaging.python.org/tutorials/packaging-projects/)