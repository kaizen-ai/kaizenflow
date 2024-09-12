

<!-- toc -->

- [Dev tools container](#dev-tools-container)
  * [dev_tools](#dev_tools)

<!-- tocstop -->

# Dev tools container

## dev_tools

- File an Issue for the release
- Create the corresponding branch in dev_tools
- Change the code
- Run the release flow end-to-end
  ```
  > i docker_release_dev_image --version 1.1.0
  > i docker_release_prod_image --version 1.1.0
  ```
  TODO(Vlad): Add a command to run the push to Dockerhub and add it to the
  single arch release flow
- Push the image to Dockerhub manually
  - Login to Dockerhub with the `sorrentum` account
  ```
  > docker login --username=sorrentum
  ```
  - Tag the dev version image as `sorrentum/dev_tools:dev`
  ```
  > docker tag 665840871993.dkr.ecr.us-east-1.amazonaws.com/dev_tools:dev-1.1.0 sorrentum/dev_tools:dev
  ```
  - Push the dev image to Dockerhub
  ```
  > docker push sorrentum/dev_tools:dev
  ```
  - Tag the prod version image as `sorrentum/dev_tools:prod`
  ```
  > docker tag 665840871993.dkr.ecr.us-east-1.amazonaws.com/dev_tools:prod sorrentum/dev_tools:prod
  ```
  - Push the prod image to Dockerhub
  ```
  > docker push sorrentum/dev_tools:prod
  ```
- Push the latest `prod` image to GHCR registry manually for GH actions to use
  it
  - Perform a Docker login using your GitHub username and PAT (Personal Access
    Token):
    ```bash
    > docker login ghcr.io -u <username>
    ```
  - Tag the `prod` image to the GHCR namespace:
    ```bash
    > docker tag 623860924167.dkr.ecr.eu-north-1.amazonaws.com/dev_tools:prod ghcr.io/cryptokaizen/dev_tools:prod
    ```
  - Push the tagged image to the GHCR registry:
    ```bash
    > docker push ghcr.io/cryptokaizen/dev_tools:prod
    ```

- Update the changelog, i.e. `//dev_tools/changelog.txt`
  - The changelog should be updated only after the image is released; otherwise
    the sanity checks will assert that the release's version is not higher than
    the latest version recorded in the changelog.
  - Specify what has changed
  - Pick the release version accordingly
    - NB! The release version should consist of 3 digits, e.g. "1.1.0" instead
      of "1.1"
    - We use [semantic versioning](https://semver.org/) convention
      - For example, adding a package to the image would mean bumping up version
        1.0.0 to 1.0.1
- Do a PR with the change including the updated `changelog.txt`
- Send a message on the `all@` chat telling people that a new version of the
  container has been released
  - Users need to do
    - `i docker_pull` from `dev_tools`,
    - `i docker_pull_dev_tools` from `cmamp`
  - Users need to make sure to pull docker after the master is up-to-date
    (including amp submodules)
