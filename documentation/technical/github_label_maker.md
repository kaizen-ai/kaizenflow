<!--ts-->
   * [Install](#install)
   * [Workflow](#workflow)
   * [GP setup](#gp-setup)



<!--te-->

# Install

- Checkout the code

  ```bash
  > git clone git@github.com:mloskot/github-label-maker.git
  ```

- Install the needed dependency

  ```bash
  > conda install -y pygithub
  ```

- Create a OAuth token from GitHub

  ```bash
  > export GH_TOKEN=...
  ```

# Workflow

- There is a script automated the steps in
  `//amp/dev_scripts/github/labels/set_labels.sh`

- Extract the current labels as a backup or for saving

  ```bash
  > python github-label-maker.py -d p1.json -o ParticleDev -r commodity_research -t $GH_TOKEN
  > python github-label-maker.py -d amp.json -o alphamatic -r amp -t $GH_TOKEN
  ```

- Colors are from:
  [here](https://github.com/ManageIQ/guides/blob/master/labels.md)

- To apply the labels
  ```
  python github-label-maker.py -m gh_tech_labels.json -o alphamatic -r amp -t $GH_TOKEN
  ```

# GP setup

- We don't install the dependencies of label maker as part of the standard
  package, so one needs to do:

  ```bash
  > conda install -y pygithub
  ```

- The token is in my `.bashrc`

- The code is:

  ```bash
  cd ~/src/github/github-label-maker
  ```

- Run the script doing a backup of the labels in `backup_and_update`

- Commit the updated changes

- Update the labels

  ```bash
  vimdiff dev_scripts/github/labels/backup/labels.ParticleDev.commodity_research.json dev_scripts/github/labels/gh_tech_labels.json
  ```

- Run the script updating the labels
