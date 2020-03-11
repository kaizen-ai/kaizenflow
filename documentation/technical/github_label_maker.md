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
