# Login Dockerhub

https://hub.docker.com/

Username: kaizenflow
Email: gp@crypto-kaizen.com

There are several public images

- kaizenflow/cmamp
- kaizenflow/dev_tools

Used in DATA605:
- kaizen-ai/kaizenflow
- kaizenflow/defi
- kaizenflow/jupyter

The page corresponding to the Kaizenflow repo is
https://hub.docker.com/u/kaizenflow

# Login through CLI

> docker login --username kaizenflow --password XYZ

# List all the images

- Without authentication
```
> curl -s "https://hub.docker.com/v2/repositories/kaizenflow/?page_size=100" | jq '.results|.[]|.name'
"kaizenflow"
"cmamp"
"jupyter"
"dev_tools"
"defi"
```

# Rename an image

> docker pull yourusername/oldimagename:tag
> docker tag yourusername/oldimagename:tag yourusername/newimagename:tag
> docker push yourusername/newimagename:tag

- To delete the old image you need to go through the GUI
