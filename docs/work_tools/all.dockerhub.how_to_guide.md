

<!-- toc -->

- [Dockerhub](#dockerhub)
  * [Login Dockerhub](#login-dockerhub)
  * [Login through CLI](#login-through-cli)
  * [List all the images](#list-all-the-images)
  * [Rename an image](#rename-an-image)

<!-- tocstop -->

# Dockerhub

## Login Dockerhub

https://hub.docker.com/

Username: sorrentum Email: gp@crypto-kaizen.com

There are several public images

- Sorrentum/cmamp
- Sorrentum/dev_tools

Used in DATA605:

- Sorrentum/sorrentum
- Sorrentum/defi
- Sorrentum/jupyter

The page corresponding to the Sorrentum repo is
https://hub.docker.com/u/sorrentum

## Login through CLI

> docker login --username sorrentum --password XYZ

## List all the images

- Without authentication
```
> curl -s "https://hub.docker.com/v2/repositories/sorrentum/?page_size=100" | jq '.results|.[]|.name'
"sorrentum"
"cmamp"
"jupyter"
"dev_tools"
"defi"
```

## Rename an image

> docker pull yourusername/oldimagename:tag docker tag
> yourusername/oldimagename:tag yourusername/newimagename:tag docker push
> yourusername/newimagename:tag

- To delete the old image you need to go through the GUI
