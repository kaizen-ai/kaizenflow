<!--ts-->
   * [Kibot timing](#kibot-timing)
   * [Build image](#build-image)
   * [Run kibot app](#run-kibot-app)
      * [Prerequisites](#prerequisites)
      * [Run locally for development](#run-locally-for-development)
      * [Stop remaining PostgreSQL containers](#stop-remaining-postgresql-containers)
   * [Development flow using stages](#development-flow-using-stages)



<!--te-->



# Build image

1. Build release candidate image

```bash
> make im.docker_buildi_image.rc
```

2. (Optional for now) Push release candidate image to ECR (Optional for now)

```bash
> make im.docker_push_image.rc
```

3. Tag release candidate image with the latest tag

```bash
> make im.docker_tag_rc_image.latest
```

4. Push latest image do ECR

```bash
> make im.docker_push_image.latest
```

# Run kibot app

Pull image.

```bash
> make im.docker_pull
```

By the default we use $KIBOT_IMAGE for all run. You can check the setup to
identify concrete image.

```bash
> make im.print_setup
# You will get something like:
IM_REPO_BASE_PATH=083233266530.dkr.ecr.us-east-2.amazonaws.com/im
IM_IMAGE_DEV=083233266530.dkr.ecr.us-east-2.amazonaws.com/im:latest
IM_IMAGE_RC=083233266530.dkr.ecr.us-east-2.amazonaws.com/im:rc
```

## Prerequisites

- IB TWS or Gateway app [should be up](./ib/connect/README.md) on `research.p1`
  with API port 4012. For example:

```bash
> IB_CONNECT_USER=gpsagg314 \
  IB_CONNECT_PASSWORD=<password> \
  IB_CONNECT_VNC_PASSWORD=12345 \
  IB_CONNECT_API_PORT=4012 \
  IB_CONNECT_VNC_PORT=5912 \
  make ib_connect.docker_up.prod
```

## Run locally for development

Build local image:

```bash
> make im.docker_build_image.rc
> make im.docker_tag_rc_image.latest
```

Basic run with PostgreSQL:

```bash
> make im.docker_up.local
```

Basic run without PostgreSQL:

```bash
> make im.docker_bash
```

## Stop remaining PostgreSQL containers

Stop a container:

```bash
> make im.docker_down.local
```

Stop a container and remove all data:

```bash
> make im.docker_rm.local
```

# Development flow using stages

- Use `local` stages for development locally. Related: target in makefile
  `im.docker_up.local`

All stages can have separate docker-compose files. All stages must have separate
targets in make file to start and stop services.
