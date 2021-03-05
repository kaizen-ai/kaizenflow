# Setup
ifdef $(GITHUB_SHA)
IMAGE_RC_SHA:=$(GITHUB_SHA)
else
# GITHUB_SHA not found. Setting up IMAGE_RC_SHA form HEAD.
IMAGE_RC_SHA:=$(shell git rev-parse HEAD)
endif
IB_CONNECT_REPO_BASE_PATH=083233266530.dkr.ecr.us-east-2.amazonaws.com/ib_connect
IB_CONNECT_IMAGE=$(IB_CONNECT_REPO_BASE_PATH):latest
IB_CONNECT_IMAGE_RC=$(IB_CONNECT_REPO_BASE_PATH):rc
IB_CONNECT_IMAGE_RC_SHA=$(IB_CONNECT_REPO_BASE_PATH):$(IMAGE_RC_SHA)

ib_connect.setup.print:
	@echo "IB_CONNECT_REPO_BASE_PATH=$(IB_CONNECT_REPO_BASE_PATH)"
	@echo "IB_CONNECT_IMAGE=$(IB_CONNECT_IMAGE)"
	@echo "IB_CONNECT_IMAGE_RC=$(IB_CONNECT_IMAGE_RC)"
	@echo "IB_CONNECT_IMAGE_RC_SHA=$(IB_CONNECT_IMAGE_RC_SHA)"

# Pull images from ecr
ib_connect.docker.pull:
	docker pull $(IB_CONNECT_IMAGE)

# Build images
ib_connect.docker.build.rc_image:
	docker build \
		--progress=plain \
		--no-cache \
		-t $(IB_CONNECT_IMAGE_RC) \
		-t $(IB_CONNECT_IMAGE_RC_SHA) \
		--file vendors_amp/ib/connect/Dockerfile .

# Push release candidate images
ib_connect.docker.push.rc_image:
	docker push $(IB_CONNECT_IMAGE_RC)
	docker push $(IB_CONNECT_IMAGE_RC_SHA)

# Tag :rc image with :latest tag
ib_connect.docker.tag.rc.latest:
	docker tag $(IB_CONNECT_IMAGE_RC) $(IB_CONNECT_IMAGE)

# Push image with :latest tag
ib_connect.docker.latest_image:
	docker push $(IB_CONNECT_IMAGE)


BASE_IMAGE?=$(IB_CONNECT_IMAGE)
VERSION?=
# Tag :latest image with specific tag
ib_connect.docker.tag.latest.version:
ifeq ($(VERSION),)
	@echo "You need to provide VERSION parameter. Example: 'make docker_tag_auto_ml_demo_rc_version VERSION=0.1'"
else
	docker tag $(BASE_IMAGE) $(IB_CONNECT_REPO_BASE_PATH):$(VERSION)
endif

# Push image wish specific tag
ib_connect.docker.push.version_image:
ifeq ($(VERSION),)
	@echo "You need to provide VERSION parameter. Example: 'make docker_push_auto_ml_demo_version_image VERSION=0.1'"
else
	docker push $(IB_CONNECT_REPO_BASE_PATH):$(VERSION)
endif


# Run ib connect app container.
IB_CONNECT_APP?=GATEWAY
IB_CONNECT_TRUSTED_IPS?=""
IB_CONNECT_VNC_PASSWORD?=
IB_CONNECT_IMAGE?=$(IB_CONNECT_IMAGE)
ib_connect.docker.local.run:
ifeq ($(IB_CONNECT_VNC_PASSWORD),)
	@echo "You need to provide IB_CONNECT_VNC_PASSWORD parameter. Example: 'IB_CONNECT_VNC_PASSWORD=12345 make ib_connect.docker.run'"
else
	IB_APP=$(IB_CONNECT_APP) \
	IMAGE=$(IB_CONNECT_IMAGE) \
	TRUSTED_IPS=$(IB_CONNECT_TRUSTED_IPS) \
	VNC_PASSWORD=$(IB_CONNECT_VNC_PASSWORD) \
	docker-compose \
		-f vendors_amp/ib/connect/compose/docker-compose.yml \
		-f vendors_amp/ib/connect/compose/docker-compose.local.yml \
    run --rm \
		-l user=$(USER) \
		-l app="ib_connect" \
		--service-ports \
		tws \
		/bin/bash
endif

# Run in detach mode.
ib_connect.docker.local.up:
ifeq ($(IB_CONNECT_VNC_PASSWORD),)
	@echo "You need to provide IB_CONNECT_VNC_PASSWORD parameter. Example: 'IB_CONNECT_VNC_PASSWORD=12345 make ib_connect.docker.run'"
else
	IB_APP=$(IB_CONNECT_APP) \
	IMAGE=$(IB_CONNECT_IMAGE) \
	TRUSTED_IPS=$(IB_CONNECT_TRUSTED_IPS) \
	VNC_PASSWORD=$(IB_CONNECT_VNC_PASSWORD) \
	docker-compose \
		-f vendors_amp/ib/connect/compose/docker-compose.yml \
	  -f vendors_amp/ib/connect/compose/docker-compose.local.yml \
		up \
		-d
endif

# Stop services.
ib_connect.docker.local.down:
	IB_APP=$(IB_CONNECT_APP) \
	IMAGE=$(IB_CONNECT_IMAGE) \
	docker-compose \
		-f vendors_amp/ib/connect/compose/docker-compose.yml \
		-f vendors_amp/ib/connect/compose/docker-compose.local.yml \
		down

# Test stage is the same as local, just ports are different
# Run ib connect app test container.
IB_CONNECT_APP?=GATEWAY
IB_CONNECT_TRUSTED_IPS?=""
IB_CONNECT_VNC_PASSWORD?=
IB_CONNECT_IMAGE?=$(IB_CONNECT_IMAGE)
ib_connect.docker.test.run:
ifeq ($(IB_CONNECT_VNC_PASSWORD),)
	@echo "You need to provide IB_CONNECT_VNC_PASSWORD parameter. Example: 'IB_CONNECT_VNC_PASSWORD=12345 make ib_connect.docker.run'"
else
	IB_APP=$(IB_CONNECT_APP) \
	IMAGE=$(IB_CONNECT_IMAGE) \
	TRUSTED_IPS=$(IB_CONNECT_TRUSTED_IPS) \
	VNC_PASSWORD=$(IB_CONNECT_VNC_PASSWORD) \
	docker-compose \
		-f vendors_amp/ib/connect/compose/docker-compose.yml \
		-f vendors_amp/ib/connect/compose/docker-compose.test.yml \
    run --rm \
		-l user=$(USER) \
		-l app="ib_connect" \
		--service-ports \
		tws \
		/bin/bash
endif

# Run test container in detach mode.
ib_connect.docker.test.up:
ifeq ($(IB_CONNECT_VNC_PASSWORD),)
	@echo "You need to provide IB_CONNECT_VNC_PASSWORD parameter. Example: 'IB_CONNECT_VNC_PASSWORD=12345 make ib_connect.docker.run'"
else
	IB_APP=$(IB_CONNECT_APP) \
	IMAGE=$(IB_CONNECT_IMAGE) \
	TRUSTED_IPS=$(IB_CONNECT_TRUSTED_IPS) \
	VNC_PASSWORD=$(IB_CONNECT_VNC_PASSWORD) \
	docker-compose \
		-f vendors_amp/ib/connect/compose/docker-compose.yml \
	  -f vendors_amp/ib/connect/compose/docker-compose.test.yml \
		up \
		-d
endif

# Stop services.
ib_connect.docker.test.down:
	IB_APP=$(IB_CONNECT_APP) \
	IMAGE=$(IB_CONNECT_IMAGE) \
	docker-compose \
		-f vendors_amp/ib/connect/compose/docker-compose.yml \
		-f vendors_amp/ib/connect/compose/docker-compose.test.yml \
		down

# Get docker logs
ib_connect.docker.logs:
	docker logs compose_tws_1
