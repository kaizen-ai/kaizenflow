## #############################################################################
## Local stage.
## #############################################################################

# Run ib connect app container.
IB_CONNECT_APP?=GATEWAY
IB_CONNECT_TRUSTED_IPS?=""
IB_CONNECT_VNC_PASSWORD?=
IB_CONNECT_IMAGE?=$(IB_CONNECT_IMAGE)
ib_connect.docker_run.local:
ifeq ($(IB_CONNECT_VNC_PASSWORD),)
	@echo "You need to provide IB_CONNECT_VNC_PASSWORD parameter. Example: 'IB_CONNECT_VNC_PASSWORD=12345 make ib_connect.docker.run'"
else
	IB_APP=$(IB_CONNECT_APP) \
	IMAGE=$(IB_CONNECT_DEV_IMAGE) \
	TRUSTED_IPS=$(IB_CONNECT_TRUSTED_IPS) \
	VNC_PASSWORD=$(IB_CONNECT_VNC_PASSWORD) \
	docker-compose \
		-f vendors_amp/ib/connect/compose/docker-compose.local.yml \
    run --rm \
		-l user=$(USER) \
		-l app="ib_connect" \
		--service-ports \
		tws \
		/bin/bash
endif

# Run in detach mode.
ib_connect.docker_up.local:
ifeq ($(IB_CONNECT_VNC_PASSWORD),)
	@echo "You need to provide IB_CONNECT_VNC_PASSWORD parameter. Example: 'IB_CONNECT_VNC_PASSWORD=12345 make ib_connect.docker.run'"
else
	IB_APP=$(IB_CONNECT_APP) \
	IMAGE=$(IB_CONNECT_DEV_IMAGE) \
	TRUSTED_IPS=$(IB_CONNECT_TRUSTED_IPS) \
	VNC_PASSWORD=$(IB_CONNECT_VNC_PASSWORD) \
	docker-compose \
	  -f vendors_amp/ib/connect/compose/docker-compose.local.yml \
		up \
		-d
endif

# Stop services.
ib_connect.docker_down.local:
	IB_APP=$(IB_CONNECT_APP) \
	IMAGE=$(IB_CONNECT_DEV_IMAGE) \
	docker-compose \
		-f vendors_amp/ib/connect/compose/docker-compose.local.yml \
		down
