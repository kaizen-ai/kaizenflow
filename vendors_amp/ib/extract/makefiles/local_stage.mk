## #############################################################################
## Local stage.
## #############################################################################

# Run ib extract app container.
IB_CONNECT_API_PORT?=4006
IB_CONNECT_VNC_PORT?=5906
ib_extract.docker_run.local:
	IMAGE=$(IB_EXTRACT_DEV_IMAGE) \
	API_PORT=$(IB_CONNECT_API_PORT) \
	VNC_PORT=$(IB_CONNECT_VNC_PORT) \
	docker-compose \
		-f vendors_amp/ib/extract/compose/docker-compose.local.yml \
    run --rm \
		-l user=$(USER) \
		-l app="ib_extract" \
		--service-ports \
		extractor \
		/bin/bash

# Run in detach mode.
ib_extract.docker_up.local:
	IMAGE=$(IB_EXTRACT_DEV_IMAGE) \
	API_PORT=$(IB_CONNECT_API_PORT) \
	VNC_PORT=$(IB_CONNECT_VNC_PORT) \
	docker-compose \
	  -f vendors_amp/ib/extract/compose/docker-compose.local.yml \
		up \
		-d

# Stop services.
ib_extract.docker_down.local:
	IMAGE=$(IB_EXTRACT_DEV_IMAGE) \
	API_PORT=$(IB_CONNECT_API_PORT) \
	VNC_PORT=$(IB_CONNECT_VNC_PORT) \
	docker-compose \
		-f vendors_amp/ib/extract/compose/docker-compose.local.yml \
		down
