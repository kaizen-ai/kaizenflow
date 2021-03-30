## #############################################################################
## Production stage.
## #############################################################################

# Run ib extract app container.
IB_CONNECT_API_PORT?=4004
ib_extract.docker_run.prod:
	IMAGE=$(IB_EXTRACT_PROD_IMAGE) \
  API_PORT=$(IB_CONNECT_API_PORT) \
	docker-compose \
		-f vendors_amp/ib/data/extract/gateway/compose/docker-compose.prod.yml \
    run --rm \
		-l user=$(USER) \
		-l app="ib_extract" \
		--service-ports \
		extractor \
		/bin/bash

# Run in detach mode.
ib_extract.docker_up.prod:
	IMAGE=$(IB_EXTRACT_PROD_IMAGE) \
	API_PORT=$(IB_CONNECT_API_PORT) \
	docker-compose \
	  -f vendors_amp/ib/data/extract/gateway/compose/docker-compose.prod.yml \
		up \
		-d

# Stop services.
ib_extract.docker_down.prod:
	IMAGE=$(IB_EXTRACT_PROD_IMAGE) \
	API_PORT=$(IB_CONNECT_API_PORT) \
	docker-compose \
		-f vendors_amp/ib/data/extract/gateway/compose/docker-compose.prod.yml \
		down
