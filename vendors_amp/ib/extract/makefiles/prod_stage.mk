## #############################################################################
## Production stage.
## #############################################################################

# Run ib extract app container.
ib_extract.docker_run.prod:
	IMAGE=$(IB_EXTRACT_PROD_IMAGE) \
	docker-compose \
		-f vendors_amp/ib/extract/compose/docker-compose.prod.yml \
    run --rm \
		-l user=$(USER) \
		-l app="ib_extract" \
		--service-ports \
		extractor \
		/bin/bash

# Run in detach mode.
ib_extract.docker_up.prod:
	IMAGE=$(IB_EXTRACT_PROD_IMAGE) \
	docker-compose \
	  -f vendors_amp/ib/extract/compose/docker-compose.prod.yml \
		up \
		-d

# Stop services.
ib_extract.docker_down.prod:
	IMAGE=$(IB_EXTRACT_PROD_IMAGE) \
	docker-compose \
		-f vendors_amp/ib/extract/compose/docker-compose.prod.yml \
		down
