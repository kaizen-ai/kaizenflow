## #############################################################################
## Local stage.
## #############################################################################

# Run ib extract app container.
ib_extract.docker_run.local:
	IMAGE=$(IB_EXTRACT_DEV_IMAGE) \
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
	docker-compose \
	  -f vendors_amp/ib/extract/compose/docker-compose.local.yml \
		up \
		-d

# Stop services.
ib_extract.docker_down.local:
	IMAGE=$(IB_EXTRACT_DEV_IMAGE) \
	docker-compose \
		-f vendors_amp/ib/extract/compose/docker-compose.local.yml \
		down
