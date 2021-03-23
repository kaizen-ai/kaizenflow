ECR_BASE_PATH=083233266530.dkr.ecr.us-east-2.amazonaws.com

OPTS?=
ib_metadata_crawler.docker_build:
	docker build \
		--progress=plain \
		-t ib_metadata_crawler \
		--file devops/Dockerfile \
		.
	docker image ls ib_metadata_crawler

ib_metadata_crawler.run:
	docker run \
		--rm \
		-i \
		-v ${PWD}:/outcome \
		ib_metadata_crawler \
		scrapy crawl ibroker \
		--loglevel INFO \
		2>&1 | tee scrapy.log

ib_metadata_crawler.bash:
	docker run \
		--rm \
		-it \
		-v ${PWD}:/outcome \
		ib_metadata_crawler \
		/bin/bash


AMP_DIR=$(shell git rev-parse --show-toplevel)
print_setup:
	@echo "AMP_DIR=$(AMP_DIR)"


J_PORT?=9999
IMAGE_DEV=$(ECR_BASE_PATH)/amp_env:latest
ib_metadata_crawler.docker_jupyter:
	J_PORT=$(J_PORT) \
	IMAGE=$(IMAGE_DEV) \
	docker-compose \
			-f $(AMP_DIR)/devops/compose/docker-compose-jupyter.yml \
			-f $(AMP_DIR)/devops/compose/docker-compose.yml \
			run \
			--rm \
			-l user=$(USER) \
			--service-ports \
			jupyter_server
