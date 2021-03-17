OPTS?=
ib_metadata_crawler.docker_build:
	docker build \
		--progress=plain \
		-t ib_metadata_crawler \
		--file devops/Dockerfile $(OPTS) \
		.
	docker image ls ib_metadata_crawler

ib_metadata_crawler.run:
	docker run \
		--rm \
		-it \
		-v ${PWD}/src:/app \
		ib_crawler \
		scrapy crawl ibroker \
		--loglevel INFO \
		-s CLOSESPIDER_ERRORCOUNT=0 \
		2>&1 | tee scrapy.log

ib_metadata_crawler.bash:
	docker run \
		--rm \
		-it \
		-v ${PWD}/src:/app \
		ib_crawler \
		/bin/bash
