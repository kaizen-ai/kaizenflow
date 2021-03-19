OPTS?=
ib_metadata_crawler.docker_build:
	docker build \
		--progress=plain \
		-t ib_metadata_crawler \
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
