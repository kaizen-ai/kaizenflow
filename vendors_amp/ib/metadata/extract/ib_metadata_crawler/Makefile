ib_metadata_crawler.docker_build:
	docker build \
		--progress=plain \
		-t ib_metadata_crawler \
		--file devops/Dockerfile \
		.
	docker image ls ib_metadata_crawler

ib_metadata_crawler.run:
	docker run -it ib_crawler scrapy crawl ibroker \
		--loglevel INFO \
		-s CLOSESPIDER_ERRORCOUNT=0
