ib_metadata_crawler.docker_build:
	docker build \
		--progress=plain \
		-t ib_metadata_crawler \
		--file devops/Dockerfile

ib_metadata_crawler.run:
	docker run -it ib_crawler scrapy crawl ibroker
