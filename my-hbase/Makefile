IMAGE_NAME = myhbase
IMAGE_TAG = 1.0

HBASE_VERSION=$(shell awk '/^ENV HBASE_VERSION/ {print $3}' Dockerfile)

build:
	@echo "Building hbase docker image $(HBASE_VERSION)"
	docker build -t $(IMAGE_NAME) .