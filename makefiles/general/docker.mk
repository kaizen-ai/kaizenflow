# #############################################################################
# Dev.
# #############################################################################

# Log in to AWS ECR.
AWSCLI_VERSION=$(shell aws --version | awk '{print $$1}' | awk -F"/" '{print $$2}')
AWSCLI_MAJOR_VERSION=$(shell echo "$(AWSCLI_VERSION)" | awk -F"." '{print $$1}')
docker_login:
	@echo AWS CLI version: $(AWSCLI_VERSION)
	@echo AWS CLI major version: $(AWSCLI_MAJOR_VERSION)
ifeq ($(AWSCLI_MAJOR_VERSION),1)
	eval `aws ecr get-login --no-include-email --region us-east-2`
else
	docker login -u AWS -p $(aws ecr get-login --region us-east-2) $(ECR_URL)
endif

# Pull an image from the registry.
docker_pull:
	$(foreach v, $(REPO_IMAGES), docker pull $(v); )

# List all running containers:
#   ```
#   > docker_ps
#   CONTAINER ID		user				IMAGE															  COMMAND				  CREATED			 STATUS			  PORTS			   service
#   2ece37303ec9		gad				 083233266530.dkr.ecr.us-east-2.amazonaws.com/particle_env:latest   "./docker_build/entr…"   5 seconds ago	   Up 4 seconds							user_space
#   ```
docker_ps:
	docker ps --format='table {{.ID}}\t{{.Label "user"}}\t{{.Image}}\t{{.Command}}\t{{.RunningFor}}\t{{.Status}}\t{{.Ports}}\t{{.Label "com.docker.compose.service"}}'

# Report container stats, e.g., CPU, RAM.
#   ```
#   > docker_stats
#   CONTAINER ID		NAME								   CPU %			   MEM USAGE / LIMIT	 MEM %			   NET I/O			 BLOCK I/O		   PIDS
#   2ece37303ec9		commodity_research_user_space_run_30   0.00%			   15.74MiB / 31.07GiB   0.05%			   351kB / 6.27kB	  34.2MB / 12.3kB	 4
#   ```
docker_stats:
	# To change output format you can use following --format flag with `docker stats` command.
	# --format='table {{.ID}}\t{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.NetIO}}\t{{.BlockIO}}\t{{.PIDs}}'
	docker stats --no-stream $(IDS)

docker_bash:
	IMAGE=$(IMAGE) \
	docker-compose \
		-f compose/docker-compose.yml \
		run --rm \
		-l user=$(USER) \
		app bash

# #############################################################################
# Admins.
# #############################################################################

ifdef $(GITHUB_SHA)
IMAGE_RC_SHA:=$(GITHUB_SHA)
else
# GITHUB_SHA not found. Setting up IMAGE_RC_SHA form HEAD.
IMAGE_RC_SHA:=$(shell git rev-parse HEAD)
endif
IMAGE_RC?=$(IMAGE_RC)
docker_build_rc_image:
	DOCKER_BUILDKIT=1 \
	docker build --progress=plain \
		--no-cache \
		-t $(IMAGE_RC) \
		-t $(ECR_REPO_BASE_PATH):$(IMAGE_RC_SHA) \
		-f docker_build/Dockerfile .

docker_push_rc_image:
	docker push $(IMAGE_RC)
	docker push $(ECR_REPO_BASE_PATH):$(IMAGE_RC_SHA)

docker_tag_rc_latest:
	docker tag $(IMAGE_RC) $(AMP_ENV_IMAGE)

docker_push_latest_image:
	docker push $(AMP_ENV_IMAGE)
