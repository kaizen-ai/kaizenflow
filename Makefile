# We need to include the variables first and then global targets.
include devops/makefiles/repo_specific.mk
include devops/makefiles/general.mk

include $(shell find apps/automl/makefiles -maxdepth 1 -name "*.mk")
include $(shell find customers/dow_jones/dj_app/makefiles -maxdepth 1 -name "*.mk")
include $(shell find edgar/headers/makefiles -maxdepth 1 -name "*.mk")
