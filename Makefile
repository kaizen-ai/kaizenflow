# We need to include the variables first and then global targets.
include devops/makefiles/repo_specific.mk
include devops/makefiles/general.mk

include $(shell find instrument_master/ib/connect/makefiles -maxdepth 1 -name "*.mk")
include $(shell find instrument_master/devops/makefiles -maxdepth 1 -name "*.mk")
