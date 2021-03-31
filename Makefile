# We need to include the variables first and then global targets.
include devops/makefiles/repo_specific.mk
include devops/makefiles/general.mk

include $(shell find vendors_amp/ib/connect/makefiles -maxdepth 1 -name "*.mk")
include $(shell find vendors_amp/devops/makefiles -maxdepth 1 -name "*.mk")
