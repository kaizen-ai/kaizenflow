# #############################################################################
# Tests
# #############################################################################
# Run fast tests locally.
test_fast:
	IMAGE=$(IMAGE) \
	docker-compose \
		-f compose/docker-compose.yml \
		run \
		-l user=$(USER) \
		--rm \
		app docker_build/run_fast_tests.sh

# Run slow tests.
test_slow:
	IMAGE=$(IMAGE) \
	docker-compose \
		-f compose/docker-compose.yml \
		run \
		-l user=$(USER) \
		--rm \
		app docker_build/run_slow_tests.sh

# Run superslow tests.
test_superslow:
	IMAGE=$(IMAGE) \
	docker-compose \
		-f compose/docker-compose.yml \
		run \
		-l user=$(USER) \
		--rm \
		app docker_build/run_superslow_tests.sh

# #############################################################################
# GH actions tests.
# #############################################################################

# Run fast tests.
test_fast_gh_action:
	IMAGE=$(IMAGE) \
	docker-compose \
		-f compose/docker-compose.yml \
		-f compose/docker-compose.gh_actions.yml \
		run \
		-l user=$(USER) \
		--rm \
		app docker_build/run_fast_tests.sh

# Run slow tests.
test_slow_gh_action:
	IMAGE=$(IMAGE) \
	docker-compose \
		-f compose/docker-compose.yml \
		-f compose/docker-compose.gh_actions.yml \
		run \
		-l user=$(USER) \
		--rm \
		app docker_build/run_slow_tests.sh

# Run superslow tests.
test_superslow_gh_action:
	IMAGE=$(IMAGE) \
	docker-compose \
		-f compose/docker-compose.yml \
		-f compose/docker-compose.gh_actions.yml \
		run \
		-l user=$(USER) \
		--rm \
		app docker_build/run_superslow_tests.sh

# #############################################################################
# GH actions release candidate tests.
# #############################################################################
# Tests using release candidate image via GH Actions.

# Run fast tests.
test_fast_gh_action_rc:
	IMAGE=$(IMAGE_RC) \
	docker-compose \
		-f compose/docker-compose.yml \
		-f compose/docker-compose.gh_actions.yml \
		run \
		-l user=$(USER) \
		--rm \
		app docker_build/run_fast_tests.sh

# Run slow tests.
test_slow_gh_action_rc:
	IMAGE=$(IMAGE_RC) \
	docker-compose \
		-f compose/docker-compose.yml \
		-f compose/docker-compose.gh_actions.yml \
		run \
		-l user=$(USER) \
		--rm \
		app docker_build/run_slow_tests.sh

# Run superslow tests.
test_superslow_gh_action_rc:
	IMAGE=$(IMAGE_RC) \
	docker-compose \
		-f compose/docker-compose.yml \
		-f compose/docker-compose.gh_actions.yml \
		run \
		-l user=$(USER) \
		--rm \
		app docker_build/run_superslow_tests.sh
