# #############################################################################
# Tests
# #############################################################################
# Run fast tests locally.
test_fast:
	IMAGE=$(IMAGE) \
	docker-compose run \
	 	-f compose/docker-compose.yml \
	 	-f compose/docker-compose.tests.yml \
	 	-l user=$(USER) \
	 	--rm \
	 	app docker_build/run_fast_tests.sh

# Run fast tests via GH Actions.
test_fast_gh_action:
	IMAGE=$(IMAGE) \
	docker-compose run \
	 	-f compose/docker-compose.yml \
	 	-f compose/docker-compose.tests.yml \
	 	-l user=$(USER) \
	 	--rm \
	 	app docker_build/run_fast_tests_gh_action.sh


# Run slow tests.
slow_tests:
	IMAGE=$(IMAGE) \
	docker-compose run -l user=$(USER) --rm slow_tests

# Run superslow tests.
superslow_tests:
	IMAGE=$(IMAGE) \
	docker-compose run -l user=$(USER) --rm superslow_tests
