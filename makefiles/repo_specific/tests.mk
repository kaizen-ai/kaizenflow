# #############################################################################
# Tests
# #############################################################################
# Run fast tests.
docker_fast:
	IMAGE=$(IMAGE) \
	docker-compose run -l user=$(USER) --rm fast_tests

# Run slow tests.
docker_slow:
	IMAGE=$(IMAGE) \
	docker-compose run -l user=$(USER) --rm slow_tests

# Run superslow tests.
docker_superslow:
	IMAGE=$(IMAGE) \
	docker-compose run -l user=$(USER) --rm superslow_tests
