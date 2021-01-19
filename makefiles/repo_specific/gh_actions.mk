# #############################################################################
# GH Actions
# #############################################################################

# Run fast tests.
docker_fast_gh_actions:
	IMAGE=$(IMAGE) \
	docker-compose run -l user=$(USER) --rm fast_tests_gh_action

# Run slow tests.
docker_slow_gh_actions:
	IMAGE=$(IMAGE) \
	docker-compose run -l user=$(USER) --rm slow_tests_gh_action

# Run superslow tests.
docker_superslow_gh_actions:
	IMAGE=$(IMAGE) \
	docker-compose run -l user=$(USER) --rm superslow_tests_gh_action
