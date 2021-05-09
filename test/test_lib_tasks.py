# This should only test helper functions from `lib_tasks.py`.
# `test_tasks.py` associated to `tasks.py` should test specific task targets.

import logging
import os
from typing import Dict

import pytest

import helpers.git as git
import helpers.printing as hprint
import helpers.system_interaction as hsinte
import helpers.unit_test as hut
import lib_tasks as ltasks

_LOG = logging.getLogger(__name__)


class TestLibTasks1(hut.TestCase):
    def test_get_build_tag1(self) -> None:
        build_tag = ltasks._get_build_tag()
        _LOG.debug("build_tag=%s", build_tag)

    def test_get_gh_issue_title1(self) -> None:
        self._gh_login()
        issue_id = 1
        repo = "amp"
        act = ltasks._get_gh_issue_title(issue_id, repo)
        exp = "AmpTask1_Bridge_Python_and_R"
        self.assert_equal(act, exp)

    def test_get_gh_issue_title2(self) -> None:
        self._gh_login()
        issue_id = 1
        repo = "lem"
        act = ltasks._get_gh_issue_title(issue_id, repo)
        exp = "LemTask1_Adapt_the_usual_infra_from_my_codebase"
        self.assert_equal(act, exp)

    def test_get_gh_issue_title3(self) -> None:
        self._gh_login()
        issue_id = 1
        repo = "dev_tools"
        act = ltasks._get_gh_issue_title(issue_id, repo)
        exp = "DevToolsTask1_Migration_from_amp"
        self.assert_equal(act, exp)

    @staticmethod
    def _gh_login() -> None:
        """
        Log in inside GitHub.
        """
        env_var = "GH_ACTION_ACCESS_TOKEN"
        if os.environ.get(env_var, None):
            # If the env var exists and it's not None.
            _LOG.warning("Using env var '%s' to log in GitHub", env_var)
            cmd = "echo $GH_ACTION_ACCESS_TOKEN | gh auth login --with-token"
            hsinte.system(cmd)
        # Check that we are logged in.
        cmd = "gh auth status"
        hsinte.system(cmd)


class TestLibTasksRemoveSpaces1(hut.TestCase):

    def test1(self) -> None:
        txt = r"""
            IMAGE=665840871993.dkr.ecr.us-east-1.amazonaws.com/amp_test:dev \
                docker-compose \
                --file $GIT_ROOT/devops/compose/docker-compose_as_submodule.yml \
                run \
                --rm \
                -l user=$USER_NAME \
                --entrypoint bash \
                user_space
            """
        act = ltasks._remove_spaces(txt)
        exp = ("IMAGE=665840871993.dkr.ecr.us-east-1.amazonaws.com/amp_test:dev"
               " docker-compose --file"
               " $GIT_ROOT/devops/compose/docker-compose_as_submodule.yml"
               " run --rm -l user=$USER_NAME --entrypoint bash user_space")
        self.assert_equal(act, exp, fuzzy_match=False)


class TestLibTasksGetDockerCmd1(hut.TestCase):

    def setUp(self):
        super().setUp()
        params = self._get_default_params()
        ltasks.set_default_params(params)

    def tearDown(self):
        ltasks.reset_default_params()
        super().tearDown()

    @staticmethod
    def _get_default_params() -> Dict[str, str]:
        """
        Get fake params pointing to a different image so we can test the code
        without affecting the official images.
        """
        ecr_base_path = "665840871993.dkr.ecr.us-east-1.amazonaws.com"
        default_params = {
            "ECR_BASE_PATH": ecr_base_path,
            "BASE_IMAGE": "amp_test",
            "DEV_TOOLS_IMAGE_PROD": f"{ecr_base_path}/dev_tools:prod",
        }
        return default_params

    @pytest.mark.skipif(not git.is_in_amp_as_submodule(),
                        reason="Only run in amp as submodule")
    def test_docker_bash1(self) -> None:
        """
        Command for docker_bash target.
        """
        stage = "dev"
        base_image = ""
        cmd = "bash"
        service_name = "app"
        entrypoint = False
        print_docker_config = False
        act = ltasks._get_docker_cmd(
            stage,
            base_image,
            cmd,
            service_name=service_name,
            entrypoint=entrypoint,
            print_docker_config=print_docker_config,
        )
        act = hut.purify_txt_from_client(act)
        exp = r"""
        IMAGE=665840871993.dkr.ecr.us-east-1.amazonaws.com/amp_test:dev \
            docker-compose \
            --file $GIT_ROOT/devops/compose/docker-compose.yml --file $GIT_ROOT/devops/compose/docker-compose_as_submodule.yml \
            --env-file devops/env/default.env \
            run \
            --rm \
            -l user=$USER_NAME \
            --entrypoint bash \
            app
        """
        self.assert_equal(act, exp, fuzzy_match=True)

    @pytest.mark.skipif(not git.is_in_amp_as_submodule(),
                        reason="Only run in amp as submodule")
    def test_docker_bash2(self) -> None:
        """
        Command for docker_bash with entrypoint.
        """
        stage = "local"
        base_image = ""
        cmd = "bash"
        print_docker_config = False
        act = ltasks._get_docker_cmd(
            stage,
            base_image,
            cmd,
            print_docker_config=print_docker_config,
        )
        act = hut.purify_txt_from_client(act)
        exp = r"""
        IMAGE=665840871993.dkr.ecr.us-east-1.amazonaws.com/amp_test:local \
            docker-compose \
            --file $GIT_ROOT/devops/compose/docker-compose.yml --file $GIT_ROOT/devops/compose/docker-compose_as_submodule.yml \
            --env-file devops/env/default.env \
            run \
            --rm \
            -l user=$USER_NAME \
            app \
            bash
        """
        self.assert_equal(act, exp, fuzzy_match=True)

    @pytest.mark.skipif(not git.is_in_amp_as_submodule(),
                        reason="Only run in amp as submodule")
    def test_docker_bash3(self) -> None:
        """
        Command for docker_bash with some env vars.
        """
        stage = "local"
        base_image = ""
        cmd = "bash"
        extra_env_vars = ["PORT=9999", "SKIP_RUN=1"]
        print_docker_config = False
        act = ltasks._get_docker_cmd(
            stage,
            base_image,
            cmd,
            extra_env_vars=extra_env_vars,
            print_docker_config=print_docker_config,
        )
        act = hut.purify_txt_from_client(act)
        exp = r"""
        IMAGE=665840871993.dkr.ecr.us-east-1.amazonaws.com/amp_test:local \
        PORT=9999 \
        SKIP_RUN=1 \
            docker-compose \
            --file $GIT_ROOT/devops/compose/docker-compose.yml --file $GIT_ROOT/devops/compose/docker-compose_as_submodule.yml \
            --env-file devops/env/default.env \
            run \
            --rm \
            -l user=$USER_NAME \
            app \
            bash
        """
        self.assert_equal(act, exp, fuzzy_match=True)

    @pytest.mark.skipif(not git.is_in_amp_as_supermodule(),
                        reason="Only run in amp as supermodule")
    def test_docker_bash4(self) -> None:
        stage = "dev"
        base_image = ""
        cmd = "bash"
        entrypoint = False
        act = ltasks._get_docker_cmd(
            stage,
            base_image,
            cmd,
            entrypoint=entrypoint,
            print_docker_config=print_docker_config,
        )
        act = hut.purify_txt_from_client(act)
        exp = r"""
        IMAGE=665840871993.dkr.ecr.us-east-1.amazonaws.com/amp_test:dev \
            docker-compose \
            --file $GIT_ROOT/devops/compose/docker-compose.yml \
            --env-file devops/env/default.env \
            run \
            --rm \
            -l user=$USER_NAME \
            --entrypoint bash \
            app
        """
        self.assert_equal(act, exp, fuzzy_match=True)

    @pytest.mark.skipif(not git.is_in_amp_as_submodule(),
                        reason="Only run in amp as submodule")
    def test_docker_jupyter1(self) -> None:
        stage = "dev"
        base_image = ""
        port = 9999
        self_test = True
        print_docker_config = False
        act = ltasks._get_docker_jupyter_cmd(stage, base_image, port, self_test,
            print_docker_config=print_docker_config)
        act = hut.purify_txt_from_client(act)
        exp = r"""
        IMAGE=665840871993.dkr.ecr.us-east-1.amazonaws.com/amp_test:dev \
        PORT=9999 \
            docker-compose \
            --file $GIT_ROOT/devops/compose/docker-compose.yml --file $GIT_ROOT/devops/compose/docker-compose_as_submodule.yml \
            --env-file devops/env/default.env \
            run \
            --rm \
            -l user=$USER_NAME \
            --service-ports \
            jupyter_server_test
        """
        self.assert_equal(act, exp, fuzzy_match=True)


class TestLibRunTests1(hut.TestCase):
    def test_run_fast_tests1(self) -> None:
        """
        Basic run fast tests.
        """
        pytest_opts = ""
        pytest_mark = ""
        dir_name = ""
        skip_submodules = False
        coverage = False
        collect_only = False
        #
        skipped_tests = "not slow and not superslow"
        act = ltasks._build_run_command_line(
            pytest_opts,
            pytest_mark,
            dir_name,
            skip_submodules,
            coverage,
            collect_only,
            skipped_tests,
        )
        exp = 'pytest -m "not slow and not superslow"'
        self.assert_equal(act, exp)

    def test_run_fast_tests2(self) -> None:
        """
        Coverage and collect-only.
        """
        pytest_opts = ""
        pytest_mark = ""
        dir_name = ""
        skip_submodules = False
        coverage = True
        collect_only = True
        #
        skipped_tests = "not slow and not superslow"
        act = ltasks._build_run_command_line(
            pytest_opts,
            pytest_mark,
            dir_name,
            skip_submodules,
            coverage,
            collect_only,
            skipped_tests,
        )
        exp = r'pytest -m "not slow and not superslow" --cov=. --cov-branch --cov-report term-missing --cov-report html --collect-only'
        self.assert_equal(act, exp)

    @pytest.mark.skipif(not git.is_lem(),
                        reason="Only run in lem")
    def test_run_fast_tests3(self) -> None:
        """
        Skip submodules.
        """
        pytest_opts = ""
        pytest_mark = ""
        dir_name = ""
        skip_submodules = True
        coverage = False
        collect_only = False
        #
        skipped_tests = ""
        act = ltasks._build_run_command_line(
            pytest_opts,
            pytest_mark,
            dir_name,
            skip_submodules,
            coverage,
            collect_only,
            skipped_tests,
        )
        exp = r"pytest --ignore amp"
        self.assert_equal(act, exp)

    @pytest.mark.skipif(not git.is_amp(),
                        reason="Only run in amp")
    def test_run_fast_tests4(self) -> None:
        """
        Select pytest_mark.
        """
        scratch_space = self.get_scratch_space(use_absolute_path=False)
        dir_name = os.path.join(scratch_space, "test")
        file_dict = {
            "test_this.py": hprint.dedent(
                """
                    foo

                    class TestHelloWorld(hut.TestCase):
                        bar
                    """
            ),
            "test_that.py": hprint.dedent(
                """
                    foo
                    baz

                    @pytest.mark.no_container
                    class TestHello_World(hut.):
                        bar
                    """
            ),
        }
        incremental = True
        hut.create_test_dir(dir_name, incremental, file_dict)

        pytest_opts = ""
        pytest_mark = "no_container"
        dir_name = scratch_space
        skip_submodules = True
        coverage = False
        collect_only = False
        #
        skipped_tests = ""
        act = ltasks._build_run_command_line(
            pytest_opts,
            pytest_mark,
            dir_name,
            skip_submodules,
            coverage,
            collect_only,
            skipped_tests,
        )
        exp = "pytest TestLibRunTests1.test_run_fast_tests4/tmp.scratch/test/test_that.py"
        self.assert_equal(act, exp)


class TestLibTasksRunTests1(hut.TestCase):
    def test_find_test_files1(self) -> None:
        """
        Find all the test files in the current dir.
        """
        files = ltasks._find_test_files()
        # For sure there are more than 1 test files: at least this one.
        self.assertGreater(len(files), 1)

    def test_find_test_files2(self) -> None:
        """
        Find all the test files from the top of the super module root.
        """
        git_root = git.get_client_root(super_module=True)
        files = ltasks._find_test_files(git_root)
        # For sure there are more than 1 test files: at least this one.
        self.assertGreater(len(files), 1)

    def test_find_test_class1(self) -> None:
        """
        Find the current test class.
        """
        git_root = git.get_client_root(super_module=True)
        file_names = ltasks._find_test_files(git_root)
        #
        file_names = ltasks._find_test_class("TestLibTasksRunTests1", file_names)
        act = hut.purify_file_names(file_names)
        exp = ["test/test_lib_tasks.py::TestLibTasksRunTests1"]
        self.assert_equal(str(act), str(exp))

    def test_find_test_class2(self) -> None:
        """
        Find the current test class.
        """
        file_names = [__file__]
        #
        file_names = ltasks._find_test_class("TestLibTasksRunTests1", file_names)
        act = hut.purify_file_names(file_names)
        exp = ["test/test_lib_tasks.py::TestLibTasksRunTests1"]
        self.assert_equal(str(act), str(exp))

    def test_find_test_class3(self) -> None:
        """
        Create synthetic code and look for a class.
        """
        scratch_space = self.get_scratch_space()
        dir_name = os.path.join(scratch_space, "test")
        file_dict = {
            "test_this.py": hprint.dedent(
                """
                    foo

                    class TestHelloWorld(hut.TestCase):
                        bar
                    """
            ),
            "test_that.py": hprint.dedent(
                """
                    foo
                    baz

                    class TestHello_World(hut.):
                        bar
                    """
            ),
        }
        incremental = True
        hut.create_test_dir(dir_name, incremental, file_dict)
        #
        file_names = ltasks._find_test_files(dir_name)
        act_file_names = [os.path.relpath(d, scratch_space) for d in file_names]
        exp_file_names = ["test/test_that.py", "test/test_this.py"]
        self.assert_equal(str(act_file_names), str(exp_file_names))
        #
        act = ltasks._find_test_class("TestHelloWorld", file_names)
        act = hut.purify_file_names(act)
        exp = [
            "test/TestLibTasksRunTests1.test_find_test_class3/tmp.scratch/"
            "test/test_this.py::TestHelloWorld"
        ]
        self.assert_equal(str(act), str(exp))

    def test_find_test_decorator1(self) -> None:
        """
        Find test functions in the "no_container" in synthetic code.
        """
        scratch_space = self.get_scratch_space()
        dir_name = os.path.join(scratch_space, "test")
        file_dict = {
            "test_this.py": hprint.dedent(
                """
                    foo

                    class TestHelloWorld(hut.TestCase):
                        bar
                    """
            ),
            "test_that.py": hprint.dedent(
                """
                    foo
                    baz

                    @pytest.mark.no_container
                    class TestHello_World(hut.):
                        bar
                    """
            ),
        }
        incremental = True
        hut.create_test_dir(dir_name, incremental, file_dict)
        #
        file_names = ltasks._find_test_files(dir_name)
        act = ltasks._find_test_decorator("no_container", file_names)
        act = hut.purify_file_names(act)
        exp = [
            "test/TestLibTasksRunTests1.test_find_test_decorator1/tmp.scratch/"
            "test/test_that.py"
        ]
        self.assert_equal(str(act), str(exp))

    @pytest.mark.skipif(not git.is_amp(),
                        reason="Only run in amp")
    def test_find_test_decorator2(self) -> None:
        """
        Find test functions in the "no_container" test list.
        """
        file_names = ["test/test_tasks.py"]
        act = ltasks._find_test_decorator("no_container", file_names)
        act = hut.purify_file_names(act)
        exp = ["test/test_tasks.py"]
        self.assert_equal(str(act), str(exp))
