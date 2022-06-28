import logging
import re
from typing import Dict, Optional

import pytest

import helpers.hgit as hgit
import helpers.hprint as hprint
import helpers.hunit_test as hunitest
import helpers.lib_tasks_docker as hlitadoc
import helpers.test.test_lib_tasks as httestlib

_LOG = logging.getLogger(__name__)


# pylint: disable=protected-access


class Test_generate_compose_file1(hunitest.TestCase):
    def helper(
        self,
        stage: str = "prod",
        use_privileged_mode: bool = False,
        use_sibling_container: bool = False,
        shared_data_dirs: Optional[Dict[str, str]] = None,
        mount_as_submodule: bool = False,
        use_network_mode_host: bool = True,
        use_main_network: bool = False,
    ) -> None:
        txt = []
        #
        params = [
            "use_privileged_mode",
            "use_sibling_container",
            "shared_data_dirs",
            "mount_as_submodule",
            "use_network_mode_host",
        ]
        txt_tmp = hprint.to_str(" ".join(params))
        txt.append(txt_tmp)
        #
        file_name = None
        txt_tmp = hlitadoc._generate_docker_compose_file(
            stage,
            use_privileged_mode,
            use_sibling_container,
            shared_data_dirs,
            mount_as_submodule,
            use_network_mode_host,
            use_main_network,
            file_name,
        )
        txt_tmp = hunitest.filter_text("AM_HOST_NAME|AM_HOST_OS_NAME", txt_tmp)
        txt.append(txt_tmp)
        #
        txt = "\n".join(txt)
        txt = hunitest.filter_text(r"working_dir", txt)
        self.check_string(txt)

    def test1(self) -> None:
        self.helper(use_privileged_mode=True)

    def test2(self) -> None:
        self.helper(shared_data_dirs={"/data/shared": "/shared_data"})

    def test3(self) -> None:
        self.helper(use_main_network=True)

    def test4(self) -> None:
        self.helper(stage="dev")


# #############################################################################


class TestLibTasksGetDockerCmd1(httestlib._LibTasksTestCase):
    """
    Test `_get_docker_compose_cmd()`.
    """

    # TODO(gp): After using a single docker file as part of AmpTask2308
    #  "Update_amp_container" we can probably run these tests in any repo, so
    #  we should be able to remove this `skipif`.
    @pytest.mark.skipif(
        not hgit.is_in_amp_as_submodule(), reason="Only run in amp as submodule"
    )
    def test_docker_bash1(self) -> None:
        """
        Command for docker_bash target.
        """
        base_image = ""
        stage = "dev"
        version = "1.0.0"
        cmd = "bash"
        service_name = "app"
        entrypoint = False
        print_docker_config = False
        act = hlitadoc._get_docker_compose_cmd(
            base_image,
            stage,
            version,
            cmd,
            service_name=service_name,
            entrypoint=entrypoint,
            print_docker_config=print_docker_config,
        )
        exp = r"""
        IMAGE=$AM_ECR_BASE_PATH/amp_test:dev-1.0.0 \
            docker-compose \
            --file $GIT_ROOT/devops/compose/docker-compose.yml \
            --env-file devops/env/default.env \
            run \
            --rm \
            --name $USER_NAME.amp_test.app.app \
            --entrypoint bash \
            app
        """
        self._check(act, exp)

    @pytest.mark.skipif(
        not hgit.is_in_amp_as_submodule(), reason="Only run in amp as submodule"
    )
    def test_docker_bash2(self) -> None:
        """
        Command for docker_bash with entrypoint.
        """
        base_image = ""
        stage = "local"
        version = "1.0.0"
        cmd = "bash"
        print_docker_config = False
        act = hlitadoc._get_docker_compose_cmd(
            base_image,
            stage,
            version,
            cmd,
            print_docker_config=print_docker_config,
        )
        exp = r"""IMAGE=$AM_ECR_BASE_PATH/amp_test:local-$USER_NAME-1.0.0 \
                docker-compose \
                --file $GIT_ROOT/devops/compose/docker-compose.yml \
                --env-file devops/env/default.env \
                run \
                --rm \
                --name $USER_NAME.amp_test.app.app \
                app \
                bash """
        self._check(act, exp)

    @pytest.mark.skipif(
        not hgit.is_in_amp_as_submodule(), reason="Only run in amp as submodule"
    )
    def test_docker_bash3(self) -> None:
        """
        Command for docker_bash with some env vars.
        """
        base_image = ""
        stage = "local"
        version = "1.0.0"
        cmd = "bash"
        extra_env_vars = ["PORT=9999", "SKIP_RUN=1"]
        print_docker_config = False
        act = hlitadoc._get_docker_compose_cmd(
            base_image,
            stage,
            version,
            cmd,
            extra_env_vars=extra_env_vars,
            print_docker_config=print_docker_config,
        )
        exp = r"""
        IMAGE=$AM_ECR_BASE_PATH/amp_test:local-$USER_NAME-1.0.0 \
        PORT=9999 \
        SKIP_RUN=1 \
            docker-compose \
            --file $GIT_ROOT/devops/compose/docker-compose.yml \
            --env-file devops/env/default.env \
            run \
            --rm \
            --name $USER_NAME.amp_test.app.app \
            app \
            bash
        """
        self._check(act, exp)

    @pytest.mark.skipif(
        not hgit.is_in_amp_as_supermodule(),
        reason="Only run in amp as supermodule",
    )
    def test_docker_bash4(self) -> None:
        base_image = ""
        stage = "dev"
        version = "1.0.0"
        cmd = "bash"
        entrypoint = False
        print_docker_config = False
        act = hlitadoc._get_docker_compose_cmd(
            base_image,
            stage,
            version,
            cmd,
            entrypoint=entrypoint,
            print_docker_config=print_docker_config,
        )
        exp = r"""
        IMAGE=$AM_ECR_BASE_PATH/amp_test:dev-1.0.0 \
            docker-compose \
            --file $GIT_ROOT/devops/compose/docker-compose.yml \
            --env-file devops/env/default.env \
            run \
            --rm \
            --name $USER_NAME.amp_test.app.app \
            --entrypoint bash \
            app
        """
        self._check(act, exp)

    def test_docker_bash5(self) -> None:
        """
        Command for running through a shell.
        """
        base_image = ""
        stage = "dev"
        version = "1.0.0"
        cmd = "ls && cd .."
        entrypoint = True
        print_docker_config = False
        use_bash = True
        act = hlitadoc._get_docker_compose_cmd(
            base_image,
            stage,
            version,
            cmd,
            entrypoint=entrypoint,
            print_docker_config=print_docker_config,
            use_bash=use_bash,
        )
        exp = r"""
        IMAGE=$AM_ECR_BASE_PATH/amp_test:dev-1.0.0 \
            docker-compose \
            --file $GIT_ROOT/devops/compose/docker-compose.yml \
            --env-file devops/env/default.env \
            run \
            --rm \
            --name $USER_NAME.amp_test.app.app \
            app \
            bash -c 'ls && cd ..'
        """
        self._check(act, exp)

    @pytest.mark.skipif(
        not hgit.is_in_amp_as_submodule(), reason="Only run in amp as submodule"
    )
    def test_docker_jupyter1(self) -> None:
        base_image = ""
        stage = "dev"
        version = "1.0.0"
        port = 9999
        self_test = True
        print_docker_config = False
        act = hlitadoc._get_docker_jupyter_cmd(
            base_image,
            stage,
            version,
            port,
            self_test,
            print_docker_config=print_docker_config,
        )
        exp = r"""
        IMAGE=$AM_ECR_BASE_PATH/amp_test:dev-1.0.0 \
        PORT=9999 \
            docker-compose \
            --file $GIT_ROOT/devops/compose/docker-compose.yml \
            --env-file devops/env/default.env \
            run \
            --rm \
            --name $USER_NAME.amp_test.jupyter_server_test.app \
            --service-ports \
            jupyter_server_test
        """
        self._check(act, exp)

    def _check(self, act: str, exp: str) -> None:
        act = hunitest.purify_txt_from_client(act)
        # This is required when different repos run Docker with user vs root / remap.
        act = hunitest.filter_text("--user", act)
        # Remove current timestamp (e.g., `20220317_232120``) from the `--name`
        # so that the tests pass.
        timestamp_regex = r"\.\d{8}_\d{6}"
        act = re.sub(timestamp_regex, "", act)
        self.assert_equal(act, exp, fuzzy_match=True)
