import logging

import pytest

import dev_scripts.git.git_hooks.utils as ghutils
import helpers.system_interaction as hsinte
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


@pytest.mark.skipif(
    hsinte.is_inside_ci(), reason="CI is not set up for committing code in GH"
)
class Test_git_hooks_utils1(hut.TestCase):
    def test_check_master1(self) -> None:
        abort_on_error = False
        ghutils.check_master(abort_on_error)

    @pytest.mark.skipif(
        hsinte.is_inside_docker(),
        reason="There are no Git credentials inside Docker",
    )
    def test_check_author1(self) -> None:
        abort_on_error = False
        ghutils.check_author(abort_on_error)

    def test_check_file_size1(self) -> None:
        abort_on_error = False
        ghutils.check_file_size(abort_on_error)
