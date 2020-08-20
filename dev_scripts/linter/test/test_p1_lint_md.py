import dev_scripts.linter.p1_lint_md as plintmd
import helpers.unit_test as hut


class Test_check_readme_is_capitalized(hut.TestCase):
    def test1(self) -> None:
        """Incorrect README name: error."""
        file_name = "linter/readme.md"
        exp = f"{file_name}:1: All README files should be named README.md"
        msg = plintmd._check_readme_is_capitalized(file_name)
        self.assertEqual(exp, msg)

    def test2(self) -> None:
        """Correct README name: no error."""
        file_name = "linter/README.md"
        msg = plintmd._check_readme_is_capitalized(file_name)
        self.assertEqual("", msg)
