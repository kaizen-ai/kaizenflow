import dev_scripts.linter2.p1_warn_incorrectly_formatted_todo as pwift
import helpers.unit_test as hut


class Test_warn_incorrectly_formatted_todo(hut.TestCase):
    def test1(self) -> None:
        """Test warning for missing assignee.

        - Given line has incorrectly formatted todo in comment
        - When function runs
        - Then a warning is returned
        """
        line = "# todo: invalid"

        msg = pwift._warn_incorrectly_formatted_todo("test.py", 1, line)
        self.assertIn("test.py:1: found incorrectly formatted TODO comment", msg)

    def test2(self) -> None:
        """Test no warning for no todo comment.

        - Given line has no todo comment
        - When function runs
        - Then no warning is returned
        """
        line = "test.method()"

        msg = pwift._warn_incorrectly_formatted_todo("test.py", 1, line)
        self.assertEqual("", msg)

    def test3(self) -> None:
        """Test no warning for code line with `todo` in variable name.

        - Given line has todo in variable name
        - When function runs
        - Then no warning is returned
        """
        line = "todo_var = 3"

        msg = pwift._warn_incorrectly_formatted_todo("test.py", 1, line)
        self.assertEqual("", msg)

    def test4(self) -> None:
        """Test warning for no assignee.

        - Given todo comment has no assignee
        - When function runs
        - Then a warning is returned
        """
        line = "# TODO: hi"

        msg = pwift._warn_incorrectly_formatted_todo("test.py", 1, line)
        self.assertIn("test.py:1: found incorrectly formatted TODO comment", msg)

    def test5(self) -> None:
        """Test no warning for valid todo.

        - Given todo comment is valid
        - When function runs
        - Then no warning is returned
        """
        line = "# TODO(test): hi"

        msg = pwift._warn_incorrectly_formatted_todo("test.py", 1, line)
        self.assertEqual("", msg)

    def test6(self) -> None:
        """Test no warning for missing trailing `.`. The check for trailing
        punctation is covered by `_fix_comment_style`.

        - Given todo comment is missing a trailing .
        - When function runs
        - Then no warning is returned
        """
        line = "# TODO(test): hi"

        msg = pwift._warn_incorrectly_formatted_todo("test.py", 1, line)
        self.assertEqual("", msg)

    def test7(self) -> None:
        """Test no warning for a comment that has 'todo' as a substring.

        - Given a comment that has a 'todo' as a substring but isn't a todo comment
        - When function runs
        - Then no warning is returned
        """
        line = "# The line is not a todo comment"

        msg = pwift._warn_incorrectly_formatted_todo("test.py", 1, line)
        self.assertEqual("", msg)

    def test8(self) -> None:
        """Test no warning for a comment that has todo in a string.

        - Given invalid todo comment that has a warning rule
        - When function runs
        - Then no warning is returned
        """
        line = 'line = "# TODO(test): hi"'

        msg = pwift._warn_incorrectly_formatted_todo("test.py", 1, line)
        self.assertEqual("", msg)
