from typing import List

import dev_scripts.linter2.p1_clean_import as pci
import helpers.unit_test as hut


class Test_get_aliased_imports(hut.TestCase):
    def test1(self) -> None:
        """No aliased imports returns an empty list."""
        content = "import test"
        expected: List[str] = []

        act = pci._get_aliased_imports(content=content)
        self.assertEqual(act, expected)

    def test2(self) -> None:
        """Test single aliased import returns an Aliased import."""
        content = "import test as te"
        expected = [pci.AliasedImport(import_path="test", alias="te")]

        act = pci._get_aliased_imports(content=content)
        self.assertEqual(act, expected)

    def test3(self) -> None:
        """Test single aliased import with a dot in path returns an Aliased
        import."""
        content = "import test.sub as tes"
        expected = [pci.AliasedImport(import_path="test.sub", alias="tes")]

        act = pci._get_aliased_imports(content=content)
        self.assertEqual(act, expected)

    def test4(self) -> None:
        """Test multiple aliased imports are parsed correctly."""
        content = """
                import test as te
                import test.sub as tes
                """
        expected = [
            pci.AliasedImport(import_path="test", alias="te"),
            pci.AliasedImport(import_path="test.sub", alias="tes"),
        ]

        act = pci._get_aliased_imports(content=content)
        self.assertEqual(act, expected)


class Test_replace_alias(hut.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.old_alias = "old_alias"
        self.new_alias = "new_alias"

    def test1(self) -> None:
        """Test no matches mean no changes."""
        content = "import test as te"
        expected = "import test as te"

        actual = pci._replace_alias(
            content=content, old_alias=self.old_alias, new_alias=self.new_alias
        )
        self.assertEqual(expected, actual)

    def test2(self) -> None:
        """Test old alias is replaced in imports."""
        content = "import test as old_alias"
        expected = "import test as new_alias"

        actual = pci._replace_alias(
            content=content, old_alias=self.old_alias, new_alias=self.new_alias
        )
        self.assertEqual(expected, actual)

    def test3(self) -> None:
        """Test old alias is replaced in function calls."""
        content = "old_alias.hello()"
        expected = "new_alias.hello()"

        actual = pci._replace_alias(
            content=content, old_alias=self.old_alias, new_alias=self.new_alias
        )
        self.assertEqual(expected, actual)

    def test4(self) -> None:
        """Test indentation is preserved."""
        content = "    old_alias.hello()"
        expected = "    new_alias.hello()"

        actual = pci._replace_alias(
            content=content, old_alias=self.old_alias, new_alias=self.new_alias
        )
        self.assertEqual(expected, actual)

    def test5(self) -> None:
        """Test attributes are not updated."""
        content = "io.old_alias.hello()"
        expected = "io.old_alias.hello()"

        actual = pci._replace_alias(
            content=content, old_alias=self.old_alias, new_alias=self.new_alias
        )
        self.assertEqual(expected, actual)

    def test6(self) -> None:
        """Test partial matches are not replaced."""
        content = "not_old_alias.hello()"
        expected = "not_old_alias.hello()"

        actual = pci._replace_alias(
            content=content, old_alias=self.old_alias, new_alias=self.new_alias
        )
        self.assertEqual(expected, actual)

    def test7(self) -> None:
        """Test when module name matches existing alias, it's not replaced
        example: import helpers.dbg as dbg
        """
        content = "import io.old_alias as old_alias"
        expected = "import io.old_alias as new_alias"

        actual = pci._replace_alias(
            content=content, old_alias=self.old_alias, new_alias=self.new_alias
        )
        self.assertEqual(expected, actual)


class Test_create_import_shortname(hut.TestCase):
    def test1(self) -> None:
        """Test shortname is first letter of each element in the path then full
        last element."""
        import_path = "amp.core.plotting"
        expected = "acplotting"

        actual = pci._create_import_shortname(import_path)
        self.assertEqual(expected, actual)

    def test2(self) -> None:
        """Test that underscores are removed."""
        import_path = "vendors.ravenpack.db_utils"
        expected = "vrdbutils"

        actual = pci._create_import_shortname(import_path)
        self.assertEqual(expected, actual)

    def test3(self) -> None:
        """Test custom mapping is respected."""
        import_path = "pandas"
        expected = "pd"

        actual = pci._create_import_shortname(import_path)
        self.assertEqual(expected, actual)


class Test_check_alias_taken(hut.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.alias = "alias"

    def test1(self) -> None:
        """Test returns true if alias used in an import statement."""
        content = "import io as alias"
        actual = pci._check_alias_taken(content=content, alias=self.alias)

        self.assertTrue(actual)

    def test2(self) -> None:
        """Test returns true if alias used in an assignment statement."""
        assignment_examples = [
            "alias = 3",
            "alias=other_var",
            "alias = 'hi'",
        ]

        for ex in assignment_examples:
            content = ex
            actual = pci._check_alias_taken(content=content, alias=self.alias)

            self.assertTrue(actual)

    def test3(self) -> None:
        """Test returns false if partial alias is used."""
        content = "other_alias=3"
        actual = pci._check_alias_taken(content=content, alias=self.alias)

        self.assertFalse(actual)


class Test_check_conflict(hut.TestCase):
    def test1(self) -> None:
        """Test returns false if new_alias matches old alias, regardless of
        content."""
        content = ""
        new_alias = "alias"
        old_alias = "alias"

        conflict = pci._check_conflict(
            content=content, old_alias=old_alias, new_alias=new_alias
        )

        self.assertFalse(conflict)

    def test2(self) -> None:
        """Test if new alias doesn't match old alias, content is checked."""
        content = "import io as new_alias"
        new_alias = "new_alias"
        old_alias = "alias"

        conflict = pci._check_conflict(
            content=content, old_alias=old_alias, new_alias=new_alias
        )

        self.assertTrue(conflict)

    def test3(self) -> None:
        """Test if new alias doesn't match old alias, content is checked."""
        content = "import io as other_alias"
        new_alias = "new_alias"
        old_alias = "alias"

        conflict = pci._check_conflict(
            content=content, old_alias=old_alias, new_alias=new_alias
        )

        self.assertFalse(conflict)
