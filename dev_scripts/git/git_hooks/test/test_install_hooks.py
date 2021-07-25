import logging

import pytest

import dev_scripts.git.git_hooks.utils as ghutils
import helpers.printing as hprint
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

    def test_caesar1(self) -> None:
        txt = """
        1 2 3 4 5 6 7 8 9 0
        This pangram contains four As, one B, two Cs, one D, thirty Es, six Fs, five
        Gs, seven Hs, eleven Is, one J, one K, two Ls, two Ms, eighteen Ns, fifteen
        Os, two Ps, one Q, five Rs, twenty-seven Ss, eighteen Ts, two Us, seven Vs,
        eight Ws, two Xs, three Ys, & one Z.
        """
        txt = hprint.dedent(txt)
        step = 3
        transformed_txt = ghutils.caesar(txt, step)
        txt2 = ghutils.caesar(transformed_txt , -step)
        self.assert_equal(txt, txt2)

    def _helper(self, txt: str, decaesarify: bool, exp: bool) -> None:
        _LOG.debug(hprint.to_str("txt decaesarify exp"))
        regex = ghutils._get_regex(decaesarify)
        m = regex.search(txt)
        _LOG.debug("  -> m=%s", bool(m))
        if m:
            val = m.group(1)
            _LOG.debug("  -> val=%s", val)
        self.assertEqual(bool(m), exp)

    def test_regex1(self) -> None:
        self._helper("Olssv LN", False, True)
        self._helper("LN go", False, True)
        self._helper("Olssv_TLN", False, False)
        self._helper("Olssv_LN_Hello", False, False)
        #
        self._helper("Ego", True, False)
        self._helper("Eg_o", True, False)
        self._helper("_EG", True, False)
        self._helper("_eggo", True, False)
        self._helper("_eg go", True, False)
        self._helper("NOTIFY_JUPYTER_TOKEN", True, False)
        #
        self._helper("LNPK", False, True)
        self._helper("This is LNPK or is not", False, True)
        self._helper("This is _LNPK or is not", False, False)
        self._helper("LNPKhello", False, False)
        words = "ln Ln LN lnpk LNPK sptl sltvuhkl slt jyfwav"
        for word in words.split():
            self._helper(word, False, True)
            self._helper("# " + word, False, True)
            self._helper(" " + word, False, True)
            self._helper(word + " ", False, True)
            self._helper(word + "a", False, False)
            self._helper(word + "_", False, False)
            self._helper("_" + word, False, False)

    def test_check_words_in_text1(self) -> None:
        txt = """
        Olssv LN
        Olssv_TLN
        Olssv_LN_Hello
        Olssv LN
        """
        txt = hprint.dedent(txt)
        file_name = "foobar.txt"
        lines = txt.split("\n")
        act = "\n".join(ghutils._check_words_in_text(file_name, lines, decaesarify=False))
        # Check.
        exp = r"""
        foobar.txt:1: Found 'SU'
        foobar.txt:4: Found 'SU'"""
        exp = hprint.dedent(exp)
        self.assert_equal(act, exp)
