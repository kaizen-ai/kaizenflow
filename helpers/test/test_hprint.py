import helpers.hprint as hprint
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


class _Object:

    def __init__(self, a: bool, b: str, c: float) -> None:
        


class Test_obj_to_str1(hunitest.TestCase):

    def test_get_df1(self) -> None:
        """
        Check the output of `_get_df()`.
        """
        # Prepare data.
        df = _get_df_example1()
        # Check.
        act = hpandas.df_to_str(df, print_shape_info=True, tag="df")
        exp = r"""# df=
        index=[2020-01-01 09:30:00-05:00, 2020-01-01 16:00:00-05:00]
        columns=idx,instr,val1,val2
        shape=(395, 4)
                                   idx instr  val1  val2
        2020-01-01 09:30:00-05:00    0     A    81    35
        2020-01-01 09:35:00-05:00    0     A    14    58
        2020-01-01 09:40:00-05:00    0     A     3    81
        ...
        2020-01-01 15:50:00-05:00    4     E    57     3
        2020-01-01 15:55:00-05:00    4     E    33    50
        2020-01-01 16:00:00-05:00    4     E    96    75"""
        self.assert_equal(act, exp, fuzzy_match=True)
