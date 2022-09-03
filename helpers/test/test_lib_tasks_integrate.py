import helpers.hunit_test as hunitest


class Test_infer_dst_dir1(hunitest.TestCase):
    def test1(self) -> None:
        # Define input variables.
        src_dir = "/src/cmamp1/im_v2/alpha_numeric_data_snapshots/"
        # Call function to test.
        act = _infer_dst_dir(src_dir)
        # Define expected output.
        exp = ("/src/amp1/im_v2/alpha_numeric_data_snapshots",
               "im_v2/alpha_numeric_data_snapshots")
        # Compare actual and expected output.
        self.assertEqual(act, exp)
