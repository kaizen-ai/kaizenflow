import argparse
import logging
import os

import helpers.hparquet as hparque
import helpers.unit_test as hunitest
from im_v2.common.data.transform.convert_pq_by_date_to_by_asset import _main
from im_v2.common.data.transform.generate_pq_example_data import generate

_LOG = logging.getLogger(__name__)


# TODO(Nikola): test _parse()
class TestPQByDateToByAsset(hunitest.TestCase):
    def test_convert_pq_by_date_to_by_asset(self) -> None:
        test_dir = self.get_scratch_space()
        by_date_dir = os.path.join(test_dir, "by_date")
        by_asset_dir = os.path.join(test_dir, "by_asset")

        # TODO(Nikola): properties are only visible with str()
        #   Example: args.src_dir is returning mock object __repr__, instead str.
        # dummy_argparse = mock.create_autospec(argparse.ArgumentParser)
        # dummy_args = mock.MagicMock()
        # dummy_args.configure_mock(
        #     src_dir=by_date_dir, dst_dir=by_asset_dir, log_level="DEBUG")
        # dummy_argparse.parse_args = lambda: dummy_args

        # temporary mock
        class DummyArgs:
            src_dir = by_date_dir
            dst_dir = by_asset_dir
            log_level = "DEBUG"

        class DummyArgumentParser(argparse.ArgumentParser):
            def parse_args(self) -> DummyArgs:
                return DummyArgs()

        generate("2021-12-30", "2022-01-02", dst_dir=by_date_dir)
        # _main(dummy_argparse())
        _main(DummyArgumentParser())

        # check directory structure
        by_date_signature = hunitest.get_dir_signature(by_date_dir, False)
        by_asset_signature = hunitest.get_dir_signature(by_asset_dir, False)
        self.check_string(by_date_signature, tag="by_date_signature")
        self.check_string(by_asset_signature, tag="by_asset_signature")

        # check parquet files content
        signatures = (
            by_date_signature.split("\n")[1:] + by_asset_signature.split("\n")[1:]
        )
        for signature in signatures:
            if signature.endswith(".parquet") or signature.endswith(".pq"):
                df = hparque.from_parquet(signature)
                df_signature = hunitest.get_df_signature(df, 12)
                self.check_string(
                    df_signature, tag=signature.split("tmp.scratch/")[-1]
                )
