import argparse
import inspect
import sys

import helpers.dbg as dbg
import helpers.io_ as io_
import helpers.parser as prsr


class KibotCommand:
    # If true, adds optional `tmp_dir` and `incremental` arguments.
    SUPPORTS_TMP_DIR: bool = False

    def __init__(self) -> None:
        self._setup_parser()

    def run(self) -> None:
        sys.exit(self._main())

    @staticmethod
    def customize_parser(parser: argparse.ArgumentParser) -> None:
        """Allow child classes to customize the parser further."""

    def customize_run(self) -> int:  # pylint: disable=no-self-use
        """Allow child classes to customize the run further."""
        return 0

    def _setup_parser(self) -> None:
        self.parser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )

        prsr.add_verbosity_arg(self.parser)

        if self.SUPPORTS_TMP_DIR:
            self.parser.add_argument(
                "--tmp_dir",
                type=str,
                nargs="?",
                help="Directory to store temporary data",
                default="tmp.kibot_downloader",
            )
            self.parser.add_argument(
                "--no_incremental",
                action="store_true",
                help="Clean the local directories",
            )

        self.customize_parser(parser=self.parser)

    def _main(self) -> int:
        self.args = self.parser.parse_args()
        dbg.init_logger(
            verbosity=self.args.log_level,
            log_filename=inspect.getfile(self.__class__) + ".log",
        )

        if self.SUPPORTS_TMP_DIR:
            io_.create_dir(
                self.args.tmp_dir, incremental=not self.args.no_incremental
            )

        return self.customize_run()
