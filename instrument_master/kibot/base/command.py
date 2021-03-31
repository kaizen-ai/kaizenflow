# TODO(*): Move it one level up and call it kibot_command.py
import argparse
import inspect
import sys

import requests

import helpers.dbg as dbg
import helpers.io_ as hio
import helpers.parser as hparse
import instrument_master.kibot.metadata.config as vkmcon


class KibotCommand:
    def __init__(
        self,
        docstring: str,
        supports_tmp_dir: bool = False,
        requires_auth: bool = False,
        requires_api_login: bool = False,
    ) -> None:
        """
        Create a kibot command line script.

        :param docstring: the command's docstring.
        :param supports_tmp_dir: If true, adds optional `tmp_dir` and `incremental` arguments.
        :param requires_auth: If true, adds username and password as required arguments.
        :param requires_api_login: If true, logs into API before calling customize_run()
        """
        self.docstring = docstring
        self.supports_tmp_dir = supports_tmp_dir
        self.requires_auth = requires_auth
        self.requires_api_login = requires_api_login

        self._file_path = inspect.getfile(self.__class__)
        self._setup_parser()

    def run(self) -> None:
        sys.exit(self._main())

    @staticmethod
    def customize_parser(parser: argparse.ArgumentParser) -> None:
        """
        Allow child classes to customize the parser further.
        """

    def customize_run(self) -> int:  # pylint: disable=no-self-use
        """
        Allow child classes to customize the run further.
        """
        return 0

    def _setup_parser(self) -> None:
        self.parser = argparse.ArgumentParser(
            description=self.docstring,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )

        hparse.add_verbosity_arg(self.parser)

        if self.supports_tmp_dir:
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

        if self.requires_auth:
            self.parser.add_argument(
                "-u",
                "--username",
                required=True,
                help="Specify username",
            )
            self.parser.add_argument(
                "-p",
                "--password",
                required=True,
                help="Specify password",
            )

        self.customize_parser(parser=self.parser)

    def _main(self) -> int:
        self.args = self.parser.parse_args()
        dbg.init_logger(
            verbosity=self.args.log_level,
            log_filename=self._file_path + ".log",
        )

        if self.supports_tmp_dir:
            hio.create_dir(
                self.args.tmp_dir, incremental=not self.args.no_incremental
            )

        if self.requires_api_login:
            dbg.dassert_eq(True, self.requires_auth)
            self._login_to_api()

        return self.customize_run()

    def _login_to_api(self) -> None:
        """
        Login to Kibot API.
        """

        response = requests.get(
            url=vkmcon.API_ENDPOINT,
            params=dict(
                action="login",
                user=self.args.username,
                password=self.args.password,
            ),
        )
        status_code = int(response.text.split()[0])
        accepted_status_codes = [
            200,  # login successfuly
            407,  # user already logged in
        ]
        dbg.dassert_in(
            status_code,
            accepted_status_codes,
            msg=f"Failed to login: {response.text}",
        )
