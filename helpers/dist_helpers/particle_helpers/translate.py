#!/usr/bin/env python

"""
This script allows translating text using AWS Translate.
It can be used as a module or CLI tool.

Supported languages and languages codes:
https://docs.aws.amazon.com/translate/latest/dg/what-is.html
"""
import re
import sys
import pathlib
import logging
import argparse
import configparser
from typing import Tuple, Optional

import boto3
import lxml.html as html


_LOG = logging.getLogger(__name__)


def _load_credentials(conf_path: pathlib.Path) -> Tuple[str, str]:
    """
    Load aws credentilas from config file.

    :param conf_path:credentials file path.
    :return: A tuple consist of aws_access and aws_secret keys.
    """
    config = configparser.ConfigParser()
    config.read(conf_path)
    try:
        access = config.get("default", "aws_access_key_id")
        secret = config.get("default", "aws_secret_access_key")
    except configparser.NoOptionError as err:
        _LOG.error(f"Unable to read option for: {err.args}")
        sys.exit(1)
    else:
        return access, secret


class TranslateAPI:
    def __init__(
        self,
        aws_access_key: str,
        aws_secret_key: str,
        region: Optional[str] = "us-east-2",
    ) -> None:
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.region = region
        self.translate = boto3.client(
            service_name="translate",
            region_name=self.region, use_ssl=True
        )

    def translate_text(self, text: str, lang_code: str) -> str:
        """
        Translate given text into English.
        Amazon has a limit on text size: 5,000 bytes.

        :param text: Foreing language text.
        :param lang_code: Language code in accordance with supported
        languages and code of Amazon.
        :return: English text.
        """
        tr = self.translate.translate_text(
            Text=text,
            SourceLanguageCode=lang_code,
            TargetLanguageCode="en"
        )
        return tr.get('TranslatedText')

    # FIXME: (Kostya): temporary unused.
    def __translate_file_html(
        self, lang_code: str, source_path: str, result_path: str
    ) -> bool:
        if not pathlib.Path(source_path).exists():
            return False
        page = html.parse(source_path)
        for elm in page.getiterator():
            if elm.text and re.search(r'[^0-9.,|\-\s]', elm.text):
                tr = self.translate_text(elm.text, lang_code)
                elm.text = tr
        page.write(result_path)
        return True


