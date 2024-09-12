"""
Simple plugin to filter out the table of contents from the markdown content.

- The MkDocs generates its own table of contents based on the markdown
headers we want to avoid the redundancy when rendering the markdown content
on the website.

Import as:

import mkdocs.mkdocs-toc-tag-filter.plugin as mmkdplug
"""

import re

from mkdocs.config import config_options
from mkdocs.plugins import BasePlugin

_PATTERN_TO_FILTER = r"<!-- toc -->[\s\S]*?<!-- tocstop -->"


class TocFilterPlugin(BasePlugin):
    config_scheme = (("param", config_options.Type(str, default="")),)

    def __init__(self):
        self.enabled = True
        self.total_time = 0

    def on_page_markdown(self, markdown, page, config, files):
        filtered_markdown = re.sub(
            _PATTERN_TO_FILTER, "", markdown, flags=re.DOTALL
        )
        return filtered_markdown
