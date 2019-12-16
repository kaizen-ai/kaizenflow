"""
Import as:

import helpers.parser as prsr
"""


def add_bool_arg(parser, name, default=False, help_=None):
    """
    Add options to a parser like --xyz and --no-xyz (e.g., for --incremental).
    """
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--" + name, dest=name, action="store_true", help=help_)
    group.add_argument("--no-" + name, dest=name, action="store_false")
    parser.set_defaults(**{name: default})
    return parser


def add_verbosity_arg(parser):
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    return parser
